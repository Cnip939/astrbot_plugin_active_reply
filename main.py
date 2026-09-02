from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api.provider import ProviderRequest
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.provider import LLMResponse
from astrbot.core.conversation_mgr import Conversation
from astrbot.api.message_components import Plain, Image,At,Reply
from astrbot.api import AstrBotConfig
from datetime import datetime
from typing import Dict, List
from astrbot.api import logger
import asyncio
import base64
import aiohttp
import re

class MyPlugin(Star):
    def __init__(self, context: Context,config: AstrBotConfig):
        super().__init__(context)

        self.config = config
        self.history: Dict[str, List] = {}          #历史消息缓存
        self.last_time = {}                         
        self.last_group_name = {}
        self.lock = asyncio.Lock()
        
        self.pending: Dict[str, List[str]] = {}      # 5秒内后文
        self.is_waiting: Dict[str, bool] = {}        # 是否正在收集中
        self.current_msg: Dict[str, str] = {}        # 本轮触发消息（当前消息）

        self.SLEEP_TIME = config.get("sleep_time")
        self.COMMAND = config.get("command")
        self.MAX_HISTORY = config.get("max_history")
        self.BOT_NAME = config.get("bot_name")
        self.PROVIDER_ID = config.get("provider_id")
        self.PROMPT = config.get("prompt")
        self.PICTURE = config.get("picture_quantity")

    def _is_gif(self, img_comp: Image) -> bool:
        """检查这个图片组件是不是 GIF"""
        url = getattr(img_comp, "url", None) or ""
        file_attr = getattr(img_comp, "file", None) or ""
        
        # 检查后缀
        if url.lower().endswith(".gif") or file_attr.lower().endswith(".gif"):
            return True
        
        # 检查 data URI（如 data:image/gif;base64,...）
        if file_attr.startswith("data:image/gif") or url.startswith("data:image/gif"):
            return True
        
        return False

    async def message_and_images(self, event: AstrMessageEvent):
        texts = []
        images_b64 = []
        self_id = str(event.get_self_id()) 
        msg_chain = event.message_obj.message
        
        if not msg_chain:
            return "", images_b64
            
        for comp in msg_chain:
            if isinstance(comp, Plain):
                if comp.text:
                    texts.append(comp.text)
                    
            elif isinstance(comp, Image):
                # 直接跳过 GIF，不下载、不转 base64、不传给 LLM
                if self._is_gif(comp):
                    texts.append("[GIF图片]")
                    continue
                b64 = await self.download_image_to_b64(comp)
                if b64:
                    images_b64.append(b64)
                    texts.append("[图片]")
                else:
                    texts.append("[图片(获取失败)]")

            elif isinstance(comp, At):
                bot_name = self.BOT_NAME
                if str(comp.qq) == self_id:
                    texts.append(f"@{bot_name}[{event.get_self_id()}]")
                else:
                    # comp.name 是昵称，comp.qq 是 ID，优先用昵称
                    name = comp.name or str(comp.qq)
                    texts.append(f"[@{name}]")
            elif isinstance(comp, Reply):
                # 被引用消息的发送者
                reply_sender = comp.sender_nickname or str(comp.sender_id) or str(comp.qq)
                # 尝试提取被引用消息的文本
                reply_text = comp.message_str or comp.text or ""
                # 如果 message_str 是空的，去 chain 里翻一翻
                if not reply_text and comp.chain:
                    for sub in comp.chain:
                        if isinstance(sub, Plain) and sub.text:
                            reply_text = sub.text
                            break
                # 格式化写进聊天记录
                if reply_text:
                    texts.append(f"引用消息[{reply_sender}: {reply_text}]")
                else:
                    texts.append(f"引用消息[{reply_sender}: (消息内容)]")
        
        return "".join(texts), images_b64

    async def download_image_to_b64(self, img_comp: Image) -> str:
        # 途径1：file 属性已经是 data URI
        file_attr = getattr(img_comp, "file", None)
        if file_attr and isinstance(file_attr, str):
            if file_attr.startswith("data:image"):
                if "," in file_attr:
                    return file_attr.split(",", 1)[1]
                return file_attr
            # 有些平台 file 直接就是 base64 字符串（很长且不是url）
            if len(file_attr) > 1000 and not file_attr.startswith("http"):
                return file_attr
        
        # 途径2：通过 url 下载（最常用，QQ/微信等平台）
        url = getattr(img_comp, "url", None) or file_attr
        if url and url.startswith(("http://", "https://")):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url, 
                        timeout=aiohttp.ClientTimeout(total=15),
                        headers={"User-Agent": "Mozilla/5.0"}
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if data[:3] == b'GIF':
                                logger.info(f"下载后发现是 GIF，丢弃: {url[:60]}...")
                                return None
                            b64 = base64.b64encode(data).decode("utf-8")
                            logger.info(f"图片下载成功: {url[:60]}... -> base64 len={len(b64)}")
                            return b64
                        else:
                            logger.warning(f"图片下载返回非200: {url[:60]}... status={resp.status}")
            except Exception as e:
                logger.warning(f"图片下载失败 {url[:60]}...: {e}")
        # 途径3：如果以上都失败，尝试通过 AstrBot 内部方式获取（扩展点）
        # 参考插件传了 context，这里预留
        logger.warning("无法获取图片 base64，所有途径均失败")
        return None
        
    #格式化时间戳
    def simple_time(self,ts) -> str:
        dt = datetime.fromtimestamp(ts)
        return f"{dt.year}/{dt.month:02d}/{dt.day:02d} {dt.hour:02d}:{dt.minute:02d}"
    
    #收集当前消息，下文消息并合并到维护的历史中
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def process_message(self,event):
        if event.get_sender_id() == event.get_self_id():   # 过滤自己
            return
        
        group_uid = event.session_id
        text_part, img_b64_list = await self.message_and_images(event)
        substantial_text = re.sub(r'\[@.*?\]', '', text_part).strip()
        
        force_reply = False
        msg_chain = event.message_obj.message
        if msg_chain:
            for comp in msg_chain:
                # 情况1：消息里 @了机器人自己
                if isinstance(comp, At):
                    if str(comp.qq) == str(event.get_self_id()):
                        force_reply = True
                        logger.info("检测到被@机器人，跳过判定直接回复")
                        break
                
                # 情况2：消息引用了机器人自己之前发的消息
                elif isinstance(comp, Reply):
                    # sender_id 是被引用消息的发送者
                    if str(comp.sender_id) == str(event.get_self_id()):
                        force_reply = True
                        logger.info("检测到引用机器人消息，跳过判定直接回复")
                        break
        
        # 基于提取后的文本判断指令
        clean_text = text_part.replace(f"[@{self.BOT_NAME}/{event.get_self_id()}]", "").strip()
        if clean_text.startswith(self.COMMAND):
            return
        
        if event.message_str.startswith(self.COMMAND):
            return
        
        if not substantial_text and not img_b64_list:
            return

        current_message = f"{event.get_sender_name()}[{event.get_sender_id()}]:{text_part}"
        # 把 base64 追加在消息后面，用换行分隔
        for b64 in img_b64_list:
            current_message += f"\n[IMG_B64:{b64}]"
        current_time = self.simple_time(event.created_at)    #解析时间戳            
        log_msg = re.sub(r"\[IMG_B64:[A-Za-z0-9+/=]+\]", "[图片]", current_message)
        logger.info(f"触发消息: {log_msg}")

        async with self.lock:        #初始化
            if group_uid not in self.history:
                self.history[group_uid] = []
                self.last_time[group_uid] = ""  
                self.last_group_name[group_uid] = ""
                self.pending[group_uid] = []
                self.is_waiting[group_uid] = False
                
            
            #后文消息处理
            if self.is_waiting[group_uid]:  #如果是后文就加入当前的消息
                self.pending[group_uid].append(current_message)
                return
            
            self.is_waiting[group_uid] = True   #把触发的当前消息放入current_msg，然后设置is waiting为true
            self.current_msg[group_uid] = current_message            

            if self.last_time[group_uid] != current_time:
                self.history[group_uid].append(f"[{current_time}]")
                self.last_time[group_uid] = current_time

        await asyncio.sleep(self.SLEEP_TIME)
        
        async with self.lock:
            # 本轮所有用户消息
            round_msgs = [self.current_msg[group_uid]]
            round_msgs.extend(self.pending[group_uid])

            self.is_waiting[group_uid] = False
            self.pending[group_uid] = []
            self.current_msg[group_uid] = ""

            history_text = "\n".join(self.history[group_uid])
            current_text = "\n".join(round_msgs)
            event.set_extra("_my_active_reply", True)
            event.set_extra("_ar_history", history_text)
            event.set_extra("_ar_current", current_text)
        
        if force_reply:
            should_reply = True
        else:
            should_reply = await self._reply(group_uid, history_text, current_text)
        try:
            if should_reply:
            # 判定通过，走原生 pipeline 生成回复
                curr_cid = await self.context.conversation_manager.get_curr_conversation_id(
                    event.unified_msg_origin
            )
                if curr_cid:
                    conv = await self.context.conversation_manager.get_conversation(
                            event.unified_msg_origin, curr_cid
                        )
                    yield event.request_llm(
                            prompt="placeholder",  # 会被 on_llm_request 钩子覆盖
                            session_id=event.session_id,
                            conversation=conv,
                        )
                else:
                    event.stop_event()   
            else:
                event.stop_event()  
    
        finally:
                async with self.lock:
                    if group_uid not in self.history:
                        self.history[group_uid] = []
                        self.last_time[group_uid] = ""
                        self.last_group_name[group_uid] = ""
                        self.pending[group_uid] = []
                        self.is_waiting[group_uid] = False
                    self.history[group_uid].extend(round_msgs)  # ← extend 列表，不是遍历字符串            
                    if len(self.history[group_uid]) > self.MAX_HISTORY:
                            # 从头部删，保留最新的
                        del self.history[group_uid][:len(self.history[group_uid]) - self.MAX_HISTORY]
                    self._compress_history_images(group_uid, max_keep=self.PICTURE)
    
    @filter.after_message_sent()
    async def process_bot_message(self,event):
        group_uid = event.session_id 
        result = event.get_result()
        text = result.chain
        bot_name = self.BOT_NAME
        bot_message_str = ''.join([comp.text for comp in text if isinstance(comp, Plain)])
        bot_message = f"{bot_name}(你自己)[{event.get_self_id()}]:{bot_message_str}"
        async with self.lock:
            if group_uid not in self.history:
                self.history[group_uid] = []
                self.last_time[group_uid] = ""  
                self.last_group_name[group_uid] = ""
                self.pending[group_uid] = []
                self.is_waiting[group_uid] = False
            self.history[group_uid].append(bot_message)
            if len(self.history[group_uid]) > self.MAX_HISTORY:
                self.history[group_uid].pop(0)
    
    async def _reply(self, group_uid: str, history_text: str, current_text: str) -> bool:
        try:
            history_clean = re.sub(r"\[IMG_B64:[A-Za-z0-9+/=]+\]", "[图片]", history_text)
            current_clean = re.sub(r"\[IMG_B64:[A-Za-z0-9+/=]+\]", "[图片]", current_text)
            prompt = self.PROMPT.format(history_text=history_clean, current_text=current_clean)
            logger.info(f"主动回复ai提示词\n{prompt}")
            llm_resp = await self.context.llm_generate(
                chat_provider_id=self.PROVIDER_ID,
                prompt=prompt
            )            
            text = (llm_resp.completion_text or "").strip().upper()
            logger.info(f"判定结果: {text}")
            return "REPLY" in text            
        except Exception as e:
            logger.error(f"主动回复判定失败: {e}")
            return False   
        
    @filter.on_llm_request()
    async def save_in_history(self, event: AstrMessageEvent, req: ProviderRequest):
        if not event.get_extra("_my_active_reply", False):
            return
        
        history_text = event.get_extra("_ar_history", "")
        current_text = event.get_extra("_ar_current", "")

        # 提取所有 [IMG_B64:...]
        pattern = re.compile(r"\[IMG_B64:([A-Za-z0-9+/=]+)\]")
        images = []
        for m in pattern.finditer(history_text + "\n" + current_text):
            images.append(m.group(1))
        
        # 清理文本：只留 [图片] 占位
        history_clean = pattern.sub("[图片]", history_text)
        current_clean = pattern.sub("[图片]", current_text)

        # 群聊流水账
        chat_log = f"""你正在群聊里和朋友们聊天。
最近的群聊记录：
{history_clean}
当前消息：
{current_clean}"""

        # 低侵入：不再整体替换 req.prompt / req.contexts。
        # 流水账只用于图片场景：有图时把群聊流水账合并进 prompt，并走 AstrBot
        # 原生的 image_urls 多模态通道（由 assemble_context 拼成统一消息）；
        # 无图时只清掉占位符，不额外注入流水账，保留其他插件已注入的内容。
        base_prompt = (getattr(req, "prompt", None) or "").strip()
        base_prompt = base_prompt.replace("placeholder", "").strip()

        if images:
            req.prompt = f"{base_prompt}\n{chat_log}".strip() if base_prompt else chat_log
            logger.info(f"注入 {len(images)} 张图片，使用 AstrBot 原生 image_urls 通道")
            extra = [f"data:image/jpeg;base64,{b64}" for b64 in images]
            existing = list(getattr(req, "image_urls", None) or [])
            req.image_urls = existing + extra
        else:
            # 最小占位，保证 prompt 非空（self_learning 等插件依赖），不算流水账
            req.prompt = base_prompt or "请基于当前上下文回复。"

        # 不再整体替换 req.contexts，保留记忆、self_learning、token_controller 等的注入
        
    def _compress_history_images(self, group_uid: str, max_keep: int = None):
        """
        扫描历史记录，只保留最新的 max_keep 个 [IMG_B64:...]，
        其余替换为 [图片]（参考插件的'降级'思路）。
        从左到右扫描，越靠前的图越老。
        """
        if max_keep is None:
            max_keep = self.PICTURE
        history = self.history[group_uid]
        # 匹配 [IMG_B64:纯base64内容]
        pattern = re.compile(r"\[IMG_B64:([A-Za-z0-9+/=]+)\]")
        
        matches = []  # [(行索引, 完整匹配字符串)]
        for idx, line in enumerate(history):
            for m in pattern.finditer(line):
                matches.append((idx, m.group(0)))
        
        if len(matches) <= max_keep:
            return
        
        # 前面的是老图，替换掉
        for idx, full_tag in matches[:-max_keep]:
            history[idx] = history[idx].replace(full_tag, "[图片]", 1)
            logger.info(f"历史图片 base64 已降级: group={group_uid[:20]}... line={idx}")
