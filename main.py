from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api.provider import ProviderRequest
from astrbot.api.message_components import Plain, Image, At, Reply
from astrbot.api import AstrBotConfig
from datetime import datetime
from typing import Dict, List
from astrbot.api import logger
import asyncio
import base64
import aiohttp
import re


class MyPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)

        self.config = config
        self.history: Dict[str, List] = {}          # 历史消息缓存
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
        # 双模式开关：true=主动回复模式（本插件判定 REPLY/SKIP 并自行接管上下文），
        # false=仅图片注入模式（回复时机与上下文交给 group_chat_plus 等插件，本插件只补图）
        self.ACTIVE_REPLY_ENABLED = config.get("active_reply_enabled", True)

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

    @staticmethod
    def _normalize_str(value) -> str:
        if value is None:
            return ""
        try:
            s = str(value)
        except Exception:
            return ""
        s = s.strip()
        if s.startswith("`") and s.endswith("`") and len(s) >= 2:
            s = s[1:-1].strip()
        return s

    @staticmethod
    def _is_emoji_sub_type(sub_type) -> bool:
        if sub_type is None:
            return False
        if sub_type == 1 or sub_type == "1":
            return True
        try:
            return int(sub_type) == 1
        except Exception:
            return False

    @staticmethod
    def _is_emoji_summary(summary) -> bool:
        s = MyPlugin._normalize_str(summary)
        if not s:
            return False
        s_lower = s.lower()
        return "表情" in s or "emoji" in s_lower or "sticker" in s_lower

    def _raw_image_segments(self, event) -> list:
        try:
            raw_message = getattr(event.message_obj, "raw_message", None)
            raw_msg_list = getattr(raw_message, "message", None)
            if isinstance(raw_msg_list, list):
                return [
                    seg
                    for seg in raw_msg_list
                    if isinstance(seg, dict) and seg.get("type") == "image"
                ]
        except Exception:
            pass
        return []

    @staticmethod
    def _segment_matches_image(data: dict, img_comp: Image) -> bool:
        for key in ("file", "url", "path"):
            seg_val = str(data.get(key, "") or "")
            comp_val = str(getattr(img_comp, key, "") or "")
            if seg_val and seg_val == comp_val:
                return True
        return False

    def is_emoji_image(self, event, img_comp: Image) -> bool:
        """与 group_chat_plus 对齐：判断图片是否为平台标记的表情包"""
        if hasattr(img_comp, "subType") and img_comp.subType is not None:
            if MyPlugin._is_emoji_sub_type(img_comp.subType):
                return True

        if hasattr(img_comp, "__dict__"):
            if MyPlugin._is_emoji_sub_type(img_comp.__dict__.get("sub_type")):
                return True

        try:
            raw_data = img_comp.toDict()
            if isinstance(raw_data, dict) and isinstance(raw_data.get("data"), dict):
                data = raw_data["data"]
                sub_type = data.get("sub_type") or data.get("subType")
                if MyPlugin._is_emoji_sub_type(sub_type):
                    return True
                if MyPlugin._is_emoji_summary(data.get("summary")):
                    return True
                img_type = data.get("type") or data.get("imageType") or data.get("image_type")
                if img_type in ("emoji", "sticker", "face", "meme"):
                    return True
        except Exception:
            pass

        segments = self._raw_image_segments(event)
        if not segments:
            return False

        image_comps = [comp for comp in event.message_obj.message if isinstance(comp, Image)]
        for seg in segments:
            data = seg.get("data")
            if not isinstance(data, dict):
                continue
            if not MyPlugin._segment_matches_image(data, img_comp):
                continue
            if MyPlugin._is_emoji_sub_type(data.get("sub_type") or data.get("subType")):
                return True
            if MyPlugin._is_emoji_summary(data.get("summary")):
                return True

        # 一条原始段 + 一个图片组件时，直接复用原始段判据
        if len(image_comps) == 1 and len(segments) == 1:
            data = segments[0].get("data")
            if not isinstance(data, dict):
                return False
            if MyPlugin._is_emoji_sub_type(data.get("sub_type") or data.get("subType")):
                return True
            if MyPlugin._is_emoji_summary(data.get("summary")):
                return True
        return False

    def _ensure_group_state(self, group_uid: str):
        if group_uid in self.history:
            return
        self.history[group_uid] = []
        self.last_time[group_uid] = ""
        self.last_group_name[group_uid] = ""
        self.pending[group_uid] = []
        self.is_waiting[group_uid] = False

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
                # 平台标记的表情包不参与注入
                if self.is_emoji_image(event, comp):
                    logger.info("检测到平台表情包图片，跳过注入")
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
                # 被引用消息里的图片也要提取，否则"引用图片"时 bot 看不到被引用的图
                # （aiocqhttp 会通过 get_msg 把被引用消息的完整 chain 取回来，Image 就在里面）。
                # 占位符先收集，最后拼进引用文本内部，保证 [图片] 出现在引用体里而不是前面
                quote_parts = []
                if comp.chain:
                    for sub in comp.chain:
                        if isinstance(sub, Image):
                            # GIF 和直发图片一样处理：跳过不传给 LLM
                            if self._is_gif(sub):
                                quote_parts.append("[GIF图片]")
                                continue
                            # 引用是主动行为，被引用的图即使被平台标成表情包也提取，
                            # 确保bot能看到用户特意引用的内容
                            b64 = await self.download_image_to_b64(sub)
                            if b64:
                                images_b64.append(b64)
                                quote_parts.append("[图片]")
                            else:
                                quote_parts.append("[图片(获取失败)]")
                quote_body = reply_text + "".join(quote_parts)
                # 格式化写进聊天记录
                if quote_body.strip():
                    texts.append(f"引用消息[{reply_sender}: {quote_body}]")
                else:
                    texts.append(f"引用消息[{reply_sender}: (消息内容)]")

        return "".join(texts), images_b64

    async def download_image_to_b64(self, img_comp: Image) -> str:
        """
        尝试多种途径获取图片的 base64。
        优先级：本地 path > data URI / base64 字符串 > url 下载 > 适配器原始字典 > AstrBot 内置解析
        """
        # 先打印组件属性，方便排查
        comp_attrs = {k: getattr(img_comp, k, None) for k in ("url", "file", "path", "raw")}
        logger.debug(f"Image 组件关键属性: {comp_attrs}")

        # ===== 途径0：本地 path（最快最稳，napcat/llonebot 等常带） =====
        local_path = getattr(img_comp, "path", None)
        if local_path and isinstance(local_path, str):
            import os
            if os.path.isfile(local_path):
                try:
                    with open(local_path, "rb") as f:
                        data = f.read()
                    if data[:3] == b'GIF':
                        logger.info(f"本地文件是 GIF，丢弃: {local_path}")
                        return None
                    b64 = base64.b64encode(data).decode("utf-8")
                    logger.info(f"本地图片读取成功: {local_path} -> base64 len={len(b64)}")
                    return b64
                except Exception as e:
                    logger.warning(f"本地图片读取失败 {local_path}: {e}")

        # ===== 途径1：file 属性已经是 data URI 或 base64 =====
        file_attr = getattr(img_comp, "file", None)
        if file_attr and isinstance(file_attr, str):
            if file_attr.startswith("data:image"):
                if "," in file_attr:
                    return file_attr.split(",", 1)[1]
                return file_attr
            # 有些平台 file 直接就是 base64 字符串（很长且不是url）
            if len(file_attr) > 1000 and not file_attr.startswith("http"):
                return file_attr

        # ===== 途径2：通过 url 下载 =====
        url = getattr(img_comp, "url", None)
        # file_attr 可能是字符串 url（兜底，但必须确保是 str）
        if not url and isinstance(file_attr, str) and file_attr.startswith(("http://", "https://")):
            url = file_attr

        # 关键修复：确保 url 是字符串，防止 file_attr 为 dict 时炸掉
        if url and isinstance(url, str) and url.startswith(("http://", "https://")):
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            # QQ/微信图片通常需要 referer 防 403
            if "qq" in url.lower() or "gchat.qpic.cn" in url or "multimedia.nt.qq" in url:
                headers["Referer"] = "https://qq.com"
            if "weixin" in url.lower() or "mmbiz" in url.lower():
                headers["Referer"] = "https://wx.qq.com/"

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=15),
                        headers=headers
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
                            body_preview = await resp.text()
                            logger.warning(
                                f"图片下载返回非200: {url[:60]}... status={resp.status}, "
                                f"body={body_preview[:100]}"
                            )
            except Exception as e:
                logger.warning(f"图片下载失败 {url[:60]}...: {e}")

        # ===== 途径3：尝试通过适配器原始字典获取 =====
        try:
            raw = getattr(img_comp, "_raw_message", None) or getattr(img_comp, "raw", None)
            if raw and isinstance(raw, dict):
                for key in ("file", "url", "path", "file_path", "image_url"):
                    val = raw.get(key)
                    if val and isinstance(val, str):
                        if val.startswith("data:image"):
                            return val.split(",", 1)[1] if "," in val else val
                        if len(val) > 1000 and not val.startswith("http"):
                            return val
                        if val.startswith(("http://", "https://")):
                            tmp = type("T", (), {"url": val, "file": None, "path": None})()
                            return await self.download_image_to_b64(tmp)
        except Exception as e:
            logger.debug(f"尝试 raw 字段获取图片失败: {e}")

        # ===== 途径4：AstrBot 内置 MediaResolver 兜底，统一处理本地路径、
        # base64://、data URI 和网络 URL 等格式 =====
        try:
            b64 = await img_comp.convert_to_base64()
            if b64:
                logger.info(f"AstrBot 内置图片解析成功: base64 len={len(b64)}")
                return b64
        except Exception as e:
            logger.warning(f"AstrBot 内置图片解析失败: {e}")

        logger.warning(f"无法获取图片 base64，所有途径均失败。Image 属性: {comp_attrs}")
        return None

    # 格式化时间戳
    def simple_time(self, ts) -> str:
        dt = datetime.fromtimestamp(ts)
        return f"{dt.year}/{dt.month:02d}/{dt.day:02d} {dt.hour:02d}:{dt.minute:02d}"

    def _append_history(self, group_uid: str, round_msgs: List[str]):
        """将本轮消息追加到历史，并做截断和图片压缩。线程安全由调用方保证。"""
        if group_uid not in self.history:
            self.history[group_uid] = []
            self.last_time[group_uid] = ""
            self.last_group_name[group_uid] = ""
            self.pending[group_uid] = []
            self.is_waiting[group_uid] = False

        self.history[group_uid].extend(round_msgs)
        if len(self.history[group_uid]) > self.MAX_HISTORY:
            # 从头部删，保留最新的（O(k)，比反复 pop(0) 快）
            del self.history[group_uid][:len(self.history[group_uid]) - self.MAX_HISTORY]
        self._compress_history_images(group_uid, max_keep=self.PICTURE)

    # 收集当前消息，下文消息并合并到维护的历史中
    # *args/**kwargs 兼容：AstrBot v4.26.5 会把指令解析参数广播给所有已激活 handler，
    # 不加的话 handler 会收到多余参数抛 TypeError
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def process_message(self, event: AstrMessageEvent, *args, **kwargs):
        if event.get_sender_id() == event.get_self_id():   # 过滤自己
            return

        group_uid = event.session_id
        text_part, img_b64_list = await self.message_and_images(event)
        # 机器人自己的 At 在 message_and_images 里生成的是 "@名字[ID]" 格式，
        # 其他人的 At 是 "[@名字]"，两种都要剥掉，空消息/指令判断才准确
        bot_at_text = f"@{self.BOT_NAME}[{event.get_self_id()}]"
        substantial_text = re.sub(
            r'\[@.*?\]', '', text_part.replace(bot_at_text, '')
        ).strip()

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

        # 基于提取后的文本判断指令（必须用和 message_and_images 一致的 At 格式剔除，
        # 旧版写的是 "[@bot名/ID]"，和实际生成的 "@bot名[ID]" 对不上，永远替换不到）
        clean_text = text_part.replace(bot_at_text, "")
        clean_text = re.sub(r'\[@.*?\]', '', clean_text).strip()
        if clean_text.startswith(self.COMMAND):
            return

        if event.message_str.startswith(self.COMMAND):
            return

        # 上面剥掉了机器人自己的 At，纯 @机器人 的消息 substantial_text 会变空，
        # 所以这里要放行 force_reply，否则 bare @机器人 会被误丢掉
        if not substantial_text and not img_b64_list and not force_reply:
            return

        current_message = f"{event.get_sender_name()}[{event.get_sender_id()}]:{text_part}"
        # 把 base64 追加在消息后面，用换行分隔
        for b64 in img_b64_list:
            current_message += f"\n[IMG_B64:{b64}]"
        current_time = self.simple_time(event.created_at)    # 解析时间戳
        log_msg = re.sub(r"\[IMG_B64:[A-Za-z0-9+/=]+\]", "[图片]", current_message)
        logger.info(f"触发消息: {log_msg}")

        # 给仅图片注入模式的 on_llm_request 钩子用的当前消息信息
        event.set_extra("_ar_current_text", text_part)
        event.set_extra("_ar_current_images", img_b64_list)
        event.set_extra("_ar_current_full", current_message)

        if not self.ACTIVE_REPLY_ENABLED:
            # 仅图片注入模式：只维护群聊流水账，不等待、不判定、不主动回复，
            # 也不 stop_event，后续交给平台/其他插件正常处理。
            async with self.lock:
                self._ensure_group_state(group_uid)
                if self.last_time[group_uid] != current_time:
                    self.history[group_uid].append(f"[{current_time}]")
                    self.last_time[group_uid] = current_time
                self.history[group_uid].append(current_message)
                self.current_msg[group_uid] = current_message
                if len(self.history[group_uid]) > self.MAX_HISTORY:
                    del self.history[group_uid][:len(self.history[group_uid]) - self.MAX_HISTORY]
                self._compress_history_images(group_uid, max_keep=self.PICTURE)
            return

        async with self.lock:        # 初始化
            if group_uid not in self.history:
                self.history[group_uid] = []
                self.last_time[group_uid] = ""
                self.last_group_name[group_uid] = ""
                self.pending[group_uid] = []
                self.is_waiting[group_uid] = False

            # 后文消息处理
            if self.is_waiting[group_uid]:  # 如果是后文就加入当前的消息
                self.pending[group_uid].append(current_message)
                return

            self.is_waiting[group_uid] = True   # 把触发的当前消息放入current_msg，然后设置is waiting为true
            self.current_msg[group_uid] = current_message

            if self.last_time[group_uid] != current_time:
                self.history[group_uid].append(f"[{current_time}]")
                self.last_time[group_uid] = current_time

        try:
            await asyncio.sleep(self.SLEEP_TIME)
        finally:
            # 收尾必须放在 finally：若 sleep 期间任务被取消（插件热重载、关机等），
            # is_waiting 不复位的话会永远卡在 True，之后该群所有消息只会进 pending，
            # 再也不会触发判定，表现为这个群永久沉默
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
            reply_reason = "被@或引用，强制回复"
        else:
            should_reply, reply_reason = await self._reply(group_uid, history_text, current_text)

        if not should_reply:
            logger.info(f"主动回复判定 SKIP，不回复理由: {reply_reason[:100] if reply_reason else '(无)'}")
            # 不回复也要保存历史，避免丢消息
            async with self.lock:
                self._append_history(group_uid, round_msgs)
            # 兼容新旧版本的事件中断方式
            try:
                event.stop_event()
            except AttributeError:
                try:
                    event.stop()
                except AttributeError:
                    pass
            return

        # 判定通过，走原生 pipeline 生成回复
        event.set_extra("_ar_reason", reply_reason)
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
            # yield 返回说明框架已处理完 LLM 请求，此时保存历史
            async with self.lock:
                self._append_history(group_uid, round_msgs)
        else:
            # 没有会话也保存历史
            async with self.lock:
                self._append_history(group_uid, round_msgs)
            try:
                event.stop_event()
            except AttributeError:
                try:
                    event.stop()
                except AttributeError:
                    pass

    @filter.after_message_sent()
    async def process_bot_message(self, event: AstrMessageEvent, *args, **kwargs):
        group_uid = event.session_id
        result = event.get_result()
        text = result.chain
        bot_name = self.BOT_NAME
        bot_message_str = ''.join([getattr(comp, 'text', '') for comp in text if isinstance(comp, Plain)])
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
                # 统一用切片删除，避免 O(n) pop(0)
                del self.history[group_uid][:len(self.history[group_uid]) - self.MAX_HISTORY]

    async def _reply(self, group_uid: str, history_text: str, current_text: str) -> tuple[bool, str]:
        """返回 (是否回复, 回复理由)"""
        try:
            history_clean = re.sub(r"\[IMG_B64:[A-Za-z0-9+/=]+\]", "[图片]", history_text)
            current_clean = re.sub(r"\[IMG_B64:[A-Za-z0-9+/=]+\]", "[图片]", current_text)
            # 用 replace 代替 str.format：format 遇到自定义提示词里的字面 { 或 }
            # 会抛 KeyError/ValueError，被下方 except 吞掉后表现为永远 SKIP 且无任何提示
            prompt = self.PROMPT.replace("{history_text}", history_clean).replace(
                "{current_text}", current_clean
            )
            logger.info(f"主动回复ai提示词\n{prompt}")
            llm_resp = await self.context.llm_generate(
                chat_provider_id=self.PROVIDER_ID,
                prompt=prompt
            )

            # 兼容 AstrBot 不同版本：返回值可能是字符串也可能是对象
            if isinstance(llm_resp, str):
                raw_text = llm_resp.strip()
            else:
                raw_text = (getattr(llm_resp, "completion_text", None) or "").strip()

            # 取文本中第一个出现的 REPLY / SKIP（词边界匹配），识别不了时按 SKIP 处理
            km = re.search(r"\b(REPLY|SKIP)\b", raw_text.upper())
            should_reply = bool(km and km.group(1) == "REPLY")

            # 理由：多行取第一行之后的内容；单行取关键词后面的内容
            lines = raw_text.split('\n', 1)
            reason = ""
            if len(lines) > 1:
                reason = lines[1].strip()
            elif km:
                after = raw_text[km.end():].strip()
                if after:
                    reason = after.lstrip(':-：，,')

            logger.info(
                f"判定结果: {'REPLY' if should_reply else 'SKIP'}, "
                f"理由: {reason[:80] if reason else '(无)'}"
            )
            return should_reply, reason
        except Exception as e:
            logger.error(f"主动回复判定失败: {e}")
            return False, ""

    async def _inject_image_only(self, event: AstrMessageEvent, req: ProviderRequest):
        """仅图片注入模式：只处理普通图片消息，不注入主动回复相关上下文"""
        if event.is_private_chat():
            return

        group_uid = event.session_id
        current_images = event.get_extra("_ar_current_images", None)

        if not current_images:
            # 兜底：钩子先于消息处理器执行时，直接从事件里重新解析
            try:
                _, current_images = await self.message_and_images(event)
            except Exception as e:
                logger.warning(f"仅图片注入模式解析当前消息失败: {e}")
                current_images = []
        async with self.lock:
            history_lines = list(self.history.get(group_uid, []) or [])

        if not current_images:
            # 图片与 @ 分开发送时，chat_plus 的等待窗口会把两条合并进同一个事件，
            # 触发 LLM 的那个事件自身没有图片。此时按「整个上下文历史中最近的
            # 两张图片信息」回退，保证群友先发图、再提问时模型仍能看到图。
            fallback_images: list[str] = []
            for _line in reversed(history_lines):
                for _m in re.finditer(r"\[IMG_B64:([A-Za-z0-9+/=]+)\]", _line):
                    fallback_images.append(_m.group(1))
                    if len(fallback_images) >= 2:
                        break
                if len(fallback_images) >= 2:
                    break
            if not fallback_images:
                # 流水账只给图片场景使用：当前和历史都没有普通图片就不注入
                return
            fallback_images.reverse()  # 恢复时间顺序
            current_images = fallback_images
            logger.info(
                f"仅图片注入模式：当前消息无图，回退取历史上最近 "
                f"{len(current_images)} 张图片"
            )
        # 只注入图片，不碰 req.prompt / req.contexts：上下文（历史、记忆、工具）
        # 由 group_chat_plus 负责拼装，本插件再追加一份群聊流水账只会让同一段
        # 对话在 prompt 里出现两遍。需要由本插件接管上下文的场景（例如没装
        # group_chat_plus），把 active_reply_enabled 打开即可——那时
        # save_in_history 会走另一条分支，用流水账自行构造 req.prompt 并判定
        # REPLY/SKIP，主动回复能力完整保留。
        extra = [f"data:image/jpeg;base64,{b64}" for b64 in current_images]
        existing = list(getattr(req, "image_urls", None) or [])
        req.image_urls = existing + extra
        logger.info(f"仅图片注入模式：注入 {len(current_images)} 张图片（流水账交由 chat_plus）")

    # 优先级必须低于其他插件的默认优先级 0（例如 group_chat_plus 是 -1、
    # gitee_aiimg 是 -20）：这些插件的 on_llm_request 会覆盖 req.prompt /
    # req.image_urls，默认优先级 0 会先执行、注入的图片随即被覆盖掉，等于白注入。
    # *args/**kwargs 兼容：AstrBot v4.26.5 会把指令解析参数广播给所有已激活 handler
    @filter.on_llm_request(priority=-30)
    async def save_in_history(self, event: AstrMessageEvent, req: ProviderRequest, *args, **kwargs):
        if not self.ACTIVE_REPLY_ENABLED:
            # 仅图片注入模式：只补图片，上下文交给其他插件
            await self._inject_image_only(event, req)
            return

        if not event.get_extra("_my_active_reply", False):
            return

        history_text = event.get_extra("_ar_history", "")
        current_text = event.get_extra("_ar_current", "")
        reason = event.get_extra("_ar_reason", "")

        # 提取所有 [IMG_B64:...]，并按出现顺序编号成 [图片1]、[图片2]...
        # 同一张图（同 base64）复用同一个编号：这样引用消息里的占位符会直接等于
        # 原消息的编号，模型才能知道"引用的是哪张图"；且同一张图不会被重复附带。
        # 附带图片的顺序 = 编号顺序（先历史后当前）
        pattern = re.compile(r"\[IMG_B64:([A-Za-z0-9+/=]+)\]")
        images: List[str] = []
        seen: Dict[str, int] = {}

        def _number_tag(m):
            b64 = m.group(1)
            if b64 in seen:
                return f"[图片{seen[b64]}]"
            seen[b64] = len(images) + 1
            images.append(b64)
            return f"[图片{seen[b64]}]"

        history_clean = pattern.sub(_number_tag, history_text)
        current_clean = pattern.sub(_number_tag, current_text)

        # 群聊流水账
        chat_log = f"""你正在群聊里和朋友们聊天。
（注：文中 [图片1]、[图片2] 等标记按出现顺序对应本条消息附带的第 1、2…张图片，同一张图编号相同；不带数字的 [图片] 表示较早的图片，只有文字占位、看不到内容）
最近的群聊记录：
{history_clean}
当前消息：（注：当前消息的第一条消息，是你准备回复的那条信息，其他消息均为辅助信息）
{current_clean}"""

        # 如果有判定理由，追加给回复 AI 作为参考
        if reason:
            chat_log += f"\n\n（你决定回复的理由：{reason}）"

        # 主动回复模式下上下文由本插件全权接管：清掉 conversation，防止
        # build_main_agent 用原生会话历史覆盖 contexts（astr_main_agent.py 中
        # if req.conversation: req.contexts = json.loads(req.conversation.history)），
        # 也避免整份流水账被反复存进原生记忆造成跨轮重复膨胀
        req.conversation = None

        if images:
            logger.info(f"[save_in_history] 注入 {len(images)} 张图片到请求中")
            for idx, b64 in enumerate(images):
                logger.debug(f"[save_in_history] 第 {idx+1} 张图片 base64 len={len(b64)}")
        else:
            # 没图时走纯文本
            logger.info("[save_in_history] 无图片，走纯文本 prompt")

        # 文本放 prompt，图片走框架的 image_urls 通道（base64:// 是框架明确支持的格式），
        # 框架会拼成一条 [文本, 图片...] 的多模态 user 消息，群聊格式不变。
        # 不能像旧版那样把 prompt 清成 ""：框架对任何非 None 的 prompt 都会追加一条
        # user 消息，空 prompt 会生成 content 为空数组的消息，被 API 400 拒绝
        # （"the message at position 2 with role 'user' must not be empty"）；
        # 且 prompt 为空且无媒体时 build_main_agent 会直接丢弃整个请求
        req.contexts = []
        req.prompt = chat_log
        req.image_urls = [f"base64://{b64}" for b64 in images]

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
