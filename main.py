from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.api.provider import ProviderRequest
from astrbot.api.message_components import Plain, Image, At, Reply
from astrbot.api import AstrBotConfig
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from astrbot.api import logger
import asyncio
import base64
import aiohttp
import json
import os
import re
import time

# 历史落盘的防抖间隔（秒）。消息一路写入内存，后台任务按这个节奏批量刷盘，
# 只有硬杀进程才会丢掉最后不到这个间隔的写入；正常重载/关闭由 terminate() 兜底。
_FLUSH_INTERVAL = 1.0


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
        # 黑名单缓存：{配置项名: (配置原始值, 解析后的集合)}，配置热重载或内容变化后自动重新解析
        self._blacklist_caches: Dict[str, Tuple[Any, set]] = {}

        # ---- 消息分段 ----
        self.SEGMENT_ENABLE_GROUP = config.get("segment_enable_group", True)
        self.SEGMENT_ENABLE_PRIVATE = config.get("segment_enable_private", True)
        self.SEGMENT_REGEX = config.get("segment_regex") or r"[\n]+"
        self.SEGMENT_MAX_GROUP = config.get("segment_max_group", 3)
        self.SEGMENT_MAX_PRIVATE = config.get("segment_max_private", 3)
        self.SEGMENT_DELAY = config.get("segment_delay", 0.5)

        # ---- 模型自主引用 ----
        # 编号只作用于"当前这一轮"的消息：轮次在 sleep 结束的瞬间冻结，之后来的消息属于下一轮
        # （编号重新从 1 开始），所以"编号 → 消息"的对应关系永远不会被改写，不需要额外快照。
        # 注意：只收群友的消息。机器人自己发出去的消息 ID 框架不返回（send_message 是 -> None），
        # 所以"引用自己刚才说的话"暂时做不到。
        self.QUOTE_ENABLE = config.get("quote_enable", True)
        self.current_id: Dict[str, str] = {}          # 本轮触发消息的 message_id
        self.pending_ids: Dict[str, List[str]] = {}   # 本轮后文的 message_id（与 pending 一一对应）
        # 引用标记：[reply3] / [Reply:3] / [ reply 3 ] 都认
        self.QUOTE_TAG_RE = re.compile(r"\[\s*reply\s*:?\s*(\d+)\s*\]", re.IGNORECASE)

        # ---- 历史持久化 ----
        # 只持久化 history / last_time 这两份"流水账"；pending / is_waiting / current_msg /
        # current_id / pending_ids 属于"正在收集的那一轮"，生命周期绑在 process_message 的
        # asyncio.sleep 上。重启后那个协程已经不存在，恢复它们只会让会话永久卡在
        # is_waiting=True（之后所有消息都被当成后文吞掉，表现为"这个会话永远不回话"）。
        self.PERSIST_HISTORY = config.get("persist_history", True)
        self.PERSIST_IMAGES = config.get("persist_images", True)
        # 把每次判定用的完整提示词以 INFO 级别打进日志（默认关闭；整段 prompt 几十行，很占地方）
        self.LOG_PROMPT = config.get("log_prompt", False)
        self._history_dir: Optional[Path] = None
        self._dirty: set = set()                     # 需要落盘的会话键
        self._flusher: Optional[asyncio.Task] = None  # 后台防抖写盘任务
        # 被 /clear_history 清空时，若该会话正好有一轮在收集窗口里，把那一轮作废掉。
        # 不作废的话，几秒后它收尾时会把自己写回刚清空的历史 —— 这就是"上下文污染"。
        self._discard_round: set = set()

        # ---- 自适应等待时长 ----
        # 等待时长跟着"群里此刻有多热"浮动：刚有人说过话就多等一会收集上下文，
        # 冷场半天突然来一句就快速判定。以下三份记录只存在内存、不持久化，
        # 重启后没有"上一条消息"的时间，按基准值处理。
        self.ADAPTIVE_SLEEP = config.get("sleep_time_adaptive", True)
        self.last_friend_ts: Dict[str, float] = {}   # 会话键 -> 上一条群友消息的时刻
        self._round_t0: Dict[str, float] = {}        # 会话键 -> 本轮触发时刻
        self._round_until: Dict[str, float] = {}     # 会话键 -> 本轮软截止时刻（可被后文顺延）

    @staticmethod
    def _detect_image_mime_from_b64(b64: str) -> str:
        """从 base64 头部魔数判断图片真实格式，用来拼 provider 能识别的 data URI。

        官方 DeepSeek API 对 data URI 的 MIME 很严格：缺失、写成 application/octet-stream
        或裸 base64，都会让整条请求 400（"You have uploaded an unsupported image"）。
        所以这里必须给出真实的 image/<fmt>。
        """
        try:
            head_b64 = (b64 or "")[:32]
            head_b64 += "=" * (-len(head_b64) % 4)
            head = base64.b64decode(head_b64)
        except Exception:
            return "image/jpeg"
        if head.startswith(b"\xff\xd8\xff"):
            return "image/jpeg"
        if head.startswith(b"\x89PNG\r\n\x1a\n"):
            return "image/png"
        if head.startswith(b"GIF8"):
            return "image/gif"
        if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
            return "image/webp"
        if head.startswith(b"BM"):
            return "image/bmp"
        return "image/jpeg"

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

    @staticmethod
    def _session_key(event: AstrMessageEvent) -> str:
        """取这个会话在历史里的键。

        私聊的 session_id 是对方 QQ 号、群聊是群号，两者可能撞号。统一加前缀区分，
        避免同一个人的私聊和某个群共用同一份记忆（落盘文件名也按这个键生成）。

        Args:
            event: 当前消息事件。

        Returns:
            形如 ``group_123456`` 或 ``private_10086`` 的会话键。
        """
        try:
            private = bool(event.is_private_chat())
        except Exception:
            private = False
        try:
            sid = str(event.session_id or "")
        except Exception:
            sid = ""
        return f"{'private' if private else 'group'}_{sid}"

    def _sleep_base(self) -> float:
        """基准等待时长（sleep_time）；配置缺失或非法时按 0 处理。

        Returns:
            非负的基准秒数。
        """
        try:
            return max(0.0, float(self.SLEEP_TIME))
        except (TypeError, ValueError):
            return 0.0

    def _calc_wait(self, gap: Optional[float], at_bot: bool = False) -> float:
        """按"距上一条群友消息多久"算出本轮要等多久。

        曲线是 ``y = 1 + (2S-1)·T/(x+T)``：一条直角双曲线（渐近线 x=-T、y=1），
        第一象限单调递减 —— x→0 顶到 2S，x→∞ 趋近 1s。取 ``T = 2(S-1)/S``，
        使 x=2 秒时 y 恰好等于基准值 S（"隔了 2 秒就按基准时间等"），S=12 时 T≈1.83。

        被@机器人（``at_bot``）时例外：那是真的在点名叫我，直接强制用 ``S/2``，
        不看密度曲线。这条不受 ``sleep_time_adaptive`` 开关影响。

        Args:
            gap: 距上一条群友消息的秒数；冷启动（还没有记录）时传 None。
            at_bot: 本条消息是否 @ 了机器人自己（引用不算）。

        Returns:
            本轮等待秒数。被@时为 ``max(S/2, 1)``；冷启动、关闭自适应、或基准 <= 0 时
            直接返回基准值（0 表示不等待）；其余情况夹在 ``[1, 2S]`` 之间。
        """
        base = self._sleep_base()
        if base <= 0:
            return base
        if at_bot:
            return max(base / 2.0, 1.0)
        if not self.ADAPTIVE_SLEEP:
            return base
        if base <= 1:
            # [1, 2S] 是空区间，自适应没有意义，直接取下界
            return 1.0
        if gap is None:
            # 插件刚重启，没有"上一条消息"的时间：按基准值处理。
            # 刻意不走曲线 —— 曲线的锚点是 x=2 秒，而不是 x=S 秒。
            return base
        t = 2.0 * (base - 1.0) / base
        wait = 1.0 + (2.0 * base - 1.0) * t / (gap + t)
        return min(max(wait, 1.0), 2.0 * base)

    def _ensure_group_state(self, group_uid: str):
        """确保这个会话的全部内存字段都已就位（幂等）。

        判据必须用 is_waiting 而不是 history：历史可能来自磁盘预加载，那时 history 已经有值，
        但 pending / is_waiting 等瞬态字段还是空的。若用 history 当判据就会跳过初始化，
        紧接着 self.is_waiting[group_uid] 直接 KeyError，该会话从此每条消息都抛异常。

        Args:
            group_uid: 会话键。
        """
        if group_uid in self.is_waiting:
            return
        self.history.setdefault(group_uid, [])
        self.last_time.setdefault(group_uid, "")
        self.last_group_name.setdefault(group_uid, "")
        self.pending.setdefault(group_uid, [])
        self.pending_ids.setdefault(group_uid, [])
        self.current_id.setdefault(group_uid, "")
        self.current_msg.setdefault(group_uid, "")
        self.is_waiting[group_uid] = False

    # ==================================================================
    # 历史持久化：data/plugin_data/<插件名>/history/<会话键>.json
    # ==================================================================
    @staticmethod
    def _strip_images(lines: List[str]) -> List[str]:
        """把流水账里的图片 base64 降级成 [图片]。

        只作用于写盘/读盘的那一份副本，内存里的真身不动（当前这一轮请求还要用它）。

        Args:
            lines: 原始流水账行。

        Returns:
            图片已被降级的新列表。
        """
        return [
            re.sub(r"\[IMG_B64:[A-Za-z0-9+/=]+\]", "[图片]", line)
            for line in (lines or [])
        ]

    def _history_file(self, session_key: str) -> Path:
        """会话键 → 落盘文件路径。

        Windows 文件名不允许 ``:``，所以统一替换掉非法字符；会话键本身就带
        ``group_``/``private_`` 前缀，替换后仍可逆（真实键存在文件内容里）。

        Args:
            session_key: 会话键。

        Returns:
            该会话的历史文件路径。
        """
        safe = re.sub(r"[^\w.-]", "_", session_key) or "unknown"
        return self._history_dir / f"{safe}.json"

    def _write_history_file(self, session_key: str, history: List[str], last_time: str) -> None:
        """原子写入一个会话的流水账（先写临时文件再替换，避免断电留下半个文件）。

        Args:
            session_key: 会话键。
            history: 已截断好的流水账副本。
            last_time: 最后一条时间戳。

        Raises:
            OSError: 目录或文件不可写时抛出，由调用方决定是否重试。
        """
        target = self._history_file(session_key)
        tmp = target.with_name(target.name + ".tmp")
        payload = {
            "v": 1,
            "session_id": session_key,
            "last_time": last_time or "",
            "history": list(history or []),
        }
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, target)

    def _remove_history_file(self, session_key: str) -> None:
        """删除某个会话的落盘文件；文件本来就不存在不算错误。

        Args:
            session_key: 会话键。
        """
        if self._history_dir is None:
            return
        try:
            self._history_file(session_key).unlink(missing_ok=True)
        except Exception as exc:
            logger.warning(f"[历史持久化] 删除文件失败 {session_key}: {exc}")

    async def _flush_history(self) -> None:
        """把所有脏会话落盘。

        锁内只取快照，文件 IO 一律放在锁外：self.lock 是所有会话共用的，
        持锁写几百 KB 会把所有群的判定一起卡住。
        """
        if not self.PERSIST_HISTORY or self._history_dir is None:
            return
        async with self.lock:
            if not self._dirty:
                return
            uids = sorted(self._dirty)
            self._dirty.clear()
            snapshots: Dict[str, Tuple[List[str], str]] = {}
            for uid in uids:
                lines = list(self.history.get(uid) or [])
                if not self.PERSIST_IMAGES:
                    lines = self._strip_images(lines)
                snapshots[uid] = (lines, self.last_time.get(uid, ""))

        failed = set()
        for uid, (lines, last_time) in snapshots.items():
            try:
                self._write_history_file(uid, lines, last_time)
            except Exception as exc:
                failed.add(uid)
                logger.warning(f"[历史持久化] 写入失败 {uid}: {exc}")
        if failed:
            # 写失败的下个周期再试，绝不能把脏标记丢掉
            async with self.lock:
                self._dirty |= failed

    async def _history_flusher(self) -> None:
        """后台防抖写盘：每 _FLUSH_INTERVAL 秒把脏会话刷一次。

        收集窗口（sleep_time）里会连着来好几条消息，防抖天然把它们合并成一次写盘，
        消息路径本身完全不碰磁盘。
        """
        while True:
            try:
                await asyncio.sleep(_FLUSH_INTERVAL)
                await self._flush_history()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"[历史持久化] 后台写盘出错: {exc}", exc_info=True)

    async def initialize(self) -> None:
        """插件加载时把磁盘上的历史读回内存，并启动后台写盘任务。

        框架会 await 这个方法（star_manager 加载完插件后调用）。这里**绝不能把异常抛出去**：
        一旦抛出，框架会判定"插件加载失败"，整个插件都不会被加载。所以坏文件一律
        用 ERROR + 堆栈吼出来，然后直接丢弃、用一份全新的空记录覆盖，该会话从头重新写。
        """
        if not self.PERSIST_HISTORY:
            logger.info("[历史持久化] 已关闭：本次启动不加载、也不保存历史")
            return

        try:
            self._history_dir = Path(StarTools.get_data_dir()) / "history"
            self._history_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            # 拿不到数据目录就只能退回旧行为（重启即清空），不能让插件加载失败
            logger.error(
                f"[历史持久化] 无法准备数据目录，本次启动不持久化: {exc}", exc_info=True
            )
            self._history_dir = None
            return

        try:
            paths = sorted(self._history_dir.glob("*.json"))
        except Exception as exc:
            logger.error(f"[历史持久化] 扫描数据目录失败: {exc}", exc_info=True)
            paths = []

        loaded = 0
        for path in paths:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    raise ValueError(f"顶层不是对象，而是 {type(data).__name__}")
                raw_history = data.get("history")
                if not isinstance(raw_history, list):
                    raise ValueError("history 字段缺失或不是数组")
                session_key = str(data.get("session_id") or "").strip()
                if not session_key:
                    raise ValueError("session_id 字段缺失")
                lines = [str(line) for line in raw_history]
                if not self.PERSIST_IMAGES:
                    lines = self._strip_images(lines)
            except Exception as exc:
                # 坏文件：吼出来，然后直接覆盖丢弃、重建一个空的，该会话从头开始写。
                # path.stem 就是消毒过的会话键，_history_file 对它是幂等的，所以覆盖的是同一个文件。
                logger.error(
                    f"[历史持久化] 文件损坏，已丢弃并重建: {path} —— {exc}", exc_info=True
                )
                try:
                    self._write_history_file(path.stem, [], "")
                except Exception as write_exc:
                    logger.error(
                        f"[历史持久化] 重建空文件失败 {path}: {write_exc}", exc_info=True
                    )
                continue

            async with self.lock:
                self.history[session_key] = lines
                self.last_time[session_key] = str(data.get("last_time") or "")
                self._ensure_group_state(session_key)
                # 配置可能被中途调小，这里再截断/压缩一次，保证和运行期的规则一致
                if len(self.history[session_key]) > self.MAX_HISTORY:
                    del self.history[session_key][
                        : len(self.history[session_key]) - self.MAX_HISTORY
                    ]
                self._compress_history_images(session_key, max_keep=self.PICTURE)
            loaded += 1

        if self._flusher is None or self._flusher.done():
            self._flusher = asyncio.create_task(self._history_flusher())
        logger.info(
            f"[历史持久化] 已恢复 {loaded} 个会话的历史，目录 {self._history_dir}"
        )

    async def terminate(self) -> None:
        """插件被禁用/重载、或 AstrBot 正常关闭时调用。

        停掉后台任务并做最后一次落盘，所以正常重启/热重载一条历史都不会丢。
        必须直接定义在本类里：框架查的是 star_cls_type.__dict__，靠继承不生效；
        而且本类不能同时定义 __del__，否则框架会走 __del__ 分支、不再调用 terminate。
        """
        if self._flusher is not None:
            self._flusher.cancel()
            try:
                await self._flusher
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._flusher = None

        if not self.PERSIST_HISTORY or self._history_dir is None:
            return
        try:
            async with self.lock:
                self._dirty |= set(self.history.keys())
            await self._flush_history()
        except Exception as exc:
            logger.error(f"[历史持久化] 退出前落盘失败: {exc}", exc_info=True)

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
                    logger.debug("检测到平台表情包图片，跳过注入")
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
                        logger.debug(f"本地文件是 GIF，丢弃: {local_path}")
                        return None
                    b64 = base64.b64encode(data).decode("utf-8")
                    logger.debug(f"本地图片读取成功: {local_path} -> base64 len={len(b64)}")
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
                                logger.debug(f"下载后发现是 GIF，丢弃: {url[:60]}...")
                                return None
                            b64 = base64.b64encode(data).decode("utf-8")
                            logger.debug(f"图片下载成功: {url[:60]}... -> base64 len={len(b64)}")
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
                logger.debug(f"AstrBot 内置图片解析成功: base64 len={len(b64)}")
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
        self._ensure_group_state(group_uid)

        self.history[group_uid].extend(round_msgs)
        if len(self.history[group_uid]) > self.MAX_HISTORY:
            # 从头部删，保留最新的（O(k)，比反复 pop(0) 快）
            del self.history[group_uid][:len(self.history[group_uid]) - self.MAX_HISTORY]
        self._compress_history_images(group_uid, max_keep=self.PICTURE)
        self._dirty.add(group_uid)

    # 黑名单通用实现：主动回复黑名单（group_blacklist）与分段黑名单（segment_blacklist）共用。
    # 条目分隔符：逗号、分号、空白（群号与 platform:GroupMessage:群号 里都不含这些字符）
    _BLACKLIST_SEP_RE = re.compile(r"[,，;；\s]+")

    def _parse_blacklist(self, config_key: str) -> set:
        """读取并规范化配置里的某个黑名单，返回一个集合。

        每个条目支持三种写法（可混用）：
          - 纯群号/QQ号：123456789
          - 会话 ID：群聊里与群号一般是同一个值
          - 完整消息来源：aiocqhttp:GroupMessage:123456789
        单个条目里用逗号/分号/空格分隔多个值也认；以 # 开头的条目按注释忽略。
        结果按配置的原始值做缓存，配置热重载或内容变化后自动重新解析。
        """
        raw = self.config.get(config_key, [])
        if raw is None:
            raw = []
        # 兜底：用户手改配置文件时可能写成单个字符串/数字，而不是数组
        if isinstance(raw, (str, int)) or not isinstance(raw, (list, tuple, set)):
            raw = [raw]

        try:
            cache_key = tuple(str(item) for item in raw)
        except Exception:
            cache_key = None
        cached = self._blacklist_caches.get(config_key)
        if cache_key is not None and cached and cached[0] == cache_key:
            return cached[1]

        parsed = set()
        for item in raw:
            text = self._normalize_str(item)
            if not text or text.startswith("#"):
                continue
            for part in self._BLACKLIST_SEP_RE.split(text):
                part = part.strip()
                if part and not part.startswith("#"):
                    parsed.add(part)

        if cache_key is not None:
            self._blacklist_caches[config_key] = (cache_key, parsed)
        return parsed

    def _blacklist_candidates(self, event: AstrMessageEvent) -> set:
        """这条消息能用来命中黑名单的所有写法：群号、会话 ID、完整消息来源及其尾段。"""
        candidates = set()

        def _add(value):
            if value is None:
                return
            text = str(value).strip()
            if text:
                candidates.add(text)

        for getter_name in ("get_group_id", "get_session_id"):
            try:
                _add(getattr(event, getter_name)())
            except Exception:
                pass

        try:
            umo = str(event.unified_msg_origin or "").strip()
        except Exception:
            umo = ""
        if umo:
            candidates.add(umo)
            # 允许只写完整来源里的会话段（群号），写全了也一样能命中
            candidates.add(umo.rsplit(":", 1)[-1].strip())

        candidates.discard("")
        return candidates

    def _hit_blacklist(self, event: AstrMessageEvent, config_key: str) -> str:
        """命中则返回命中的那条配置，否则返回空串。"""
        blacklist = self._parse_blacklist(config_key)
        if not blacklist:
            return ""
        hits = self._blacklist_candidates(event) & blacklist
        return sorted(hits)[0] if hits else ""

    def _group_blacklist_hit(self, event: AstrMessageEvent) -> str:
        """主动回复黑名单（群聊专用：私聊不参与比对，避免用户 QQ 号与群号撞号被误伤）。"""
        try:
            if event.is_private_chat():
                return ""
        except Exception:
            pass
        return self._hit_blacklist(event, "group_blacklist")

    # 收集当前消息，下文消息并合并到维护的历史中
    # *args/**kwargs 兼容：AstrBot v4.26.5 会把指令解析参数广播给所有已激活 handler，
    # 不加的话 handler 会收到多余参数抛 TypeError
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def process_message(self, event: AstrMessageEvent, *args, **kwargs):
    async def process_message(self, event: AstrMessageEvent, *args, **kwargs):
        if event.get_sender_id() == event.get_self_id():   # 过滤自己
            return

        group_uid = self._session_key(event)
        text_part, img_b64_list = await self.message_and_images(event)
        # 机器人自己的 At 在 message_and_images 里生成的是 "@名字[ID]" 格式，
        # 其他人的 At 是 "[@名字]"，两种都要剥掉，空消息/指令判断才准确
        bot_at_text = f"@{self.BOT_NAME}[{event.get_self_id()}]"
        substantial_text = re.sub(
            r'\[@.*?\]', '', text_part.replace(bot_at_text, '')
        ).strip()

        force_reply = False
        at_bot = False          # 只统计"@了机器人自己"；引用机器人不算（两者触发的是同一个强制回复）
        msg_chain = event.message_obj.message
        if msg_chain:
            for comp in msg_chain:
                # 情况1：消息里 @了机器人自己
                if isinstance(comp, At):
                    if str(comp.qq) == str(event.get_self_id()):
                        force_reply = True
                        at_bot = True
                        logger.debug("检测到被@机器人，跳过判定直接回复")
                        break

                # 情况2：消息引用了机器人自己之前发的消息
                elif isinstance(comp, Reply):
                    # sender_id 是被引用消息的发送者
                    if str(comp.sender_id) == str(event.get_self_id()):
                        force_reply = True
                        logger.debug("检测到引用机器人消息，跳过判定直接回复")
                        break

        # 基于提取后的文本判断指令（必须用和 message_and_images 一致的 At 格式剔除，
        # 旧版写的是 "[@bot名/ID]"，和实际生成的 "@bot名[ID]" 对不上，永远替换不到）
        clean_text = text_part.replace(bot_at_text, "")
        clean_text = re.sub(r'\[@.*?\]', '', clean_text).strip()
        if clean_text.startswith(self.COMMAND) or event.message_str.startswith(self.COMMAND):
            logger.debug(f"[轨迹] 命中命令屏蔽符，跳过: {event.message_str[:40]!r}")
            return

        # 上面剥掉了机器人自己的 At，纯 @机器人 的消息 substantial_text 会变空，
        # 所以这里要放行 force_reply，否则 bare @机器人 会被误丢掉
        if not substantial_text and not img_b64_list and not force_reply:
            logger.debug("[轨迹] 空消息（无文本、无图、且没@机器人），跳过")
            return

        # ---- 自适应等待：先量出"距上一条群友消息多久"，再把记录更新到此刻 ----
        # 只算群友的消息（机器人自己发的不算，否则它刚回完一句，下一条一定被判成"热聊"）。
        # 命令屏蔽符挡掉的消息和空消息在上面已经 return，不会计入。
        # 冷启动（还没有记录）时按基准值 S 处理。这份记录只存在内存，不持久化。
        base = self._sleep_base()
        now = time.time()
        prev_ts = self.last_friend_ts.get(group_uid)
        gap = (now - prev_ts) if prev_ts is not None else None
        self.last_friend_ts[group_uid] = now
        wait = self._calc_wait(gap, at_bot=at_bot)
        if at_bot:
            wait_desc = "被@，强制减半"
        elif gap is None:
            wait_desc = "冷启动"
        else:
            wait_desc = f"静默 {gap:.1f}s"

        current_message = f"{event.get_sender_name()}[{event.get_sender_id()}]:{text_part}"
        # 把 base64 追加在消息后面，用换行分隔
        for b64 in img_b64_list:
            current_message += f"\n[IMG_B64:{b64}]"
        current_time = self.simple_time(event.created_at)    # 解析时间戳
        log_msg = re.sub(r"\[IMG_B64:[A-Za-z0-9+/=]+\]", "[图片]", current_message)
        logger.debug(f"[轨迹] 触发消息: {log_msg}")

        # 给仅图片注入模式的 on_llm_request 钩子用的当前消息信息
        event.set_extra("_ar_current_text", text_part)
        event.set_extra("_ar_current_images", img_b64_list)
        event.set_extra("_ar_current_full", current_message)

        # 这一轮消息的 message_id：模型可以用 [replyN] 引用"当前消息"里的第 N 条。
        # 编号只在本轮内有效，轮次冻结之后不再变化。
        message_id = str(getattr(getattr(event, "message_obj", None), "message_id", "") or "")

        # 群聊黑名单提前算出来：下面的日志与分支都要按它和 force_reply 决定走向
        blacklist_hit = self._group_blacklist_hit(event)
        if blacklist_hit:
            logger.debug(
                f"群 {group_uid} 命中主动回复黑名单（{blacklist_hit}）"
                + (
                    "，但被@或引用机器人，仍由本插件强制回复"
                    if force_reply
                    else "，跳过主动回复判定，记忆与图片注入照常"
                )
            )

        if not self.ACTIVE_REPLY_ENABLED or (blacklist_hit and not force_reply):
            # 仅图片注入模式 / 黑名单群里的普通消息：只维护群聊流水账，不等待、不判定、
            # 不主动回复，也不 stop_event，后续交给平台/其他插件正常处理。
            # 黑名单只关掉"要不要回复"的判定；被@或引用机器人时不算主动回复，
            # 上面的条件会放它走下面那条强制回复的路。
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
                self._dirty.add(group_uid)
            logger.debug(
                f"[轨迹] 本轮不接管主动回复（模式={self.ACTIVE_REPLY_ENABLED} "
                f"黑名单={blacklist_hit or '无'}），只记账不发消息"
            )
            return

        async with self.lock:        # 初始化
            self._ensure_group_state(group_uid)

            # 后文消息处理
            if self.is_waiting[group_uid]:  # 如果是后文就加入当前的消息
                self.pending[group_uid].append(current_message)
                self.pending_ids.setdefault(group_uid, []).append(message_id)
                self._dirty.add(group_uid)
                if self.ADAPTIVE_SLEEP:
                    # 还在来消息就重新算截止时刻：只往后推、不提前（新消息不该让 bot 更早开口），
                    # 并且整轮总时长封顶 2S —— 否则热闹的群会一直续命、永远等不到头
                    t0 = self._round_t0.get(group_uid, now)
                    self._round_until[group_uid] = min(
                        t0 + 2.0 * base,
                        max(self._round_until.get(group_uid, 0.0), now + wait),
                    )
                # 必须停掉事件！这条消息已经被并进当前这一轮了，若不停，框架会继续往下走到
                # 原生 LLM 阶段（process_stage 里紧跟 star_request 的那段 agent 调用），
                # 于是同一句话被回两次。被@的消息尤其明显：is_at_or_wake_command 为真，
                # 那段原生调用必然执行 —— 这就是"被@一次、回两次"的成因。
                event.stop_event()
                logger.debug(
                    f"[轨迹] 本轮已在收集窗口内，作为后文并入上一轮（{wait_desc}）"
                )
                return

            self.is_waiting[group_uid] = True   # 把触发的当前消息放入current_msg，然后设置is waiting为true
            self.current_msg[group_uid] = current_message
            self.current_id[group_uid] = message_id
            self._round_t0[group_uid] = now
            self._round_until[group_uid] = now + wait

            if self.last_time[group_uid] != current_time:
                self.history[group_uid].append(f"[{current_time}]")
                self.last_time[group_uid] = current_time
                self._dirty.add(group_uid)

        logger.info(
            f"[轨迹] 本轮等待约 {wait:.1f}s（{wait_desc}，基准 {base:.1f}s）"
        )

        discarded = False
        try:
            # 到点就再看一眼软截止时刻：等待期间来过后文消息的话它已经被顺延了
            while True:
                remaining = self._round_until.get(group_uid, 0.0) - time.time()
                if remaining <= 0:
                    break
                await asyncio.sleep(remaining)
        finally:
            # 收尾必须放在 finally：若 sleep 期间任务被取消（插件热重载、关机等），
            # is_waiting 不复位的话会永远卡在 True，之后该群所有消息只会进 pending，
            # 再也不会触发判定，表现为这个群永久沉默
            async with self.lock:
                # 这一轮是否在收集期间被 /clear_history 清掉了；是的话不能再入账
                discarded = group_uid in self._discard_round
                self._discard_round.discard(group_uid)

                # 本轮所有用户消息（不带编号的版本，进历史、给判定用）
                round_msgs = [self.current_msg[group_uid]]
                round_msgs.extend(self.pending[group_uid])
                round_ids = [self.current_id.get(group_uid, "")]
                round_ids.extend(self.pending_ids.get(group_uid, []))

                self.is_waiting[group_uid] = False
                self.pending[group_uid] = []
                self.current_msg[group_uid] = ""
                self.pending_ids[group_uid] = []
                self.current_id[group_uid] = ""
                # 本轮结束，清掉等待计划；last_friend_ts 保留（那是群里的说话记录，不属于本轮）
                self._round_t0.pop(group_uid, None)
                self._round_until.pop(group_uid, None)

                history_text = "\n".join(self.history[group_uid])
                current_text = "\n".join(round_msgs)
                # 给回复 prompt 的版本：每句话前面加编号，模型用 [replyN] 引用第 N 条。
                # 历史里存的是不带编号的 round_msgs，所以记忆格式不受影响。
                if self.QUOTE_ENABLE and any(round_ids):
                    current_numbered = "\n".join(
                        f"{i}. {m}" for i, m in enumerate(round_msgs, 1)
                    )
                else:
                    current_numbered = current_text
                if not discarded:
                    event.set_extra("_my_active_reply", True)
                    event.set_extra("_ar_history", history_text)
                    event.set_extra("_ar_current", current_numbered)
                    # 编号 -> message_id：序号就是本轮消息的顺序（从 1 开始），发送阶段直接按序号取
                    event.set_extra("_ar_round_ids", list(round_ids))

        if discarded:
            # 刚被清空的历史不能被这一轮重新写回来：不判定、不回复、不记账
            logger.debug(
                f"[清空历史] 会话 {group_uid} 有正在收集的轮次，已作废（不判定不记账）"
            )
            return

        if force_reply:
            should_reply = True
            reply_reason = "被@或引用，强制回复"
        else:
            should_reply, reply_reason = await self._reply(group_uid, history_text, current_text)

        if not should_reply:
            logger.debug(
                f"[轨迹] 判定 SKIP，不回复。理由: {reply_reason[:100] if reply_reason else '(无)'}"
            )
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
        history_saved = False
        try:
            # 关键：先把本轮记进流水账，再发起请求。
            # yield 之后框架才真正跑 agent（工具循环也发生在那里）；一旦那一侧报错或被中止，
            # 生成器会被直接丢弃 —— yield 之后的代码永远不会执行，本轮消息就从记忆里消失了
            # （表现为"bot 完全不记得刚才那句话"）。所以必须"先记账、再请求"。
            async with self.lock:
                self._append_history(group_uid, round_msgs)
            history_saved = True

            curr_cid = await self.context.conversation_manager.get_curr_conversation_id(
                event.unified_msg_origin
            )
            conv = None
            if curr_cid:
                conv = await self.context.conversation_manager.get_conversation(
                    event.unified_msg_origin, curr_cid
                )
            if conv is None:
                # 没有"当前对话"（或它已被删除）时必须自己建一个：本插件接管了本群所有消息，
                # 框架的原生链路不会再走到，也就没机会创建对话。原实现这里直接 stop_event
                # 静默返回 —— 一旦指针丢失，本群会永久沉默（什么都不回，日志也查不到原因）。
                old_cid = curr_cid
                curr_cid = await self.context.conversation_manager.new_conversation(
                    event.unified_msg_origin, event.get_platform_id()
                )
                conv = await self.context.conversation_manager.get_conversation(
                    event.unified_msg_origin, curr_cid
                )
                logger.warning(
                    f"当前会话没有可用对话（原 cid={old_cid}），已新建对话 {curr_cid} "
                    f"umo={event.unified_msg_origin}"
                )
            logger.debug(
                f"[主动回复] 发起 LLM 请求: umo={event.unified_msg_origin} cid={curr_cid} "
                f"force_reply={force_reply} 本轮 {len(round_msgs)} 条"
            )
            yield event.request_llm(
                prompt="placeholder",  # 会被 on_llm_request 钩子覆盖
                session_id=event.session_id,
                conversation=conv,
            )
            # yield 返回说明框架已处理完 LLM 请求（本轮历史已在上面记好，不再重复记）
            logger.debug("[主动回复] LLM 请求已返回")
        except Exception as exc:
            # 任何异常都必须留下痕迹，否则表现为"发消息没反应、日志也没有"
            logger.error(f"[主动回复] 回复流程异常: {exc}", exc_info=True)
            if not history_saved:
                async with self.lock:
                    self._append_history(group_uid, round_msgs)

    @filter.after_message_sent()
    async def process_bot_message(self, event: AstrMessageEvent, *args, **kwargs):
        group_uid = self._session_key(event)
        result = event.get_result()
        if result is None or not getattr(result, "chain", None):
            # 结果已被清空（分段发送后 clear_result、或本轮模型没有输出），没有可记账的内容
            return
        if event.get_extra("_ar_skip_record", False):
            # 清空历史等管理命令的回复本身不该被记进流水账
            return
        text = result.chain
        bot_name = self.BOT_NAME
        bot_message_str = ''.join([getattr(comp, 'text', '') for comp in text if isinstance(comp, Plain)])
        bot_message = f"{bot_name}(你自己)[{event.get_self_id()}]:{bot_message_str}"
        async with self.lock:
            self._ensure_group_state(group_uid)
            self.history[group_uid].append(bot_message)
            if len(self.history[group_uid]) > self.MAX_HISTORY:
                # 统一用切片删除，避免 O(n) pop(0)
                del self.history[group_uid][:len(self.history[group_uid]) - self.MAX_HISTORY]
            self._dirty.add(group_uid)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("clear_history", alias={"清空历史", "清空记忆"})
    async def clear_history(self, event: AstrMessageEvent):
        """清空当前会话的聊天记忆（内存里的流水账和落盘文件一起清掉）。

        只清 history / last_time。pending、is_waiting、current_* 属于"正在收集的那一轮"，
        直接抹掉会让还在 sleep 里的那次 process_message 收尾时读到空值、往新历史里塞一行空记录；
        所以改成给那一轮打一个"作废"标记，让它在收尾时自己放弃判定和记账。

        注意：这里**不能**写 *args/**kwargs。CommandFilter 会把 self、event 之后的每个参数
        都当成指令参数，varargs 会被登记成名为 args/kwargs 的必填参数，用户不带参数就必报
        "必要参数缺失"。事件钩子才需要 *args/**kwargs 去吞掉框架广播的多余参数。

        Args:
            event: 当前消息事件。

        Yields:
            一条确认回复。
        """
        session_key = self._session_key(event)
        async with self.lock:
            self._ensure_group_state(session_key)
            self.history[session_key] = []
            self.last_time[session_key] = ""
            self._dirty.discard(session_key)
            if self.is_waiting.get(session_key):
                # 有一轮正在收集：作废它，否则几秒后它会把自己写回刚清空的历史
                self._discard_round.add(session_key)
        self._remove_history_file(session_key)
        # 这条确认回复本身不该被记进刚清空的流水账
        event.set_extra("_ar_skip_record", True)
        logger.info(f"[清空历史] 已清空 {session_key} 的聊天记忆")
        yield event.plain_result("这个会话的聊天记忆已经清空了。")

    # ==================================================================
    # 消息分段 + 模型自主引用
    # ==================================================================
    # 引用编号只作用于"当前这一轮"的消息（见 process_message 里轮次冻结那段）：
    # 编号 1..N 就是本轮消息的顺序，轮次一冻结就定死，所以这里不需要任何快照或循环编号，
    # 直接把这一轮的 message_id 列表按序号取即可。
    @staticmethod
    def _flatten_chain(chain: List[Any]) -> Tuple[str, Dict[int, Tuple[int, int]], Dict[int, int]]:
        """把消息链摊平成一段纯文本，并记录每个消息段在文本里的位置。

        返回 (全文, {Plain 索引: (起, 止)}, {非 Plain 索引: 位置})
        """
        texts: List[str] = []
        plain_spans: Dict[int, Tuple[int, int]] = {}
        comp_offsets: Dict[int, int] = {}
        pos = 0
        for index, comp in enumerate(chain):
            if isinstance(comp, Plain):
                text = comp.text or ""
                plain_spans[index] = (pos, pos + len(text))
                texts.append(text)
                pos += len(text)
            else:
                comp_offsets[index] = pos
        return "".join(texts), plain_spans, comp_offsets

    def plan_reply_blocks(
        self,
        chain: List[Any],
        allow_split: bool,
        max_segments: Any,
        quote_map: Dict[int, str],
        split_regex: str,
    ) -> List[dict]:
        """把一条回复规划成若干"消息块"，每块最多带一个引用。

        规则：
          - 按 split_regex（默认换行）切分，最多切 max_segments-1 刀；再多的分隔符不再切，
            剩下的内容全部并进最后一段（原来的换行原样保留）。max_segments <= 0 表示不限制。
          - 每个**有效**的 [replyN] 标记强制开新块，不受刀数限制（引用优先于分段上限）。
          - 无效编号（越界/清单里没有）只把标记删掉、文字保留，不强制分块。
          - 不分段时（开关关闭 / 命中黑名单）完全不切，只认模型写的第一个有效引用，
            其余标记一律删掉。
          - 链上原有的 Reply 组件丢弃，引用一律由标记重新生成。
          - 图片/@等非文本组件不参与切分，跟着它所在的那一块走。
        """
        full, plain_spans, comp_offsets = self._flatten_chain(chain)

        # 先找出所有引用标记；msg_id 为 None 表示编号无效
        markers: List[Tuple[int, int, Optional[str]]] = []
        for m in self.QUOTE_TAG_RE.finditer(full):
            try:
                index = int(m.group(1))
            except Exception:
                continue
            markers.append((m.start(), m.end(), quote_map.get(index)))

        try:
            max_seg = int(max_segments)
        except Exception:
            max_seg = 3

        cuts = set()
        if allow_split:
            try:
                sep_re = re.compile(split_regex)
            except re.error:
                logger.warning(f"[分段] 分段正则不合法，回退到换行: {split_regex!r}")
                sep_re = re.compile(r"[\n]+")
            honored = 0
            limit = (max_seg - 1) if max_seg > 0 else 10 ** 9
            for m in sep_re.finditer(full):
                if m.end() <= m.start():
                    continue
                if honored >= limit:
                    break
                cuts.add(m.end())
                honored += 1
            # 有效引用标记强制分块（不占上面的刀数）
            for start, _end, msg_id in markers:
                if msg_id:
                    cuts.add(start)

        starts = [0] + sorted(c for c in cuts if c > 0)
        blocks: List[dict] = []
        for i, start in enumerate(starts):
            end = starts[i + 1] if i + 1 < len(starts) else len(full)
            blocks.append({"start": start, "end": end, "reply_id": None})

        marker_at: Dict[int, Tuple[int, Optional[str]]] = {}
        for start, end, msg_id in markers:
            marker_at.setdefault(start, (end, msg_id))
        for blk in blocks:
            info = marker_at.get(blk["start"])
            if info and info[1]:
                blk["reply_id"] = info[1]
        if not allow_split:
            first_valid = next((m for m in markers if m[2]), None)
            if first_valid:
                blocks[0]["reply_id"] = first_valid[2]

        strip_ranges = [(s, e) for (s, e, _mid) in markers]

        def _text_of(text_start: int, text_end: int) -> str:
            """取出 [text_start, text_end) 的内容，并删掉其中的引用标记。"""
            if text_end <= text_start:
                return ""
            parts: List[str] = []
            cursor = text_start
            for s, e in strip_ranges:
                if e <= cursor or s >= text_end:
                    continue
                s2, e2 = max(s, text_start), min(e, text_end)
                if s2 > cursor:
                    parts.append(full[cursor:s2])
                cursor = max(cursor, e2)
            if cursor < text_end:
                parts.append(full[cursor:text_end])
            return "".join(parts)

        def _block_of(pos: int) -> int:
            for i, blk in enumerate(blocks):
                if blk["start"] <= pos < blk["end"]:
                    return i
            return len(blocks) - 1

        block_chains: List[List[Any]] = [[] for _ in blocks]
        for index, comp in enumerate(chain):
            if isinstance(comp, Reply):
                continue
            if isinstance(comp, Plain):
                cstart, cend = plain_spans[index]
                for bi, blk in enumerate(blocks):
                    s, e = max(blk["start"], cstart), min(blk["end"], cend)
                    if e <= s:
                        continue
                    piece = _text_of(s, e)
                    if piece:
                        block_chains[bi].append(Plain(piece))
            else:
                block_chains[_block_of(comp_offsets[index])].append(comp)

        result: List[dict] = []
        for bi, comps in enumerate(block_chains):
            first_plain = next((c for c in comps if isinstance(c, Plain) and c.text), None)
            last_plain = next((c for c in reversed(comps) if isinstance(c, Plain) and c.text), None)
            if first_plain is not None:
                first_plain.text = re.sub(r"^[\s\u200b]+", "", first_plain.text)
            if last_plain is not None:
                last_plain.text = re.sub(r"[\s\u200b]+$", "", last_plain.text)
            comps = [c for c in comps if not (isinstance(c, Plain) and not (c.text or "").strip())]
            if not comps:
                continue
            result.append({"reply_id": blocks[bi]["reply_id"], "chain": comps})
        return result

    async def _record_bot_reply(self, event: AstrMessageEvent, texts: List[str]) -> None:
        """自己发送之后补记账：把这一轮 bot 说过的话**合成一行**写进流水账。

        必须补：context.send_message 绕过了 RespondStage，after_message_sent
        （process_bot_message）不会被触发，不补的话 bot 说过的话就永远不进记忆。
        """
        joined = " ".join(t.strip() for t in texts if t and t.strip())
        if not joined:
            # 只发了图片之类、没抓到文字：干脆不记，避免流水账里出现空行
            return
        session_uid = self._session_key(event)
        line = f"{self.BOT_NAME}(你自己)[{event.get_self_id()}]:{joined}"
        async with self.lock:
            self._ensure_group_state(session_uid)
            self.history[session_uid].append(line)
            if len(self.history[session_uid]) > self.MAX_HISTORY:
                del self.history[session_uid][: len(self.history[session_uid]) - self.MAX_HISTORY]
            self._dirty.add(session_uid)

    # 优先级 -50：必须排在 SelfDel(100，负责跳过/删除标记) 与 attool(2，把 [at:ID] 转成
    # 真正的 At 组件) 之后，否则会把字面量标签发出去、或者把已经判定"不发送"的消息又发一遍。
    @filter.on_decorating_result(priority=-50)
    async def send_segmented_reply(self, event: AstrMessageEvent, *args, **kwargs):
        try:
            if not self.ACTIVE_REPLY_ENABLED:
                return
            # 只接管"本插件判定出来的回复"；仅图片注入模式下回复由别的插件负责，一律不碰
            if not event.get_extra("_my_active_reply", False):
                return
            if event.is_stopped():
                # 最常见来源是"判定 SKIP 后 stop_event"（模型决定不回复，属正常静默）
                logger.debug("[分段/引用] 本轮事件已停止（多为判定 SKIP 或被他处停掉），不发消息")
                return
            result = event.get_result()
            # 流式输出时正文已经一段段发出去了，这里再改也来不及。必须先于"空结果"判断：
            # 流式结果的 chain 本身就是空的（正文在 async_stream 里），先判空会把它误当成
            # "模型没输出"，每个流式回合都刷一条吓人的 WARNING。
            content_type = getattr(result, "result_content_type", None) if result else None
            type_name = (getattr(content_type, "name", "") or str(content_type)).upper()
            if "STREAMING" in type_name:
                return
            if result is None or not getattr(result, "chain", None):
                # 本轮由本插件接管、判定通过要回复，结果却是空的：说明模型没有输出可发送
                # 内容（或已被别的插件清空）。必须留日志，否则表现为"什么都不发、也查不到原因"
                logger.warning(
                    "[分段/引用] 本轮结果为空，模型没有输出可发送内容"
                    f"（result={'None' if result is None else '空链'}）"
                )
                return

            is_private = bool(event.is_private_chat())
            allow_split = self.SEGMENT_ENABLE_PRIVATE if is_private else self.SEGMENT_ENABLE_GROUP
            if self._hit_blacklist(event, "segment_blacklist"):
                allow_split = False
            max_segments = self.SEGMENT_MAX_PRIVATE if is_private else self.SEGMENT_MAX_GROUP

            # 编号 -> message_id：编号就是"当前这一轮消息"的序号（从 1 开始）。
            # 轮次在本轮 sleep 结束时已冻结，所以这个顺序不会再变。
            round_ids = event.get_extra("_ar_round_ids", []) or []
            if not isinstance(round_ids, (list, tuple)):
                round_ids = []
            quote_map = {i: mid for i, mid in enumerate(round_ids, 1) if mid}

            blocks = self.plan_reply_blocks(
                result.chain, allow_split, max_segments, quote_map, self.SEGMENT_REGEX
            )
            if not blocks:
                # 这里没有任何可发送的内容。绝大多数情况不是"模型真的没说话"，而是
                # **工具调用的中间轮**：这一轮模型只返回了 tool_calls，正文是空字符串，
                # 框架把它包成 [Plain("")] —— 链非空、但一个字都没有，于是走到这里。
                #
                # 关键：本钩子运行在 agent 的工具循环内部（框架每 yield 一次中间结果，
                # 都会重跑一遍结果装饰阶段）。此处一旦 stop_event，调度器会立刻中断整个
                # agent 生成器、工具循环被当成"用户中止"结束，后面那次真正带正文的回复
                # 就永远不会发生 —— 表现为"模型一用工具就什么都不回，而且不报错"。
                # 所以只清掉这一条空结果（框架因此不会发空消息），让 agent 继续跑。
                logger.debug(
                    "[分段/引用] 本轮结果没有可发送内容（多为工具调用中间轮），跳过不发。"
                    f"原始 chain={[(type(c).__name__, repr(getattr(c, 'text', None))) for c in result.chain]}"
                )
                event.clear_result()
                return

            if len(blocks) == 1:
                # 单条：只把引用组件插进去，发送仍然交给框架（after_message_sent 照常触发）
                only = blocks[0]
                result.chain = ([Reply(id=only["reply_id"])] if only["reply_id"] else []) + list(only["chain"])
                if only["reply_id"]:
                    logger.debug(f"[自主引用] 单条回复，引用消息 {only['reply_id']}")
                return

            logger.debug(f"[分段/引用] 拆成 {len(blocks)} 条发送（分段开关={allow_split}）")
            texts: List[str] = []
            for i, blk in enumerate(blocks):
                chain_list = ([Reply(id=blk["reply_id"])] if blk["reply_id"] else []) + list(blk["chain"])
                mc = MessageChain()
                mc.chain = chain_list
                sent = await self.context.send_message(event.unified_msg_origin, mc)
                if not sent:
                    logger.warning(
                        f"[分段/引用] 第 {i + 1} 条没有匹配到平台，未真正发出"
                        f"（umo={event.unified_msg_origin}）"
                    )
                texts.append("".join(c.text for c in chain_list if isinstance(c, Plain)))
                if self.SEGMENT_DELAY and i < len(blocks) - 1:
                    try:
                        await asyncio.sleep(max(0.0, float(self.SEGMENT_DELAY)))
                    except Exception:
                        pass
            await self._record_bot_reply(event, texts)
            # 清空结果就足以让框架不再重复发送原始消息（RespondStage 见到 result is None 直接返回），
            # 不需要 stop_event。原因同上：本钩子可能在 agent 的工具循环里被调用（比如模型先
            # 说一句"我查一下"、同一轮再发起工具调用），此时停事件会把工具循环打断，
            # 真正的回答就永远不来了。
            event.clear_result()
        except Exception as exc:
            logger.error(f"[分段/引用] 处理失败，按原样交给框架发送: {exc}")

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
            # 提示词整段打出来很占地方（几十行），所以由 log_prompt 开关控制；
            # 关闭时连 debug 都不打，避免刷屏。用 INFO 是因为调提示词时要一眼看到。
            if self.LOG_PROMPT:
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

            # 判定结论常开（INFO）：这是"群里为什么不回"最直接的答案
            logger.info(
                f"[轨迹] 判定结果: {'REPLY' if should_reply else 'SKIP'}, "
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

        group_uid = self._session_key(event)
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
            logger.debug(
                f"仅图片注入模式：当前消息无图，回退取历史上最近 "
                f"{len(current_images)} 张图片"
            )
        # 只注入图片，不碰 req.prompt / req.contexts：上下文（历史、记忆、工具）
        # 由 group_chat_plus 负责拼装，本插件再追加一份群聊流水账只会让同一段
        # 对话在 prompt 里出现两遍。需要由本插件接管上下文的场景（例如没装
        # group_chat_plus），把 active_reply_enabled 打开即可——那时
        # save_in_history 会走另一条分支，用流水账自行构造 req.prompt 并判定
        # REPLY/SKIP，主动回复能力完整保留。
        # 同样显式带真实 MIME（原来硬编码 image/jpeg，PNG/GIF 会被官方 API 判为不支持）
        extra = [
            f"data:{self._detect_image_mime_from_b64(b64)};base64,{b64}"
            for b64 in current_images
        ]
        existing = list(getattr(req, "image_urls", None) or [])
        req.image_urls = existing + extra
        logger.debug(f"仅图片注入模式：注入 {len(current_images)} 张图片（流水账交由 chat_plus）")

    # 优先级必须低于其他插件的默认优先级 0（例如 group_chat_plus 是 -1、
    # gitee_aiimg 是 -20）：这些插件的 on_llm_request 会覆盖 req.prompt /
    # req.image_urls，默认优先级 0 会先执行、注入的图片随即被覆盖掉，等于白注入。
    # *args/**kwargs 兼容：AstrBot v4.26.5 会把指令解析参数广播给所有已激活 handler
    @filter.on_llm_request(priority=-30)
    async def save_in_history(self, event: AstrMessageEvent, req: ProviderRequest, *args, **kwargs):
        # _my_active_reply 只在 process_message 里"本轮由本插件接管回复"的分支上设置：
        # 主动回复判定通过，或者（黑名单群里）被@/引用而强制回复——这两种都必须接管上下文。
        # 注意黑名单群里被@也属于这一种，若在这里按黑名单直接转去 _inject_image_only，
        # request_llm 里那个占位 prompt="placeholder" 就会原样发给模型。
        if not event.get_extra("_my_active_reply", False):
            # 仅图片注入模式，或黑名单群里本轮不由本插件回复：只补图片，上下文交给其他插件
            if not self.ACTIVE_REPLY_ENABLED or self._group_blacklist_hit(event):
                await self._inject_image_only(event, req)
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
当前消息：
{current_clean}"""

        # 主动回复模式由本插件完全接管对话上下文。conversation 仍用于加载人格、
        # Skills 和工具，但它自动带入的历史必须清空，否则会与 self.history 生成的
        # 累计流水账重复，并在每轮保存后继续膨胀。
        req.contexts = []
        req.prompt = chat_log

        # 如果有判定理由，追加给回复 AI 作为参考；末尾追加恒定提示，
        # 防止模型把这段判定理由当成需要回复的群聊内容
        if reason:
            chat_log += (
                f"\n\n（你决定回复的理由：{reason}）\n"
                "（以上说明了你为什么要回复以及回复谁）"
            )

        # 模型自主引用：只给"当前消息"里的每句话编号（历史不带编号），
        # 这里补一句说明告诉模型编号怎么用。编号在 process_message 冻结轮次时已写进
        # _ar_current，对应的 message_id 列表也在事件上，发送阶段按序号取。
        if self.QUOTE_ENABLE:
            try:
                if not event.is_private_chat():
                    round_ids = event.get_extra("_ar_round_ids", []) or []
                    if any(round_ids):
                        chat_log += (
                            "\n\n（当前消息里每句话前面的数字就是它的编号。要引用其中某一条时，"
                            "在那一句的开头写 [replyN]，N 用那个编号，一句最多写一个；"
                            "写了引用的句子会单独作为一条消息发出。不想引用就别写。）"
                        )
            except Exception as exc:
                logger.warning(f"[自主引用] 注入引用说明失败: {exc}")

        # 主动回复模式下上下文由本插件全权接管：清掉 conversation，防止
        # build_main_agent 用原生会话历史覆盖 contexts（astr_main_agent.py 中
        # if req.conversation: req.contexts = json.loads(req.conversation.history)），
        # 也避免整份流水账被反复存进原生记忆造成跨轮重复膨胀
        req.conversation = None

        if images:
            logger.debug(f"[save_in_history] 注入 {len(images)} 张图片到请求中")
            for idx, b64 in enumerate(images):
                logger.debug(f"[save_in_history] 第 {idx+1} 张图片 base64 len={len(b64)}")
        else:
            # 没图时走纯文本
            logger.debug("[save_in_history] 无图片，走纯文本 prompt")

        # 文本放 prompt，图片走框架的 image_urls 通道（base64:// 是框架明确支持的格式），
        # 框架会拼成一条 [文本, 图片...] 的多模态 user 消息，群聊格式不变。
        # 不能像旧版那样把 prompt 清成 ""：框架对任何非 None 的 prompt 都会追加一条
        # user 消息，空 prompt 会生成 content 为空数组的消息，被 API 400 拒绝
        # （"the message at position 2 with role 'user' must not be empty"）；
        # 且 prompt 为空且无媒体时 build_main_agent 会直接丢弃整个请求
        req.contexts = []
        req.prompt = chat_log
        # 明写 data:<真实MIME>;base64,…：官方 DeepSeek API 对 data URI 的 MIME 很严格，
        # 用 base64:// 由框架兜底转换时，一旦 MIME 缺失/变成 octet-stream 就会整条 400。
        req.image_urls = [
            f"data:{self._detect_image_mime_from_b64(b64)};base64,{b64}"
            for b64 in images
        ]

    # 诊断：记录本插件接管轮次里模型实际返回了什么（SelfDel 关掉后也能定位问题）。
    # 只看 _my_active_reply 的轮次，避免刷屏。
    @filter.on_llm_response()
    async def log_llm_response(self, event: AstrMessageEvent, response, *args, **kwargs):
        if not event.get_extra("_my_active_reply", False):
            return
        try:
            text = getattr(response, "completion_text", None) or ""
            reasoning = getattr(response, "reasoning_content", None) or ""
            tool_names = getattr(response, "tools_call_name", None) or []
            logger.debug(
                f"[主动回复][响应] role={getattr(response, 'role', None)} "
                f"chunk={getattr(response, 'is_chunk', None)} "
                f"正文={len(text)}字 reasoning={len(reasoning)}字 "
                f"工具调用={list(tool_names)} usage={getattr(response, 'usage', None)} "
                f"正文预览={text[:120]!r}"
            )
        except Exception as exc:
            logger.warning(f"[主动回复][响应] 记录失败: {exc}")

    def _compress_history_images(self, group_uid: str, max_keep: int = None):
        """
        扫描历史记录，只保留最新的 max_keep 个 [IMG_B64:...]，
        其余替换为 [图片]，避免历史中的 base64 持续膨胀。
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
            logger.debug(f"历史图片 base64 已降级: group={group_uid[:20]}... line={idx}")
