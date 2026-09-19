import asyncio
import importlib.util
import logging
import sys
import types
from pathlib import Path
from types import SimpleNamespace


def _install_astrbot_stubs():
    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")
    event = types.ModuleType("astrbot.api.event")
    star = types.ModuleType("astrbot.api.star")
    provider = types.ModuleType("astrbot.api.provider")
    components = types.ModuleType("astrbot.api.message_components")
    core = types.ModuleType("astrbot.core")
    conversation_mgr = types.ModuleType("astrbot.core.conversation_mgr")

    class _Filter:
        class EventMessageType:
            GROUP_MESSAGE = object()

        @staticmethod
        def _decorator(*args, **kwargs):
            return lambda func: func

        event_message_type = _decorator
        after_message_sent = _decorator
        on_llm_request = _decorator

    class Star:
        def __init__(self, context):
            self.context = context

    class ProviderRequest:
        def __init__(self, prompt="", contexts=None, image_urls=None):
            self.prompt = prompt
            self.contexts = list(contexts or [])
            self.image_urls = list(image_urls or [])

    class MessageComponent:
        pass

    event.filter = _Filter
    event.AstrMessageEvent = object
    event.MessageEventResult = object
    star.Context = object
    star.Star = Star
    star.register = lambda *args, **kwargs: lambda cls: cls
    provider.ProviderRequest = ProviderRequest
    provider.LLMResponse = object
    components.Plain = type("Plain", (MessageComponent,), {})
    components.Image = type("Image", (MessageComponent,), {})
    components.At = type("At", (MessageComponent,), {})
    components.Reply = type("Reply", (MessageComponent,), {})
    api.AstrBotConfig = dict
    api.logger = logging.getLogger("active_reply_test")
    conversation_mgr.Conversation = object

    modules = {
        "aiohttp": types.ModuleType("aiohttp"),
        "astrbot": astrbot,
        "astrbot.api": api,
        "astrbot.api.event": event,
        "astrbot.api.star": star,
        "astrbot.api.provider": provider,
        "astrbot.api.message_components": components,
        "astrbot.core": core,
        "astrbot.core.conversation_mgr": conversation_mgr,
    }
    sys.modules.update(modules)
    return ProviderRequest


ProviderRequest = _install_astrbot_stubs()
MODULE_PATH = Path(__file__).resolve().parents[1] / "main.py"
SPEC = importlib.util.spec_from_file_location("active_reply_main_test", MODULE_PATH)
PLUGIN_MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PLUGIN_MODULE)
MyPlugin = PLUGIN_MODULE.MyPlugin


class FakeEvent:
    def __init__(self, extras=None):
        self.extras = dict(extras or {})
        self.session_id = "group-1"

    def get_extra(self, key, default=None):
        return self.extras.get(key, default)

    def is_private_chat(self):
        return False


class AsyncNullLock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def _plugin(active_reply_enabled):
    plugin = MyPlugin.__new__(MyPlugin)
    plugin.ACTIVE_REPLY_ENABLED = active_reply_enabled
    plugin.lock = AsyncNullLock()
    plugin.history = {}
    return plugin


def test_full_takeover_keeps_text_only_round_and_discards_conversation_history():
    plugin = _plugin(True)
    event = FakeEvent(
        {
            "_my_active_reply": True,
            "_ar_history": "[2026/09/20 21:00]\nA:123\nB:[图片]\npoi(你自己):456",
            "_ar_current": "D:789",
        }
    )
    req = ProviderRequest(
        prompt="D:789",
        contexts=[
            {"role": "user", "content": "旧的完整流水账"},
            {"role": "assistant", "content": "456"},
        ],
    )

    asyncio.run(plugin.save_in_history(event, req))

    assert req.contexts == []
    assert "A:123" in req.prompt
    assert "poi(你自己):456" in req.prompt
    assert "当前消息：\nD:789" in req.prompt
    assert "旧的完整流水账" not in req.prompt
    assert req.image_urls == []


def test_full_takeover_adds_images_without_duplicating_existing_attachments():
    plugin = _plugin(True)
    image = "YWJjZA=="
    event = FakeEvent(
        {
            "_my_active_reply": True,
            "_ar_history": "A:之前的消息",
            "_ar_current": f"B:[图片]\n[IMG_B64:{image}]",
        }
    )
    data_url = f"data:image/jpeg;base64,{image}"
    req = ProviderRequest(
        prompt="B:[图片]",
        contexts=[{"role": "user", "content": "旧上下文"}],
        image_urls=[data_url],
    )

    asyncio.run(plugin.save_in_history(event, req))

    assert req.contexts == []
    assert req.prompt.count("你正在群聊里和朋友们聊天。") == 1
    assert req.prompt.count("[图片]") == 1
    assert req.image_urls == [data_url]


def test_image_only_mode_without_image_leaves_context_untouched():
    plugin = _plugin(False)

    async def no_images(event):
        return "纯文本", []

    plugin.message_and_images = no_images
    event = FakeEvent({"_ar_current_images": []})
    original_contexts = [{"role": "user", "content": "外部插件上下文"}]
    req = ProviderRequest(prompt="外部插件提示词", contexts=original_contexts)

    asyncio.run(plugin.save_in_history(event, req))

    assert req.prompt == "外部插件提示词"
    assert req.contexts == original_contexts
    assert req.image_urls == []


def test_image_only_mode_only_appends_image():
    plugin = _plugin(False)
    image = "YWJjZA=="
    event = FakeEvent({"_ar_current_images": [image]})
    original_contexts = [{"role": "user", "content": "外部插件上下文"}]
    req = ProviderRequest(prompt="外部插件提示词", contexts=original_contexts)

    asyncio.run(plugin.save_in_history(event, req))

    assert req.prompt == "外部插件提示词"
    assert req.contexts == original_contexts
    assert req.image_urls == [f"data:image/jpeg;base64,{image}"]
