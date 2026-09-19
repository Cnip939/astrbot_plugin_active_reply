# astrbot-plugin-active-Reply

使用大模型判断的主动回复插件

## 两种工作模式

由配置项 `active_reply_enabled` 选择，两者互斥：

- `true` —— **主动回复模式**：本插件自行判定回复时机，并完全接管本次请求的上下文。
- `false` —— **仅图片注入模式**：回复时机交给负责上下文拼装的插件，本插件只负责把图片补进多模态通道。

如果已经有其他插件或平台机制负责判断何时开口，就把本项设为 `false`，避免重复判定和重复回复。

### 主动回复模式（`active_reply_enabled = true`）

插件等待一段时间后调用大模型判定是否回复；判定通过后会走 AstrBot 原生 pipeline 生成回复，判定不通过时停止当前消息事件，避免重复回复。

该模式下上下文由本插件自己拼装：在 `on_llm_request` 里清空 AstrBot conversation 自动带入的 `req.contexts`，再用唯一一份群聊流水账构造 `req.prompt`。无论本轮是否有图片，文本流水账都会进入模型；图片独立追加到 `req.image_urls`。conversation 仍用于加载人格、Skills 和工具，但不会与插件流水账重复累积。

### 仅图片注入模式（`active_reply_enabled = false`）

插件不再等待消息、发起主动回复判定、调用 `request_llm` 或停止消息事件。它只维护群聊流水账，并在普通图片消息经过 LLM 请求时：

- 把图片 base64 追加到 `req.image_urls`
- 不修改 `req.prompt` / `req.contexts`：历史、记忆、工具都由负责上下文的插件拼装，本插件再追加一份群聊流水账只会让同一段对话在 prompt 里出现两遍
- 不替换其他插件注入的 `contexts` / `system_prompt` / 已有 `image_urls`

「先发图、再 @ 机器人」时，如果合并消息的插件把两条消息并进同一个事件，触发 LLM 的那个事件自身可能没有图片。此时插件会从自己维护的群聊历史里回退取最近两张图片注入，保证模型仍能看到图。

平台判定为表情包/贴纸的图片（`sub_type=1`、`summary` 含表情关键词等）会被跳过，不参与图片注入。
