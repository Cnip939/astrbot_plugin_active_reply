# astrbot-plugin-active-Reply

使用大模型判断的主动回复插件

## 主动回复

默认开启主动回复判定。判定通过后会走 AstrBot 原生 pipeline 生成回复；判定不通过时会停止当前消息事件，避免重复回复。

## 仅图片注入模式

将配置项 `active_reply_enabled` 设为 `false` 后，插件不再等待消息、发起主动回复判定、调用 `request_llm` 或停止消息事件。它只维护群聊流水账，并在普通图片消息经过 LLM 请求时：

- 把图片 base64 追加到 `req.image_urls`
- 把最近的群聊流水账合并进 `req.prompt`
- 不替换其他插件注入的 `contexts` / `system_prompt` / 已有 `image_urls`

平台判定为表情包/贴纸的图片（`sub_type=1`、`summary` 含表情关键词等）会被跳过，不参与图片注入。
