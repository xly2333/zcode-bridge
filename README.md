# zcode-bridge (DSH bundle)

A DSH host bundle that owns the ZCode message endpoints end to end:

- **Anthropic-compatible endpoint** (`gateway/llm-endpoint.mjs`, default 127.0.0.1:47830):
  serves `/v1/messages`, `/responses`, `/chat/completions`; point the ZCode
  desktop client at it and the request is relayed into DSH.
- **Relay bridge** (`gateway/bridge.mjs`, default 127.0.0.1:47821): hook event
  queue (UserPromptSubmit / Cancel), result harvest, plugin status.
- **New-session sync**: stable per-conversation session keys, full history seed
  into a fresh DSH session (`seedFromHistory`), edit-resend fork with
  `session/end-seed` marker, and a text-replay fallback when seeding fails.
- **Stop generation / image attachments / reliable outbox** with idempotent
  redelivery.

Everything here is plugin-resident: both services start inside the DSH process
via `gateway-runtime.js` and stop with it. No scheduled tasks, no daemons.

## Install

```jsonc
// in the DSH profile package.json
"@local/zcode-bridge": "link:<path-to-this-folder>"
```
then restart DSH. First start derives the bridge token and listens on
127.0.0.1:47830 -> 127.0.0.1:47821 automatically.

## Local patches (2026-11-18)

- `/responses` + `/chat/completions` now forward the full conversation history
  and derive a **stable** session key from the first user turn (previously the
  key changed per turn, creating a new DSH session every message and losing
  all context).
- endpoint probe logs include `msgs=` and `session=` for verification.

## 一键安装包（Release）

Releases 页提供完整套件压缩包（dsh-zcode-cli + zcode-bridge + install.mjs 一体）：
**https://github.com/xly2333/zcode-bridge/releases**

下载解压后执行 `node install.mjs` 即完成安装，重启 DSH 生效。
