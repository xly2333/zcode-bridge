# dsh-zcode 套件

让 **ZCode 桌面端**和 **DSH（DeepSeek Harness）**共用同一个会话的两件套 DSH 插件。
所有功能都是插件内禀的：随 DSH 进程启动而启动、关闭而停止，无常驻服务、无计划任务、
无手工维护的运行时状态（首次启动全部自举派生）。

```
ZCode 桌面端 UI
   │  (Anthropic 兼容请求)
   ▼
127.0.0.1:47830  dsh-llm-endpoint      ┐
   │  /hook 投递队列                    │  zcode-bridge (DSH bundle)
   ▼                                   │  — 随 DSH 启停
127.0.0.1:47821  zcode-bridge 队列     ┘
   │  pull/ack/push-result
   ▼
DSH agent 会话(sess_zb_*,稳定会话键+历史回灌)
   │
   ▼
dsh-zcode-cli provider                ┐
   │  spawn zcode.cjs app-server       │  dsh-zcode-cli (DSH provider 插件)
   ▼                                   │  — 随 DSH 启停
zcode.z.ai (GLM-5.3-Flash,官方客户端身份) ┘
```

## 包内清单

| 目录 | 是什么 | 版本 |
|---|---|---|
| `dsh-zcode-cli/` | DSH 的 zcode 路由 provider：拉起官方 ZCode CLI 子进程（app-server 协议）、环境契约自举、桌面版本号注入、上下文窗口修正 | 0.3.1 + 本地补丁 |
| `zcode-bridge/` | DSH host bundle：伪装 Anthropic 端点 + 中继队列 + 会话键/历史回灌/fork/停止/图片附件 | 1.1.3 + 本地补丁 |
| `install.mjs` | 一键安装脚本（复制 → 注册依赖 → 物化 node_modules → 验证清单） | — |
| `INSTALL.md` | 手工安装/卸载步骤 | — |

## 本地补丁相对上游的增量（全部经过实机验收，2026-11-18/19）

| 问题 | 根因 | 修复 | 验证 |
|---|---|---|---|
| 每轮报 3012 "This client is not supported" | CLI 以环境变量 `ZCODE_APP_VERSION` 自报版本；DSH 封闭环境白名单丢失它 → 回退旧 bundle 版本号 0.17.1 被 zcode.z.ai 拒 | bootstrap 从活跃桌面快照推导版本 + 白名单放行 | 重启后 UA 精确切换 3.18.2，0 条真实 3012 |
| 多轮对话上下文丢失 | `/responses` `/chat/completions` 路径会话键只锚最后一轮、不传 history → 每轮新建 DSH 会话 | 会话键锚定整段对话 + history 全量回灌 | E2E 两轮同键、模型记得上一轮 |
| 编辑重发/新会话同步 | —（历史回灌 seed + fork 标记 + 失败文本回放兜底） | 已实测：336 条历史种子完整落盘 | 会话文件核对 |
| 大会话报 "Model request exceeded the provider context window" | CLI 派生配置的 GLM-5.3-Flash `contextWindow` 被写成 400k（桌面源是 1M） | 派生时显式对齐 1M + stamp v2 自愈 | 服务端实测 819,037 tokens 通过 |

## 要求

- DSH（DeepSeek Harness）+ 其内置的 `@deepseek-ai/*` 运行时（peer 依赖由宿主提供）
- Node ≥ 22（仅安装脚本需要；DSH 自带 node 亦可）
- ZCode 桌面端已安装（provider 从其 `resources/glm/zcode.cjs` 拉起官方 CLI，官方签名通道）

## 快速开始 / 更新 / 卸载

```
node install.mjs                 # 安装(对已装机器重跑=更新,旧版自动备份)
node install.mjs --dry-run       # 预览
node install.mjs --uninstall     # 卸载(--purge 连备份清理)
```

详细步骤与手工安装见 `INSTALL.md`。安装后重启 DSH 即生效；所有功能随 DSH 启停。

## 已知边界

- 修改插件文件后需要重启 DSH 才生效（无热加载）。
- 一个已经超过模型真实上下文窗口的大会话无法靠配置救回——切回大窗口模型或新开会话。
