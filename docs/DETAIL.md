# ZCode ↔ DSH 统一转发插件（1.1.0）

用户消息端点、中转队列与 DSH 收发逻辑统一属于本 bundle。默认随 DSH 宿主启动，随插件卸载或宿主退出停止；正常运行依赖插件而不是独立启动脚本。

## 链路

```text
ZCode Anthropic 请求 /v1/messages
  → 插件内 endpoint :47830
  → 插件内 bridge :47821（hook、租约、ACK、取消、回推）
  → DSH 插件认领并投递对应会话
  → 等该轮 turn/end
  → 可靠 outbox /push-result
  → endpoint 返回 JSON 或 SSE
```

## 自动生命周期

1. Host 加载 [插件入口](<zcode-bridge/dsh-bundle/index.js>)。
2. [管理器](<zcode-bridge/dsh-bundle/gateway-runtime.js>) 先启动 [bridge](<zcode-bridge/dsh-bundle/gateway/bridge.mjs>)，取得实际地址与本实例 token，再启动 [endpoint](<zcode-bridge/dsh-bundle/gateway/llm-endpoint.mjs>)。
3. 两端完成监听后才设置内部鉴权、开始 pull 与结果回推。
4. 部分启动失败会回滚已经启动的服务并记录错误；入口按配置间隔重试。
5. 插件 dispose 先停止接入、取消在途等待、关闭 SSE/HTTP/keepalive，再关闭中转监听，清理轮询、重试计时器和插件拥有的会话句柄。
6. 服务器位于 DSH 宿主进程，宿主退出会释放其监听；没有由插件分离出去的 Node 常驻子进程。

未知端口占用会作为 EADDRINUSE 报告，入口保留重试，不按端口盲杀其他进程。

## 标准地址

| 用途 | 地址 |
|---|---|
| ZCode provider base URL | `http://127.0.0.1:47830/v1` |
| endpoint 公共 health | `http://127.0.0.1:47830/health` |
| bridge 公共 health | `http://127.0.0.1:47821/health` |
| bridge hook/pull/push-result/results | `http://127.0.0.1:47821`，内部 token 鉴权 |

## 默认配置

[Bundle patch](<zcode-bridge/dsh-bundle/cordis.patch.yml>) 已开启：

```yaml
autoServices: true
gatewayHost: '127.0.0.1'
bridgePort: 47821
endpointPort: 47830
persistState: true
startupRetryMs: 2000
pollIntervalMs: 1200
```

`stateDir` 空值使用服务默认状态目录；显式 `tokenFile` 传入 bridge，endpoint 直接使用本实例 token。`persistState:false` 用于离线/内存测试，服务不会读取或写入状态文件。

新实例/新会话、取消与 fork 配置保持原接口；结果仅在对应 turn/end 后回推。网络异常、503 或回执缺失会保留结果重试，成功后才 done。

## 当前加载与旧入口迁移

当前 desktop profile 已声明本 bundle 的工作区 link。已加载的 Host JS 需要由现用 DSH 重新加载一次，之后启停全部由 DSH 负责。

[旧 source 入口](<zcode-bridge/src/bridge.mjs>) 与 [旧 endpoint 入口](<zcode-bridge/src/llm-endpoint.mjs>) 仅保留兼容导出；作为主脚本运行时输出插件管理提示后结束。[旧 CMD](<zcode-bridge/services.cmd>) 和 [旧 shell 启动器](<zcode-bridge/start-services.sh>) 同样退役，旧计划任务通过这些入口运行会结束而不会再启动独立服务。

标准操作是启动 DSH；这份文档取代早期的手工启动服务说明。真实 DSH 加载状态与最新验证结果见 [自动启停说明](<zcode-bridge/README-autostart.md>)。
