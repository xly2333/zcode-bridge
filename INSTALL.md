# 安装 / 卸载

## 更新

**安装与更新是同一个动作**：对已安装的机器重跑 `node install.mjs` 即为更新——
旧版本自动备份到 `zcode-extensions\\backups\\<时间戳>\\`，可随时回滚。无需先卸载。

## 方式一：一键脚本（推荐）

```
node install.mjs                       # 自动探测 profile 并安装
node install.mjs --dry-run             # 预览将执行的动作,不写任何文件
node install.mjs --profile <目录>      # 显式指定 DSH profile(目录下应有 package.json)
node install.mjs --dsh-home <目录>     # 显式指定 DSH_HOME(默认 %USERPROFILE%\.dsh)
```

脚本动作：
1. 把 `dsh-zcode-cli/` 与 `zcode-bridge/` 复制到 `<DSH_HOME>\zcode-extensions\`（稳定目录，
   旧版本自动备份到 `zcode-extensions\backups\<时间戳>\`）；
2. 在 profile 的 `package.json` 注册两个绝对 `file:` 依赖；
3. 物化 `node_modules`：优先 pnpm（全局 / corepack），失败则手工复制兜底
   （两种状态都与 package.json 一致，后续插件管理器的 pnpm 操作不会破坏）；
4. 打印验证清单。

完成后 **重启 DSH** 即生效。

## 方式二：手工安装

1. 复制插件到稳定目录（任意你愿意长期保存的位置，下面以
   `%USERPROFILE%\.dsh\zcode-extensions\` 为例）：

   ```
   xcopy /E /I dsh-zcode-cli  "%USERPROFILE%\.dsh\zcode-extensions\dsh-zcode-cli"
   xcopy /E /I zcode-bridge   "%USERPROFILE%\.dsh\zcode-extensions\zcode-bridge"
   ```

2. 编辑 `%USERPROFILE%\.dsh\profiles\desktop\package.json`，在 `dependencies` 里加：

   ```jsonc
   "dsh-zcode-cli": "file:C:/Users/<你>/.dsh/zcode-extensions/dsh-zcode-cli",
   "@local/zcode-bridge": "file:C:/Users/<你>/.dsh/zcode-extensions/zcode-bridge"
   ```

   （路径用正斜杠；如果原来已有同名依赖，替换其值。）

3. 物化二选一：
   - 有 pnpm：在 profile 目录执行 `pnpm install`；
   - 无 pnpm：直接把两个插件文件夹复制进 `profiles\desktop\node_modules\`
     （`dsh-zcode-cli` 放根下；`zcode-bridge` 放 `@local\zcode-bridge`）。

4. 重启 DSH。

## 安装后验证

| 检查 | 预期 |
|---|---|
| `http://127.0.0.1:47830/health` | `{"ok":true,"service":"dsh-llm-endpoint",...}` |
| `http://127.0.0.1:47821/health`（带 `authorization: Bearer <token>`） | `{"ok":true,"service":"zcode-bridge"}` |
| token 位置 | `<DSH_HOME>\zcode-bridge\token`（首次启动自动生成） |
| ZCode 桌面端模型 baseUrl | `http://127.0.0.1:47830` |
| DSH 里新建会话发消息 | DSH 侧出现 `sess_zb_*` 会话，正常回复 |
| 发图片 | 模型能描述图片内容（附件通道） |
| 点停止 | DSH 侧对应轮次取消 |

## 卸载

```
node install.mjs --uninstall           # 移除依赖+node_modules,重启 DSH 后插件不再加载
node install.mjs --uninstall --purge   # 连 zcode-extensions 程序副本与备份一并清理
```

手工卸载：从 profile `package.json` 删除两个依赖、删除 `node_modules\dsh-zcode-cli` 与
`node_modules\@local\zcode-bridge`、重启 DSH。运行时数据目录 `%USERPROFILE%\.zcode-bridge`
（token/日志）默认保留，确认不需要时手动删除。

## 回滚到旧版本

每次安装/更新前，旧版本自动备份到 `zcode-extensions\backups\<时间戳>\`。回滚：
把备份目录的内容复制回 `zcode-extensions\dsh-zcode-cli`（或 `zcode-bridge`），
重跑 `node install.mjs`（它会把备份副本重新物化进 node_modules），重启 DSH。

## 常见问题

- **47830/47821 没起来**：DSH 没有装上插件（检查 package.json 与 node_modules），或端口被
  旧进程占用（结束残留的 node 进程后重启 DSH）。
- **health 有了但模型报 "ZCode executable not found"**：本机没有 ZCode 桌面端，或不在
  `F:\app\zcode` 等默认位置——provider 会按 `ZCODE_CLI_ENTRY` →
  `%ZCODE_WINDOWS_APP_INSTALL_DIR%` → 常见盘符的顺序探测，也可用环境变量显式指定。
- **报 3012 client not supported**：说明桌面端运行时快照不在默认位置导致版本推导失败；
  确认 `%USERPROFILE%\.zcode\v2\runtime\provider\` 下有桌面当前版本的快照目录。
- **报 context window 超限**：会话历史超过模型真实窗口——切回大窗口模型或新开会话；
  派生配置已在启动时自动对齐桌面声明（自愈机制见 README 补丁表）。
