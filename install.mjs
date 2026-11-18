// install.mjs — dsh-zcode 套件安装/更新/卸载(DSH 插件:dsh-zcode-cli provider + zcode-bridge bundle)
//
// 用法:
//   node install.mjs [--profile <DSH profile 目录>] [--dsh-home <目录>] [--dry-run]
//   node install.mjs --uninstall [--purge] [--profile <目录>] [--dsh-home <目录>] [--dry-run]
//
// 安装=更新:对已安装的机器重跑同一命令即为更新——旧版本自动备份到
// zcode-extensions\backups\<时间戳>\,无需先卸载。两者不分离,但备份让每次更新可回滚。
//
// 默认探测:
//   DSH_HOME 环境变量 → %USERPROFILE%\.dsh ;profile = <dsh-home>\profiles\desktop
//   (校验 package.json 存在,否则报错并提示 --profile)
//
// 安装/更新行为:
//   1. 把 dsh-zcode-cli 与 zcode-bridge 复制到稳定目录 <dsh-home>\zcode-extensions\
//      (旧版本自动备份到 zcode-extensions\backups\<时间戳>\)
//   2. 在 profile 的 package.json 注册两个绝对 file: 依赖
//      ("dsh-zcode-cli" / "@local/zcode-bridge")
//   3. 物化 node_modules:优先 pnpm(全局/corepack),失败则手工复制兜底
//      ——两种状态都与 package.json 一致,后续插件管理器的 pnpm 操作不会破坏
//   4. 打印验证清单(重启 DSH → 双端口 health → 预期行为)
//
// 卸载行为(--uninstall):
//   1. 从 profile package.json 移除两个依赖
//   2. 移除 node_modules 里的两个插件目录
//   3. zcode-extensions 的程序副本与 backups 默认保留(--purge 连备份一并删除)
//   4. 重启 DSH 后两个插件即不再加载
//
// --dry-run: 全流程打印但只读不写。
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { homedir } from 'node:os';

const args = process.argv.slice(2);
const getArg = (k) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : undefined; };
const DRY = args.includes('--dry-run');
const UNINSTALL = args.includes('--uninstall');
const PURGE = args.includes('--purge');

const SRC = path.dirname(path.resolve(decodeURIComponent(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'))));
const CLI_SRC = path.join(SRC, 'dsh-zcode-cli');
const BRIDGE_SRC = path.join(SRC, 'zcode-bridge');

const log = (m) => console.log('[install] ' + m);
const die = (m) => { console.error('[install] 错误: ' + m); process.exit(1); };

if (UNINSTALL) {
  // 卸载不要求压缩包源码在位——只动 profile 与 node_modules。
} else {
  if (!fs.existsSync(path.join(CLI_SRC, 'package.json'))) die('压缩包不完整: 找不到 ' + CLI_SRC + '\\package.json');
  if (!fs.existsSync(path.join(BRIDGE_SRC, 'package.json'))) die('压缩包不完整: 找不到 ' + BRIDGE_SRC + '\\package.json');
}

// ---- 定位 DSH profile ----
const dshHome = path.resolve(getArg('--dsh-home') ?? process.env.DSH_HOME ?? path.join(homedir(), '.dsh'));
const profile = path.resolve(getArg('--profile') ?? path.join(dshHome, 'profiles', 'desktop'));
const profilePkg = path.join(profile, 'package.json');
if (!fs.existsSync(profilePkg)) {
  die('未找到 DSH profile: ' + profilePkg + '\n  用 --profile <目录> 显式指定(目录下应有 package.json)');
}
const pkgRaw = fs.readFileSync(profilePkg, 'utf8');
let pkg;
try { pkg = JSON.parse(pkgRaw.replace(/^\uFEFF/, '')); } catch (e) { die('profile package.json 解析失败: ' + e.message); }
pkg.dependencies = pkg.dependencies ?? {};

const extDir = path.join(dshHome, 'zcode-extensions');
const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
const backupDir = path.join(extDir, 'backups', ts);

const targetCli = path.join(extDir, 'dsh-zcode-cli');
const targetBridge = path.join(extDir, 'zcode-bridge');
const depCli = 'file:' + targetCli.split(path.sep).join('/');
const depBridge = 'file:' + targetBridge.split(path.sep).join('/');
const nmCli = path.join(profile, 'node_modules', 'dsh-zcode-cli');
const nmBridge = path.join(profile, 'node_modules', '@local', 'zcode-bridge');

// ---- 卸载分支 ----
if (UNINSTALL) {
  log('卸载开始' + (DRY ? ' [dry]' : '') + (PURGE ? ' (--purge: 连程序副本与备份一并删除)' : ' (zcode-extensions 保留)'));
  let removedDeps = 0;
  for (const key of ['dsh-zcode-cli', '@local/zcode-bridge']) {
    if (pkg.dependencies[key] !== undefined) { delete pkg.dependencies[key]; removedDeps++; }
  }
  if (!DRY) fs.writeFileSync(profilePkg, JSON.stringify(pkg, null, 2) + '\n', 'utf8');
  log('package.json 依赖已移除 ' + removedDeps + ' 项' + (DRY ? ' [dry]' : ''));
  for (const [p, label] of [[nmCli, 'node_modules/dsh-zcode-cli'], [nmBridge, 'node_modules/@local/zcode-bridge']]) {
    if (fs.existsSync(p)) {
      if (!DRY) fs.rmSync(p, { recursive: true, force: true });
      log(label + ' 已移除' + (DRY ? ' [dry]' : ''));
    } else log(label + ' 本就不存在');
  }
  if (PURGE) {
    for (const [p, label] of [[targetCli, 'zcode-extensions/dsh-zcode-cli'], [targetBridge, 'zcode-extensions/zcode-bridge']]) {
      if (fs.existsSync(p)) { if (!DRY) fs.rmSync(p, { recursive: true, force: true }); log(label + ' 已删除 (--purge)'); }
    }
    if (fs.existsSync(backupDir)) { if (!DRY) fs.rmSync(backupDir, { recursive: true, force: true }); log('本次时间戳的备份目录已删除 (--purge)'); }
  }
  console.log(`
==== 卸载${DRY ? '(dry-run)' : ''}完成 ====
重启 DSH 后两个插件即不再加载。运行时数据目录 C:/Users/Administrator/.zcode-bridge
(token/日志) 未动;确认不再需要时可手动删除。
${PURGE ? '' : 'zcode-extensions 下的程序副本与 backups 已保留,重装时重跑 install.mjs 即可。\n'}`);
  process.exit(0);
}

log('profile: ' + profile);
log('稳定目录: ' + extDir);
log('依赖将注册为: dsh-zcode-cli=' + depCli);
log('              @local/zcode-bridge=' + depBridge);
if (DRY) log('--dry-run: 以下全部只读不写');

// 把目录让位到备份位:rename 优先;Windows 下目录可能被杀软/索引器瞬时锁住
// (EPERM),重试几次仍失败则退化为"先复制后删除",绝不让备份失败中断安装。
function moveAside(src, dst, label) {
  fs.mkdirSync(path.dirname(dst), { recursive: true });
  for (let i = 0; i < 4; i++) {
    try {
      fs.renameSync(src, dst);
      return 'renamed';
    } catch (e) {
      if (i === 3) {
        fs.cpSync(src, dst, { recursive: true });
        fs.rmSync(src, { recursive: true, force: true });
        return 'copied';
      }
      const wait = 400 * (i + 1);
      log(label + ' 让位被占用(' + String(e.code ?? e.message).slice(0, 40) + '),等 ' + wait + 'ms 重试');
      const until = Date.now() + wait;
      while (Date.now() < until) { /* 同步等待 */ }
    }
  }
  return 'failed';
}

function copyWithBackup(src, dst, label) {
  if (fs.existsSync(dst)) {
    const bak = path.join(backupDir, label);
    log(label + ' 已存在 -> 备份到 ' + bak);
    if (!DRY) moveAside(dst, bak, label);
  }
  if (!DRY) fs.cpSync(src, dst, { recursive: true });
  let n = 0;
  (function count(d) { for (const x of fs.readdirSync(d)) { const p = path.join(d, x); if (fs.statSync(p).isDirectory()) count(p); else n++; } })(src);
  log(label + ' ' + n + ' 个文件 -> ' + dst + (DRY ? ' [dry,未写入]' : ' 已写入'));
}

copyWithBackup(CLI_SRC, targetCli, 'dsh-zcode-cli');
copyWithBackup(BRIDGE_SRC, targetBridge, 'zcode-bridge');

// ---- 注册 package.json 依赖 ----
const beforeCli = pkg.dependencies['dsh-zcode-cli'];
const beforeBridge = pkg.dependencies['@local/zcode-bridge'];
pkg.dependencies['dsh-zcode-cli'] = depCli;
pkg.dependencies['@local/zcode-bridge'] = depBridge;
if (!DRY) {
  fs.writeFileSync(profilePkg, JSON.stringify(pkg, null, 2) + '\n', 'utf8');
  log('package.json 已更新: dsh-zcode-cli ' + (beforeCli ?? '(无)') + ' -> ' + depCli);
  log('package.json 已更新: @local/zcode-bridge ' + (beforeBridge ?? '(无)') + ' -> ' + depBridge);
} else {
  log('[dry] package.json 将更新: dsh-zcode-cli ' + (beforeCli ?? '(无)') + ' -> ' + depCli);
  log('[dry] package.json 将更新: @local/zcode-bridge ' + (beforeBridge ?? '(无)') + ' -> ' + depBridge);
}

// ---- 物化 node_modules ----
let pnpmCmd = null; // 命令数组;找到可用的 pnpm 执行体即非空
function probePnpm(cmd) {
  try { execFileSync(cmd[0], [...cmd.slice(1), '--version'], { timeout: 30000, stdio: 'ignore' }); return true; } catch { return false; }
}
if (!DRY) {
  if (probePnpm(['pnpm'])) pnpmCmd = ['pnpm'];
  if (!pnpmCmd) {
    const corepack = path.join(path.dirname(process.execPath), 'corepack' + (process.platform === 'win32' ? '.cmd' : ''));
    if (fs.existsSync(corepack) && probePnpm([corepack, 'pnpm'])) pnpmCmd = [corepack, 'pnpm'];
  }
}
if (DRY) {
  log('[dry] 物化策略: 有 pnpm 则 pnpm install;否则手工把两个插件复制进 node_modules(与 package.json 一致)');
} else if (pnpmCmd) {
  log('pnpm 可用(' + pnpmCmd[0] + '),正规物化 node_modules...');
  try {
    execFileSync(pnpmCmd[0], [...pnpmCmd.slice(1), 'install', '--dir', profile], { encoding: 'utf8', timeout: 600000, stdio: ['ignore', 'pipe', 'pipe'] });
    log('pnpm install 完成,node_modules 已按 package.json 物化');
  } catch (e) {
    log('pnpm install 失败(' + String(e.message).slice(0, 120) + '),改用手工兜底');
    pnpmCmd = null;
  }
} else {
  log('pnpm 不可用,手工兜底物化 node_modules');
}
if (!DRY && !pnpmCmd) {
  for (const [src, dst] of [[targetCli, nmCli], [targetBridge, nmBridge]]) {
    if (fs.existsSync(dst)) {
      // node_modules 里已有副本(可能是用户当前正在用的版本)——先备份再让位
      const bak = path.join(backupDir, 'node_modules', path.basename(dst));
      moveAside(dst, bak, 'node_modules 旧副本');
      log('node_modules 旧副本已备份 -> ' + bak);
    }
    fs.mkdirSync(path.dirname(dst), { recursive: true });
    fs.cpSync(src, dst, { recursive: true });
    log('已铺入 node_modules: ' + dst);
  }
}

// ---- 验证清单 ----
console.log(`
==== 安装${DRY ? '(dry-run)' : ''}完成,验证清单 ====
1. 重启 DSH(关闭 DeepSeek Harness 后重新打开)。
2. 等约 20 秒,验证两个本地服务:
     http://127.0.0.1:47830/health  -> {"ok":true,"service":"dsh-llm-endpoint",...}
     http://127.0.0.1:47821/health  -> {"ok":true,"service":"zcode-bridge"}(带 token 头)
3. ZCode 桌面端的模型 baseUrl 指向 http://127.0.0.1:47830。
4. 预期行为:
   - 新会话在 DSH 侧自动创建并随对话延续(稳定会话键);
   - 图片附件走附件通道,模型可直接看图;
   - 点停止会取消 DSH 侧对应轮次;
   - CLI 子进程以桌面当前版本号自报(不再出现 3012 client not supported);
   - GLM-5.3-Flash 的上下文记账按 1M 真实窗口(派生配置自动修正)。
5. 卸载/回滚: 从 zcode-extensions\\backups\\<时间戳> 还原旧版本,并把
   profile package.json 的两个依赖改回原值,重启 DSH。
`);
