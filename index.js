/**
 * zcode-bridge — DSH Host plugin (v4)
 *
 * 两种用法都支持：
 *   1) LLM 端点模式：本地端点把 ZCode 的对话请求转进来（主用）
 *   2) hook 模式：ZCode 的 UserPromptSubmit hook 投递（备用）
 *
 * 投递后由 DSH agent 执行，回复用 seq 水位线收集后回推。
 */
import { readFileSync, existsSync, appendFileSync } from 'node:fs'
import { join } from 'node:path'
import { homedir } from 'node:os'
import { ReliableOutbox, settlementForTurn } from './reliable-outbox.js'
import { createGatewayLifecycle } from './gateway-runtime.js'

export const name = 'zcode-bridge'

// 插桩：把关键路径写到文件，Electron 进程的 stdout 拿不到时靠它诊断
const DEBUG_LOG = join(homedir(), '.zcode-bridge', 'dsh-plugin-debug.log')
function debugLog(tag, detail) {
  try {
    appendFileSync(DEBUG_LOG, `${new Date().toISOString()} [${tag}] ${detail}\n`, 'utf8')
  } catch {
    /* 诊断日志失败不影响主流程 */
  }
}

export const inject = ['agents', 'approval']

const REPLY_TIMEOUT_MS = 300_000
const MAX_AWAITING = 64
// 完整复读优先：上限放到 200k，正常消息根本碰不到
const MAX_PROMPT_CHARS = 2_000_000 // 完整转发优先：上限抬到 200 万字，实际几乎不会触发截断
const SEEN_MAX = 500
const SEEN_TTL_MS = 30 * 60_000
const SESSION_IDLE_MS = 30 * 60_000
const SESSION_SWEEP_MS = 60_000
const MAX_SESSION_AGENTS = 8

function resolveToken(config) {
  const explicit = config?.tokenFile
  const file =
    typeof explicit === 'string' && explicit.length > 0
      ? explicit
      : join(process.env.ZCODE_BRIDGE_DIR || join(homedir(), '.zcode-bridge'), 'token')
  try {
    if (existsSync(file)) return readFileSync(file, 'utf8').trim()
  } catch {
    /* no token */
  }
  return ''
}

function newMessageId() {
  return globalThis.crypto?.randomUUID?.() ?? `zb-${Date.now()}-${Math.random().toString(16).slice(2)}`
}

/**
 * 把 ZCode 侧的对话历史还原成 DSH 原生事件序列（seed）。
 * 每一轮按 DSH 真实落盘的形状构造：turn/start -> step/start -> user/message
 * -> assistant/message -> step/end -> turn/end。
 * envelope 必须是 {type, seq, time, data}；assistant/message 的 message 必须带
 * id、role='assistant'、source.kind='model' 且 source 里有 provider/model，
 * data 里必须有 turn/step/stream(array)/usage。
 */
function seedFromHistory(history, provider, model, title, injectAsUser = true) {
  if (!Array.isArray(history) || history.length === 0) return undefined
  const evs = []
  let seq = 0
  let turn = 0
  const base = Date.now()
  // seq 从 0 连续、time 单调不减（与官方导入器 LogBuilder 一致）
  const push = (type, data, extra) =>
    evs.push({ type, seq: seq++, time: base + seq, data, ...extra })

  let pendingUser = null
  for (const h of history) {
    if (!h || typeof h.text !== 'string') continue
    if (h.role === 'user') {
      pendingUser = h.text
      continue
    }
    if (h.role !== 'assistant') continue
    turn += 1
    push('turn/start', { turn })
    push('step/start', { turn, step: 1 })
    if (turn === 1) {
      // 空的 system 头占住 surface 节点 0；恢复时 Agent 会用真实提示词替换它。
      // 缺了它，DSH 会把系统消息追加在历史之后，存储格式直接拒绝。
      push(
        'system/message',
        { turn, step: 1, message: { id: newMessageId(), role: 'system', content: [], source: { kind: 'system-prompt' } } },
        { surfaceOp: 'append' },
      )
      if (typeof title === 'string' && title.trim()) {
        push('session/title', { title: title.trim().slice(0, 120), messageSeqs: [], source: { kind: 'user' } })
      }
    }
    push(
      'user/message',
      {
        id: newMessageId(),
        role: 'user',
        content: [{ type: 'text', text: pendingUser ?? '' }],
        source: injectAsUser ? { kind: 'user' } : { kind: 'relay' },
      },
      { surfaceOp: 'append' },
    )
    push(
      'assistant/message',
      {
        turn,
        step: 1,
        message: {
          role: 'assistant',
          content: [{ type: 'text', text: h.text }],
          source: { kind: 'model', provider: provider || 'deepseek', model: model || 'dsh-harness' },
          id: newMessageId(),
        },
        usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
        stream: [],
      },
      { surfaceOp: 'append' },
    )
    push('step/end', { turn, step: 1 })
    push('turn/end', { turn, reason: { kind: 'completed' } })
    pendingUser = null
  }
  return evs.length > 0 ? evs : undefined
}

/** seed 建会话失败时的兜底：把历史拼成一段带明确边界的上下文文本。 */
function historyTranscript(history) {
  if (!Array.isArray(history) || history.length === 0) return ''
  const lines = []
  for (const h of history) {
    if (!h || typeof h.text !== 'string') continue
    lines.push((h.role === 'user' ? '【我】' : '【你】') + h.text)
  }
  return lines.join('\n\n')
}

/** DSH Session 的事件数组在 `session.log`（不是 session.events）。 */
function sessionEvents(agent) {
  const s = agent?.session
  if (!s) return undefined
  if (Array.isArray(s.log)) return s.log
  if (Array.isArray(s.events)) return s.events
  return undefined
}

/** 会话事件信封是 { type, seq, time, data, ...surface }，载荷在 data 里。 */
function assistantTextFrom(ev) {
  const msg = ev?.data?.message ?? ev?.message
  const blocks = msg?.content ?? ev?.data?.content ?? ev?.content
  if (typeof blocks === 'string') return blocks.trim()
  if (!Array.isArray(blocks)) return ''
  return blocks
    .filter((b) => b?.type === 'text' && typeof b.text === 'string')
    .map((b) => b.text)
    .join('\n')
    .trim()
}

/** 会话里已提交的最大事件 seq。 */
function lastSeqOf(agent) {
  try {
    const evs = sessionEvents(agent)
    if (!Array.isArray(evs)) return -1
    let s = -1
    for (const ev of evs) {
      const n = Number(ev?.seq)
      if (Number.isFinite(n) && n > s) s = n
    }
    return s
  } catch {
    return -1
  }
}

/** 诊断快照：把最近事件的结构带出去，便于排查字段差异。 */
function diagOf(agent, baselineSeq) {
  try {
    const evs = sessionEvents(agent)
    if (!Array.isArray(evs)) {
      const s = agent?.session
      return { error: 'no events array', sessionKeys: s ? Object.keys(s) : null }
    }
    return {
      total: evs.length,
      baselineSeq,
      tail: evs.slice(-5).map((e) => ({
        type: e?.type,
        seq: e?.seq,
        keys: e ? Object.keys(e) : [],
        dataKeys: e?.data ? Object.keys(e.data) : [],
        hasDataMessage: !!(e?.data?.message),
        contentIsArray: Array.isArray(e?.data?.message?.content ?? e?.data?.content),
      })),
    }
  } catch (err) {
    return { error: String(err?.message ?? err) }
  }
}

export function apply(ctx, config) {
  const cfg = {
    autoServices: config?.autoServices !== false,
    gatewayHost: typeof config?.gatewayHost === 'string' && config.gatewayHost ? config.gatewayHost : '127.0.0.1',
    bridgePort: config?.bridgePort ?? 47821,
    endpointPort: config?.endpointPort ?? 47830,
    persistState: config?.persistState !== false,
    stateDir: typeof config?.stateDir === 'string' && config.stateDir ? config.stateDir : undefined,
    startupRetryMs: Number.isFinite(Number(config?.startupRetryMs)) ? Math.max(300, Math.min(30000, Number(config.startupRetryMs))) : 2000,
    bridgeUrl: String(config?.bridgeUrl || 'http://127.0.0.1:47821').replace(/\/+$/, ''),
    pollIntervalMs: Math.max(300, Number(config?.pollIntervalMs) || 1200),
    injectAsUser: config?.injectAsUser !== false,
    forwardResults: config?.forwardResults !== false,
    preferSteer: config?.preferSteer !== false,
    // 默认不添加来源标记：实际执行时只复述用户原文
    tagSource: config?.tagSource === true,
    // 默认跟随 ZCode 新建会话
    followSession: config?.followSession !== false,
    // 停止生成：ZCode 点停止 → 取消 DSH 侧对应轮次
    followCancel: config?.followCancel !== false,
    // 编辑重发：历史被改写 → fork 新分支
    followFork: config?.followFork !== false,
    // 新建会话绑定的 agent preset（决定工具集）。留空则自动继承/默认 standard
    agentPreset: typeof config?.agentPreset === 'string' ? config.agentPreset : '',
    // 新会话建立时把 ZCode 侧已有的历史整段灌进 DSH 会话
    seedHistory: config?.seedHistory !== false,
  }

  let base = cfg.bridgeUrl
  let token = cfg.autoServices ? '' : resolveToken(config)
  let headers = token ? { authorization: `Bearer ${token}` } : null

  let disposed = false
  let busy = false
  const tracked = new Map()
  const seen = new Map()
  const awaiting = []
  const outbox = new ReliableOutbox({ retryDelayMs: cfg.pollIntervalMs })
  let inflight = null
  let startupRetryTimer
  let cleanupPromise
  const gateway = cfg.autoServices ? createGatewayLifecycle({
    host: cfg.gatewayHost,
    bridgePort: cfg.bridgePort,
    endpointPort: cfg.endpointPort,
    stateDir: cfg.stateDir,
    tokenFile: typeof config?.tokenFile === 'string' && config.tokenFile.length > 0 ? config.tokenFile : undefined,
    persistState: cfg.persistState,
    logger(record) {
      if (!record || typeof record !== 'object' || typeof record.state !== 'string') return
      const suffix = record.error ? `: ${record.error}` : ''
      console.log(`[zcode-bridge] gateway ${record.state}${suffix}`)
    },
  }) : null

  function listAgents() {
    try {
      const list = ctx.agents?.list?.()
      return Array.isArray(list) ? list : []
    } catch {
      return []
    }
  }

  function pickAgent() {
    const list = listAgents()
    if (list.length === 0) return undefined
    return list.find((a) => a?.status === 'running') ?? list[0]
  }

  function hasPendingFor(agent) {
    return awaiting.some((it) => it.agent === agent)
  }

  // ---- ZCode 会话 -> DSH 会话 映射 ----
  const sessionAgents = new Map()
  // seed 失败时待回放的历史（按 zcodeSessionId 记），下一轮注入前拼进正文
  const replayPending = new Map()

  /** 记录/刷新某会话的最后活动时间，用于空闲回收。 */
  function touchSession(zcodeSessionId) {
    const rec = sessionAgents.get(zcodeSessionId)
    if (rec) {
      rec.lastUsedAt = Date.now()
      return rec
    }
    return undefined
  }

  /** 回收空闲会话，防止 ZCode 侧删了会话、DSH 这边 agent 泄漏。 */
  async function sweepSessions() {
    const now = Date.now()
    for (const [sid, rec] of [...sessionAgents]) {
      // 2026-11-20 分组自愈:外部某处会把桥会话写进归档集(灰显沉底)。每个 tick
      // 对活映射的会话再断言一次;registry 的 mutate 对 no-op 幂等,无写放大。
      try {
        const registry = ctx.get?.('workspaceRegistry')
        const agentId = rec.agent?.session?.id
        if (registry && agentId) {
          const archived = Array.isArray(registry.archivedSessionIds) && registry.archivedSessionIds.includes(agentId)
          const ws = typeof registry.list === 'function' ? registry.list()[0] : undefined
          const attached = ws && Array.isArray(ws.record?.sessionIds) && ws.record.sessionIds.includes(agentId)
          if (archived || attached === false) {
            if (archived && typeof registry.unarchiveSession === 'function') await registry.unarchiveSession(agentId)
            if (ws && typeof ws.attachSession === 'function') await ws.attachSession(agentId)
            debugLog('workspace-heal', `id=${agentId} wasArchived=${archived} wasAttached=${attached === true}`)
          }
        }
      } catch {
        /* 分组自愈失败不阻塞回收主流程 */
      }
      if (now - (rec.lastUsedAt || 0) < SESSION_IDLE_MS) continue
      if (hasPendingFor(rec.agent)) continue
      sessionAgents.delete(sid)
      try {
        await rec.handle?.dispose?.()
        console.log(`[zcode-bridge] 回收空闲会话 ${sid}`)
      } catch {
        /* ignore */
      }
    }
  }

  /**
   * 为 ZCode 的一个会话找到（或创建）对应的 DSH agent。
   * ZCode 新建会话时，这里会跟着建一个新的 DSH 会话，而不是复用前一个。
   * 创建失败（如没有 driver factory）则回退到给定的 fallback。
   *
   * opts.fork === true 时：从既有会话 fork 出一条新分支，替换映射表里的条目。
   */
  async function agentForSession(zcodeSessionId, fallback, zcCwd, opts) {
    debugLog('afs-enter', `session=${zcodeSessionId} followSession=${cfg.followSession} history=${opts?.history?.length ?? 0} hasCreate=${typeof ctx.agents?.create}`)
    if (!cfg.followSession || !zcodeSessionId) return fallback

    const existing = sessionAgents.get(zcodeSessionId)
    const wantFork = opts?.fork === true

    if (existing && !wantFork) {
      existing.lastUsedAt = Date.now()
      return existing.agent
    }

    if (typeof ctx.agents?.create !== 'function') {
      debugLog('afs-no-create', `session=${zcodeSessionId} ctx.agents=${Object.keys(ctx.agents ?? {}).join(',')}`)
      return existing?.agent ?? fallback
    }

    try {
      const fopts = fallback?.options
      const agentOptions = {}
      if (fopts?.provider) agentOptions.provider = fopts.provider
      if (fopts?.model) agentOptions.model = fopts.model

      // 关键：必须带 cwd，否则会话会落到 _no-cwd 分组，UI 按工作区加载时找不到
      let cwd
      try {
        cwd = fallback?.session?.header?.cwd
      } catch {
        /* ignore */
      }
      if (!cwd) cwd = zcCwd || undefined
      if (!cwd && existing) {
        try {
          cwd = existing.agent?.session?.header?.cwd
        } catch {
          /* ignore */
        }
      }

      const clean = String(zcodeSessionId).replace(/[^a-zA-Z0-9]/g, '').slice(-32)
      const meta = {}
      if (cwd) meta.cwd = cwd

      // 关键：必须带 agentPreset，否则新会话是「preset-free composition」——
      // 没有任何工具（pwsh/fs/web 等全部缺失），模型会说"我没有工具"。
      // 优先继承 fallback 会话已选的 preset，其次用配置值，最后用 standard。
      const presetId = resolvePresetId(fallback, existing?.agent)
      if (presetId) meta.agentPreset = presetId

      let newId
      // seed 是 create() 的顶层参数，不是 meta 字段：塞进 meta 会被当成未知元数据丢弃，
      // 会话建出来了但一条历史事件都不会落盘。
      let seed
      let inheritedEventCount
      if (wantFork) {
        newId = `sess_zb_${clean}_f${Date.now().toString(36)}`
        meta.parentSession = existing?.agent?.session?.id ?? `sess_zb_${clean}`
        const forkSeed = seedForFork(existing?.agent, opts.forkAt)
        if (forkSeed) {
          seed = forkSeed.events
          inheritedEventCount = forkSeed.inheritedEventCount
          meta.isSeeded = true
        }
      } else {
        newId = `sess_zb_${clean}`
      }

      // 新会话：把 ZCode 侧已有的历史整段灌进来（原生 seed 事件）
      let seedFailed = false
      if (!wantFork && cfg.seedHistory && Array.isArray(opts?.history) && opts.history.length > 0) {
        const hs = seedFromHistory(opts.history, agentOptions.provider, agentOptions.model, opts.title, cfg.injectAsUser)
        if (hs) {
          seed = hs
          console.log(`[zcode-bridge] 历史回灌：${opts.history.length} 条 -> ${hs.length} 个 seed 事件`)
        }
      }

      const createOnce = (withSeed) => ctx.agents.create({
        sessionId: newId,
        meta: Object.keys(meta).length > 0 ? meta : undefined,
        ...(withSeed ? { seed } : {}),
        ...(withSeed && inheritedEventCount !== undefined ? { inheritedEventCount } : {}),
        agentOptions: Object.keys(agentOptions).length > 0 ? agentOptions : undefined,
      })

      // 审批策略固化为 never(2026-11-18):桥接会话没有 GUI 审批渠道,ask 政策会让
      // 回合卡在审批上(会话日志只有 approval/policy ask 就停了,永远不出 request)。
      // 与 dsh-subagent 的 delegation 固化同构:直接向会话日志追加 approval/policy
      // 旋钮事件,折叠读取以最后一条为准(source 仅接受 'delegation')。
      // 注意必须先于 try 定义:fallback 路径(catch)也会调用。
      const pinApprovalNever = (ag) => {
        try {
          const sess = ag?.session ?? ag?.sessionStore?.session ?? undefined
          if (typeof sess?.append === 'function') {
            sess.append('approval/policy', { policy: 'never', source: 'delegation' })
            debugLog('approval-policy', 'pinned to never (session append)')
          } else {
            debugLog('approval-policy', 'skipped: no writable session handle (append unavailable)')
          }
        } catch (e) {
          debugLog('approval-policy-err', String(e?.message ?? e))
        }
      }

      let handle
      try {
        debugLog('create-attempt', `id=${newId} seed=${seed ? seed.length + 'evts' : 'none'} inherited=${inheritedEventCount ?? '-'} agentOptions=${JSON.stringify(agentOptions)}`)
        handle = await createOnce(seed !== undefined)
        debugLog('create-ok', `id=${newId}`)
      } catch (err) {
        debugLog('create-err', `id=${newId} seed=${seed ? 'yes' : 'no'} :: ${err?.stack ?? err?.message ?? err}`)
        const existsErr = /already exists/i.test(String(err?.message ?? err))
        if (existsErr && !/-r[a-z0-9]{6,}$/i.test(newId)) {
          // 同名会话已存在（上一轮 DSH 进程建的，内存映射随进程消失）。
          // 2026-11-18 关键修复：本分支成功后绝不能再掉进下面的 seed 回退里对
          // 同一个 newId 再 createOnce 一次——刚建好的会话已登记进 dsh-session
          // 内存注册表，二次 create 必然 Proxy.prepare "already exists"，把成功
          // 会话变成孤儿、消息掉进 fallback（23:28-00:54 的每次转发失败全是它）。
          // 顺序：先试打开既有会话（跨重启续命），打不开再改名重建。
          let opened = false
          for (const tryOpen of [
            () => ctx.agents.open?.({ sessionId: newId }),
            () => ctx.agents.open?.(newId),
            () => ctx.agents.get?.(newId),
            () => ctx.agents.resume?.({ sessionId: newId }),
          ]) {
            try {
              const h = await tryOpen?.()
              const ag = h?.agent ?? h
              if (ag && (ag.session || typeof ag.followup === 'function')) {
                handle = h
                opened = true
                debugLog('create-open', `id=${newId} (opened existing session)`)
                break
              }
            } catch {
              /* 换下一种打开方式 */
            }
          }
          if (!opened) {
            newId = `${newId}-r${Date.now().toString(36).slice(-7)}${Math.floor(Math.random() * 36).toString(36)}`
            try {
              handle = await createOnce(seed !== undefined)
              debugLog('create-ok', `id=${newId} (renamed retry)`)
            } catch (err2) {
              debugLog('create-err-renamed', `id=${newId} :: ${err2?.stack ?? err2?.message ?? err2}`)
              if (seed === undefined) throw err2
              // 改名重试仍失败且带着 seed：丢 seed 再试一次（沿用改名后的新 id）
              console.warn(`[zcode-bridge] seed 建会话失败，退回文本回放: ${err2?.message ?? err2}`)
              report({ phase: 'seed-failed', error: String(err2?.message ?? err2) })
              delete meta.isSeeded
              inheritedEventCount = undefined
              seedFailed = true
              try {
                handle = await createOnce(false)
              } catch (err3) {
                debugLog('create-err-replay', `id=${newId} :: ${err3?.stack ?? err3?.message ?? err3}`)
                throw err3
              }
            }
          }
        } else if (seed === undefined) {
          throw err
        } else {
          // seed 不合规就丢掉 seed 重来，改用文本回放兜底，绝不让消息卡死
          console.warn(`[zcode-bridge] seed 建会话失败，退回文本回放: ${err?.message ?? err}`)
          report({ phase: 'seed-failed', error: String(err?.message ?? err) })
          delete meta.isSeeded
          inheritedEventCount = undefined
          seedFailed = true
          handle = await createOnce(false)
        }
      }
      const agent = handle?.agent ?? handle
      if (seedFailed) replayPending.set(zcodeSessionId, opts.history)
      pinApprovalNever(agent)

      // fork 时把旧会话还给它自己保留（append-only 历史可回溯），只替换映射
      sessionAgents.set(zcodeSessionId, { agent, handle, lastUsedAt: Date.now() })

      // 2026-11-20 分组修复:桥建的会话不显式 attach 进工作区时,DSH 侧栏把它们
      // 沉到「未分组」最底下,还会被写进 workspace.json 的 archivedSessionIds
      // (灰显、不可点开)。创建成功后立即:反归档 + attach 到工作区(workspace
      // 的 attachSession 自己会校验会话头部的 cwd 是否等于工作区路径,不匹配会
      // 抛错,这里 catch 住只记日志,绝不让分组问题影响转发主流程)。
      try {
        const registry = ctx.get?.('workspaceRegistry')
        if (registry) {
          if (typeof registry.unarchiveSession === 'function') await registry.unarchiveSession(newId)
          const ws = typeof registry.list === 'function' ? registry.list()[0] : undefined
          if (ws && typeof ws.attachSession === 'function') {
            await ws.attachSession(newId)
            debugLog('workspace-attach', `id=${newId} ws=${ws.record?.path ?? '?'} cwd=${cwd ?? '-'}`)
          } else {
            debugLog('workspace-skip', `id=${newId} registry 无可用的 workspace 实体`)
          }
        } else {
          debugLog('workspace-skip', `id=${newId} ctx 无 workspaceRegistry`)
        }
      } catch (e) {
        debugLog('workspace-attach-err', `id=${newId} :: ${e?.stack ?? e?.message ?? e}`)
      }
      console.log(
        `[zcode-bridge] 为 ZCode 会话 ${zcodeSessionId} 新建了 DSH 会话 ${newId}` +
          (wantFork ? '（fork）' : ''),
      )

      // 超出上限就淘汰最久没用的一个
      if (sessionAgents.size > MAX_SESSION_AGENTS) {
        let oldestKey
        let oldest = Infinity
        for (const [k, v] of sessionAgents) {
          if (k === zcodeSessionId) continue
          if ((v.lastUsedAt || 0) < oldest) {
            oldest = v.lastUsedAt || 0
            oldestKey = k
          }
        }
        if (oldestKey) {
          const victim = sessionAgents.get(oldestKey)
          sessionAgents.delete(oldestKey)
          try {
            await victim?.handle?.dispose?.()
          } catch {
            /* ignore */
          }
        }
      }
      return agent
    } catch (err) {
      console.warn(`[zcode-bridge] 会话创建失败，回退到当前会话: ${err?.message ?? err}`)
      debugLog('create-fallback', `session=${zcodeSessionId} :: ${err?.stack ?? err?.message ?? err}`)
      report({ phase: 'session-create-failed', error: String(err?.message ?? err), sessionId: zcodeSessionId })
      const fbAgent = existing?.agent ?? fallback
      pinApprovalNever(fbAgent)
      return fbAgent
    }
  }

  /**
   * 解析新会话该绑哪个 preset（preset 决定工具集与提示段）。
   * 顺序：配置显式值 → fallback 会话已记录的 preset → 注册表默认值 → 'standard'。
   * 取不到就返回 undefined（会退化成无工具会话，所以最后兜底 standard）。
   */
  function resolvePresetId(fallback, existingAgent) {
    try {
      if (cfg.agentPreset) return cfg.agentPreset
    } catch {
      /* ignore */
    }

    // 已存在（fork 前）会话的 preset 最准
    for (const agent of [existingAgent, fallback]) {
      try {
        const session = agent?.session
        if (!session) continue
        const fromHeader = session?.header?.agentPreset
        if (typeof fromHeader === 'string' && fromHeader) return fromHeader
        const registry = ctx.agentPresets
        const recorded = registry?.presetForSession?.(session)
        if (typeof recorded === 'string' && recorded) return recorded
      } catch {
        /* ignore */
      }
    }

    // 注册表默认值
    try {
      const def = ctx.agentPresets?.defaultId
      if (typeof def === 'string' && def) return def
    } catch {
      /* ignore */
    }

    // 最后兜底：standard 是内置第一顺位（order 1），带全套工具
    return 'standard'
  }

  /** 从现有会话取编辑点之前的事件，作为 fork 的种子。 */
  function seedForFork(agent, forkAt) {
    try {
      const evs = sessionEvents(agent)
      if (!Array.isArray(evs) || !Number.isFinite(forkAt) || forkAt <= 0) return undefined
      // forkAt 是 ZCode 侧 messages 的下标，粗略映射到 DSH 的 user 消息序号
      const userSeqs = []
      for (const ev of evs) {
        if (ev?.type === 'user/message') userSeqs.push(ev?.seq)
      }
      if (userSeqs.length === 0) return undefined
      const idx = Math.max(0, Math.min(forkAt - 1, userSeqs.length - 1))
      const cut = userSeqs[idx]
      if (!Number.isFinite(cut)) return undefined
      const seed = []
      for (const ev of evs) {
        if (!Number.isFinite(ev?.seq)) continue
        if (ev.seq >= cut) break
        seed.push(ev)
      }
      if (seed.length === 0) return undefined
      // fork 种子必须在继承前缀末尾打一个 session/end-seed 标记，
      // 否则恢复时无法区分「继承来的历史」和「本会话新产生的事件」。
      seed.push({
        type: 'session/end-seed',
        seq: seed.length,
        time: seed[seed.length - 1]?.time ?? Date.now(),
        data: { inherited: true },
      })
      return { events: seed, inheritedEventCount: seed.length }
    } catch {
      return undefined
    }
  }

  function markSeen(id) {
    seen.set(id, Date.now())
    if (seen.size <= SEEN_MAX) return
    const pendingIds = new Set(awaiting.map((it) => it.evt?.id))
    const now = Date.now()
    for (const [k, ts] of seen) {
      if (seen.size <= Math.floor(SEEN_MAX * 0.6)) break
      if (pendingIds.has(k)) continue
      if (now - ts < SEEN_TTL_MS) continue
      seen.delete(k)
    }
  }

  async function post(path, payload, { idempotencyKey } = {}) {
    if (!headers) return { ok: false, error: 'bridge token unavailable' }
    try {
      const response = await fetch(`${base}${path}`, {
        method: 'POST',
        headers: {
          ...headers,
          'content-type': 'application/json',
          ...(idempotencyKey ? { 'idempotency-key': idempotencyKey } : {}),
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(3000),
      })
      if (!response.ok) {
        void response.body?.cancel?.()
        return { ok: false, status: response.status, error: `bridge ${path} HTTP ${response.status}` }
      }
      if (path === '/push-result') {
        const receipt = await response.json()
        if (receipt?.ok !== true) return { ok: false, error: 'bridge reply acknowledgement missing' }
      } else {
        void response.body?.cancel?.()
      }
      return { ok: true, status: response.status }
    } catch (error) {
      // State/ACK reports remain best-effort. The outbox consumes this failure result
      // for replies and retains the immutable payload until a later successful POST.
      return { ok: false, error: String(error?.message ?? error) }
    }
  }

  async function setState(rec, state, extra) {
    rec.state = state
    rec.updatedAt = new Date().toISOString()
    await post('/status', {
      messageId: rec.messageId,
      eventId: rec.evt?.id,
      zcodeSessionId: rec.evt?.sessionId,
      state,
      turn: extra?.turn ?? rec.turn,
      agentCount: listAgents().length,
      at: rec.updatedAt,
    })
  }

  function report(patch) {
    void post('/plugin-status', {
      ...patch,
      agents: listAgents().length,
      tracking: tracked.size,
      awaiting: awaiting.length,
      gateway: gateway ? gateway.status : { state: 'external' },
      diag: awaiting.length > 0 ? diagOf(awaiting[0].agent, awaiting[0].baselineSeq) : null,
    })
  }

  async function ack(ids) {
    if (!headers || ids.length === 0) return
    await post('/ack', { ids })
  }

  async function pull() {
    if (!headers) return []
    inflight = new AbortController()
    const res = await fetch(`${base}/pull?max=20`, {
      headers,
      signal: AbortSignal.any([inflight.signal, AbortSignal.timeout(5000)]),
    })
    if (!res.ok) throw new Error(`pull ${res.status}`)
    const data = await res.json()
    return Array.isArray(data?.events) ? data.events : []
  }

  async function pushResult(blocks, rec) {
    if (!cfg.forwardResults) return { status: 'disabled' }
    const key = rec.evt?.id
    outbox.enqueue(key, {
      text: blocks?.text || '',
      reasoning: blocks?.reasoning || '',
      sessionId: rec.evt?.sessionId,
      forEventId: key,
      messageId: rec.messageId,
    })
    return outbox.deliver(key, (payload, idempotencyKey) =>
      post('/push-result', payload, { idempotencyKey }),
    )
  }
  // ---- 送达确认：认领事件提供本条消息真正所属的 turn ----
  const onClaimed = (payload) => {
    const id = payload?.message?.id
    const rec = id ? tracked.get(id) : undefined
    if (!rec) return
    if (Number.isSafeInteger(payload?.turn) && payload.turn > 0 && rec.turn === undefined) {
      rec.turn = payload.turn
    }
    if (rec.state === 'claimed' || rec.state === 'running' || rec.cancelled) return
    void setState(rec, 'claimed', { turn: rec.turn })
    void setState(rec, 'running', { turn: rec.turn })
  }

  const onDiscarded = (payload) => {
    const id = payload?.message?.id
    const rec = id ? tracked.get(id) : undefined
    if (!rec) return
    void setState(rec, 'dropped')
    tracked.delete(id)
  }

  const onInserted = (payload) => {
    const id = payload?.message?.id
    const rec = id ? tracked.get(id) : undefined
    if (!rec) return
    void setState(rec, rec.state === 'steering' ? 'steering' : 'queued')
  }

  function progressFor(item, events) {
    const rec = tracked.get(item.messageId)
    if (!rec || rec.cancelled || !Number.isSafeInteger(rec.turn)) return
    // 2026-11-19 防御:会话句柄异常时 sessionEvents 可能返回 undefined,这里直接跳过
    // 而不是让 events.filter 抛 TypeError 打满 tick-err。
    if (!Array.isArray(events)) return
    const fresh = events.filter(event => Number.isSafeInteger(event?.seq) && event.seq > item.progressSeq && event.data?.turn === rec.turn)
    if (!fresh.length) return
    item.progressSeq = Math.max(item.progressSeq ?? item.baselineSeq, ...fresh.map(event => event.seq))
    const summaries = fresh.map(event => {
      if (event.type === 'tool/call') return `正在执行工具：${String(event.data?.name || '未知工具')}`
      if (event.type === 'tool/result') return event.data?.error ? `工具执行失败：${String(event.data.error.message || event.data.error.name || '未知错误')}` : '工具执行完成'
      if (event.type === 'step/start') return `开始第 ${event.data?.step ?? '?'} 步`
      if (event.type === 'step/end') return `第 ${event.data?.step ?? '?'} 步完成`
      if (event.type === 'todo/write') {
        const todos = Array.isArray(event.data?.todos) ? event.data.todos : []
        return todos.filter(todo => todo.status === 'in_progress').map(todo => `处理中：${todo.content}`).join('；')
      }
      return ''
    }).filter(Boolean)
    for (const event of fresh) {
      if (event.type !== 'assistant/message') continue
      const blocks = event.data?.message?.content
      if (Array.isArray(blocks)) {
        item.progressText = item.progressText || ''
        for (const block of blocks) {
          // 2026-11-19 契约修正:只累加 text 块。reasoning 块此前也被累进 progressText,
          // 桌面端会把思考内容当正文流式渲染(与 DSH 侧 reasoning-delta 修复同源的问题,
          // 只是发生在 progress 通道)。
          if (block?.type === 'text' && typeof block.text === 'string') {
            const seen = item.progressBlocks || (item.progressBlocks = new Set())
            const key = `${event.seq}:${block.type}:${block.text}`
            if (!seen.has(key)) {
              seen.add(key)
              item.progressText += block.text
            }
          }
          // 2026-11-20 思考流恢复:reasoning 单独走自己的字段,不再混进 text。
          // 网关 /responses 现在能把它映射成桌面端 SDK 认识的
          // response.reasoning_summary_* SSE 事件;/v1/messages 也用它补 finalize。
          if (block?.type === 'reasoning' && typeof block.text === 'string' && block.text) {
            const seen = item.progressReasoningBlocks || (item.progressReasoningBlocks = new Set())
            const key = `${event.seq}:reasoning:${block.text}`
            if (!seen.has(key)) {
              seen.add(key)
              item.progressReasoning = (item.progressReasoning || '') + block.text
            }
          }
        }
      }
    }
    void post('/progress', {
      eventId: item.evt?.id,
      messageId: item.messageId,
      sessionId: item.evt?.sessionId,
      turn: rec.turn,
      seq: item.progressSeq,
      events: [...new Set(fresh.map(event => event.type).filter(type => typeof type === 'string'))],
      text: item.progressText || undefined,
      reasoning: item.progressReasoning || undefined,
      summaries: summaries.length ? summaries : undefined,
    })
  }

  try {
    ctx.on('agent/inbox/claimed', onClaimed)
    ctx.on('agent/inbox/discarded', onDiscarded)
    ctx.on('agent/inbox/inserted', onInserted)
  } catch (err) {
    console.warn(`[zcode-bridge] event listeners failed: ${err?.message ?? err}`)
  }

  /** 把 data:URL 或本地图片路径解析成附件服务要的 {mediaType, data}；解析不了返回 null。 */
  function imageBytesOf(entry) {
    if (typeof entry !== 'string' || !entry) return null
    const m = /^data:([^;,]+);base64,([\s\S]+)$/.exec(entry)
    if (m) {
      try {
        return { mediaType: m[1], data: Buffer.from(m[2], 'base64') }
      } catch {
        return null
      }
    }
    if (!existsSync(entry)) return null
    try {
      const dot = entry.lastIndexOf('.')
      const ext = dot > 0 ? entry.slice(dot + 1).toLowerCase() : ''
      const mediaType =
        ext === 'jpg' || ext === 'jpeg'
          ? 'image/jpeg'
          : ext === 'gif'
            ? 'image/gif'
            : ext === 'webp'
              ? 'image/webp'
              : 'image/png'
      const slash = Math.max(entry.lastIndexOf('\\'), entry.lastIndexOf('/'))
      return { mediaType, data: readFileSync(entry), name: slash >= 0 ? entry.slice(slash + 1) : undefined }
    } catch {
      return null
    }
  }

  /**
   * 图片转成 DSH 真正的 image 内容块（走附件服务拿 attachmentId），
   * 让视觉模型能直接看图；附件服务缺席或失败时退回文本标记，保证内容不丢。
   */
  async function imagePartsOf(images) {
    // 2026-11-18:ctx.attachments 属性读取在 cordis 里要求 inject 声明,未声明会抛
    // "cannot get property without inject"——曾经把每一次注入都炸在 buildContent 里
    // (rejection 被宿主吞掉,表现为 create-ok 后永远没有 user 消息)。改用无注入
    // 要求的 ctx.get,缺失时返回 undefined → 走文本标记兜底。
    let store
    try {
      store = ctx?.get?.('attachments') ?? ctx?.get?.('attachment')
    } catch {
      store = undefined
    }
    // saveImages 若悬挂（解码/压缩队列卡住）会拖死整条注入链且事件无法 ack：
    // 加 5s 上限，超时按附件服务缺席处理，退回文本标记，绝不阻塞投递。
    const withDeadline = (promise, ms, label) => {
      let timer
      const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timeout ${ms}ms`)), ms)
        timer.unref?.()
      })
      return Promise.race([Promise.resolve(promise), timeout]).finally(() => clearTimeout(timer))
    }
    const parts = []
    for (const entry of Array.isArray(images) ? images : []) {
      const bytes = imageBytesOf(entry)
      if (!bytes) continue
      if (store && typeof store.saveImages === 'function') {
        try {
          const refs = await withDeadline(store.saveImages([bytes]), 5000, 'saveImages')
          const ref = Array.isArray(refs) ? refs[0] : refs
          if (ref?.attachmentId) {
            parts.push({ type: 'image', attachment: ref })
            continue
          }
        } catch (err) {
          console.warn(`[zcode-bridge] 图片转附件失败: ${err?.message ?? err}`)
        }
      }
      parts.push({ type: 'text', text: `【附件图片】${entry}` })
    }
    return parts
  }

  /** 把 ZCode 传来的一次用户输入拼成 DSH 的一条消息内容。 */
  async function filePartsOf(files) {
    let store
    try { store = ctx?.get?.('attachments') ?? ctx?.get?.('attachment') } catch { store = undefined }
    const parts = []
    for (const file of Array.isArray(files) ? files : []) {
      if (typeof file?.data !== 'string' || typeof file?.name !== 'string') continue
      const data = Buffer.from(file.data, 'base64')
      if (data.toString('base64') !== file.data) continue
      try {
        const attachment = await store?.saveFile?.({ data, name: file.name })
        if (attachment?.attachmentId) {
          parts.push({ type: 'file', attachment })
          continue
        }
      } catch {
        console.warn('[zcode-bridge] 文件附件存储失败，回退到文本')
      }
      parts.push({ type: 'text', text: `[附件文本：${file.name}]\n${data.toString('utf8')}` })
    }
    return parts
  }

  async function buildContent(text, images, files, rec) {
    const parts = []
    // 默认原样传递用户文本；只有 tagSource=true 时才加来源标记
    const body = cfg.tagSource ? `[来源: ZCode 中转 · 非本地直输]\n${text}` : text
    if (body) parts.push({ type: 'text', text: body })
    parts.push(...(await filePartsOf(files)))
    parts.push(...(await imagePartsOf(images)))
    if (parts.length === 0) parts.push({ type: 'text', text: '' })
    return parts
  }

  async function injectPrompt(evt) {
    if (seen.has(evt.id)) return
    if (evt.cancelled) {
      // 投递后、插件取到之前就被用户取消了，直接确认丢掉
      markSeen(evt.id)
      void ack([evt.id])
      report({ phase: 'pre-cancelled', dropped: evt.id })
      return
    }

    const fallback = pickAgent()
    if (!fallback) {
      report({ phase: 'no-agent', dropped: evt.id })
      return
    }
    // 完整复读：原文不动（不 trim、不改缩进），只有超长才截断并留标记
    const rawPrompt = typeof evt.prompt === 'string' ? evt.prompt : ''
    const text =
      rawPrompt.length > MAX_PROMPT_CHARS
        ? `${rawPrompt.slice(0, MAX_PROMPT_CHARS)}\n\n…[已截断：原文 ${rawPrompt.length} 字，上限 ${MAX_PROMPT_CHARS}]`
        : rawPrompt
    const images = Array.isArray(evt.images) ? evt.images : []
    if (!text && images.length === 0 && !evt.files?.length) {
      markSeen(evt.id)
      void ack([evt.id])
      return
    }
    if (awaiting.length >= MAX_AWAITING) {
      report({ phase: 'backpressure', queued: awaiting.length })
      return
    }

    // ZCode 会话 -> 对应 DSH 会话（新会话会新建；编辑重发会 fork）
    const agent = await agentForSession(evt.sessionId, fallback, evt.cwd, {
      fork: cfg.followFork && evt.fork === true,
      forkAt: Number.isFinite(Number(evt.forkAt)) ? Number(evt.forkAt) : undefined,
      history: Array.isArray(evt.history) ? evt.history : undefined,
    })
    if (hasPendingFor(agent)) return

    // seed 建会话失败时的兜底：历史拼成一段上下文放在正文最前面
    let finalText = text
    const pend = replayPending.get(evt.sessionId)
    if (pend) {
      replayPending.delete(evt.sessionId)
      const tr = historyTranscript(pend)
      if (tr) {
        finalText = `【以下是 ZCode 侧此前的对话历史，共 ${pend.length} 条，按原样回放】\n\n${tr}\n\n【历史回放结束，以下是当前消息】\n\n${text}`
        console.log(`[zcode-bridge] 文本回放历史 ${pend.length} 条`)
      }
    }

    const messageId = newMessageId()
    const rec = { messageId, evt, agent, injectedAt: Date.now(), state: 'received', text: finalText }
    tracked.set(messageId, rec)

    const message = {
      id: messageId,
      role: 'user',
      content: await buildContent(finalText, images, evt.files, rec),
      source: cfg.injectAsUser ? { kind: 'user' } : { kind: 'relay' },
    }

    const baselineSeq = lastSeqOf(agent)
    const running = agent.status === 'running'
    debugLog('inject-step', `mid=${messageId} running=${running} baselineSeq=${baselineSeq}`)
    try {
      if (running && cfg.preferSteer) {
        agent.steer(message)
        void setState(rec, 'steering')
      } else {
        agent.followup(message)
        void setState(rec, 'queued')
      }
      debugLog('inject-queued', `mid=${messageId} agentStatus=${agent?.status}`)
    } catch (err) {
      debugLog('inject-followup-err', `${err?.stack ?? err?.message ?? err}`)
      void setState(rec, 'failed')
      report({ phase: 'inject-failed', error: String(err?.message ?? err) })
      tracked.delete(messageId)
      return
    }

    markSeen(evt.id)
    awaiting.push({ evt, agent, messageId, injectedAt: Date.now(), baselineSeq, progressSeq: baselineSeq })
    void ack([evt.id])
    console.log(
      `[zcode-bridge] injected ${running ? 'steer' : 'followup'} len=${text.length}` +
        ` imgs=${images.length} baselineSeq=${baselineSeq}${evt.fork ? ' (fork)' : ''}`,
    )
  }

  /** 用户停止：使用 DSH cause 对象；已认领条目保留到其 turn/end。 */
  async function handleCancel(evt) {
    if (seen.has(evt.id)) return
    if (!cfg.followCancel) {
      markSeen(evt.id)
      void ack([evt.id])
      return
    }
    const targetId = evt.targetEventId
    const requestId = evt.targetRequestId
    let cancelledAny = false
    for (let i = awaiting.length - 1; i >= 0; i--) {
      const item = awaiting[i]
      const matches =
        (targetId && item.evt?.id === targetId) ||
        (requestId && item.evt?.requestId === requestId)
      if (!matches) continue
      const rec = tracked.get(item.messageId)
      if (rec) {
        rec.cancelled = true
        await setState(rec, 'cancelled', { turn: rec.turn })
      }
      outbox.drop(item.evt?.id)
      const ended = settlementForTurn(sessionEvents(item.agent), rec?.turn, item.baselineSeq)
      if (!ended) {
        try {
          item.agent?.cancel?.({ kind: 'user' })
        } catch (err) {
          console.warn(`[zcode-bridge] cancel failed: ${err?.message ?? err}`)
        }
      }
      // Unclaimed input was removed by cancel()/inbox.clear() and has no turn/end.
      // A claimed turn keeps its tracking record until that exact terminal arrives.
      if (ended || !Number.isSafeInteger(rec?.turn)) {
        awaiting.splice(i, 1)
        tracked.delete(item.messageId)
      }
      cancelledAny = true
    }
    markSeen(evt.id)
    void ack([evt.id])
    report({ phase: cancelledAny ? 'cancelled' : 'cancel-miss', target: targetId })
  }

  async function harvest() {
    if (awaiting.length === 0) return
    for (let i = awaiting.length - 1; i >= 0; i--) {
      const item = awaiting[i]
      const rec = tracked.get(item.messageId)
      if (!rec) {
        outbox.drop(item.evt?.id)
        awaiting.splice(i, 1)
        continue
      }
      // agent/inbox/claimed supplied rec.turn. Neither agent.status nor the
      // presence of an assistant block establishes that this turn has finished.
      const events = sessionEvents(item.agent)
      progressFor(item, events)
      const settled = settlementForTurn(events, rec.turn, item.baselineSeq)
      if (!settled) {
        if (!rec.timeoutReported && Date.now() - item.injectedAt > REPLY_TIMEOUT_MS) {
          rec.timeoutReported = true
          await setState(rec, 'timeout', { turn: rec.turn })
          report({ phase: 'reply-timeout', waitingForTerminal: item.evt?.id })
        }
        continue
      }
      if (rec.cancelled) {
        outbox.drop(item.evt?.id)
        awaiting.splice(i, 1)
        tracked.delete(item.messageId)
        continue
      }
      const delivery = await pushResult(settled.blocks, { evt: item.evt, messageId: item.messageId })
      if (disposed) return
      if (delivery.status === 'delivered' || delivery.status === 'disabled') {
        awaiting.splice(i, 1)
        await setState(rec, 'done', { turn: rec.turn })
        tracked.delete(item.messageId)
      } else if (delivery.status === 'retry') {
        await setState(rec, 'reply-retry', { turn: rec.turn })
        report({
          phase: 'reply-retry',
          eventId: item.evt?.id,
          attempts: delivery.attempts,
          error: delivery.error,
        })
      }
    }
  }

  async function tick() {
    if (disposed || busy || !headers) return
    busy = true
    try {
      const events = await pull()
      for (const evt of events) {
        if (disposed) break
        if (evt.kind === 'UserPromptSubmit') await injectPrompt(evt)
        else if (evt.kind === 'Cancel') await handleCancel(evt)
      }
      await harvest()
      await sweepSessions()
      report({ phase: awaiting.length > 0 ? 'tracking' : 'idle' })
    } catch (err) {
      // 2026-11-18:必须落日志——这里只 report 到 /plugin-status 时,注入阶段的
      // 任何异常都会变成"create-ok 之后无声无息"的悬案。
      debugLog('tick-err', String(err?.stack ?? err?.message ?? err))
      report({ phase: 'error', error: String(err?.message ?? err) })
    } finally {
      busy = false
      inflight = null
    }
  }

  const timer = setInterval(tick, cfg.pollIntervalMs)

  function disposeBridge() {
    if (cleanupPromise) return cleanupPromise
    disposed = true
    clearInterval(timer)
    clearInterval(tokenTimer)
    clearTimeout(startupRetryTimer)
    startupRetryTimer = undefined
    try { inflight?.abort() } catch { /* best-effort */ }
    const handles = [...new Set([...sessionAgents.values()].map(rec => rec.handle).filter(Boolean))]
    awaiting.length = 0
    tracked.clear()
    seen.clear()
    outbox.clear()
    sessionAgents.clear()
    replayPending.clear()
    cleanupPromise = Promise.allSettled([
      ...(gateway ? [Promise.resolve().then(() => gateway.stop())] : []),
      ...handles.filter(handle => typeof handle.dispose === 'function').map(handle => Promise.resolve().then(() => handle.dispose())),
    ]).then(results => {
      for (const result of results) {
        if (result.status === 'rejected') console.warn(`[zcode-bridge] cleanup failed: ${result.reason?.message ?? result.reason}`)
      }
    })
    return cleanupPromise
  }

  async function bootstrapGateway() {
    if (disposed || !gateway) return
    try {
      const ready = await gateway.start()
      if (disposed) { await gateway.stop(); return }
      base = ready.bridgeUrl
      token = ready.token
      headers = { authorization: `Bearer ${token}` }
      console.log(`[zcode-bridge] managed forwarding ready: ${ready.endpointUrl} -> ${base}`)
      report({ phase: 'gateway-ready', gateway: gateway.status })
      void tick()
    } catch (error) {
      if (disposed) return
      headers = null
      console.warn(`[zcode-bridge] gateway startup failed: ${error?.code || ''} ${error?.message ?? error}`)
      clearTimeout(startupRetryTimer)
      startupRetryTimer = setTimeout(() => {
        startupRetryTimer = undefined
        void bootstrapGateway()
      }, cfg.startupRetryMs)
      startupRetryTimer.unref?.()
    }
  }

  const tokenTimer = cfg.autoServices ? undefined : setInterval(() => {
    if (token || disposed) return
    const t = resolveToken(config)
    if (!t) return
    token = t
    headers = { authorization: `Bearer ${t}` }
    console.log('[zcode-bridge] token acquired, polling started')
    void tick()
  }, 3000)

  let cleanupBound = false
  if (typeof ctx.effect === 'function') {
    try {
      ctx.effect(() => disposeBridge)
      cleanupBound = true
    } catch (err) {
      console.warn(`[zcode-bridge] effect hook failed: ${err?.message ?? err}`)
    }
  }
  if (!cleanupBound && typeof ctx.on === 'function') {
    ctx.on('dispose', disposeBridge)
    cleanupBound = true
  }
  if (!cleanupBound) {
    void disposeBridge()
    throw new Error('DSH disposal hook is required for managed forwarding services')
  }

  if (gateway) void bootstrapGateway()
  else if (headers) void tick()
  else console.warn('[zcode-bridge] external bridge token pending')
  console.log(`[zcode-bridge] started v5 (${gateway ? 'managed' : 'external'}), polling interval ${cfg.pollIntervalMs}ms`)
}
