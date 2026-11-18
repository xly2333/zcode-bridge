// dsh-llm-endpoint: 伪装成 Anthropic Messages API 的本地端点
//
// ZCode 把 baseUrl 指向这里 → 它以为在调模型，实际由 DSH 接手干活。
// 请求进来：抽出最后一条 user 消息 → 交给 zcode-bridge → DSH agent 执行
// 响应回去：把 DSH 的回答包装成 Anthropic 的 message 格式
//
// ��持流式（SSE）与非流式两种返回，ZCode 两种都可能用。
//
// 完美同步要点（v2）：
//   1) 按 ZCode 会话并行（不再全局串行）：每个 sessionKey 一条 Promise 链
//   2) 停止生成：客户端断开 → 立刻取消本次等待，并通知 DSH 侧 agent.cancel
//   3) 编辑重发检测：对比本会话上一轮 messages 指纹，历史被改写则要求 fork
//   4) 图片附件：base64 image 块落临时文件，把路径告诉 DSH
import { createServer } from 'node:http'
import { randomUUID, createHash } from 'node:crypto'
import { readFileSync, existsSync, appendFileSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { homedir, tmpdir } from 'node:os'
import { pathToFileURL, fileURLToPath } from 'node:url'

// All service state and effects are owned by one explicit start/close lifecycle.
export async function startEndpoint(options = {}) {
const signal = options.signal
signal?.throwIfAborted()
const PORT = Number(options.port ?? (process.env.DSH_ENDPOINT_PORT || 47830))
const HOST = options.host ?? (process.env.DSH_ENDPOINT_HOST || '127.0.0.1')
if (typeof HOST !== 'string' || !HOST) throw new TypeError('invalid endpoint host')
if (!Number.isSafeInteger(PORT) || PORT < 0 || PORT > 65535) throw new TypeError('invalid endpoint port')
const persistState = options.persistState !== false
const STATE_DIR = options.stateDir ?? process.env.ZCODE_BRIDGE_DIR ?? join(homedir(), '.zcode-bridge')
const TOKEN_FILE = options.tokenFile ?? join(STATE_DIR, 'token')
const BRIDGE = String(options.bridgeUrl ?? process.env.ZCODE_BRIDGE_URL ?? 'http://127.0.0.1:47821').replace(/\/+$/, '')
const explicitToken = options.bridgeToken ?? options.token
if (explicitToken !== undefined && (typeof explicitToken !== 'string' || !explicitToken.trim())) throw new TypeError('invalid bridge token')
const LOG_FILE = join(STATE_DIR, 'llm-endpoint.log')
const WAIT_MS = Number(options.waitMs ?? process.env.DSH_WAIT_MS ?? 240000)
const POLL_MS = Number(options.pollIntervalMs ?? 400)
if (!Number.isFinite(WAIT_MS) || WAIT_MS <= 0 || !Number.isFinite(POLL_MS) || POLL_MS <= 0) throw new TypeError('invalid endpoint timing')
const ATTACH_DIR = join(tmpdir(), 'zcode-attachments')
const zcodeDbPath = options.zcodeDbPath ?? (persistState ? join(homedir(), '.zcode', 'cli', 'db', 'db.sqlite') : undefined)
const pendingRequests = new Map()
const sockets = new Set()
let closing = false
let closePromise

if (persistState) mkdirSync(STATE_DIR, { recursive: true })

function log(msg) {
  const line = `${new Date().toISOString()} ${msg}\n`
  if (persistState) {
    try { appendFileSync(LOG_FILE, line) } catch { /* optional metadata log */ }
  }
  try {
    if (typeof options.logger === 'function') options.logger(msg)
    else options.logger?.info?.(msg)
  } catch { /* logging does not own the service lifecycle */ }
}

function bridgeToken() {
  if (typeof explicitToken === 'string') return explicitToken.trim()
  // Memory mode ignores even an explicitly supplied tokenFile.
  if (!persistState) return ''
  try {
    return existsSync(TOKEN_FILE) ? readFileSync(TOKEN_FILE, 'utf8').trim() : ''
  } catch {
    return ''
  }
}

function trackRequest(res, requestId, sessionId) {
  const state = { controller: new AbortController(), requestId, sessionId, submitted: false }
  const onClose = () => {
    if (!res.writableFinished) {
      state.controller.abort(new Error('user-cancel'))
      void notifyCancellation(state, 'user-cancel')
    }
  }
  state.cleanup = () => {
    res.off('close', onClose)
    pendingRequests.delete(requestId)
  }
  pendingRequests.set(requestId, state)
  res.on('close', onClose)
  return state
}

async function notifyCancellation(state, reason) {
  if (!state?.submitted) return
  const key = state.eventId || state.requestId
  if (state.cancelKey === key && state.cancelPromise) return state.cancelPromise
  state.cancelKey = key
  // Separate from the aborted model-request signal: the receiver must see cancel
  // before its manager closes it. A slow receiver cannot hold shutdown for WAIT_MS.
  state.cancelPromise = (async () => {
    try {
      const response = await fetch(`${BRIDGE}/cancel`, {
        method: 'POST',
        headers: { ...state.auth, 'content-type': 'application/json' },
        body: JSON.stringify({ eventId: state.eventId, requestId: state.requestId, sessionId: state.sessionId, reason }),
        signal: AbortSignal.timeout(750),
      })
      await response.body?.cancel?.()
    } catch { /* cancellation delivery is bounded and best-effort */ }
  })()
  return state.cancelPromise
}

function waitForPoll(abort) {
  if (abort.aborted) return Promise.resolve()
  return new Promise((resolve) => {
    const finish = () => {
      clearTimeout(timer)
      abort.removeEventListener('abort', finish)
      resolve()
    }
    const timer = setTimeout(finish, POLL_MS)
    abort.addEventListener('abort', finish, { once: true })
    if (abort.aborted) finish()
  })
}

function json(res, code, body) {
  if (res.writableEnded || res.destroyed) return
  const data = JSON.stringify(body)
  res.writeHead(code, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(data),
  })
  res.end(data)
}

function readBody(req, limit = 64 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    let settled = false
    const finish = (fn, v) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      fn(v)
    }
    const timer = setTimeout(() => {
      finish(reject, new Error('body timeout'))
      try {
        req.destroy()
      } catch {
        /* ignore */
      }
    }, 30000)
    req.on('data', (c) => {
      size += c.length
      if (size > limit) {
        chunks.length = 0
        finish(reject, new Error('body too large'))
        try {
          req.destroy()
        } catch {
          /* ignore */
        }
        return
      }
      chunks.push(c)
    })
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8')
      if (!raw) return finish(resolve, {})
      try {
        finish(resolve, JSON.parse(raw))
      } catch (e) {
        finish(reject, new Error('bad json: ' + e.message))
      }
    })
    req.on('aborted', () => finish(reject, new Error('aborted')))
    req.on('error', (e) => finish(reject, e))
  })
}

/**
 * 稳定的会话标识：用「系统提示 + 会话第一条 user 消息」的哈希。
 * 同一 ZCode 会话的多轮对话首条消息不变，所以哈希稳定 -> 复用同一个 DSH 会话；
 * 新建会话时首条消息不同 -> 哈希变化 -> 新建 DSH 会话。
 */
function sessionKeyOf(body) {
  try {
    const msgs = Array.isArray(body?.messages) ? body.messages : []
    const first = msgs.find((m) => m?.role === 'user')
    const firstText = typeof first?.content === 'string' ? first.content : JSON.stringify(first?.content ?? '')
    const sys = JSON.stringify(body?.system ?? '')
    const h = createHash('sha1').update(sys).update('\u0000').update(firstText).digest('hex').slice(0, 16)
    return `zc_${h}`
  } catch {
    return `zc_${randomUUID().slice(0, 8)}`
  }
}

/** 每一轮 user 消息的指纹序列，用来检测「历史被改写」（编辑已发消息重发）。 */
function historyFingerprint(body) {
  const msgs = Array.isArray(body?.messages) ? body.messages : []
  const parts = []
  for (const m of msgs) {
    const role = m?.role ?? '?'
    const c = m?.content
    let s
    if (typeof c === 'string') s = c
    else if (Array.isArray(c)) {
      s = c
        .map((b) => (b?.type === 'text' ? `T:${b.text ?? ''}` : b?.type === 'image' ? 'I' : `X:${b?.type ?? ''}`))
        .join('\u0001')
    } else s = JSON.stringify(c ?? '')
    parts.push(`${role}\u0002${createHash('sha1').update(s).digest('hex').slice(0, 12)}`)
  }
  return parts
}

/**
 * 判断本次请求相对上次是否「改写历史」：
 *  - 上次指纹是本次的前缀 → 纯追加（正常多轮）
 *  - 否则（某条被改/被删）→ 编辑重发，需要 fork 新会话
 */
const sessionHistory = new Map()
function detectRewrite(sessionKey, fingerprint) {
  const prev = sessionHistory.get(sessionKey)
  sessionHistory.set(sessionKey, fingerprint)
  if (!prev) return { rewrite: false }
  const isAppend =
    prev.length <= fingerprint.length && prev.every((v, i) => v === fingerprint[i])
  if (isAppend) return { rewrite: false }
  // 找出第一个不同的位置
  let at = 0
  const n = Math.min(prev.length, fingerprint.length)
  while (at < n && prev[at] === fingerprint[at]) at++
  return { rewrite: true, at, prevLen: prev.length, nextLen: fingerprint.length }
}

/** 从 ZCode 请求里抽工作目录；抽不到返回 undefined。 */
function cwdOf(body) {
  try {
    const direct = body?.metadata?.cwd
    if (typeof direct === 'string' && direct.trim()) return direct.trim()

    const sys = JSON.stringify(body?.system ?? '')
    // ZCode 的 system-reminder 里形如：Primary working directory: C:\...
    let m = sys.match(/Primary working directory:\s*([^"\\\n]+)/i)
    if (m) return m[1].trim()

    // 退一步：看整个请求体里的 working directory
    const all = JSON.stringify(body ?? {})
    m = all.match(/working directory:\s*([^"\\\n]+)/i)
    if (m) return m[1].trim()
  } catch {
    /* ignore */
  }
  return undefined
}

/**
 * ZCode 会在 user 消息前面塞 <system-reminder> / <untrusted_gen_ui_state> /
 * <environment> 这类包装层（内容是 total_tokens、环境信息等）。
 * 默认剥离这些块，只保留用户真正打的字。
 * 正文格式（缩进、空行、首尾空白）完全不动；只移除包装块本身。
 * 空值判断由调用方使用 trim 完成，返回内容始终保留原始字符。
 * STRIP_INTERNAL=0 可关闭剥离，完全原样透传。
 */
const STRIP_INTERNAL = process.env.STRIP_INTERNAL !== '0'

function stripSystemBlocks(text) {
  if (!text) return ''
  if (!STRIP_INTERNAL) return text
  let out = text
  let removed = false
  // 反复剥离：包装块可能嵌套或连续出现
  let prev
  do {
    prev = out
    for (const tag of ['system-reminder', 'untrusted_gen_ui_state', 'environment', 'total_tokens']) {
      const re = new RegExp(`<${tag}>[\\s\\S]*?<\\/${tag}>`, 'gi')
      out = out.replace(re, () => { removed = true; return '' })
    }
    // ZCode 的中断标记是客户端残留,投进 DSH 只会污染上下文和会话标题
    out = out.replace(/\[Request interrupted by user\]/gi, () => { removed = true; return '' })
  } while (out !== prev)
  // 删掉包装块会在原位留下整行空白，表现为「一进来就是一大堆空行」。
  // 只在确实剥离过块时，把 3 个以上连续换行压成一个空行；
  // 正文自身的段落格式与首尾空白仍然原样保留。
  if (removed) {
    out = out.replace(/\n(?:[ \t]*\n){2,}/g, '\n\n')
    // 被删掉的包装块通常正好占满开头的几行；连它们留下的空行一起收掉，
    // 否则 DSH 里看到的就是一条「先空十行再说正事」的消息。
    out = out.replace(/^(?:[ \t]*\n)+/, '')
    out = out.replace(/(?:\n[ \t]*)+$/, '')
  }
  // 包装块以外的原始字符全部保留，包括正文两端空白。
  return out
}

/** 把 Anthropic image 块落成临时文件，返回路径列表。 */
function saveImages(blocks) {
  const paths = []
  if (!Array.isArray(blocks)) return paths
  let i = 0
  for (const b of blocks) {
    if (b?.type !== 'image') continue
    const src = b.source ?? {}
    if (src.type !== 'base64' || typeof src.data !== 'string') continue
    const media = String(src.media_type || 'image/png')
    if (!persistState) {
      paths.push(`data:${media};base64,${src.data}`)
      continue
    }
    const ext = media.includes('jpeg') ? 'jpg' : media.includes('gif') ? 'gif' : media.includes('webp') ? 'webp' : 'png'
    try {
      mkdirSync(ATTACH_DIR, { recursive: true })
      const p = join(ATTACH_DIR, `zc-${Date.now()}-${i++}-${randomUUID().slice(0, 6)}.${ext}`)
      writeFileSync(p, Buffer.from(src.data, 'base64'))
      paths.push(p)
    } catch (e) {
      log(`  图片落盘失败: ${e.message}`)
    }
  }
  return paths
}

/** 附件名只用于临时文件名，去掉路径分隔符等不安全字符。 */
function safeName(name) {
  return String(name || 'attachment').replace(/[^A-Za-z0-9._-]/g, '_').slice(-64) || 'attachment'
}

// 这些块不携带要投给 DSH 的用户可见正文，静默跳过而不是写成占位噪声。
const NON_TEXT_BLOCKS = new Set(['image', 'thinking', 'redacted_thinking', 'tool_use', 'tool_result'])

/**
 * 非 text 块的正文提取。ZCode 若把文件作为 Anthropic document/file 块发来，
 * 文本型直接内联、二进制型落成临时文件并给出路径，避免附件被静默丢弃。
 */
function attachmentParts(block) {
  const type = block?.type
  if (NON_TEXT_BLOCKS.has(type)) return []
  const src = block?.source ?? {}
  const name = block?.name ?? block?.title ?? `${type || 'attachment'}`
  if (typeof src.data === 'string' && src.data) {
    if (src.type === 'text') return [src.data]
    if (!persistState) return [`[附件 ${name}：内存模式未落盘]`]
    try {
      mkdirSync(ATTACH_DIR, { recursive: true })
      const ext = safeName(name).includes('.') ? '' : '.bin'
      const p = join(ATTACH_DIR, `zc-att-${Date.now()}-${randomUUID().slice(0, 6)}-${safeName(name)}${ext}`)
      writeFileSync(p, Buffer.from(src.data, 'base64'))
      return [`[附件已保存：${p}]`]
    } catch (e) {
      log(`  附件落盘失败: ${e.message}`)
      return [`[附件 ${name} 落盘失败]`]
    }
  }
  if (typeof block?.text === 'string' && block.text) return [block.text]
  // 未知块不写进正文，但留一条日志便于后续定位客户端又发了什么新类型。
  log(`  跳过未知内容块 type=${type || 'unknown'}`)
  return []
}

/** 把 content（字符串或块数组）还原成正文：按原顺序取所有携带正文的块。 */
function contentToText(c) {
  if (typeof c === 'string') return c
  if (!Array.isArray(c)) return ''
  const parts = []
  for (const b of c) {
    if (!b || typeof b !== 'object') continue
    if (b.type === 'text') {
      if (typeof b.text === 'string' && b.text) parts.push(b.text)
      continue
    }
    parts.push(...attachmentParts(b))
  }
  // 只在非空片段之间补一个换行，避免空块制造出一连串空行。
  return parts.join('\n')
}

/** 解 data: URL，返回 media / 是否 base64 / 载荷。 */
function parseDataUrl(value) {
  if (typeof value !== 'string') return null
  const m = /^data:([^;,]*)(;base64)?,([\s\S]*)$/.exec(value)
  if (!m) return null
  return { media: m[1] || 'application/octet-stream', base64: Boolean(m[2]), data: m[3] }
}

/** 附件内容落盘（或内存模式只标注），返回要写进正文的那一行。 */
function attachPart(name, parsed) {
  if (!parsed) return `[附件 ${name}：未提供内容]`
  const textual =
    parsed.media.startsWith('text/') || /json|xml|markdown|javascript|yaml|csv/.test(parsed.media)
  if (textual) {
    try {
      return parsed.base64 ? Buffer.from(parsed.data, 'base64').toString('utf8') : decodeURIComponent(parsed.data)
    } catch {
      return `[附件 ${name}：文本解码失败]`
    }
  }
  if (!persistState) return `[附件 ${name}：内存模式未落盘]`
  try {
    mkdirSync(ATTACH_DIR, { recursive: true })
    const p = join(ATTACH_DIR, `zc-att-${Date.now()}-${randomUUID().slice(0, 6)}-${safeName(name)}`)
    writeFileSync(p, Buffer.from(parsed.data, 'base64'))
    return `[附件已保存：${p}]`
  } catch (e) {
    log(`  附件落盘失败: ${e.message}`)
    return `[附件 ${name} 落盘失败]`
  }
}

function localFilePart(name, filePath) {
  let p
  try {
    const url = new URL(filePath)
    if (url.protocol !== 'file:') return `[附件 ${name}：远程文件引用 ${filePath}]`
    p = fileURLToPath(url)
  } catch {
    return `[附件 ${name}：文件 URL 无效]`
  }
  try {
    const bytes = readFileSync(p)
    const slash = Math.max(p.lastIndexOf('\\'), p.lastIndexOf('/'))
    const displayName = name && name !== 'attachment' ? name : slash >= 0 ? p.slice(slash + 1) : p
    if (bytes.length <= 65536 && !bytes.subarray(0, 8192).includes(0)) {
      return `[附件 ${displayName} 内容开始]\\n${bytes.toString('utf8')}\\n[附件 ${displayName} 内容结束]`
    }
    if (!persistState) return `[附件 ${displayName}：二进制文件 ${p}（${bytes.length} 字节，内存模式未落盘）]`
    mkdirSync(ATTACH_DIR, { recursive: true })
    const out = join(ATTACH_DIR, `zc-att-${Date.now()}-${randomUUID().slice(0, 6)}-${safeName(displayName)}`)
    writeFileSync(out, bytes)
    return `[附件已保存：${out}（${bytes.length} 字节）]`
  } catch (error) {
    return `[附件 ${name}：本机文件读取失败（${error?.code || error?.message || 'unknown'}）]`
  }
}

/** Responses API 的 input 块转成正文：input_text / input_file / input_image。 */
function responsesContentTurn(content) {
  const parts = []
  const images = []
  if (typeof content === 'string') return { text: content, images }
  if (!Array.isArray(content)) return { text: '', images }
  for (const b of content) {
    if (!b || typeof b !== 'object') continue
    if (b.type === 'input_text' || b.type === 'output_text' || b.type === 'text') {
      if (typeof b.text === 'string' && b.text) parts.push(b.text)
      continue
    }
    if (b.type === 'input_image' || b.type === 'image_url') {
      const src = typeof b.image_url === 'string' ? b.image_url : b.image_url?.url
      const parsed = parseDataUrl(src)
      if (parsed) images.push(`data:${parsed.media};base64,${parsed.data}`)
      else if (typeof src === 'string' && /^https?:/i.test(src)) parts.push(`[图片链接：${src}]`)
      else parts.push('[图片：无法解析]')
      continue
    }
    if (b.type === 'input_file' || b.type === 'file') {
      const name = b.filename || b.name || b.file_id || 'attachment'
      const inline = parseDataUrl(b.file_data ?? b.file_data_url)
      if (inline) {
        parts.push(attachPart(name, inline))
      } else if (typeof b.file_url === 'string') {
        parts.push(localFilePart(name, b.file_url))
      } else if (typeof b.file_id === 'string' && b.file_id) {
        parts.push(`[附件 ${name}：客户端仅提供 file_id=${b.file_id}，当前 API 未提供下载内容]`)
        log(`  input_file 缺少可读载荷: name=${safeName(name)} file_id=${safeName(b.file_id)}`)
      } else {
        parts.push(`[附件 ${name}：未提供可读取的 file_data/file_url]`)
      }
      continue
    }
    if (NON_TEXT_BLOCKS.has(b.type)) continue
    if (typeof b.text === 'string' && b.text) parts.push(b.text)
  }
  return { text: parts.join('\n'), images }
}

/**
 * 清理 ZCode 混进正文的本地附件引用「[Image: source: 路径]」「[File: source: 路径]」。
 * - 图片：真图已随 input_image 走 images[] 通道（hasImageBlocks=true）→ 路径行是
 *   纯噪声，直接剥掉；请求里一张图块都有的情况才剥。没有图块时路径是唯一载体，
 *   读盘转成 data:URL 补进 images。
 * - 文件：路径是唯一载体（桥接与 ZCode 同机）——文本型 ≤64KB 直接内联内容，
 *   大文本/二进制保留路径并注明字节数，DSH 侧用读文件工具取。
 * 读不到的引用原样保留，绝不吞正文。
 */
function resolveLocalAttachments(text, images, hasImageBlocks) {
  if (!text || !text.includes(': source: ')) return text
  let changed = false
  const out = text.replace(/(?:\n[ \t]*)*\[(Image|File|文件|图片): source: ([^\]]*)\]/g, (all, kind, rawPath) => {
    const p = rawPath.trim()
    // 只处理本机绝对路径；其余原样保留（也许只是普通文本）
    if (!/^[A-Za-z]:[\\/]/.test(p) && !p.startsWith('/')) return all
    const isImageKind = kind === 'Image' || kind === '图片'
    if (isImageKind && hasImageBlocks) {
      changed = true
      return ''
    }
    let bytes
    try {
      bytes = readFileSync(p)
    } catch {
      return all
    }
    changed = true
    const dot = p.lastIndexOf('.')
    const ext = dot > 0 ? p.slice(dot + 1).toLowerCase() : ''
    if (isImageKind) {
      const media =
        ext === 'jpg' || ext === 'jpeg' ? 'image/jpeg'
        : ext === 'gif' ? 'image/gif'
        : ext === 'webp' ? 'image/webp'
        : 'image/png'
      images.push(`data:${media};base64,${bytes.toString('base64')}`)
      return ''
    }
    const slash = Math.max(p.lastIndexOf('\\'), p.lastIndexOf('/'))
    const name = slash >= 0 ? p.slice(slash + 1) : p
    if (!bytes.subarray(0, 8192).includes(0)) {
      if (bytes.length <= 65536) {
        return `\n[附件 ${name} 内容开始]\n${bytes.toString('utf8')}\n[附件 ${name} 内容结束]`
      }
      return `\n[附件 ${name}：本机文件 ${p}（${bytes.length} 字节，超出内联上限，请用读文件工具分次读取）]`
    }
    return `\n[附件 ${name}：二进制文件 ${p}（${bytes.length} 字节，请用读文件工具读取）]`
  })
  return changed ? out.replace(/^\n+/, '') : out
}

/** 取 Responses 请求里最后一条 user 输入。 */
// ZCode can replay Read reminders from older turns before the actual user input.
// Use the promoted input as the turn boundary and only emit text already in this request.
async function zcodeTextAttachments(input, headers) {
  const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i
  const session = headers['x-session-id']
  const query = headers['x-query-id']
  if (!zcodeDbPath || !Array.isArray(input) || typeof session !== 'string' || typeof query !== 'string' || !uuid.test(session) || !uuid.test(query)) return []
  const maxBytes = 256 * 1024
  const normalize = text => text.replace(/\r\n/g, '\n').trimEnd()
  const candidates = new Map()
  for (const item of input) {
    if (item?.role !== 'user') continue
    const texts = typeof item.content === 'string' ? [item.content] : Array.isArray(item.content) ? item.content.filter(b => b?.type === 'input_text').map(b => b.text) : []
    for (const text of texts) {
      if (typeof text !== 'string' || !text.startsWith('<system-reminder>') || text.length > maxBytes) continue
      const match = /^<system-reminder>\nCalled the Read tool with the following input: (\{[^\n]{1,4096}\})\nResult of calling the Read tool:\n([\s\S]+)\n<\/system-reminder>$/u.exec(normalize(text))
      if (!match) continue
      let selector
      try { selector = JSON.parse(match[1]) } catch { continue }
      if (typeof selector?.file_path !== 'string') continue
      const lines = match[2].split('\n')
      if (!lines.every((line, i) => line.startsWith(`${i + 1}\t`))) continue
      const content = lines.map(line => line.slice(line.indexOf('\t') + 1)).join('\n')
      if (!content.trim() || Buffer.byteLength(content) > maxBytes) continue
      const digest = createHash('sha256').update(normalize(content)).digest('hex')
      candidates.set(JSON.stringify([selector.file_path, digest]), content)
    }
  }
  if (!candidates.size) return []
  let db
  try {
    const { DatabaseSync } = await import('node:sqlite')
    if (closing) return []
    db = new DatabaseSync(zcodeDbPath, { readOnly: true })
    const rows = db.prepare(`SELECT p.data FROM session_input AS q
      JOIN part AS p ON p.session_id=q.session_id AND p.message_id=q.promoted_message_id
      WHERE q.session_id=? AND q.id=? AND q.status='promoted'
        AND length(p.data)<=? AND json_extract(p.data,'$.type')='file'
      ORDER BY p.sequence LIMIT 16`).all(`sess_${session}`, `queue_${query}`, maxBytes * 2)
    const files = []
    let bytes = 0
    for (const row of rows) {
      const part = JSON.parse(row.data)
      const preview = part.metadata?.preview?.text
      if (part.mime !== 'text/plain' || part.metadata?.storageKind !== 'inline' || typeof preview !== 'string' || typeof part.source?.path !== 'string') continue
      const digest = createHash('sha256').update(normalize(preview)).digest('hex')
      const key = JSON.stringify([part.source.path, digest])
      const content = candidates.get(key)
      if (content === undefined) continue
      candidates.delete(key)
      bytes += Buffer.byteLength(content)
      if (bytes > maxBytes) break
      const name = part.source.path.split(/[\\/]/).at(-1).replace(/[\x00-\x1f\x7f\[\]]/g, '_').slice(0, 180) || 'file.txt'
      files.push({ name, data: Buffer.from(content, 'utf8').toString('base64'), mediaType: 'text/plain; charset=utf-8' })
    }
    return files
  } catch {
    log('responses: ZCode attachment index unavailable')
    return []
  } finally {
    db?.close()
  }
}

function extractResponsesTurn(body) {
  if (typeof body?.input === 'string') {
    const images = []
    return { text: resolveLocalAttachments(stripSystemBlocks(body.input), images, false), images }
  }
  const input = body?.input
  if (!Array.isArray(input)) return { text: '', images: [] }
  for (let i = input.length - 1; i >= 0; i--) {
    const item = input[i]
    if (item?.role && item.role !== 'user') continue
    const turn = responsesContentTurn(item?.content ?? item?.text)
    let clean = stripSystemBlocks(turn.text)
    clean = resolveLocalAttachments(clean, turn.images, turn.images.length > 0)
    if (!clean.trim() && turn.images.length === 0) continue
    return { text: clean, images: turn.images }
  }
  return { text: '', images: [] }
}

/** 从 Anthropic messages 请求里抽出最后一条 user 文本 + 附件路径。 */
function extractUserTurn(body) {
  const msgs = Array.isArray(body?.messages) ? body.messages : []
  for (let i = msgs.length - 1; i >= 0; i--) {
    const m = msgs[i]
    if (m?.role !== 'user') continue
    const c = m.content
    let raw = ''
    let images = []
    if (typeof c === 'string') raw = c
    else if (Array.isArray(c)) {
      raw = contentToText(c)
      images = saveImages(c)
    }
    const clean = stripSystemBlocks(raw)
    // 剥离后为空（整条都是系统注入）就跳过，继续往前找
    if (!clean.trim() && images.length === 0) continue
    return { text: clean, images }
  }
  return { text: '', images: [] }
}

/**
 * 每个 ZCode 会话一条 Promise 链 —— 同会话串行（结果不会互相抢），
 * 不同会话并行（多会话同时干活）。
 */
const askChains = new Map()

/** 把请求交给 DSH：写 bridge 队列，等 DSH 回推结果；可被 abort 取消。 */
async function askDsh({ text, images, files, sessionId, cwd, rewrite, abort, requestId, history, state, onProgress }) {
  const run = async () => {
    if (abort.aborted || closing) throw new Error('已取消')
    const token = bridgeToken()
    if (!token) throw new Error('bridge token 缺失，先启动 zcode-bridge')
    const auth = { authorization: `Bearer ${token}` }
    state.auth = auth
    state.submitted = true
    let eventId
    try {
      const post = await fetch(`${BRIDGE}/hook`, {
        method: 'POST',
        headers: { ...auth, 'content-type': 'application/json' },
        body: JSON.stringify({
          hook_event_name: 'UserPromptSubmit', prompt: text, images,
          files: files?.length ? files : undefined,
          session_id: sessionId, cwd: cwd || undefined, request_id: requestId,
          fork: rewrite?.rewrite === true, forkAt: rewrite?.at,
          history: Array.isArray(history) && history.length > 0 ? history : undefined,
          timestamp: new Date().toISOString(), source: 'llm-endpoint',
        }),
        signal: AbortSignal.any([abort, AbortSignal.timeout(5000)]),
      })
      if (!post.ok) { await post.body?.cancel?.(); throw new Error(`bridge /hook ${post.status}`) }
      const posted = await post.json().catch(() => ({}))
      eventId = posted?.id
      state.eventId = eventId
    } catch (error) {
      if (abort.aborted || closing) {
        await notifyCancellation(state, closing ? 'endpoint-shutdown' : 'user-cancel')
        throw new Error('已取消')
      }
      throw error
    }

    const deadline = Date.now() + WAIT_MS
    while (Date.now() < deadline) {
      if (abort.aborted || closing) {
        await notifyCancellation(state, closing ? 'endpoint-shutdown' : 'user-cancel')
        throw new Error('已取消')
      }
      await waitForPoll(abort)
      if (abort.aborted || closing) continue
      try {
        const response = await fetch(`${BRIDGE}/results?forEventId=${encodeURIComponent(eventId ?? '')}&max=5`, {
          headers: auth,
          signal: AbortSignal.any([abort, AbortSignal.timeout(4000)]),
        })
        if (!response.ok) { await response.body?.cancel?.(); continue }
        const data = await response.json()
        const rows = Array.isArray(data?.results) ? data.results : []
        for (const row of rows) {
          if (row?.text || row?.reasoning) return { text: row.text || '', reasoning: row.reasoning || '' }
        }
        if (typeof onProgress === 'function') {
          try {
          const progress = await fetch(`${BRIDGE}/progress?forEventId=${encodeURIComponent(eventId ?? '')}&since=${state.progressCursor || 0}`, {
            headers: auth,
            signal: AbortSignal.any([abort, AbortSignal.timeout(4000)]),
          })
          if (progress.ok) {
            const updates = await progress.json()
            for (const update of updates.rows || []) {
              state.progressCursor = Math.max(state.progressCursor || 0, Number(update.progressSeq) || 0)
              onProgress(update)
            }
          } else await progress.body?.cancel?.()
          } catch { /* progress is best-effort; final response remains authoritative */ }
        }
      } catch { /* keep waiting unless this request was cancelled */ }
    }
    throw new Error('等待 DSH 回复超时')
  }

  const key = sessionId || 'default'
  const prev = askChains.get(key) || Promise.resolve()
  const result = prev.then(run, run)
  const next = result.catch(() => {})
  askChains.set(key, next)
  next.then(() => { if (askChains.get(key) === next) askChains.delete(key) })
  return result
}

/** Anthropic thinking 块要求的签名字段；本端不是真模型，给一个格式合法的占位值即可。 */
function thinkingSignature() {
  return Buffer.from(randomUUID().replaceAll('-', ''), 'utf8').toString('base64')
}

/**
 * 取 ZCode 侧这次请求携带的完整对话历史，用于在新会话建立时回灌到 DSH 会话。
 * 规则：逐条抽 text 块（user 侧剥掉 <system-reminder> 之类包装），跳过空条目，
 * 去掉最后一条 user（那条就是本次 prompt，另外单独投递）。不做摘要、不改写。
 */
function historyOf(body) {
  const msgs = Array.isArray(body?.messages) ? body.messages : []
  const out = []
  for (const m of msgs) {
    const role = m?.role
    if (role !== 'user' && role !== 'assistant') continue
    const c = m.content
    let raw = ''
    if (typeof c === 'string') raw = c
    else if (Array.isArray(c)) {
      raw = contentToText(c)
    }
    const text = role === 'user' ? stripSystemBlocks(raw) : raw
    if (!text || (role === 'user' && !text.trim())) continue
    out.push({ role, text })
  }
  // 最后一条 user 就是本次要投的那条，从历史里剔掉
  for (let i = out.length - 1; i >= 0; i--) {
    if (out[i].role === 'user') {
      out.splice(i, 1)
      break
    }
  }
  return out
}

/**
 * Responses API 的 input 列表还原成 {role, content:string} 消息序列：
 * user/assistant 的文本块（user 侧剥包装层），function/reasoning 等非文本项跳过。
 * 供 sessionKey 稳定化与 history 回灌共用；空文本 user 项直接丢弃。
 */
function responsesMessagesOf(body) {
  const input = Array.isArray(body?.input) ? body.input : []
  const out = []
  for (const item of input) {
    if (!item || typeof item !== 'object') continue
    const role = item.role === 'assistant' ? 'assistant' : item.role === 'user' ? 'user' : null
    if (!role) continue
    const raw = responsesContentTurn(item.content ?? item.text).text
    const text = role === 'user' ? stripSystemBlocks(raw) : raw
    if (!text || (role === 'user' && !text.trim())) continue
    out.push({ role, content: text })
  }
  return out
}

/** 包装成 Anthropic message；思考内容作为 thinking 块一并返回。 */
function anthropicMessage(payload, model) {
  const text = payload?.text || ''
  const reasoning = payload?.reasoning || ''
  const content = []
  // Anthropic 协议里 thinking 块必须带 signature，缺了客户端会丢弃或报错。
  if (reasoning) content.push({ type: 'thinking', thinking: reasoning, signature: thinkingSignature() })
  if (text) content.push({ type: 'text', text })
  if (content.length === 0) content.push({ type: 'text', text: '' })
  return {
    id: `msg_${randomUUID().replaceAll('-', '').slice(0, 24)}`,
    type: 'message',
    role: 'assistant',
    model: model || 'dsh-harness',
    content,
    stop_reason: 'end_turn',
    stop_sequence: null,
    usage: {
      input_tokens: 1,
      output_tokens: Math.max(1, Math.ceil((text.length + reasoning.length) / 4)),
    },
  }
}

async function handle(req, res) {
  if (closing) return json(res, 503, { type: 'error', error: { type: 'api_error', message: 'endpoint is closing' } })
  let url
  try {
    url = new URL(req.url || '/', `http://${HOST}:${PORT}`)
  } catch {
    return json(res, 400, { type: 'error', error: { type: 'invalid_request_error', message: 'bad url' } })
  }
  log(`${req.method} ${url.pathname}`)

  if (url.pathname === '/health') {
    return json(res, 200, { ok: true, service: 'dsh-llm-endpoint', bridge: BRIDGE, sessions: askChains.size, zcodeTextAttachmentReplay: Boolean(zcodeDbPath) })
  }

  // Anthropic 兼容：/v1/messages
  if (url.pathname === '/v1/messages' && req.method === 'POST') {
    let body
    try {
      body = await readBody(req)
    } catch (e) {
      return json(res, 400, { type: 'error', error: { type: 'invalid_request_error', message: e.message } })
    }

    const sessionKey = sessionKeyOf(body)
    const fp = historyFingerprint(body)
    const rewrite = detectRewrite(sessionKey, fp)
    const { text, images } = extractUserTurn(body)
    const wantStream = body?.stream === true
    const requestId = `req_${randomUUID().replaceAll('-', '').slice(0, 16)}`

    log(
      `messages: model=${body?.model} stream=${wantStream} textLen=${text.length}` +
        ` imgs=${images.length} session=${sessionKey}` +
        (rewrite.rewrite ? ` REWRITE(at=${rewrite.at} ${rewrite.prevLen}->${rewrite.nextLen})` : ''),
    )

    try {
      const msgs = Array.isArray(body?.messages) ? body.messages : []
      const kinds = new Set()
      for (const m of msgs) {
        if (Array.isArray(m?.content)) for (const b of m.content) if (b?.type) kinds.add(b.type)
        else if (typeof m?.content === 'string') kinds.add('string')
      }
      log(`  probe: msgCount=${msgs.length} | blockTypes=${[...kinds].join(',')} | tools=${Array.isArray(body?.tools) ? body.tools.length : 0}`)
    } catch {
      /* 探测失败不影响主流程 */
    }

    if (!text.trim() && images.length === 0) {
      return json(res, 400, { type: 'error', error: { type: 'invalid_request_error', message: '未找到用户消息' } })
    }

    // ---- 停止生成：客户端断开就取消 ----
    const state = trackRequest(res, requestId, sessionKey)
    const abort = state.controller
    let streamedText = ''
    let streamStarted = false
    let nextBlockIndex = 0
    let summaryBlock = -1
    let textBlock = -1
    const startStream = () => {
      if (streamStarted || !wantStream || res.writableEnded || res.destroyed) return
      streamStarted = true
      res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache', connection: 'keep-alive' })
      res.write('event: message_start\ndata: ' + JSON.stringify({ type: 'message_start', message: { id: 'msg_' + randomUUID().replaceAll('-', '').slice(0, 16), type: 'message', role: 'assistant', model: body?.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 1, output_tokens: 0 } } }) + '\n\n')
      // 2026-11-19 重构:块号动态分配,进度叙述/正文/思考各占独立块——
      // 旧实现叙述与正文共用块 0,finalize 的思考块撞号(0 重复)且叙述块永不闭合。
    }
    const writeBlockStart = (idx, type) => {
      if (res.writableEnded || res.destroyed) return
      res.write('event: content_block_start\ndata: ' + JSON.stringify({ type: 'content_block_start', index: idx, content_block: { type, text: '' } }) + '\n\n')
    }
    const writeDelta = (idx, delta) => {
      if (res.writableEnded || res.destroyed) return
      res.write('event: content_block_delta\ndata: ' + JSON.stringify({ type: 'content_block_delta', index: idx, delta }) + '\n\n')
    }
    const onProgress = (update) => {
      if (!wantStream || res.writableEnded || res.destroyed) return
      startStream()
      // 正文独占自己的块:进度文本是累计前缀时增量下发
      if (typeof update.text === 'string' && update.text.startsWith(streamedText) && update.text.length > streamedText.length) {
        if (textBlock < 0) { textBlock = nextBlockIndex++; writeBlockStart(textBlock, 'text') }
        writeDelta(textBlock, { type: 'text_delta', text: update.text.slice(streamedText.length) })
        streamedText = update.text
      }
      // 进程叙述([DSH] 正在执行工具…)进独立块,绝不与正文混流
      for (const summary of Array.isArray(update.summaries) ? update.summaries : []) {
        if (summaryBlock < 0) { summaryBlock = nextBlockIndex++; writeBlockStart(summaryBlock, 'text') }
        writeDelta(summaryBlock, { type: 'text_delta', text: `[DSH] ${summary}\n` })
      }
    }

    let answer
    try {
      answer = await askDsh({
        text,
        images,
        sessionId: sessionKey,
        cwd: cwdOf(body),
        rewrite,
        abort: abort.signal,
        requestId,
        history: historyOf(body),
        state,
        onProgress,
      })
    } catch (e) {
      log(`DSH 失败: ${e.message}`)
      if (abort.aborted || /已取消/.test(e.message)) {
        // ZCode 那边已经断开，不用再写任何东西
        return
      }
      answer = { text: `[DSH 未返回] ${e.message}`, reasoning: '' }
    } finally {
      state.cleanup()
    }
    log(`回复: text=${(answer.text || '').length} reasoning=${(answer.reasoning || '').length}`)

    const msg = anthropicMessage(answer, body?.model)

    if (!wantStream) return json(res, 200, msg)

    if (res.writableEnded || res.destroyed) return
    const send = (evt, data) => {
      if (!res.writableEnded && !res.destroyed) res.write(`event: ${evt}\ndata: ${JSON.stringify(data)}\n\n`)
    }
    const CH = 400
    const emitThinkingBlock = (idx) => {
      send('content_block_start', {
        type: 'content_block_start',
        index: idx,
        content_block: { type: 'thinking', thinking: '' },
      })
      for (let i = 0; i < answer.reasoning.length; i += CH) {
        send('content_block_delta', {
          type: 'content_block_delta',
          index: idx,
          delta: { type: 'thinking_delta', thinking: answer.reasoning.slice(i, i + CH) },
        })
      }
      // thinking 块收尾前必须给签名，否则客户端按协议会判为非法块。
      send('content_block_delta', {
        type: 'content_block_delta',
        index: idx,
        delta: { type: 'signature_delta', signature: thinkingSignature() },
      })
      send('content_block_stop', { type: 'content_block_stop', index: idx })
    }
    const emitTextBlock = (idx, body) => {
      send('content_block_start', {
        type: 'content_block_start',
        index: idx,
        content_block: { type: 'text', text: '' },
      })
      for (let i = 0; i < body.length; i += CH) {
        send('content_block_delta', {
          type: 'content_block_delta',
          index: idx,
          delta: { type: 'text_delta', text: body.slice(i, i + CH) },
        })
      }
      send('content_block_stop', { type: 'content_block_stop', index: idx })
    }
    if (!streamStarted) startStream()
    // 2026-11-19 收尾(动态块号):先闭合进度叙述块,再按标准顺序补思考块与正文块。
    // 已流出的正文块原地补尾;进度文本与最终正文分叉(理论罕见)时另起完整正文块。
    if (summaryBlock >= 0) {
      send('content_block_stop', { type: 'content_block_stop', index: summaryBlock })
      summaryBlock = -1
    }
    const streamedAll = answer.text.startsWith(streamedText)
    const hasStreamedText = textBlock >= 0
    let idx = nextBlockIndex
    if (answer.reasoning) { emitThinkingBlock(idx); idx += 1 }
    if (hasStreamedText && streamedAll) {
      const tail = answer.text.slice(streamedText.length)
      if (tail) writeDelta(textBlock, { type: 'text_delta', text: tail })
      send('content_block_stop', { type: 'content_block_stop', index: textBlock })
    } else if (hasStreamedText) {
      send('content_block_stop', { type: 'content_block_stop', index: textBlock })
      emitTextBlock(idx, answer.text || '')
      idx += 1
    } else {
      emitTextBlock(idx, answer.text || '')
      idx += 1
    }

    send('message_delta', {
      type: 'message_delta',
      delta: { stop_reason: 'end_turn', stop_sequence: null },
      usage: { output_tokens: msg.usage.output_tokens },
    })
    send('message_stop', { type: 'message_stop' })
    if (!res.writableEnded) res.end()
    return
  }

  // OpenAI Responses API：ZCode 会探测这个形状，input_file / input_image 才带得动文件
  if ((url.pathname === '/responses' || url.pathname === '/v1/responses') && req.method === 'POST') {
    let body
    try {
      body = await readBody(req)
    } catch (e) {
      return json(res, 400, { error: { message: e.message } })
    }
    const turn = extractResponsesTurn(body)
    // Keep ZCode's original conversation fingerprint independent of the recovered attachment.
    const files = await zcodeTextAttachments(body?.input, req.headers)
    const forwardedText = files.length && turn.text === '(no content)' ? '' : turn.text
    // 会话键必须锚定整段对话（首条 user 文本），不能只锚最后一轮——
    // 旧实现每轮都换 key，DSH 侧每轮新建会话，多轮上下文全丢。
    const convMsgs = responsesMessagesOf(body)
    const pseudo = {
      messages: convMsgs.length > 0 ? convMsgs : [{ role: 'user', content: turn.text }],
      system: body?.instructions,
    }
    const sessionKey = sessionKeyOf(pseudo)
    const rewrite = detectRewrite(sessionKey, historyFingerprint(pseudo))
    const requestId = `req_${randomUUID().replaceAll('-', '').slice(0, 16)}`
    log(`responses: model=${body?.model} stream=${body?.stream === true} textLen=${turn.text.length} imgs=${turn.images.length} msgs=${convMsgs.length} session=${sessionKey}`)
    try {
      const items = Array.isArray(body?.input) ? body.input : []
      const kinds = new Set()
      for (const item of items) {
        if (Array.isArray(item?.content)) {
          for (const b of item.content) if (b?.type) kinds.add(b.type)
        } else kinds.add(typeof item?.content === 'string' ? 'string' : item?.type || 'item')
      }
      log(`  responses-probe: items=${items.length} | blockTypes=${[...kinds].join(',')}`)
    } catch {
      /* 探测失败不影响主流程 */
    }
    if (!forwardedText.trim() && turn.images.length === 0 && files.length === 0) {
      return json(res, 400, { error: { message: '未找到用户输入' } })
    }
    if (files.length) log(`responses: recovered ZCode text attachments=${files.length}`)
    const state = trackRequest(res, requestId, sessionKey)
    const outputId = `msg_${randomUUID().replaceAll('-', '').slice(0, 16)}`
    const responseId = `resp_${randomUUID().replaceAll('-', '').slice(0, 16)}`
    const stream = body?.stream === true
    let streamedText = ''
    let sendProgress
    if (stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache', connection: 'keep-alive' })
      sendProgress = (update) => {
        if (res.writableEnded || !res.writable) return
        if (typeof update.text === 'string' && update.text.startsWith(streamedText)) {
          const delta = update.text.slice(streamedText.length)
          streamedText = update.text
          if (delta) res.write(`event: response.output_text.delta\ndata: ${JSON.stringify({ type: 'response.output_text.delta', item_id: outputId, output_index: 0, content_index: 0, delta })}\n\n`)
        }
        if (Array.isArray(update.events) && update.events.some(type => type.startsWith('tool/'))) {
          res.write(`event: response.in_progress\ndata: ${JSON.stringify({ type: 'response.in_progress', response_id: responseId, dsh_progress: { turn: update.turn, events: update.events, summaries: update.summaries || [] } })}\n\n`)
        }
      }
      res.write(`event: response.created\ndata: ${JSON.stringify({ type: 'response.created', response: { id: responseId, object: 'response', status: 'in_progress', output: [] } })}\n\n`)
    }
    let answer
    try {
      answer = await askDsh({
        text: forwardedText,
        images: turn.images,
        files,
        sessionId: sessionKey,
        cwd: cwdOf(body),
        rewrite,
        abort: state.controller.signal,
        requestId,
        history: historyOf(pseudo),
        state,
        onProgress: sendProgress,
      })
    } catch (e) {
      if (state.controller.signal.aborted || closing) return
      answer = { text: `[DSH 未返回] ${e.message}`, reasoning: '' }
    } finally {
      state.cleanup()
    }
    if (stream && answer.text.startsWith(streamedText)) {
      const delta = answer.text.slice(streamedText.length)
      if (delta && !res.writableEnded) res.write(`event: response.output_text.delta\ndata: ${JSON.stringify({ type: 'response.output_text.delta', item_id: outputId, output_index: 0, content_index: 0, delta })}\n\n`)
      streamedText = answer.text
    }
    const payload = {
      id: responseId,
      object: 'response',
      created_at: Math.floor(Date.now() / 1000),
      status: 'completed',
      model: body?.model || 'dsh-harness',
      output: [
        {
          type: 'message',
          id: outputId,
          status: 'completed',
          role: 'assistant',
          content: [{ type: 'output_text', text: answer.text || '', annotations: [] }],
        },
      ],
      usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 },
    }
    if (!stream) return json(res, 200, payload)
    const sendEvent = (obj) => res.write(`event: ${obj.type}
data: ${JSON.stringify(obj)}

`)

    if (answer.text) sendEvent({ type: 'response.output_text.done', item_id: outputId, output_index: 0, content_index: 0, text: answer.text })
    sendEvent({ type: 'response.completed', response: payload })
    res.write('data: [DONE]\n\n')
    res.end()
    return
  }

  // 模型列表：让客户端能确认可用模型，而不是收到 404
  if ((url.pathname === '/models' || url.pathname === '/v1/models') && req.method === 'GET') {
    return json(res, 200, {
      object: 'list',
      data: [{ id: 'dsh-harness', object: 'model', type: 'model', display_name: 'DSH Harness', created: 0 }],
    })
  }

  // OpenAI 兼容：/chat/completions 与 /v1/chat/completions，支持块数组与附件
  if ((url.pathname === '/chat/completions' || url.pathname === '/v1/chat/completions') && req.method === 'POST') {
    let body
    try {
      body = await readBody(req)
    } catch (e) {
      return json(res, 400, { error: { message: e.message } })
    }
    const msgs = Array.isArray(body?.messages) ? body.messages : []
    let text = ''
    let images = []
    for (let i = msgs.length - 1; i >= 0; i--) {
      if (msgs[i]?.role !== 'user') continue
      const content = msgs[i].content
      const clean = stripSystemBlocks(contentToText(content))
      const imgs = saveImages(content)
      if (!clean.trim() && imgs.length === 0) continue
      text = clean
      images = imgs
      break
    }
    log(`chat/completions: model=${body?.model} textLen=${text.length} imgs=${images.length}`)
    if (!text.trim() && images.length === 0) {
      return json(res, 400, { error: { message: '未找到用户消息' } })
    }
    const sessionKey = sessionKeyOf(body)
    const rewrite = detectRewrite(sessionKey, historyFingerprint(body))
    const requestId = `req_${randomUUID().replaceAll('-', '').slice(0, 16)}`
    const state = trackRequest(res, requestId, sessionKey)
    let answer
    try {
      answer = await askDsh({
        text,
        images,
        sessionId: sessionKey,
        cwd: cwdOf(body),
        rewrite,
        abort: state.controller.signal,
        requestId,
        history: historyOf(body),
        state,
      })
    } catch (e) {
      if (state.controller.signal.aborted || closing) return
      answer = { text: `[DSH 未返回] ${e.message}`, reasoning: '' }
    } finally {
      state.cleanup()
    }
    return json(res, 200, {
      id: `chatcmpl-${randomUUID().slice(0, 8)}`,
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model: body?.model || 'dsh-harness',
      choices: [
        {
          index: 0,
          message: {
            role: 'assistant',
            content: answer.text || '',
            ...(answer.reasoning ? { reasoning_content: answer.reasoning } : {}),
          },
          finish_reason: 'stop',
        },
      ],
      usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
    })
  }

  log(`未实现的路由: ${req.method} ${url.pathname}`)
  json(res, 404, { type: 'error', error: { type: 'not_found_error', message: `未实现: ${url.pathname}` } })
}

const server = createServer((req, res) => {
  handle(req, res).catch((err) => {
    log(`handler error: ${err?.message ?? err}`)
    try {
      json(res, 500, { type: 'error', error: { type: 'api_error', message: String(err?.message ?? err) } })
    } catch {
      /* ignore */
    }
  })
})

server.on('connection', (socket) => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)) })
server.on('clientError', (_error, socket) => {
  if (socket.writable) socket.end('HTTP/1.1 400 Bad Request\r\n\r\n')
})
server.on('error', (error) => log(`server error: ${error?.code} ${error?.message}`))

async function close() {
  if (closePromise) return closePromise
  closing = true
  signal?.removeEventListener('abort', onAbort)
  closePromise = (async () => {
    const requests = [...pendingRequests.values()]
    for (const state of requests) state.controller.abort(new Error('endpoint-shutdown'))
    // The receiver stays alive until these short cancellation notices finish.
    await Promise.allSettled(requests.map((state) => notifyCancellation(state, 'endpoint-shutdown')))
    await new Promise((resolve, reject) => {
      const done = (error) => {
        if (error && error.code !== 'ERR_SERVER_NOT_RUNNING') reject(error)
        else resolve()
      }
      try { server.close(done) } catch (error) { done(error) }
      server.closeIdleConnections?.()
      server.closeAllConnections?.()
      for (const socket of sockets) socket.destroy()
    })
    pendingRequests.clear()
    askChains.clear()
    sessionHistory.clear()
  })()
  return closePromise
}
const onAbort = () => { void close().catch((error) => log(`close failed: ${error.message}`)) }
signal?.addEventListener('abort', onAbort, { once: true })
try {
  signal?.throwIfAborted()
  await new Promise((resolve, reject) => {
    const cleanup = () => {
      server.off('listening', onListening)
      server.off('error', onError)
      signal?.removeEventListener('abort', onStartingAbort)
    }
    const onListening = () => { cleanup(); resolve() }
    const onError = (error) => { cleanup(); reject(error) }
    const onStartingAbort = () => { cleanup(); reject(signal.reason ?? new Error('endpoint start aborted')) }
    server.once('listening', onListening)
    server.once('error', onError)
    signal?.addEventListener('abort', onStartingAbort, { once: true })
    if (signal?.aborted) return onStartingAbort()
    try { server.listen({ host: HOST, port: PORT, ...(signal ? { signal } : {}) }) } catch (error) { onError(error) }
  })
  signal?.throwIfAborted()
} catch (error) {
  await close().catch(() => {})
  throw error
}
const address = server.address()
if (!address || typeof address === 'string') { await close(); throw new Error('endpoint listener unavailable') }
const urlHost = HOST.includes(':') ? `[${HOST}]` : HOST
const url = `http://${urlHost}:${address.port}`
log(`[dsh-llm-endpoint] listening ${url} bridge=${BRIDGE}`)
return { server, host: HOST, port: address.port, url, close }
}

const isMain = Boolean(process.argv?.[1] && pathToFileURL(process.argv[1]).href === import.meta.url)
if (isMain) {
  startEndpoint({ logger: console }).then((service) => {
    const stop = () => { void service.close().catch((error) => console.error(error)) }
    process.once('SIGINT', stop)
    process.once('SIGTERM', stop)
  }).catch((error) => { console.error(`[dsh-llm-endpoint] ${error?.message ?? error}`); process.exitCode = 1 })
}
