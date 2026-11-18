// Instance-owned local bridge. Importing this module performs no I/O or listening.
import { createServer } from 'node:http'
import { randomUUID, timingSafeEqual } from 'node:crypto'
import { appendFile, mkdirSync, readFileSync, writeFileSync, existsSync, renameSync, statSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { homedir } from 'node:os'
import { pathToFileURL } from 'node:url'

export async function startBridge(options = {}) {
  const signal = options.signal
  signal?.throwIfAborted()
  const PORT = Number(options.port ?? (process.env.ZCODE_BRIDGE_PORT || 47821))
  const HOST = options.host ?? (process.env.ZCODE_BRIDGE_HOST || '127.0.0.1')
  if (typeof HOST !== 'string' || !HOST) throw new TypeError('invalid bridge host')
  if (!Number.isSafeInteger(PORT) || PORT < 0 || PORT > 65535) throw new TypeError('invalid bridge port')
  const persistState = options.persistState !== false
  const STATE_DIR = options.stateDir ?? process.env.ZCODE_BRIDGE_DIR ?? join(homedir(), '.zcode-bridge')
  const TOKEN_FILE = options.tokenFile ?? join(STATE_DIR, 'token')
  const LOG_FILE = join(STATE_DIR, 'events.jsonl')
  const LOG_MAX_BYTES = 8 * 1024 * 1024
  const LEASE_MS = 60_000
  const MAX_PROMPT_CHARS = 2_000_000
  const emit = (level, message) => {
    try {
      if (typeof options.logger === 'function') options.logger(message)
      else options.logger?.[level]?.(message)
    } catch { /* logging does not own the service lifecycle */ }
  }

  // In memory mode even an explicitly supplied tokenFile is never inspected.
  let TOKEN = typeof options.token === 'string' ? options.token.trim() : undefined
  if (options.token !== undefined && !TOKEN) throw new TypeError('invalid bridge token')
  if (persistState) {
    mkdirSync(STATE_DIR, { recursive: true })
    if (TOKEN === undefined && existsSync(TOKEN_FILE)) TOKEN = readFileSync(TOKEN_FILE, 'utf8').trim()
  }
  if (TOKEN === undefined) TOKEN = randomUUID().replaceAll('-', '')
  if (!TOKEN) throw new Error('bridge token is empty')
  if (persistState && (!existsSync(TOKEN_FILE) || options.token !== undefined)) {
    mkdirSync(dirname(TOKEN_FILE), { recursive: true })
    writeFileSync(TOKEN_FILE, TOKEN, { mode: 0o600 })
  }
  const TOKEN_BUF = Buffer.from(TOKEN)
  const inbound = []
  const outbound = []
  const progressLog = []
  let progressSeq = 0
  const statuses = new Map()
  const statusLog = []
  let seq = 0
  let statusSeq = 0
  let lastPluginStatus = null
  let logBytes = 0
  if (persistState) {
    try { logBytes = existsSync(LOG_FILE) ? statSync(LOG_FILE).size : 0 } catch { /* start with a fresh counter */ }
  }
  let closing = false
  let closePromise
  const sockets = new Set()

  function json(res, code, body) {
    if (res.writableEnded || res.destroyed) return
    const data = JSON.stringify(body)
    res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(data) })
    res.end(data)
  }

  function readBody(req, limit = 4 * 1024 * 1024, timeoutMs = 10_000) {
    return new Promise((resolve, reject) => {
      let settled = false
      const chunks = []
      let size = 0
      const finish = (fn, value) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        fn(value)
      }
      const fail = (error) => finish(reject, error)
      const timer = setTimeout(() => {
        fail(new Error('body timeout'))
        req.destroy()
      }, timeoutMs)
      req.on('data', (chunk) => {
        size += chunk.length
        if (size > limit) {
          chunks.length = 0
          fail(new Error('body too large'))
          req.destroy()
          return
        }
        chunks.push(chunk)
      })
      req.on('end', () => {
        const raw = Buffer.concat(chunks).toString('utf8')
        if (!raw) return finish(resolve, {})
        try { finish(resolve, JSON.parse(raw)) } catch (error) { fail(new Error('invalid json: ' + error.message)) }
      })
      req.on('aborted', () => fail(new Error('aborted')))
      req.on('close', () => { if (!req.complete) fail(new Error('client closed early')) })
      req.on('error', fail)
    })
  }

  function authorized(req) {
    const header = req.headers.authorization || ''
    const token = header.startsWith('Bearer ') ? header.slice(7) : ''
    const value = Buffer.from(token)
    return value.length === TOKEN_BUF.length && timingSafeEqual(value, TOKEN_BUF)
  }

  function logEvent(rec) {
    if (!persistState) return
    const slim = {
      id: rec.id, kind: rec.kind, sessionId: rec.sessionId, turnId: rec.turnId,
      timestamp: rec.timestamp, promptLen: rec.prompt ? rec.prompt.length : 0,
    }
    const line = JSON.stringify(slim) + '\n'
    logBytes += Buffer.byteLength(line)
    if (logBytes > LOG_MAX_BYTES) {
      try { renameSync(LOG_FILE, LOG_FILE + '.1') } catch { /* optional metadata log */ }
      logBytes = 0
    }
    appendFile(LOG_FILE, line, () => {})
  }

  async function handle(req, res) {
    let url
    try { url = new URL(req.url || '/', 'http://127.0.0.1') } catch { return json(res, 400, { ok: false, error: 'bad url' }) }
    if (closing) return json(res, 503, { ok: false, error: 'bridge is closing' })
    if (url.pathname === '/health') return json(res, 200, { ok: true, service: 'zcode-bridge' })
    if (!authorized(req)) return json(res, 401, { ok: false, error: 'bad token' })

    if (url.pathname === '/hook' && req.method === 'POST') {
      let body
      try { body = await readBody(req, 16 * 1024 * 1024, 30_000) } catch (error) { return json(res, 400, { ok: false, error: error.message }) }
      const evt = String(body.hook_event_name || body.hookEventName || '')
      let files
      if (evt === 'UserPromptSubmit' && body.files !== undefined) {
        const limit = 256 * 1024
        const input = body.files
        if (!Array.isArray(input) || input.length > 16) return json(res, 400, { ok: false, error: 'invalid files' })
        let bytes = 0
        files = []
        for (const file of input) {
          if (typeof file?.name !== 'string' || !file.name.trim() || file.name.length > 180 || /[\\/\x00-\x1f\x7f]/.test(file.name) ||
              file.mediaType !== 'text/plain; charset=utf-8' || typeof file.data !== 'string' || file.data.length > Math.ceil(limit / 3) * 4) {
            return json(res, 400, { ok: false, error: 'invalid file' })
          }
          const data = Buffer.from(file.data, 'base64')
          bytes += data.length
          if (bytes > limit || data.toString('base64') !== file.data) return json(res, 400, { ok: false, error: 'invalid file payload' })
          files.push({ name: file.name, data: file.data, mediaType: file.mediaType })
        }
      }
      const rec = {
        id: `${Date.now()}-${++seq}`,
        kind: evt,
        prompt: typeof body.prompt === 'string' ? body.prompt.slice(0, MAX_PROMPT_CHARS) : undefined,
        images: Array.isArray(body.images) ? body.images.filter((path) => typeof path === 'string') : undefined,
        files,
        sessionId: body.session_id || body.sessionId,
        turnId: body.turnId,
        cwd: body.cwd,
        requestId: body.request_id,
        fork: body.fork === true,
        forkAt: Number.isFinite(Number(body.forkAt)) ? Number(body.forkAt) : undefined,
        history: Array.isArray(body.history)
          ? body.history.filter((item) => item && (item.role === 'user' || item.role === 'assistant') && typeof item.text === 'string' && item.text)
            .map((item) => ({ role: item.role, text: item.text }))
          : undefined,
        cancelled: false,
        timestamp: body.timestamp || new Date().toISOString(),
        source: body.source,
        attachmentsSummary: body.attachmentsSummary,
        responseText: typeof body.responseText === 'string' ? body.responseText : undefined,
        leasedUntil: 0,
      }
      if (evt === 'UserPromptSubmit' || evt === 'Stop' || evt === 'SessionStart') {
        inbound.push(rec)
        logEvent(rec)
        while (inbound.length > 200) inbound.shift()
      }
      return json(res, 200, { ok: true, id: rec.id, kind: rec.kind })
    }

    if (url.pathname === '/pull' && req.method === 'GET') {
      const max = Math.min(Math.max(Number(url.searchParams.get('max')) || 20, 1), 100)
      const now = Date.now()
      const batch = []
      for (const rec of inbound) {
        if (batch.length >= max) break
        if (rec.leasedUntil && rec.leasedUntil > now) continue
        rec.leasedUntil = now + LEASE_MS
        batch.push(rec)
      }
      return json(res, 200, { ok: true, events: batch, leaseMs: LEASE_MS })
    }

    if (url.pathname === '/ack' && req.method === 'POST') {
      let body
      try { body = await readBody(req) } catch (error) { return json(res, 400, { ok: false, error: error.message }) }
      const ids = new Set(Array.isArray(body?.ids) ? body.ids : [])
      let removed = 0
      for (let i = inbound.length - 1; i >= 0; i--) {
        if (ids.has(inbound[i].id)) { inbound.splice(i, 1); removed++ }
      }
      return json(res, 200, { ok: true, acked: removed })
    }

    if (url.pathname === '/cancel' && req.method === 'POST') {
      let body
      try { body = await readBody(req) } catch (error) { return json(res, 400, { ok: false, error: error.message }) }
      const eventId = String(body?.eventId || '')
      const requestId = String(body?.requestId || '')
      let hit = 0
      for (const rec of inbound) {
        if ((eventId && rec.id === eventId) || (requestId && rec.requestId === requestId)) {
          rec.cancelled = true
          rec.cancelReason = String(body?.reason || 'user-cancel')
          rec.cancelledAt = new Date().toISOString()
          hit++
        }
      }
      const cancelRec = {
        id: `${Date.now()}-${++seq}`, kind: 'Cancel', targetEventId: eventId, targetRequestId: requestId,
        reason: String(body?.reason || 'user-cancel'), sessionId: body?.sessionId,
        timestamp: new Date().toISOString(), leasedUntil: 0,
      }
      if (eventId || requestId) inbound.push(cancelRec)
      return json(res, 200, { ok: true, marked: hit, cancelEventId: cancelRec.id })
    }

    if (url.pathname === '/status' && req.method === 'POST') {
      let body
      try { body = await readBody(req) } catch (error) { return json(res, 400, { ok: false, error: error.message }) }
      const messageId = String(body.messageId || '')
      if (messageId) {
        const rec = { ...body, seq: ++statusSeq, at: body.at || new Date().toISOString() }
        statuses.set(messageId, rec)
        statusLog.push(rec)
        while (statusLog.length > 1000) statusLog.shift()
        if (statuses.size > 500) for (const key of [...statuses.keys()].slice(0, 200)) statuses.delete(key)
      }
      return json(res, 200, { ok: true })
    }
    if (url.pathname === '/status' && req.method === 'GET') {
      const since = Number(url.searchParams.get('since') || 0)
      return json(res, 200, { ok: true, cursor: statusSeq, rows: statusLog.filter((rec) => rec.seq > since) })
    }
    if (url.pathname === '/plugin-status' && req.method === 'POST') {
      let body
      try { body = await readBody(req) } catch (error) { return json(res, 400, { ok: false, error: error.message }) }
      lastPluginStatus = { ...body, at: new Date().toISOString() }
      return json(res, 200, { ok: true })
    }
    if (url.pathname === '/plugin-status' && req.method === 'GET') {
      return json(res, 200, {
        ok: true, status: lastPluginStatus,
        counts: { inbound: inbound.length, outbound: outbound.length, statuses: statuses.size },
      })
    }
    if (url.pathname === '/progress' && req.method === 'POST') {
      let body
      try { body = await readBody(req, 256 * 1024, 10_000) } catch (error) { return json(res, 400, { ok: false, error: error.message }) }
      if (typeof body.eventId !== 'string' || typeof body.messageId !== 'string' || !Number.isSafeInteger(body.turn) || !Number.isSafeInteger(body.seq)) {
        return json(res, 400, { ok: false, error: 'invalid progress event' })
      }
      const rec = { ...body, progressSeq: ++progressSeq, at: new Date().toISOString() }
      progressLog.push(rec)
      while (progressLog.length > 2000) progressLog.shift()
      return json(res, 200, { ok: true, seq: rec.progressSeq })
    }
    if (url.pathname === '/progress' && req.method === 'GET') {
      const eventId = url.searchParams.get('forEventId') || ''
      const since = Number(url.searchParams.get('since') || 0)
      const rows = progressLog.filter(rec => rec.eventId === eventId && rec.progressSeq > since)
      const cursor = rows.at(-1)?.progressSeq ?? since
      return json(res, 200, { ok: true, cursor, rows })
    }
    if (url.pathname === '/push-result' && req.method === 'POST') {
      let body
      try { body = await readBody(req, 64 * 1024 * 1024, 60_000) } catch (error) { return json(res, 400, { ok: false, error: error.message }) }
      const rec = {
        id: `${Date.now()}-r${++seq}`, text: String(body.text || ''), reasoning: String(body.reasoning || ''),
        sessionId: body.sessionId, forEventId: body.forEventId, messageId: body.messageId,
        timestamp: new Date().toISOString(),
      }
      outbound.push(rec)
      while (outbound.length > 200) outbound.shift()
      return json(res, 200, { ok: true, id: rec.id })
    }
    if (url.pathname === '/results' && req.method === 'GET') {
      const max = Math.min(Number(url.searchParams.get('max') || 20), 100)
      const forEventId = url.searchParams.get('forEventId') || ''
      if (forEventId) {
        const picked = []
        for (let i = 0; i < outbound.length && picked.length < max; i++) {
          if (outbound[i]?.forEventId === forEventId) { picked.push(outbound[i]); outbound.splice(i, 1); i-- }
        }
        return json(res, 200, { ok: true, results: picked })
      }
      return json(res, 200, { ok: true, results: outbound.splice(0, max) })
    }
    if (url.pathname === '/results/drain' && req.method === 'POST') {
      const count = outbound.length
      outbound.length = 0
      return json(res, 200, { ok: true, drained: count })
    }
    json(res, 404, { ok: false, error: 'not found' })
  }

  const server = createServer((req, res) => {
    handle(req, res).catch((error) => {
      emit('error', `[zcode-bridge] handler failed: ${error?.message ?? error}`)
      json(res, 500, { ok: false, error: 'internal error' })
    })
  })
  server.on('connection', (socket) => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)) })
  server.on('clientError', (_error, socket) => { if (socket.writable) socket.end('HTTP/1.1 400 Bad Request\r\n\r\n') })
  server.on('error', (error) => emit('error', `[zcode-bridge] server error: ${error?.code || ''} ${error?.message || error}`))

  async function close() {
    if (closePromise) return closePromise
    closing = true
    signal?.removeEventListener('abort', onAbort)
    closePromise = new Promise((resolve, reject) => {
      const done = (error) => {
        if (error && error.code !== 'ERR_SERVER_NOT_RUNNING') reject(error)
        else resolve()
      }
      try { server.close(done) } catch (error) { done(error) }
      server.closeIdleConnections?.()
      server.closeAllConnections?.()
      for (const socket of sockets) socket.destroy()
    })
    return closePromise
  }
  const onAbort = () => { void close().catch((error) => emit('error', `[zcode-bridge] close: ${error.message}`)) }
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
      const onStartingAbort = () => { cleanup(); reject(signal.reason ?? new Error('bridge start aborted')) }
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
  if (!address || typeof address === 'string') { await close(); throw new Error('bridge listener unavailable') }
  const urlHost = HOST.includes(':') ? `[${HOST}]` : HOST
  const url = `http://${urlHost}:${address.port}`
  emit('info', `[zcode-bridge] listening ${url}`)
  return { server, host: HOST, port: address.port, url, token: TOKEN, close }
}

const isMain = Boolean(process.argv?.[1] && pathToFileURL(process.argv[1]).href === import.meta.url)
if (isMain) {
  startBridge({ logger: console }).then((service) => {
    const stop = () => { void service.close().catch((error) => console.error(error)) }
    process.once('SIGINT', stop)
    process.once('SIGTERM', stop)
  }).catch((error) => { console.error(`[zcode-bridge] ${error?.message ?? error}`); process.exitCode = 1 })
}
