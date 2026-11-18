/** Pure turn settlement and process-local reliable delivery helpers. */

/** A turn is settled only by its own, structurally valid turn/end event. */
export function settlementForTurn(events, turn, baselineSeq = -1) {
  if (!Array.isArray(events) || !Number.isSafeInteger(turn) || turn < 1) return null
  const terminal = events.find((event) =>
    event?.type === 'turn/end' &&
    Number.isSafeInteger(event.seq) && event.seq > baselineSeq &&
    event.data?.turn === turn &&
    typeof event.data?.reason?.kind === 'string' && event.data.reason.kind.length > 0,
  )
  if (!terminal) return null

  const texts = []
  const reasoning = []
  for (const event of events) {
    if (event?.type !== 'assistant/message' || event.data?.turn !== turn) continue
    if (!Number.isSafeInteger(event.seq) || event.seq <= baselineSeq || event.seq >= terminal.seq) continue
    const blocks = event.data?.message?.content
    if (!Array.isArray(blocks)) continue
    for (const block of blocks) {
      if (typeof block?.text !== 'string' || block.text.length === 0) continue
      if (block.type === 'text') texts.push(block.text)
      else if (block.type === 'reasoning') reasoning.push(block.text)
    }
  }
  return {
    terminal,
    turn,
    blocks: { text: texts.join('\n'), reasoning: reasoning.join('\n') },
  }
}

/**
 * Keeps one immutable payload per forEventId until the receiver acknowledges it.
 * This is process-local: durable receiver idempotency still requires receiver support.
 * send(payload, key) resolves to true or {ok:true}; failures retain the entry.
 */
export class ReliableOutbox {
  constructor({ now = () => Date.now(), retryDelayMs = 1200, maxRetryDelayMs = 30000 } = {}) {
    this.now = now
    this.retryDelayMs = retryDelayMs
    this.maxRetryDelayMs = maxRetryDelayMs
    this.pending = new Map()
    this.acknowledged = new Set()
  }

  get size() { return this.pending.size }

  enqueue(key, payload) {
    if (typeof key !== 'string' || !key || payload?.forEventId !== key) {
      throw new Error('reply requires a stable forEventId')
    }
    if (this.acknowledged.has(key)) return false
    if (this.pending.has(key)) return false
    // Reply fields are flat strings/undefined. Snapshot once so later events or callers
    // cannot change the body of a retry under the same idempotency key.
    this.pending.set(key, {
      payload: Object.freeze({ ...payload }),
      attempts: 0,
      nextAttemptAt: 0,
      inflight: false,
    })
    return true
  }

  drop(key) { this.pending.delete(key) }

  clear() {
    this.pending.clear()
    this.acknowledged.clear()
  }

  async deliver(key, send) {
    if (this.acknowledged.has(key)) return { status: 'delivered' }
    const entry = this.pending.get(key)
    if (!entry) return { status: 'missing' }
    if (entry.inflight || this.now() < entry.nextAttemptAt) return { status: 'pending' }
    entry.inflight = true
    try {
      const ack = await send(entry.payload, key)
      if (ack !== true && ack?.ok !== true) {
        throw new Error(ack?.error || `reply POST failed${ack?.status ? ` (${ack.status})` : ''}`)
      }
      // A concurrent cancellation/disposal may have removed this exact entry.
      if (this.pending.get(key) !== entry) return { status: 'cancelled' }
      this.pending.delete(key)
      this.acknowledged.add(key)
      if (this.acknowledged.size > 1000) this.acknowledged.delete(this.acknowledged.values().next().value)
      return { status: 'delivered' }
    } catch (error) {
      if (this.pending.get(key) !== entry) return { status: 'cancelled' }
      entry.attempts += 1
      const delay = Math.min(this.maxRetryDelayMs, this.retryDelayMs * (2 ** Math.min(entry.attempts - 1, 8)))
      entry.nextAttemptAt = this.now() + delay
      return {
        status: 'retry',
        attempts: entry.attempts,
        nextAttemptAt: entry.nextAttemptAt,
        error: String(error?.message ?? error),
      }
    } finally {
      entry.inflight = false
    }
  }
}
