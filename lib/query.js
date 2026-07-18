const { Readable, getStreamError } = require('streamx')
const b4a = require('b4a')
const peer = require('./peer')
const { DOWN_HINT } = require('./commands')
const { TRANSPORT_INVALID_RESPONSE } = require('./errors')

const DONE = []
const DOWN = []

module.exports = class Query extends Readable {
  constructor(dht, target, internal, command, value, opts = {}) {
    super()

    this.force = !!opts.force
    this.dht = dht
    let transportContext = null
    if (this.dht.outboundPolicy === 'transport-only') {
      const context = opts.transportContext
      transportContext = context === undefined ? null : context
    }
    Object.defineProperty(this, '_transportContext', { value: transportContext })
    this.k = this.dht.outboundPolicy === 'transport-only' ? this.dht._queryK : this.dht.table.k
    this.target = target
    this.internal = internal
    this.command = command
    this.value = value
    this.errors = 0
    this.successes = 0
    this.concurrency = opts.concurrency || this.dht.concurrency
    this.inflight = 0
    this.map = opts.map || defaultMap
    this.retries =
      opts.retries === 0 ? 0 : opts.retries || (this.internal && command === DOWN_HINT ? 3 : 5)
    this.closestReplies = []

    this._slow = 0
    this._slowdown = false
    this._seen = new Map()
    this._pending = []
    this._transportCandidates = new Map()
    this._requestCandidates = new WeakMap()
    this._replyCandidates = new WeakMap()
    this._fromTable = false
    this._commit = opts.commit === true ? autoCommit : opts.commit || null
    this._commiting = false
    this._session = null
    this._autoDestroySession = false
    this._onlyClosestNodes = false

    this._onvisitbound = this._onvisit.bind(this)
    this._onerrorbound = this._onerror.bind(this)
    this._oncyclebound = this._oncycle.bind(this)

    try {
      const nodes = opts.nodes || opts.closestNodes
      const replies = opts.replies || opts.closestReplies
      const seedLimit =
        this.dht.outboundPolicy === 'transport-only'
          ? this.dht.maxTransportCandidates
          : Number.MAX_SAFE_INTEGER
      const seeds = nodes
        ? snapshotSeeds(nodes, false, seedLimit)
        : snapshotSeeds(replies, true, seedLimit)

      // add them reverse as we pop below
      for (let i = seeds.length - 1; i >= 0; i--) {
        this._addPending(this._admitNode(seeds[i]), null)
      }
    } catch (error) {
      this._transportCandidates.clear()
      if (
        this.dht.outboundPolicy === 'transport-only' &&
        (!error || error.code !== 'TRANSPORT_INVALID_RESPONSE')
      ) {
        throw TRANSPORT_INVALID_RESPONSE()
      }
      throw error
    }

    if (opts.onlyClosestNodes) this._onlyClosestNodes = true

    const borrowSession = this.dht.outboundPolicy === 'direct' && opts.session
    this._session = borrowSession
      ? opts.session
      : opts.session
        ? opts.session._child((error) => this.destroy(error))
        : dht.session()
    this._autoDestroySession = !borrowSession
    if (this._session.destroyed) {
      this._transportCandidates.clear()
      throw this._session.error
    }
    dht.stats.queries.total++
    dht.stats.queries.active++
  }

  get closestNodes() {
    const nodes = new Array(this.closestReplies.length)

    for (let i = 0; i < nodes.length; i++) {
      nodes[i] = this.closestReplies[i].from
    }

    return nodes
  }

  finished() {
    return new Promise((resolve, reject) => {
      if (this.destroyed) {
        const error = getStreamError(this)
        if (error) reject(error)
        else resolve()
        return
      }

      const self = this
      let error = null

      this.resume()
      this.on('error', onerror)
      this.on('close', onclose)

      function onclose() {
        self.removeListener('error', onerror)
        self.removeListener('close', onclose)
        if (error) reject(error)
        else resolve()
      }

      function onerror(err) {
        error = err
      }
    })
  }

  _addFromTable() {
    if (this._pending.length >= this.k) return
    this._fromTable = true

    const added = []
    let closest = null
    try {
      closest = this.dht._closestQueryNodes(
        this.target,
        this.k - this._pending.length,
        this._transportCandidates,
        added,
        this._transportContext
      )
    } catch (error) {
      this._rollbackCandidates(added)
      throw error
    }

    for (const node of closest) {
      this._addPending(node, null)
    }
  }

  async _open(cb) {
    try {
      this._addFromTable()
      this._throwIfTerminal()
      if (this._pending.length >= this.k) return cb(null)

      const added = []
      try {
        for await (const node of this.dht._resolveQueryBootstrap(
          this.target,
          this.k - this._pending.length,
          this._transportCandidates,
          added,
          this._transportContext
        )) {
          this._throwIfTerminal()
          this._addPending(node, null)
          this._throwIfTerminal()
        }
      } catch (error) {
        this._rollbackCandidates(added)
        throw error
      }

      this._throwIfTerminal()
      cb(null)
    } catch (error) {
      cb(error)
    }
  }

  _isCloser(node) {
    const id = this.dht._nodeId(node)
    const last = this.closestReplies[this.closestReplies.length - 1]
    return (
      this.closestReplies.length < this.k ||
      this._compare(id, this.dht._nodeId(this._replyCandidates.get(last))) < 0
    )
  }

  _addPending(node, ref) {
    if (this._onlyClosestNodes) return false

    const key = this.dht._nodeKey(node)
    const refs = this._seen.get(key)
    const isCloser = this._isCloser(node)

    if (refs === DONE) {
      return isCloser
    }

    if (refs === DOWN) {
      if (ref) this._downHint(ref, node)
      return isCloser
    }

    if (refs) {
      if (ref !== null) refs.push(ref)
      return isCloser
    }

    if (!isCloser) {
      return false
    }

    this._seen.set(key, ref === null ? [] : [ref])
    this._pending.push(node)

    return true
  }

  _read(cb) {
    this._readMore()
    cb(null)
  }

  _readMore() {
    if (this.destroying || this._commiting) return

    const concurrency = (this._slowdown ? 3 : this.concurrency) + this._slow

    while (this.inflight < concurrency && this._pending.length > 0) {
      const next = this._pending.pop()
      if (next && !this._isCloser(next)) continue
      this._visit(next)
    }

    // if reusing closest nodes, slow down after the first readMore tick to allow
    // the closest node a chance to reply before going broad to question more
    if (!this._fromTable && this.successes === 0 && this.errors === 0) {
      this._slowdown = true
    }

    if (this._pending.length > 0) return

    // if no inflight OR all the queries we are waiting on are marked as slow and we have a full result.
    if (
      this.inflight === 0 ||
      (this._slow === this.inflight && this.closestReplies.length >= this.k)
    ) {
      // if more than 3/4 failed and we only used cached nodes, try again from the routing table
      if (!this._fromTable && this.successes < this.k / 4) {
        this._addFromTable()
        this._readMore()
        return
      }

      this._flush()
    }
  }

  _flush() {
    if (this._commiting) return
    this._commiting = true

    if (this._commit === null) {
      this.push(null)
      return
    }

    const p = []
    for (const m of this.closestReplies) p.push(this._commit(m, this.dht, this))
    this._endAfterCommit(p)
  }

  _endAfterCommit(ps) {
    if (!ps.length) {
      this.destroy(new Error('Too few nodes responded'))
      return
    }

    const self = this

    let pending = ps.length
    let success = 0

    for (const p of ps) p.then(ondone, onerror)

    function ondone() {
      success++
      if (--pending === 0) self.push(null)
    }

    function onerror(err) {
      if (--pending > 0) return
      if (success) self.push(null)
      else self.destroy(err)
    }
  }

  _dec(req) {
    if (req.oncycle === noop) {
      this._slow--
    } else {
      req.oncycle = noop
    }
    this.inflight--
  }

  _onvisit(m, req) {
    const requested = this._requestCandidates.get(req)
    if (requested === undefined) {
      this._discardReply(m)
      return
    }
    this._requestCandidates.delete(req)

    if (this.destroying || this.destroyed) {
      this._discardReply(m)
      this._dec(req)
      return
    }

    if (this._session.destroyed) {
      this._discardReply(m)
      this._dec(req)
      this.destroy(this._session.error)
      return
    }

    if (this._commiting) {
      this._dec(req)
      this._seen.set(this.dht._nodeKey(requested), DONE)
      return
    }

    let normalized = null
    try {
      normalized = this._normalizeReply(m)
    } catch (error) {
      this._dec(req)
      this.destroy(error)
      return
    }
    this._dec(req)
    this._seen.set(this.dht._nodeKey(requested), DONE)
    m = normalized.reply
    const from = normalized.from

    if (m.error === 0) this.successes++
    else this.errors++

    if (m.error === 0 && normalized.fromHasId && this._isCloser(from)) {
      this._pushClosest(m, from)
    }

    if (m.closerNodes !== null) {
      for (const node of normalized.closerNodes) {
        if (
          this.dht.outboundPolicy === 'direct' &&
          this.dht._filterNode !== null &&
          !this.dht._filterNode(node.destination)
        ) {
          continue
        }
        if (
          this.dht.outboundPolicy === 'direct' &&
          b4a.equals(this.dht._nodeId(node), this.dht.table.id)
        ) {
          continue
        }
        // TODO: we could continue here instead of breaking to ensure that one of the nodes in the closer list
        // is later marked as DOWN that we gossip that back
        if (!this._addPending(node, from)) break
      }
    }

    if (!this._fromTable && this.successes + this.errors >= this.concurrency) {
      this._slowdown = false
    }

    if (m.error !== 0) {
      this._readMore()
      return
    }

    const data = this.map(m)
    if (!data || this.push(data) !== false) {
      this._readMore()
    }
  }

  _onerror(err, req) {
    const requested = this._requestCandidates.get(req)
    if (requested === undefined) return
    this._requestCandidates.delete(req)

    if (this.destroying || this.destroyed) {
      this._dec(req)
      return
    }

    if (this._session.destroyed) {
      this._dec(req)
      this.destroy(this._session.error || err)
      return
    }

    const key = this.dht._nodeKey(requested)
    const refs = this._seen.get(key)

    if (err.code === 'TRANSPORT_INVALID_RESPONSE') {
      this._dec(req)
      this.destroy(err)
      return
    }

    if (err.code === 'REQUEST_TIMEOUT') {
      this._seen.set(key, DOWN)
      for (const node of refs) this._downHint(node, requested)
    }

    this._dec(req)
    this.errors++
    this._readMore()
  }

  _oncycle(req) {
    if (!this._requestCandidates.has(req)) {
      req.oncycle = noop
      return
    }
    if (this.destroying || this.destroyed || this._session.destroyed) {
      req.oncycle = noop
      this._slow++
      req.destroy(this._session.error || getStreamError(this))
      return
    }
    req.oncycle = noop
    this._slow++
    this._readMore()
  }

  _downHint(node, down) {
    if (this.dht.outboundPolicy === 'transport-only') return null

    // Check rate limit
    if (
      this.dht._downHintsRateLimit !== -1 &&
      this.dht._downHintsSentPerTick >= this.dht._downHintsRateLimit
    ) {
      return null
    }

    this.dht._downHintsSentPerTick++

    const state = { start: 0, end: 6, buffer: b4a.allocUnsafe(6) }
    peer.ipv4.encode(state, down.destination)
    this.dht._request(
      node.destination,
      false,
      true,
      DOWN_HINT,
      null,
      state.buffer,
      this._session,
      noop,
      noop
    )
  }

  _pushClosest(m, from) {
    this.closestReplies.push(m)
    this._replyCandidates.set(m, from)
    for (let i = this.closestReplies.length - 2; i >= 0; i--) {
      const prev = this.closestReplies[i]
      const cmp = this._compare(
        this.dht._nodeId(this._replyCandidates.get(prev)),
        this.dht._nodeId(from)
      )
      // if sorted, done!
      if (cmp < 0) break
      // if dup, splice it out (rare)
      if (cmp === 0) {
        this.closestReplies.splice(i + 1, 1)
        break
      }
      // swap and continue down
      this.closestReplies[i + 1] = prev
      this.closestReplies[i] = m
    }
    if (this.closestReplies.length > this.k) this.closestReplies.pop()
  }

  _compare(a, b) {
    for (let i = 0; i < a.length; i++) {
      if (a[i] === b[i]) continue
      const t = this.target[i]
      return (t ^ a[i]) - (t ^ b[i])
    }
    return 0
  }

  _visit(to) {
    this.inflight++

    const req = this.dht._request(
      to.destination,
      this.force,
      this.internal,
      this.command,
      this.target,
      this.value,
      this._session,
      this._onvisitbound,
      this._onerrorbound,
      (req) => {
        req.retries = this.force ? 0 : this.retries
        req.oncycle = this._oncyclebound
      },
      to,
      this._transportContext
    )
    if (req === null) {
      this.destroy(this._session.destroyed ? this._session.error : new Error('Node was destroyed'))
      return
    }
    this._requestCandidates.set(req, to)
  }

  _destroy(cb) {
    this.dht.stats.queries.active--
    if (this._autoDestroySession) this._session.destroy()
    this._transportCandidates.clear()
    cb(null)
  }

  _discardReply(reply) {
    if (this.dht.outboundPolicy === 'transport-only') {
      this.dht.io.discardReplyCandidates(reply)
    }
  }

  _throwIfTerminal() {
    if (this._session.destroyed) throw this._session.error
    if (this.destroying || this.destroyed) throw getStreamError(this, { all: true })
  }

  _admitNode(destination, added = null) {
    return this.dht._queryCandidate(destination, this._transportCandidates, added)
  }

  _admitCandidate(candidate, added = null) {
    return this.dht._registerQueryCandidate(candidate, this._transportCandidates, added)
  }

  _rollbackCandidates(added) {
    for (const key of added) this._transportCandidates.delete(key)
  }

  _normalizeReply(reply) {
    if (this.dht.outboundPolicy === 'direct') {
      const fromHasId = reply.from.id !== null
      const from = this._admitNode(reply.from)
      const closerNodes = []
      if (reply.closerNodes !== null) {
        for (const destination of reply.closerNodes) {
          destination.id = peer.id(destination.host, destination.port)
          closerNodes.push(this._admitNode(destination))
        }
      }
      return { reply, from, closerNodes, fromHasId }
    }

    const added = []
    try {
      const validated = this.dht.io.takeReplyCandidates(reply)
      const from = this._admitCandidate(validated.from, added)
      const closerNodes = []
      if (validated.closerNodes !== null) {
        for (const candidate of validated.closerNodes) {
          closerNodes.push(this._admitCandidate(candidate, added))
        }
      }
      const publicReply = {
        rtt: reply.rtt,
        from: from.destination,
        to: reply.to,
        token: reply.token,
        closerNodes:
          reply.closerNodes === null ? null : closerNodes.map((candidate) => candidate.destination),
        error: reply.error,
        value: reply.value
      }
      return { reply: publicReply, from, closerNodes, fromHasId: true }
    } catch (error) {
      this._rollbackCandidates(added)
      throw error
    }
  }
}

function autoCommit(reply, dht, query) {
  if (!reply.token) return Promise.reject(new Error('No token received for closest node'))
  return dht._queryCandidateRequest(
    {
      token: reply.token,
      target: query.target,
      command: query.command,
      value: query.value
    },
    query._replyCandidates.get(reply),
    { session: query._session },
    query._transportContext
  )
}

function defaultMap(m) {
  return m
}

function snapshotSeeds(source, replies, limit) {
  if (!source) return []

  const length = source.length
  if (!Number.isInteger(length) || length < 0 || length > limit) {
    throw new Error('Invalid query seeds')
  }

  const seeds = new Array(length)
  for (let i = 0; i < length; i++) {
    const value = source[i]
    seeds[i] = replies ? value.from : value
  }
  return seeds
}

function noop() {}
