const b4a = require('b4a')
const {
  REQUEST_TIMEOUT,
  REQUEST_DESTROYED,
  IO_SUSPENDED,
  TRANSPORT_INVALID,
  TRANSPORT_UNAVAILABLE,
  TRANSPORT_INVALID_RESPONSE
} = require('./errors')

const REQUIRED_METHODS = [
  'ready',
  'suspend',
  'resume',
  'destroy',
  'bootstrap',
  'closest',
  'key',
  'id',
  'request'
]

const DEFAULT_TIMER = {
  set: (fn, ms) => setTimeout(fn, ms),
  clear: (handle) => clearTimeout(handle)
}

class RequestTransport {
  constructor(
    adapter,
    { requestTimeout, maxTransportCandidates, requestTimer, ontransporterror = noop }
  ) {
    validateTransport(adapter)
    validateTimer(requestTimer)

    this.adapter = adapter
    this.inflight = []
    this.suspended = false
    this.destroyed = false
    this.requestTimeout = requestTimeout
    this.maxTransportCandidates = maxTransportCandidates
    this.stats = {
      requests: {
        active: 0,
        total: 0,
        responses: 0,
        timeouts: 0,
        retries: 0
      },
      commands: [
        { tx: 0, rx: 0 },
        { tx: 0, rx: 0 },
        { tx: 0, rx: 0 },
        { tx: 0, rx: 0 }
      ]
    }

    this._timer = requestTimer || DEFAULT_TIMER
    this._ontransporterror = ontransporterror
    this._destinationRegistry = new Map()
    this._responseRegistries = 0
    this._readying = null
    this._desiredSuspended = false
    this._stateTransition = null
    this._stateQueue = []
    this._destroying = null
    this._lifecycle = Promise.resolve()
  }

  ready() {
    if (this._readying !== null) return this._readying
    this._readying = this._enqueueLifecycle('ready')
    return this._readying
  }

  suspend() {
    return this._setSuspended(true)
  }

  resume() {
    return this._setSuspended(false)
  }

  destroy() {
    if (this._destroying !== null) return this._destroying
    this.destroyed = true
    const error = REQUEST_DESTROYED()
    this._destroyRequests(error)
    while (this._stateQueue.length > 0) this._stateQueue.shift().reject(error)
    this._desiredSuspended = this.suspended
    this._destroying = this._enqueueLifecycle('destroy')
    return this._destroying
  }

  bootstrap(opts) {
    return this.adapter.bootstrap(opts)
  }

  closest(opts) {
    return this.adapter.closest(opts)
  }

  key(destination) {
    return this.adapter.key(destination)
  }

  id(destination) {
    return this.adapter.id(destination)
  }

  request(message) {
    return this.adapter.request(message)
  }

  createRequest(to, token, internal, command, target, value, session) {
    if (this.destroyed || this.suspended || this._stateTransition !== null) return null

    const identity = this._identity(to, false)
    const req = new TransportRequest(
      this,
      to,
      identity,
      token,
      internal,
      command,
      target,
      value,
      session
    )

    this.inflight.push(req)
    this._destinationRegistry.set(req, identity)
    if (session) session._attach(req)

    if (internal && command < this.stats.commands.length) {
      this.stats.commands[command].tx++
    }

    this.stats.requests.active++
    this.stats.requests.total++
    return req
  }

  _identity(destination, response) {
    try {
      const key = this.adapter.key(destination)
      const id = this.adapter.id(destination)
      if (typeof key !== 'string' || !b4a.isBuffer(id) || id.byteLength !== 32) {
        throw new Error('Invalid transport identity')
      }
      const copiedId = b4a.from(id)
      if (!b4a.isBuffer(copiedId) || copiedId.byteLength !== 32) {
        throw new Error('Invalid copied transport identity')
      }
      return { key, id: copiedId }
    } catch {
      throw response ? TRANSPORT_INVALID_RESPONSE() : TRANSPORT_INVALID()
    }
  }

  _validateReply(reply, retained) {
    if (reply === null || typeof reply !== 'object') throw TRANSPORT_INVALID_RESPONSE()

    let rtt = null
    let from = null
    let to = null
    let token = null
    let closerNodes = null
    let error = null
    let value = null

    try {
      rtt = reply.rtt
      from = reply.from
      const replyTo = reply.to
      const replyToken = reply.token
      const replyCloserNodes = reply.closerNodes
      error = reply.error
      const replyValue = reply.value
      to = replyTo === undefined ? null : replyTo
      token = replyToken === undefined ? null : replyToken
      if (replyCloserNodes === undefined || replyCloserNodes === null) {
        closerNodes = null
      } else {
        if (!Array.isArray(replyCloserNodes)) throw TRANSPORT_INVALID_RESPONSE()
        const length = replyCloserNodes.length
        if (!Number.isInteger(length) || length < 0 || length > 20) {
          throw TRANSPORT_INVALID_RESPONSE()
        }
        closerNodes = new Array(length)
        for (let i = 0; i < length; i++) closerNodes[i] = replyCloserNodes[i]
      }
      value = replyValue === undefined ? null : replyValue
    } catch {
      throw TRANSPORT_INVALID_RESPONSE()
    }

    if (!Number.isInteger(error) || error < 0) throw TRANSPORT_INVALID_RESPONSE()
    if (!Number.isInteger(rtt) || rtt < 0) throw TRANSPORT_INVALID_RESPONSE()
    if (from === null || typeof from !== 'object') throw TRANSPORT_INVALID_RESPONSE()

    const registry = new Map()
    const io = this
    this._responseRegistries++

    try {
      admit(from)
      if (closerNodes !== null) {
        for (const destination of closerNodes) admit(destination)
      }
    } finally {
      registry.clear()
      this._responseRegistries--
    }

    return {
      rtt,
      from,
      to,
      token,
      closerNodes,
      error,
      value
    }

    function sameId(a, b) {
      return b4a.equals(a, b)
    }

    function conflict(identity, other) {
      return identity.key === other.key && !sameId(identity.id, other.id)
    }

    function admissionError() {
      throw TRANSPORT_INVALID_RESPONSE()
    }

    function add(identity) {
      const previous = registry.get(identity.key)
      if (previous !== undefined && !sameId(previous, identity.id)) admissionError()
      if (conflict(identity, retained)) admissionError()
      registry.set(identity.key, identity.id)
    }

    function admit(destination) {
      const identity = io._identity(destination, true)
      add(identity)
    }
  }

  _detach(req) {
    const index = this.inflight.indexOf(req)
    if (index !== -1) {
      if (index === this.inflight.length - 1) this.inflight.pop()
      else this.inflight[index] = this.inflight.pop()
    }

    this._destinationRegistry.delete(req)
    if (req.session) req.session._detach(req)
  }

  _destroyRequests(error) {
    while (this.inflight.length > 0) this.inflight[0].destroy(error)
  }

  _setSuspended(suspended) {
    if (this.destroyed) return Promise.resolve()

    const tail =
      this._stateQueue.length > 0
        ? this._stateQueue[this._stateQueue.length - 1]
        : this._stateTransition
    if (tail !== null && tail.suspended === suspended) return tail.promise
    if (tail === null && this.suspended === suspended) return Promise.resolve()

    if (suspended) this._destroyRequests(IO_SUSPENDED())
    const transition = createStateTransition(suspended)
    this._stateQueue.push(transition)
    this._desiredSuspended = suspended
    this._drainStateTransitions()
    return transition.promise
  }

  _drainStateTransitions() {
    if (this.destroyed || this._stateTransition !== null) return

    while (this._stateQueue.length > 0) {
      const transition = this._stateQueue.shift()
      if (transition.suspended === this.suspended) {
        transition.resolve()
        continue
      }

      this._stateTransition = transition
      const method = transition.suspended ? 'suspend' : 'resume'
      this._enqueueLifecycle(method).then(
        () => this._finishStateTransition(transition, null),
        (error) => this._finishStateTransition(transition, error)
      )
      return
    }

    this._desiredSuspended = this.suspended
  }

  _finishStateTransition(transition, error) {
    if (this._stateTransition !== transition) return
    if (error === null && !this.destroyed) this.suspended = transition.suspended
    this._stateTransition = null
    if (error === null) transition.resolve()
    else transition.reject(error)

    if (this._stateQueue.length > 0) {
      this._desiredSuspended = this._stateQueue[this._stateQueue.length - 1].suspended
    } else {
      this._desiredSuspended = this.suspended
    }
    this._drainStateTransitions()
  }

  _enqueueLifecycle(method) {
    const operation = this._lifecycle.catch(noop).then(() => callLifecycle(this.adapter, method))
    this._lifecycle = operation
    return operation
  }

  _emitTransportError(error) {
    Promise.resolve().then(() => this._ontransporterror(error))
  }
}

class TransportRequest {
  constructor(io, to, identity, token, internal, command, target, value, session) {
    this.to = to
    this.token = token
    this.internal = internal
    this.command = command
    this.target = target
    this.value = value
    this.session = session
    this.index = -1
    this.sent = 0
    this.retries = 3
    this.destroyed = false
    this.timeout = 0
    this.oncycle = noop
    this.onresponse = noop
    this.onerror = noop

    this._io = io
    this._identity = identity
    this._generation = 0
    this._operation = null
    this._timer = null
    this._started = false
  }

  send() {
    if (this.destroyed || this._io.suspended || this._io.destroyed) return
    if (this._started) return
    this._started = true

    this._sendAttempt()
  }

  _sendAttempt() {
    if (this.destroyed || this._io.suspended || this._io.destroyed) return

    const generation = ++this._generation
    const attempt = ++this.sent
    Promise.resolve().then(() => this._start(generation, attempt))
  }

  destroy(error) {
    if (this.destroyed) return
    const terminal = error || REQUEST_DESTROYED()
    this._cancelAttempt(terminal)
    this._settle(terminal, null)
  }

  _start(generation, attempt) {
    if (this.destroyed || generation !== this._generation) return

    let operation = null
    try {
      operation = this._io.adapter.request({
        to: this.to,
        token: this.token,
        internal: this.internal,
        command: this.command,
        target: this.target,
        value: this.value,
        attempt
      })
    } catch (error) {
      this._unavailable(generation, error)
      return
    }

    const fields = operationFields(operation)
    if (fields === null) {
      this._settle(TRANSPORT_INVALID(), null)
      return
    }

    this._operation = {
      cancel: (reason) => fields.cancel.call(operation, reason)
    }
    const timeout = this.timeout || this._io.requestTimeout
    try {
      this._timer = this._io._timer.set(() => this._ontimeout(generation), timeout)
    } catch (cause) {
      const error = invalidTimer(cause)
      this._cancelAttempt(error)
      this._settle(error, null)
      return
    }
    fields.promise.then(
      (reply) => this._onreply(generation, reply),
      (error) => this._unavailable(generation, error)
    )
  }

  _onreply(generation, reply) {
    if (this.destroyed || generation !== this._generation) return
    this._operation = null

    let normalized = null
    try {
      normalized = this._io._validateReply(reply, this._identity)
    } catch (error) {
      this._settle(error, null)
      return
    }

    this._settle(null, normalized)
  }

  _unavailable(generation, error) {
    if (this.destroyed || generation !== this._generation) return
    const timerError = this._clearTimer()
    if (timerError !== null) this._io._emitTransportError(timerError)
    this._operation = null
    this.oncycle(this)
    if (this.destroyed) return

    if (this.sent > this.retries) {
      this._settle(unavailable(error), null)
      return
    }

    this._io.stats.requests.retries++
    this._sendAttempt()
  }

  _ontimeout(generation) {
    if (this.destroyed || generation !== this._generation) return
    const timerError = this._clearTimer()
    if (timerError !== null) this._io._emitTransportError(timerError)
    this.oncycle(this)
    if (this.destroyed) return
    const error = REQUEST_TIMEOUT()
    this._cancelAttempt(error)

    if (this.sent > this.retries) {
      this._io.stats.requests.timeouts++
      this._settle(error, null)
      return
    }

    this._io.stats.requests.retries++
    this._sendAttempt()
  }

  _cancelAttempt(reason) {
    const timerError = this._clearTimer()
    if (timerError !== null) this._io._emitTransportError(timerError)
    if (this._operation === null) return

    const operation = this._operation
    this._operation = null
    try {
      operation.cancel(reason)
    } catch (error) {
      this._io._emitTransportError(unavailable(error))
    }
  }

  _clearTimer() {
    if (this._timer === null) return null
    const handle = this._timer
    this._timer = null
    try {
      this._io._timer.clear(handle)
      return null
    } catch (cause) {
      return invalidTimer(cause)
    }
  }

  _settle(error, reply) {
    if (this.destroyed) return
    this.destroyed = true
    this._generation++
    const timerError = this._clearTimer()
    if (timerError !== null) {
      if (error === null) {
        error = timerError
        reply = null
      } else {
        this._io._emitTransportError(timerError)
      }
    }
    this._operation = null
    this._io._detach(this)
    this._io.stats.requests.active--

    const callback = error === null ? this.onresponse : this.onerror
    if (error === null) {
      this._io.stats.requests.responses++
      if (this.internal && this.command < this._io.stats.commands.length) {
        this._io.stats.commands[this.command].rx++
      }
    }

    this.token = null
    this.value = null
    Promise.resolve().then(() => callback(error === null ? reply : error, this))
  }
}

function validateTransport(adapter) {
  if (adapter === null || typeof adapter !== 'object') throw TRANSPORT_INVALID()

  for (const method of REQUIRED_METHODS) {
    let implementation = null
    try {
      implementation = adapter[method]
    } catch {
      throw TRANSPORT_INVALID(`Unable to read request transport ${method}()`)
    }

    if (typeof implementation !== 'function') {
      throw TRANSPORT_INVALID(`Request transport is missing ${method}()`)
    }
  }

  return adapter
}

function validateTimer(timer) {
  if (timer === undefined) return
  if (timer === null || typeof timer !== 'object') throw TRANSPORT_INVALID()

  for (const method of ['set', 'clear']) {
    let implementation = null
    try {
      implementation = timer[method]
    } catch {
      throw TRANSPORT_INVALID()
    }
    if (typeof implementation !== 'function') throw TRANSPORT_INVALID()
  }
}

function operationFields(operation) {
  if (operation === null || typeof operation !== 'object') return null

  try {
    const promise = operation.promise
    const cancel = operation.cancel
    if (promise === null) return null
    if (typeof promise !== 'object' && typeof promise !== 'function') return null
    const then = promise.then
    if (typeof then !== 'function' || typeof cancel !== 'function') return null
    return { promise: safePromise(promise, then), cancel }
  } catch {
    return null
  }
}

function safePromise(thenable, then) {
  return new Promise((resolve, reject) => {
    try {
      then.call(thenable, resolve, reject)
    } catch (error) {
      reject(error)
    }
  })
}

function createStateTransition(suspended) {
  let resolve = null
  let reject = null
  const promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })
  return { suspended, promise, resolve, reject }
}

function callLifecycle(adapter, method) {
  return Promise.resolve()
    .then(() => adapter[method]())
    .catch((error) => {
      throw unavailable(error)
    })
}

function unavailable(cause) {
  const error = TRANSPORT_UNAVAILABLE()
  error.cause = cause
  return error
}

function invalidTimer(cause) {
  const error = TRANSPORT_INVALID('Request timer failed')
  error.cause = cause
  return error
}

function noop() {}

module.exports = RequestTransport
module.exports.validateTransport = validateTransport
