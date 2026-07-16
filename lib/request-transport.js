const { TRANSPORT_INVALID } = require('./errors')

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

class RequestTransport {
  constructor(adapter, { requestTimeout, maxTransportCandidates }) {
    validateTransport(adapter)

    this.adapter = adapter
    this.inflight = []
    this.suspended = false
    this.destroyed = false
    this._destroying = null
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
  }

  ready() {
    return this.adapter.ready()
  }

  suspend() {
    this.suspended = true
    return this.adapter.suspend()
  }

  resume() {
    this.suspended = false
    return this.adapter.resume()
  }

  destroy() {
    if (this._destroying !== null) return this._destroying
    this.destroyed = true
    this._destroying = Promise.resolve().then(() => this.adapter.destroy())
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

module.exports = RequestTransport
module.exports.validateTransport = validateTransport
