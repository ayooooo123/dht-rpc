module.exports = class DHTError extends Error {
  constructor(msg, code, fn = DHTError) {
    super(`${code}: ${msg}`)
    this.code = code

    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, fn)
    }
  }

  get name() {
    return 'DHTError'
  }

  static UNKNOWN_COMMAND = 1
  static INVALID_TOKEN = 2

  static REQUEST_TIMEOUT(msg = 'Request timed out') {
    return new DHTError(msg, 'REQUEST_TIMEOUT', DHTError.REQUEST_TIMEOUT)
  }

  static REQUEST_DESTROYED(msg = 'Request destroyed') {
    return new DHTError(msg, 'REQUEST_DESTROYED', DHTError.REQUEST_DESTROYED)
  }

  static IO_SUSPENDED(msg = 'I/O suspended') {
    return new DHTError(msg, 'IO_SUSPENDED', DHTError.IO_SUSPENDED)
  }

  static DIRECT_IO_FORBIDDEN(msg = 'Direct I/O is forbidden') {
    return new DHTError(msg, 'DIRECT_IO_FORBIDDEN', DHTError.DIRECT_IO_FORBIDDEN)
  }

  static TRANSPORT_INVALID(msg = 'Invalid request transport') {
    return new DHTError(msg, 'TRANSPORT_INVALID', DHTError.TRANSPORT_INVALID)
  }

  static TRANSPORT_UNAVAILABLE(msg = 'Request transport unavailable') {
    return new DHTError(msg, 'TRANSPORT_UNAVAILABLE', DHTError.TRANSPORT_UNAVAILABLE)
  }

  static TRANSPORT_INVALID_RESPONSE(msg = 'Invalid request transport response') {
    return new DHTError(msg, 'TRANSPORT_INVALID_RESPONSE', DHTError.TRANSPORT_INVALID_RESPONSE)
  }
}
