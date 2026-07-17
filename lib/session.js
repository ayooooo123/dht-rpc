const { REQUEST_DESTROYED } = require('./errors')

module.exports = class Session {
  constructor(dht, parent = null) {
    this.dht = dht
    this.inflight = []
    this.children = new Set()
    this._parent = parent
    this._ondestroy = null
    this.destroyed = false
    this.error = null
  }

  _child(ondestroy = null) {
    const child = new this.constructor(this.dht, this)
    if (this.destroyed) {
      child.destroy(this.error)
      return child
    }
    child._ondestroy = ondestroy
    this.children.add(child)
    return child
  }

  _attach(req) {
    if (this.destroyed) return false
    req.index = this.inflight.push(req) - 1
    return true
  }

  _detach(req) {
    const i = req.index
    if (i === -1) return
    req.index = -1

    if (i === this.inflight.length - 1) this.inflight.pop()
    else {
      const req = (this.inflight[i] = this.inflight.pop())
      req.index = i
    }
  }

  query({ target, command, value }, opts = {}) {
    if (this.destroyed) throw this.error
    return this.dht.query({ target, command, value }, { ...opts, session: this })
  }

  request({ token, command, target, value }, to, opts = {}) {
    if (this.destroyed) return Promise.reject(this.error)
    return this.dht.request({ token, command, target, value }, to, { ...opts, session: this })
  }

  ping(to, opts = {}) {
    if (this.destroyed) return Promise.reject(this.error)
    return this.dht.ping(to, { ...opts, session: this })
  }

  destroy(err) {
    if (this.destroyed) return
    this.destroyed = true
    this.error = err || REQUEST_DESTROYED()
    const ondestroy = this._ondestroy
    this._ondestroy = null
    if (ondestroy !== null) ondestroy(this.error)
    while (this.children.size > 0) this.children.values().next().value.destroy(this.error)
    while (this.inflight.length) {
      const req = this.inflight[0]
      req.destroy(this.error)
    }
    if (this._parent !== null) {
      this._parent.children.delete(this)
      this._parent = null
    }
  }
}
