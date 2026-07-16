module.exports = class Session {
  constructor(dht) {
    this.dht = dht
    this.inflight = []
  }

  _attach(req) {
    req.index = this.inflight.push(req) - 1
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
    return this.dht.query({ target, command, value }, { ...opts, session: this })
  }

  request({ token, command, target, value }, to, opts = {}) {
    return this.dht.request({ token, command, target, value }, to, { ...opts, session: this })
  }

  ping(to, opts = {}) {
    return this.dht.ping(to, { ...opts, session: this })
  }

  destroy(err) {
    while (this.inflight.length) {
      const req = this.inflight[0]
      req.destroy(err)
    }
  }
}
