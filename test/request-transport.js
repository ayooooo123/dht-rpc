const test = require('brittle')
const b4a = require('b4a')
const UDX = require('udx-native')
const DHT = require('..')

test('direct remains the default', async (t) => {
  const dht = new DHT({ bootstrap: false })

  t.is(dht.outboundPolicy, 'direct')
  t.ok(dht.io)
  t.alike(DHT.DEFAULTS, {
    concurrency: 10,
    maxWindow: 80,
    maxPingDelay: 10_000
  })

  await dht.destroy()
})

test('direct routing table size remains pinned', async (t) => {
  const dht = new DHT({ bootstrap: false })

  t.is(dht.table.k, 20)

  await dht.destroy()
})

test('direct construction uses an explicit UDX before the factory', async (t) => {
  const udx = new UDX()
  let factoryCalls = 0
  const dht = new DHT({
    bootstrap: false,
    udx,
    udxFactory() {
      factoryCalls++
      throw new Error('factory must not run')
    }
  })

  t.is(dht.udx === udx, true)
  t.is(factoryCalls, 0)

  await dht.destroy()
})

test('direct construction uses the configured UDX factory', async (t) => {
  let factoryCalls = 0
  let udx = null
  const dht = new DHT({
    bootstrap: false,
    udxFactory() {
      factoryCalls++
      udx = new UDX()
      return udx
    }
  })

  t.is(factoryCalls, 1)
  t.is(dht.udx === udx, true)

  await dht.destroy()
})

test('direct construction ignores transport-only request limits', async (t) => {
  const dht = new DHT({
    bootstrap: false,
    requestTimeout: 0,
    maxTransportCandidates: 1
  })

  t.is(dht.outboundPolicy, 'direct')
  t.is(dht.requestTimeout, undefined)
  t.is(dht.maxTransportCandidates, undefined)
  t.alike(dht.config, {
    concurrency: 10,
    maxWindow: 80,
    randomPunchInterval: undefined,
    connectionKeepAlive: undefined,
    sendDownHints: true,
    downHintsRateLimit: 50
  })

  await dht.destroy()
})

test('transport-only construction has no direct network state', (t) => {
  const transport = createTransport()
  let factoryCalls = 0
  const dht = new DHT({
    outboundPolicy: 'transport-only',
    requestTransport: transport,
    concurrency: 7,
    maxPingDelay: 500,
    requestTimeout: 750,
    maxTransportCandidates: 64,
    udxFactory() {
      factoryCalls++
      throw new Error('transport-only constructed UDX')
    }
  })

  t.is(dht.outboundPolicy, 'transport-only')
  t.is(dht.io.adapter, transport)
  t.is(dht.table, null)
  t.is(dht.nodes, null)
  t.is(dht.udx, null)
  t.is(dht.health, null)
  t.is(dht._nat, null)
  t.is(dht._queryId && dht._queryId.byteLength, 32)
  t.is(dht._queryK, 20)
  t.is(dht.firewalled, true)
  t.is(dht.ephemeral, true)
  t.is(dht.id, null)
  t.is(dht.host, null)
  t.is(dht.port, null)
  t.is(dht.socket, null)
  t.is(dht.address(), null)
  t.is(dht.localAddress(), null)
  t.is(dht.remoteAddress(), null)
  t.alike(dht.toArray(), [])
  t.alike(dht.config, {
    concurrency: 7,
    maxPingDelay: 500,
    outboundPolicy: 'transport-only',
    requestTimeout: 750,
    maxTransportCandidates: 64
  })
  t.alike(dht.stats, {
    queries: { active: 0, total: 0 },
    requests: {
      active: 0,
      total: 0,
      responses: 0,
      timeouts: 0,
      retries: 0
    },
    commands: {
      ping: { tx: 0, rx: 0 },
      pingNat: { tx: 0, rx: 0 },
      findNode: { tx: 0, rx: 0 },
      downHint: { tx: 0, rx: 0 }
    }
  })
  t.is(factoryCalls, 0)
  t.is(transport.calls.key.length, 0)
  t.is(transport.calls.id.length, 0)

  for (const field of [
    '_tickInterval',
    '_onrow',
    '_repinging',
    '_checks',
    '_tick',
    '_refreshTicks',
    '_stableTicks',
    '_nonePersistentSamples',
    '_bootstrapping',
    '_sendDownHints',
    '_downHintsRateLimit',
    '_downHintsSentPerTick'
  ]) {
    t.is(field in dht, false, `${field} is not initialized`)
  }
})

test('transport-only defaults routed request limits', (t) => {
  const dht = new DHT({
    outboundPolicy: 'transport-only',
    requestTransport: createTransport()
  })

  t.is(dht.requestTimeout, 1_000)
  t.is(dht.maxTransportCandidates, 256)
})

test('transport-only destroy is idempotent', async (t) => {
  const transport = createTransport()
  const dht = new DHT({
    outboundPolicy: 'transport-only',
    requestTransport: transport
  })
  let closes = 0
  dht.on('close', () => closes++)

  await Promise.all([dht.destroy(), dht.destroy()])
  await dht.destroy()

  t.is(transport.calls.destroy.length, 1)
  t.is(closes, 1)
})

test('transport-only requires an adapter', async (t) => {
  await constructorError(t, { outboundPolicy: 'transport-only' }, 'TRANSPORT_INVALID')
})

test('transport-only requires every adapter method', async (t) => {
  for (const method of [
    'ready',
    'suspend',
    'resume',
    'destroy',
    'bootstrap',
    'closest',
    'key',
    'id',
    'request'
  ]) {
    const transport = createTransport()
    transport[method] = null

    await constructorError(
      t,
      { outboundPolicy: 'transport-only', requestTransport: transport },
      'TRANSPORT_INVALID',
      method
    )
  }
})

test('transport-only normalizes adapter method getter errors', async (t) => {
  const transport = createTransport()
  Object.defineProperty(transport, 'ready', {
    get() {
      throw new Error('getter failed')
    }
  })

  await constructorError(
    t,
    { outboundPolicy: 'transport-only', requestTransport: transport },
    'TRANSPORT_INVALID'
  )

  for (const calls of Object.values(transport.calls)) t.is(calls.length, 0)
})

test('constructor rejects unknown outbound policies before UDX', async (t) => {
  let factoryCalls = 0

  await constructorError(
    t,
    {
      outboundPolicy: 'masked',
      udxFactory() {
        factoryCalls++
      }
    },
    'TRANSPORT_INVALID'
  )
  t.is(factoryCalls, 0)
})

test('direct mode rejects a request transport before UDX', async (t) => {
  let factoryCalls = 0

  await constructorError(
    t,
    {
      requestTransport: createTransport(),
      udxFactory() {
        factoryCalls++
      }
    },
    'TRANSPORT_INVALID'
  )
  t.is(factoryCalls, 0)
})

test('constructor validates maxTransportCandidates before UDX', async (t) => {
  for (const value of [19, 4097, 20.5, '256', null]) {
    let factoryCalls = 0

    await constructorError(
      t,
      {
        outboundPolicy: 'transport-only',
        requestTransport: createTransport(),
        maxTransportCandidates: value,
        udxFactory() {
          factoryCalls++
        }
      },
      'TRANSPORT_INVALID',
      String(value)
    )
    t.is(factoryCalls, 0)
  }
})

test('transport-only accepts maxTransportCandidates boundaries', (t) => {
  for (const value of [20, 4096]) {
    const dht = new DHT({
      outboundPolicy: 'transport-only',
      requestTransport: createTransport(),
      maxTransportCandidates: value
    })

    t.is(dht.maxTransportCandidates, value)
  }
})

test('constructor validates requestTimeout before UDX', async (t) => {
  for (const value of [0, -1, 1.5, '1000', null]) {
    let factoryCalls = 0

    await constructorError(
      t,
      {
        outboundPolicy: 'transport-only',
        requestTransport: createTransport(),
        requestTimeout: value,
        udxFactory() {
          factoryCalls++
        }
      },
      'TRANSPORT_INVALID',
      String(value)
    )
    t.is(factoryCalls, 0)
  }
})

test('transport-only accepts a positive integer requestTimeout', (t) => {
  const dht = new DHT({
    outboundPolicy: 'transport-only',
    requestTransport: createTransport(),
    requestTimeout: 1
  })

  t.is(dht.requestTimeout, 1)
})

test('transport-only rejects direct constructor options', async (t) => {
  const cases = [
    ['bootstrap false', 'bootstrap', false],
    ['bootstrap array', 'bootstrap', []],
    ['nodes', 'nodes', []],
    ['udx', 'udx', {}],
    ['port', 'port', 0],
    ['host', 'host', '127.0.0.1'],
    ['firewalled', 'firewalled', false],
    ['anyPort', 'anyPort', false],
    ['ephemeral', 'ephemeral', false],
    ['socket', 'socket', null]
  ]

  for (const [name, option, value] of cases) {
    const opts = {
      outboundPolicy: 'transport-only',
      requestTransport: createTransport(),
      [option]: value
    }

    await constructorError(t, opts, 'DIRECT_IO_FORBIDDEN', name)
  }
})

function createTransport(overrides = {}) {
  const calls = {
    ready: [],
    suspend: [],
    resume: [],
    destroy: [],
    bootstrap: [],
    closest: [],
    key: [],
    id: [],
    request: [],
    cancel: []
  }
  const destinations = [
    { ref: { route: 'a' }, id: b4a.alloc(32, 1) },
    { ref: { route: 'b' }, id: b4a.alloc(32, 2) }
  ]
  const requests = []
  const transport = {
    calls,
    destinations,
    requests,
    ready() {
      calls.ready.push([])
      return Promise.resolve()
    },
    suspend() {
      calls.suspend.push([])
      return Promise.resolve()
    },
    resume() {
      calls.resume.push([])
      return Promise.resolve()
    },
    destroy() {
      calls.destroy.push([])
      return Promise.resolve()
    },
    bootstrap(opts) {
      calls.bootstrap.push([opts])
      return Promise.resolve(destinations)
    },
    closest(opts) {
      calls.closest.push([opts])
      return destinations
    },
    key(destination) {
      calls.key.push([destination])
      return destination === destinations[0] ? 'route-a' : 'route-b'
    },
    id(destination) {
      calls.id.push([destination])
      return destination.id
    },
    request(message) {
      calls.request.push([message])
      const pending = deferred()
      requests.push(pending)
      return {
        promise: pending.promise,
        cancel(reason) {
          calls.cancel.push([reason])
        }
      }
    },
    ...overrides
  }

  return transport
}

function deferred() {
  let resolve
  let reject
  const promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })

  return { promise, resolve, reject }
}

async function constructorError(t, opts, code, message) {
  let dht = null
  let error = null

  try {
    dht = new DHT(opts)
  } catch (err) {
    error = err
  }

  if (dht !== null) await dht.destroy()

  t.is(error && error.code, code, message)
}
