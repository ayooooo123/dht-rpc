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
    '_sendDownHints',
    '_downHintsRateLimit',
    '_downHintsSentPerTick'
  ]) {
    t.is(field in dht, false, `${field} is not initialized`)
  }

  t.ok(dht._bootstrapping && typeof dht._bootstrapping.then === 'function')
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

test('transport-only request preserves opaque authority and normalizes replies', async (t) => {
  const transport = createTransport()
  const dht = createTransportDHT(transport)
  const to = transport.destinations[0]
  const from = transport.destinations[1]
  const target = b4a.alloc(32, 3)
  const value = b4a.from('hello')
  const token = b4a.alloc(32, 4)
  let settled = false

  const result = dht.request({ token, command: 7, target, value }, to).then((reply) => {
    settled = true
    return reply
  })

  t.is(settled, false, 'does not settle in send stack')
  await tick()
  t.alike(transport.calls.request[0][0], {
    to,
    token,
    internal: false,
    command: 7,
    target,
    value,
    attempt: 1
  })
  t.is('host' in transport.calls.request[0][0], false)
  t.is('port' in transport.calls.request[0][0], false)
  t.is('socket' in transport.calls.request[0][0], false)
  t.is('ttl' in transport.calls.request[0][0], false)
  t.alike(dht.stats.requests, {
    active: 1,
    total: 1,
    responses: 0,
    timeouts: 0,
    retries: 0
  })

  transport.requests[0].resolve({ from, error: 0, rtt: 12 })
  const reply = await result

  t.is(reply.from, from, 'preserves opaque from')
  t.is(reply.to, null)
  t.is(reply.token, null)
  t.is(reply.closerNodes, null)
  t.is(reply.value, null)
  t.alike(dht.stats.requests, {
    active: 0,
    total: 1,
    responses: 1,
    timeouts: 0,
    retries: 0
  })
  t.is(dht.io._destinationRegistry.size, 0, 'clears standalone registry')
  t.is(dht.io._responseRegistries, 0, 'disposes response registry')

  await dht.destroy()
})

test('transport-only ping variants preserve opaque destinations', async (t) => {
  const transport = createTransport()
  const dht = createTransportDHT(transport)
  const to = transport.destinations[0]

  const ping = dht.ping(to)
  await tick()
  t.is(transport.calls.request[0][0].to, to)
  transport.requests[0].resolve(validReply(transport.destinations[1]))
  await ping

  const delayed = dht.delayedPing(to, 5)
  await tick()
  t.is(transport.calls.request[1][0].to, to)
  transport.requests[1].resolve(validReply(transport.destinations[1]))
  await delayed

  await dht.destroy()
})

test('transport-only validates standalone destinations before adapter request', async (t) => {
  for (const override of [
    { key: () => 1 },
    {
      key: () => {
        throw new Error('key failed')
      }
    },
    { id: () => b4a.alloc(31) },
    {
      id: () => {
        throw new Error('id failed')
      }
    }
  ]) {
    const transport = createTransport(override)
    const dht = createTransportDHT(transport)
    const error = await promiseError(
      dht.request({ command: 7 }, transport.destinations[0], { retry: false })
    )

    t.is(error && error.code, 'TRANSPORT_INVALID')
    t.is(transport.calls.request.length, 0)
    t.is(dht.io._destinationRegistry.size, 0)
    await dht.destroy()
  }
})

test('transport-only normalizes hostile caller destination ids', async (t) => {
  for (const [name, id] of hostileIds()) {
    const transport = createTransport({ id: () => id })
    const dht = createTransportDHT(transport)
    const error = await promiseError(
      dht.request({ command: 7 }, transport.destinations[0], { retry: false })
    )

    t.is(error && error.code, 'TRANSPORT_INVALID', name)
    t.is(transport.calls.request.length, 0, `${name} request`)
    t.is(dht.stats.requests.active, 0, `${name} active`)
    t.is(dht.io._destinationRegistry.size, 0, `${name} registry`)
    await dht.destroy()
  }
})

test('transport-only rejects caller ids whose copy shrinks', async (t) => {
  const transport = createTransport()
  transport.destinations[0].id = shrinkingId()
  const dht = createTransportDHT(transport)
  const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  if (transport.requests[0]) {
    transport.requests[0].resolve(validReply(transport.destinations[1]))
  }
  const error = await promiseError(result)

  t.is(error && error.code, 'TRANSPORT_INVALID')
  t.is(transport.calls.request.length, 0)
  t.is(dht.stats.requests.active, 0)
  t.is(dht.io._destinationRegistry.size, 0)
  await dht.destroy()
})

test('transport-only rejects malformed adapter operations without retry', async (t) => {
  for (const operation of [null, {}, { promise: Promise.resolve(), cancel: null }]) {
    const transport = createTransport({ request: () => operation })
    const dht = createTransportDHT(transport)
    const error = await promiseError(dht.request({ command: 7 }, transport.destinations[0]))

    t.is(error && error.code, 'TRANSPORT_INVALID')
    t.is(dht.stats.requests.retries, 0)
    t.is(dht.stats.requests.active, 0)
    t.is(dht.io._destinationRegistry.size, 0)
    await dht.destroy()
  }
})

test('transport-only captures thenable authority once', async (t) => {
  const transport = createTransport()
  let reads = 0
  let calls = 0
  let receiver = null
  const thenable = {}
  Object.defineProperty(thenable, 'then', {
    get() {
      reads++
      if (reads > 1) throw new Error('then authority reread')
      return function (resolve) {
        calls++
        receiver = this
        resolve(validReply(transport.destinations[1]))
      }
    }
  })
  transport.request = (message) => {
    transport.calls.request.push([message])
    return { promise: thenable, cancel() {} }
  }
  const dht = createTransportDHT(transport)
  const outcome = await dht
    .request({ command: 7 }, transport.destinations[0], { retry: false })
    .then(
      (reply) => ({ reply, error: null }),
      (error) => ({ reply: null, error })
    )

  t.is(outcome.error, null)
  if (outcome.reply) t.is(outcome.reply.from, transport.destinations[1])
  t.is(reads, 1)
  t.is(calls, 1)
  t.is(receiver, thenable)
  t.is(transport.calls.request.length, 1)
  t.is(dht.stats.requests.retries, 0)
  t.is(dht.stats.requests.active, 0)
  await dht.destroy()
})

test('transport-only rejects invalid first then authority without retry', async (t) => {
  const cases = [
    { get: () => 1 },
    {
      get() {
        throw new Error('then getter failed')
      }
    }
  ]

  for (const descriptor of cases) {
    const thenable = {}
    Object.defineProperty(thenable, 'then', descriptor)
    const transport = createTransport({
      request: () => ({ promise: thenable, cancel() {} })
    })
    const dht = createTransportDHT(transport)
    const error = await promiseError(
      dht.request({ command: 7 }, transport.destinations[0], { retry: false })
    )

    t.is(error && error.code, 'TRANSPORT_INVALID')
    t.is(dht.stats.requests.retries, 0)
    t.is(dht.stats.requests.active, 0)
    await dht.destroy()
  }
})

test('transport-only validates each response atomically', async (t) => {
  const cases = [
    { reply: null },
    { reply: { from: { route: 'bad' }, error: -1, rtt: 1 } },
    { reply: { from: { route: 'bad' }, error: 0, rtt: -1 } },
    { reply: { from: { route: 'bad' }, error: 0.5, rtt: 1 } },
    { reply: { from: { route: 'bad' }, error: 0, rtt: 1.5 } },
    {
      reply: {
        from: { route: 'from', id: b4a.alloc(32, 8) },
        error: 0,
        rtt: 1,
        closerNodes: new Array(21).fill({ route: 'closer', id: b4a.alloc(32, 9) })
      }
    }
  ]

  for (const { reply } of cases) {
    const transport = createTransport()
    const dht = createTransportDHT(transport)
    const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
    await tick()
    transport.requests[0].resolve(reply)
    const error = await promiseError(result)

    t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE')
    t.is(dht.stats.requests.active, 0)
    t.is(dht.stats.requests.responses, 0)
    t.is(dht.io._destinationRegistry.size, 0)
    t.is(dht.io._responseRegistries, 0)
    await dht.destroy()
  }
})

test('transport-only maps throwing reply accessors to invalid response', async (t) => {
  const transport = createTransport()
  const dht = createTransportDHT(transport)
  const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  transport.requests[0].resolve(
    new Proxy(validReply(transport.destinations[1]), {
      get(target, property, receiver) {
        if (property === 'error') throw new Error('hostile getter')
        return Reflect.get(target, property, receiver)
      }
    })
  )

  const error = await promiseError(result)
  t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE')
  t.is(dht.stats.requests.active, 0)
  t.is(dht.io._destinationRegistry.size, 0)
  await dht.destroy()
})

test('transport-only normalizes hostile reply destination ids', async (t) => {
  for (const [name, id] of hostileIds()) {
    for (const placement of ['from', 'closer']) {
      const transport = createTransport()
      const dht = createTransportDHT(transport)
      const hostile = { route: `${name}-${placement}`, id }
      const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
      await tick()
      transport.requests[0].resolve(
        validReply(placement === 'from' ? hostile : transport.destinations[1], {
          closerNodes: placement === 'closer' ? [hostile] : null
        })
      )
      const error = await promiseError(result)

      t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE', `${name} ${placement}`)
      t.is(dht.stats.requests.active, 0, `${name} ${placement} active`)
      t.is(dht.io._destinationRegistry.size, 0, `${name} ${placement} registry`)
      t.is(dht.io._responseRegistries, 0, `${name} ${placement} response registry`)
      await dht.destroy()
    }
  }
})

test('transport-only rejects reply ids whose copy shrinks', async (t) => {
  const transport = createTransport()
  const dht = createTransportDHT(transport)
  const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  transport.requests[0].resolve(validReply({ route: 'shrinking', id: shrinkingId() }))
  const error = await promiseError(result)

  t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE')
  t.is(dht.stats.requests.active, 0)
  t.is(dht.stats.requests.responses, 0)
  t.is(dht.io._destinationRegistry.size, 0)
  t.is(dht.io._responseRegistries, 0)
  await dht.destroy()
})

test('transport-only snapshots every logical reply field once', async (t) => {
  const transport = createTransport()
  const dht = createTransportDHT(transport)
  const validFrom = transport.destinations[1]
  const accesses = new Map()
  const values = {
    rtt: 1,
    from: validFrom,
    to: { local: true },
    token: b4a.alloc(32, 7),
    closerNodes: null,
    error: 0,
    value: b4a.from('snapshot')
  }
  const reply = {}

  for (const field of Object.keys(values)) {
    Object.defineProperty(reply, field, {
      enumerable: true,
      get() {
        const count = (accesses.get(field) || 0) + 1
        accesses.set(field, count)
        if (field === 'from' && count > 1) return { route: 'invalid' }
        return values[field]
      }
    })
  }

  const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  transport.requests[0].resolve(reply)
  const outcome = await result.then(
    (value) => ({ value, error: null }),
    (error) => ({ value: null, error })
  )

  t.is(outcome.error, null)
  if (outcome.value) t.is(outcome.value.from, validFrom)
  for (const field of Object.keys(values)) t.is(accesses.get(field), 1, field)
  await dht.destroy()
})

test('transport-only maps throwing closer length to invalid response', async (t) => {
  const closerNodes = new Proxy([], {
    get(target, property, receiver) {
      if (property === 'length') throw new Error('hostile length')
      return Reflect.get(target, property, receiver)
    }
  })
  const { dht, error } = await invalidCloserReply(closerNodes)

  t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE')
  t.is(dht.stats.requests.active, 0)
  t.is(dht.io._destinationRegistry.size, 0)
  await dht.destroy()
})

test('transport-only maps throwing closer index to invalid response', async (t) => {
  const closerNodes = new Proxy([{ route: 'closer', id: b4a.alloc(32, 9) }], {
    get(target, property, receiver) {
      if (property === '0') throw new Error('hostile index')
      return Reflect.get(target, property, receiver)
    }
  })
  const { dht, error } = await invalidCloserReply(closerNodes)

  t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE')
  t.is(dht.stats.requests.active, 0)
  t.is(dht.io._destinationRegistry.size, 0)
  await dht.destroy()
})

test('transport-only snapshots closer indexes without iterator authority', async (t) => {
  const transport = createTransport()
  const closer = transport.destinations[0]
  let lengths = 0
  let indexes = 0
  let iterators = 0
  const closerNodes = new Proxy([closer], {
    get(target, property, receiver) {
      if (property === 'length') lengths++
      if (property === '0') indexes++
      if (property === Symbol.iterator) {
        iterators++
        throw new Error('iterator must not be consulted')
      }
      return Reflect.get(target, property, receiver)
    }
  })
  const dht = createTransportDHT(transport)
  const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  transport.requests[0].resolve(
    validReply(transport.destinations[1], {
      closerNodes
    })
  )
  const outcome = await result.then(
    (value) => ({ value, error: null }),
    (error) => ({ value: null, error })
  )

  t.is(outcome.error, null)
  if (outcome.value) {
    t.is(Array.isArray(outcome.value.closerNodes), true)
    t.is(outcome.value.closerNodes === closerNodes, false)
    t.is(outcome.value.closerNodes[0], closer)
  }
  t.is(lengths, 1)
  t.is(indexes, 1)
  t.is(iterators, 0)
  t.is(dht.stats.requests.active, 0)
  t.is(dht.io._destinationRegistry.size, 0)
  await dht.destroy()
})

test('transport-only rejects response key collisions and retained-to conflicts', async (t) => {
  const to = { route: 'to', id: b4a.alloc(32, 1) }
  const from = { route: 'from', id: b4a.alloc(32, 2) }
  const conflict = { route: 'conflict', id: b4a.alloc(32, 3) }
  const transport = createTransport({
    key(destination) {
      transport.calls.key.push([destination])
      return destination === to || destination === conflict ? 'same' : 'from'
    },
    id(destination) {
      transport.calls.id.push([destination])
      return destination.id
    }
  })
  const dht = createTransportDHT(transport)
  const result = dht.request({ command: 7 }, to, { retry: false })
  await tick()
  transport.requests[0].resolve({
    from,
    closerNodes: [conflict],
    error: 0,
    rtt: 1
  })

  const error = await promiseError(result)
  t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE')
  t.is(dht.io._destinationRegistry.size, 0)
  await dht.destroy()
})

test('transport-only retries deterministically and cancels before retry', async (t) => {
  const timer = manualTimer()
  const events = []
  const transport = createTransport({
    request(message) {
      events.push(`request:${message.attempt}`)
      transport.calls.request.push([message])
      const pending = deferred()
      transport.requests.push(pending)
      return {
        promise: pending.promise,
        cancel(reason) {
          events.push(`cancel:${message.attempt}`)
          transport.calls.cancel.push([reason])
        }
      }
    }
  })
  const dht = createTransportDHT(transport, { requestTimer: timer, requestTimeout: 1_000 })
  const req = dht._request(
    transport.destinations[0],
    false,
    false,
    7,
    null,
    null,
    null,
    () => events.push('response'),
    () => events.push('error')
  )
  req.retries = 1
  req.oncycle = () => events.push(`cycle:${req.sent}`)
  await tick()

  timer.advance(1_000)
  await tick()
  t.alike(events, ['request:1', 'cycle:1', 'cancel:1', 'request:2'])
  t.alike(
    transport.calls.request.map(([message]) => message.attempt),
    [1, 2]
  )
  t.is(transport.calls.cancel[0][0].code, 'REQUEST_TIMEOUT')
  t.alike(dht.stats.requests, {
    active: 1,
    total: 1,
    responses: 0,
    timeouts: 0,
    retries: 1
  })

  timer.advance(1_000)
  await tick()
  t.alike(events, ['request:1', 'cycle:1', 'cancel:1', 'request:2', 'cycle:2', 'cancel:2', 'error'])
  t.alike(dht.stats.requests, {
    active: 0,
    total: 1,
    responses: 0,
    timeouts: 1,
    retries: 1
  })
  t.is(dht.io._destinationRegistry.size, 0)

  await dht.destroy()
})

test('transport-only retry false performs one timeout attempt', async (t) => {
  const timer = manualTimer()
  const transport = createTransport()
  const dht = createTransportDHT(transport, { requestTimer: timer })
  const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  timer.advance(1_000)
  const error = await promiseError(result)

  t.is(error && error.code, 'REQUEST_TIMEOUT')
  t.is(transport.calls.request.length, 1)
  t.is(transport.calls.cancel.length, 1)
  t.is(dht.stats.requests.timeouts, 1)
  await dht.destroy()
})

test('transport-only cancels a valid operation when timer set throws', async (t) => {
  const setError = new Error('set failed')
  const timer = {
    set() {
      throw setError
    },
    clear() {
      throw new Error('must not clear an unset timer')
    }
  }
  const transport = createTransport()
  const dht = createTransportDHT(transport, { requestTimer: timer })
  const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  const error = await promiseError(result)

  t.is(error && error.code, 'TRANSPORT_INVALID')
  t.is(error && error.cause, setError)
  t.is(transport.calls.cancel.length, 1)
  if (transport.calls.cancel.length > 0) t.is(transport.calls.cancel[0][0], error)
  t.is(dht.stats.requests.active, 0)
  t.is(dht.io._destinationRegistry.size, 0)
  transport.requests[0].resolve(validReply(transport.destinations[1]))
  await tick()
  t.is(dht.stats.requests.responses, 0, 'late response ignored')
  await dht.destroy()
})

test('transport-only timer clear failure is terminal on reply', async (t) => {
  const clearError = new Error('clear failed')
  const timer = throwingClearTimer(clearError)
  const transport = createTransport()
  const dht = createTransportDHT(transport, { requestTimer: timer })
  const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  transport.requests[0].resolve(validReply(transport.destinations[1]))
  const error = await promiseError(result)

  t.is(error && error.code, 'TRANSPORT_INVALID')
  t.is(error && error.cause, clearError)
  t.is(dht.stats.requests.active, 0)
  t.is(dht.stats.requests.responses, 0)
  t.is(dht.io._destinationRegistry.size, 0)
  await dht.destroy()
})

test('transport-only timeout wins over timer clear failure', async (t) => {
  const clearError = new Error('clear failed')
  const timer = throwingClearTimer(clearError)
  const transport = createTransport()
  const dht = createTransportDHT(transport, { requestTimer: timer })
  const emitted = []
  dht.on('transport-error', (error) => emitted.push(error))
  const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  timer.fire()
  const error = await promiseError(result)
  await tick()

  t.is(error && error.code, 'REQUEST_TIMEOUT')
  t.is(transport.calls.cancel.length, 1)
  t.is(emitted.length, 1)
  t.is(emitted[0].code, 'TRANSPORT_INVALID')
  t.is(emitted[0].cause, clearError)
  t.is(dht.stats.requests.active, 0)
  t.is(dht.io._destinationRegistry.size, 0)
  await dht.destroy()
})

test('transport-only lifecycle error wins over timer clear failure', async (t) => {
  for (const [name, terminate, code] of [
    ['session', (dht, session, error) => session.destroy(error), null],
    ['suspend', (dht) => dht.suspend(), 'IO_SUSPENDED'],
    ['destroy', (dht) => dht.destroy(), 'REQUEST_DESTROYED']
  ]) {
    const clearError = new Error(`${name} clear failed`)
    const timer = throwingClearTimer(clearError)
    const transport = createTransport()
    const dht = createTransportDHT(transport, { requestTimer: timer })
    const session = dht.session()
    const custom = new Error('session terminal')
    const emitted = []
    dht.on('transport-error', (error) => emitted.push(error))
    const result = dht.request({ command: 7 }, transport.destinations[0], { session, retry: false })
    await tick()
    await terminate(dht, session, custom)
    const error = await promiseError(result)
    await tick()

    if (code === null) t.is(error, custom, name)
    else t.is(error && error.code, code, name)
    t.is(transport.calls.cancel.length, 1)
    t.is(emitted.length, 1)
    t.is(emitted[0].code, 'TRANSPORT_INVALID')
    t.is(emitted[0].cause, clearError)
    t.is(dht.stats.requests.active, 0)
    t.is(dht.io._destinationRegistry.size, 0)
    transport.requests[0].reject(new Error('late rejection'))
    await tick()
    if (!dht.destroyed) await dht.destroy()
  }
})

test('transport-only retries synchronous throws and rejects asynchronously', async (t) => {
  let calls = 0
  let synchronous = true
  const transport = createTransport({
    request() {
      calls++
      throw new Error('offline')
    }
  })
  const dht = createTransportDHT(transport)
  const result = dht
    .request({ command: 7 }, transport.destinations[0], { retry: false })
    .catch((error) => {
      t.is(synchronous, false)
      return error
    })
  synchronous = false
  const error = await result

  t.is(calls, 1)
  t.is(error.code, 'TRANSPORT_UNAVAILABLE')
  t.is(error.cause && error.cause.message, 'offline')
  t.is(dht.io._destinationRegistry.size, 0)
  await dht.destroy()
})

test('transport-only session destroy cancels and ignores late settlement', async (t) => {
  const transport = createTransport()
  const dht = createTransportDHT(transport)
  const session = dht.session()
  const custom = new Error('session closed')
  const result = dht.request({ command: 7 }, transport.destinations[0], { session, retry: false })
  await tick()

  session.destroy(custom)
  const error = await promiseError(result)
  t.is(error, custom)
  t.is(transport.calls.cancel.length, 1)
  t.is(transport.calls.cancel[0][0], custom)
  t.is(session.inflight.length, 0)
  t.is(dht.stats.requests.active, 0)

  transport.requests[0].resolve(validReply(transport.destinations[1]))
  await tick()
  t.is(dht.stats.requests.responses, 0)
  t.is(dht.stats.requests.active, 0)
  await dht.destroy()
})

test('transport-only session request and ping preserve opaque destinations', async (t) => {
  const transport = createTransport()
  const dht = createTransportDHT(transport)
  const session = dht.session()
  const to = transport.destinations[0]

  const request = session.request({ command: 7 }, to, { retry: false })
  await tick()
  t.is(transport.calls.request[0][0].to, to)
  transport.requests[0].resolve(validReply(transport.destinations[1]))
  await request

  const ping = session.ping(to, { retry: false })
  await tick()
  t.is(transport.calls.request[1][0].to, to)
  transport.requests[1].resolve(validReply(transport.destinations[1]))
  await ping

  await dht.destroy()
})

test('transport-only suspend resume and destroy are ordered and idempotent', async (t) => {
  const events = []
  const transport = createTransport({
    ready() {
      events.push('ready')
      return Promise.resolve()
    },
    suspend() {
      events.push('suspend')
      return Promise.resolve()
    },
    resume() {
      events.push('resume')
      return Promise.resolve()
    },
    destroy() {
      events.push('destroy')
      return Promise.resolve()
    }
  })
  const dht = createTransportDHT(transport)
  let listening = 0
  let ready = 0
  dht.on('listening', () => listening++)
  dht.on('ready', () => ready++)
  await dht.fullyBootstrapped()
  t.alike(events, ['ready'])
  t.is(ready, 1)
  t.is(listening, 0)

  const active = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  await Promise.all([dht.suspend(), dht.suspend()])
  const suspendError = await promiseError(active)
  t.is(suspendError && suspendError.code, 'IO_SUSPENDED')
  t.is(transport.calls.cancel.length, 1)
  t.is(transport.calls.cancel[0][0].code, 'IO_SUSPENDED')
  t.alike(events, ['ready', 'suspend'])

  await Promise.all([dht.resume(), dht.resume()])
  t.alike(events, ['ready', 'suspend', 'resume'])

  const activeOnDestroy = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  await Promise.all([dht.destroy(), dht.destroy()])
  const destroyError = await promiseError(activeOnDestroy)
  t.is(destroyError && destroyError.code, 'REQUEST_DESTROYED')
  t.is(transport.calls.cancel[1][0].code, 'REQUEST_DESTROYED')
  t.alike(events, ['ready', 'suspend', 'resume', 'destroy'])
})

test('transport-only retries suspend after failure without changing actual state', async (t) => {
  const failures = [new Error('first suspend failed'), new Error('second suspend failed')]
  let suspends = 0
  const transport = createTransport({
    suspend() {
      return Promise.reject(failures[suspends++])
    }
  })
  const dht = createTransportDHT(transport)
  await dht.fullyBootstrapped()

  for (let i = 0; i < failures.length; i++) {
    const error = await promiseError(dht.suspend())
    t.is(error && error.code, 'TRANSPORT_UNAVAILABLE', `failure ${i + 1}`)
    t.is(error && error.cause, failures[i], `cause ${i + 1}`)
    t.is(dht.suspended, false, `DHT actual ${i + 1}`)
    t.is(dht.io.suspended, false, `transport actual ${i + 1}`)
  }

  t.is(suspends, 2)
  const requestError = promiseError(
    dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  )
  await tick()
  t.is(transport.calls.request.length, 1)
  if (transport.requests[0]) {
    transport.requests[0].resolve(validReply(transport.destinations[1]))
  }
  t.is(await requestError, null)
  await dht.destroy()
})

test('transport-only queues suspend requested during resume', async (t) => {
  const lifecycle = []
  const emitted = []
  const resume = deferred()
  const transport = createTransport({
    ready() {
      lifecycle.push('ready')
      return Promise.resolve()
    },
    suspend() {
      lifecycle.push('suspend')
      return Promise.resolve()
    },
    resume() {
      lifecycle.push('resume')
      return resume.promise
    }
  })
  const dht = createTransportDHT(transport)
  dht.on('suspend', () => emitted.push('suspend'))
  dht.on('resume', () => emitted.push('resume'))
  await dht.fullyBootstrapped()
  await dht.suspend()

  const resuming = dht.resume()
  await tick()
  const suspending = dht.suspend()
  t.alike(lifecycle, ['ready', 'suspend', 'resume'])
  t.is(dht.suspended, true)
  t.is(dht.io.suspended, true)

  resume.resolve()
  await Promise.all([resuming, suspending])

  t.alike(lifecycle, ['ready', 'suspend', 'resume', 'suspend'])
  t.alike(emitted, ['suspend', 'resume', 'suspend'])
  t.is(dht.suspended, true)
  t.is(dht.io.suspended, true)
  await dht.destroy()
})

test('transport-only maps lifecycle failures and preserves cancel outcomes', async (t) => {
  for (const [method, invoke] of [
    ['ready', (dht) => dht.fullyBootstrapped()],
    ['suspend', (dht) => dht.suspend()],
    [
      'resume',
      async (dht) => {
        await dht.suspend()
        return dht.resume()
      }
    ],
    ['destroy', (dht) => dht.destroy()]
  ]) {
    const original = new Error(`${method} failed`)
    const transport = createTransport({ [method]: () => Promise.reject(original) })
    const dht = createTransportDHT(transport)
    if (method !== 'ready') await dht.fullyBootstrapped()
    const error = await promiseError(invoke(dht))

    t.is(error && error.code, 'TRANSPORT_UNAVAILABLE', method)
    t.is(error && error.cause, original, `${method} cause`)
    if (method !== 'destroy') {
      transport.destroy = () => Promise.resolve()
      await dht.destroy()
    }
  }

  const timer = manualTimer()
  const cancelError = new Error('cancel failed')
  const transport = createTransport({
    request(message) {
      transport.calls.request.push([message])
      return {
        promise: new Promise(() => {}),
        cancel: () => {
          throw cancelError
        }
      }
    }
  })
  const dht = createTransportDHT(transport, { requestTimer: timer })
  const emitted = []
  dht.on('transport-error', (error) => emitted.push(error))
  const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  timer.advance(1_000)
  const error = await promiseError(result)

  t.is(error && error.code, 'REQUEST_TIMEOUT')
  t.is(emitted.length, 1)
  t.is(emitted[0].code, 'TRANSPORT_UNAVAILABLE')
  t.is(emitted[0].cause, cancelError)
  await dht.destroy()
})

test('transport-only rejects direct request authority before adapter activity', async (t) => {
  for (const [method, args] of [
    ['request', [{ command: 7 }, { route: 'a', id: b4a.alloc(32) }, { socket: {} }]],
    ['request', [{ command: 7 }, { route: 'a', id: b4a.alloc(32) }, { ttl: 1 }]],
    ['ping', [{ route: 'a', id: b4a.alloc(32) }, { ttl: 1 }]],
    ['delayedPing', [{ route: 'a', id: b4a.alloc(32) }, 1, { ttl: 1 }]]
  ]) {
    const transport = createTransport()
    const dht = createTransportDHT(transport)
    const error = await callError(() => dht[method](...args))
    t.is(error && error.code, 'DIRECT_IO_FORBIDDEN')
    t.is(transport.calls.request.length, 0)
    await dht.destroy()
  }
})

test('transport-only rejects direct packet entrypoints before adapter activity', async (t) => {
  const transport = createTransport()
  const dht = createTransportDHT(transport)

  for (const invoke of [
    () => dht.bind(),
    () => dht.onmessage({}, b4a.from([1, 2]), { host: '127.0.0.1', port: 1 })
  ]) {
    const error = syncError(invoke)
    t.is(error && error.code, 'DIRECT_IO_FORBIDDEN')
  }

  for (const calls of Object.values(transport.calls)) {
    if (calls === transport.calls.ready) continue
    t.is(calls.length, 0)
  }
  await dht.destroy()
})

test('constructor validates transport request timer', async (t) => {
  for (const requestTimer of [null, {}, { set() {}, clear: null }]) {
    await constructorError(
      t,
      {
        outboundPolicy: 'transport-only',
        requestTransport: createTransport(),
        requestTimer
      },
      'TRANSPORT_INVALID'
    )
  }
})

test('direct session destruction releases real congestion for subsequent requests', async (t) => {
  const dht = new DHT({ bootstrap: false, host: '127.0.0.1' })
  await dht.io.bind()

  const firstSession = dht.session()
  const firstError = new Error('first closed')
  const firstResult = promiseError(
    firstSession.request({ command: 7 }, { host: '127.0.0.1', port: 1 }, { retry: false })
  )

  t.is(firstSession.inflight.length, 1)
  t.is(dht.io.inflight.length, 1)
  t.is(dht.io.inflight[0].sent, 1)
  t.is(dht.io.congestion._total, 1)
  t.is(dht.stats.requests.active, 1)
  firstSession.destroy(firstError)
  t.is(await firstResult, firstError)
  t.is(firstSession.inflight.length, 0)
  t.is(dht.io.inflight.length, 0)
  t.is(dht.io.congestion._total, 0)
  t.is(dht.stats.requests.active, 0)

  const secondSession = dht.session()
  const secondError = new Error('second closed')
  const secondResult = promiseError(
    secondSession.request({ command: 7 }, { host: '127.0.0.1', port: 1 }, { retry: false })
  )
  t.is(secondSession.inflight.length, 1)
  t.is(dht.io.inflight[0].sent, 1, 'subsequent request sends immediately')
  t.is(dht.io.congestion._total, 1)
  secondSession.destroy(secondError)
  t.is(await secondResult, secondError)
  t.is(dht.io.congestion._total, 0)
  t.is(dht.stats.requests.active, 0)

  await dht.destroy()
})

test('direct request installs callbacks before send', (t) => {
  const onresponse = () => {}
  const onerror = () => {}
  const req = {
    onresponse: null,
    onerror: null,
    send(force) {
      t.is(this.onresponse, onresponse)
      t.is(this.onerror, onerror)
      t.is(force, true)
    }
  }
  const dht = Object.create(DHT.prototype)
  dht._sendDownHints = true
  dht.io = { createRequest: () => req }

  t.is(dht._request({}, true, false, 7, null, null, null, onresponse, onerror), req)
})

test('request configuration runs before first send', (t) => {
  const oncycle = () => {}
  const req = {
    retries: 3,
    oncycle: null,
    send() {
      t.is(this.retries, 7)
      t.is(this.oncycle, oncycle)
    }
  }
  const dht = Object.create(DHT.prototype)
  dht._sendDownHints = true
  dht.io = { createRequest: () => req }

  dht._request(
    {},
    false,
    false,
    7,
    null,
    null,
    null,
    () => {},
    () => {},
    (request) => {
      request.retries = 7
      request.oncycle = oncycle
    }
  )
})

test('transport-only query traverses opaque destinations by adapter identity', async (t) => {
  const a = { ref: 'a' }
  const b = { ref: 'b' }
  const c = { ref: 'c' }
  const bAgain = { ref: 'b' }
  const ids = new Map([
    [a, b4a.alloc(32, 3)],
    [b, b4a.alloc(32, 2)],
    [c, b4a.alloc(32, 1)],
    [bAgain, b4a.alloc(32, 2)]
  ])
  const visited = []
  let query = null
  const admitted = new Set()
  const transport = createOpaqueTransport({
    bootstrap: [a],
    ids,
    request(message) {
      visited.push(message.to)
      if (query && query._transportCandidates) {
        for (const candidate of query._transportCandidates.values()) admitted.add(candidate)
      }
      const closerNodes = message.to === a ? [b] : message.to === b ? [c] : [bAgain]
      return immediateOperation(validReply(message.to, { closerNodes }))
    }
  })
  const dht = createTransportDHT(transport)
  query = dht.query({ target: b4a.alloc(32), command: 7 }, { concurrency: 1 })
  const replies = []
  query.on('data', (reply) => replies.push(reply))

  await query.finished()

  t.alike(visited, [a, b, c])
  t.alike(
    replies.map((reply) => reply.from),
    [a, b, c]
  )
  t.alike(replies[0].closerNodes, [b])
  t.alike(replies[1].closerNodes, [c])
  t.alike(replies[2].closerNodes, [bAgain])
  t.alike(query.closestNodes, [c, b, a])
  t.alike([...query._seen.keys()], ['a', 'b', 'c'])
  t.is(admitted.size, 3, 'same adapter key is admitted once')
  for (const candidate of admitted) {
    t.ok(Object.isFrozen(candidate))
    t.alike(Object.keys(candidate), ['destination', 'key', 'id'])
    t.is(candidate.id === ids.get(candidate.destination), false)
    t.alike(candidate.id, ids.get(candidate.destination))
    t.is('host' in candidate.destination, false)
    t.is('port' in candidate.destination, false)
    t.is('id' in candidate.destination, false)
  }
  t.is(query._transportCandidates.size, 0, 'query teardown clears candidates')
  t.is('_refreshTicks' in dht, false, 'query does not create direct refresh state')
  await dht.destroy()
})

test('transport-only query rejects one key with conflicting ids before visiting it', async (t) => {
  const a = { ref: 'a' }
  const b = { ref: 'b' }
  const conflict = { ref: 'a' }
  const ids = new Map([
    [a, b4a.alloc(32, 1)],
    [b, b4a.alloc(32, 2)],
    [conflict, b4a.alloc(32, 3)]
  ])
  const visited = []
  let stateBeforeInvalidReply = null
  let query = null
  const transport = createOpaqueTransport({
    bootstrap: [a],
    ids,
    request(message) {
      visited.push(message.to)
      if (message.to === b) stateBeforeInvalidReply = query._seen.get('b')
      return immediateOperation(
        validReply(message.to, { closerNodes: message.to === a ? [b] : [conflict] })
      )
    }
  })
  const dht = createTransportDHT(transport)
  query = dht.query({ target: b4a.alloc(32), command: 7 }, { concurrency: 1 })
  const error = await promiseError(query.finished())

  t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE')
  t.alike(visited, [a, b])
  t.is(query._seen.get('b'), stateBeforeInvalidReply, 'invalid reply cannot update seen state')
  t.is(query._transportCandidates.size, 0)
  await dht.destroy()
})

test('transport-only query admits closest bootstrap caller nodes and replies', async (t) => {
  const a = { ref: 'closest' }
  const b = { ref: 'bootstrap' }
  const c = { ref: 'caller-node' }
  const d = { ref: 'caller-reply' }
  const destinations = [a, b, c, d]
  const ids = opaqueIds(destinations)
  const visited = []
  const transport = createOpaqueTransport({
    closest: [a],
    bootstrap: async function* () {
      yield b
    },
    ids,
    request(message) {
      visited.push(message.to)
      return immediateOperation(validReply(message.to))
    }
  })
  const dht = createTransportDHT(transport)
  const target = b4a.alloc(32)
  const fromNodes = dht.query({ target, command: 7 }, { nodes: [c], concurrency: 1 })
  await fromNodes.finished()

  t.alike(new Set(visited), new Set([a, b, c]))
  t.alike(transport.calls.closest[0][0], { target, limit: 19 })
  t.alike(transport.calls.bootstrap[0][0], { target, limit: 18 })

  transport.localClosest = []
  transport.remoteBootstrap = []
  visited.length = 0
  const fromReplies = dht.query({ target, command: 7 }, { replies: [{ from: d }], concurrency: 1 })
  await fromReplies.finished()
  t.alike(visited, [d])
  await dht.destroy()
})

test('transport-only query validates every bootstrap entry before requesting', async (t) => {
  for (const [name, bootstrap, ids] of invalidBootstrapCases()) {
    const transport = createOpaqueTransport({ bootstrap, ids })
    const dht = createTransportDHT(transport)
    const query = dht.query({ target: b4a.alloc(32), command: 7 })
    const error = await promiseError(query.finished())

    t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE', name)
    t.is(transport.calls.request.length, 0, `${name} requests`)
    t.is(query._transportCandidates.size, 0, `${name} registry`)
    await dht.destroy()
  }
})

test('transport-only query construction is atomic for hostile caller seeds', async (t) => {
  for (const { name, nodes, replies, ids } of hostileCallerSeedCases()) {
    const transport = createOpaqueTransport({ ids })
    const dht = createTransportDHT(transport)
    let sessions = 0
    const createSession = dht.session.bind(dht)
    dht.session = function () {
      sessions++
      return createSession()
    }
    const error = syncError(() =>
      dht.query(
        { target: b4a.alloc(32), command: 7 },
        nodes === undefined ? { replies } : { nodes }
      )
    )

    t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE', name)
    t.alike(dht.stats.queries, { active: 0, total: 0 }, `${name} stats`)
    t.is(sessions, 0, `${name} session`)
    t.is(transport.calls.request.length, 0, `${name} requests`)
    t.is(dht.io._destinationRegistry.size, 0, `${name} request registry`)
    await dht.destroy()
  }
})

test('transport-only query rolls back hostile closest and bootstrap iterables', async (t) => {
  for (const source of ['closest', 'bootstrap']) {
    const destination = { ref: source }
    const ids = opaqueIds([destination])
    const hostile = throwingIterable(
      destination,
      new Error(`${source} iterator failed`),
      source === 'bootstrap'
    )
    const transport = createOpaqueTransport({
      closest: source === 'closest' ? hostile : [],
      bootstrap: source === 'bootstrap' ? hostile : [],
      ids
    })
    const dht = createTransportDHT(transport)
    const query = dht.query({ target: b4a.alloc(32), command: 7 })
    const error = await promiseError(query.finished())

    t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE', source)
    t.is(transport.calls.request.length, 0, `${source} requests`)
    t.is(query._transportCandidates.size, 0, `${source} candidates`)
    t.alike(dht.stats.queries, { active: 0, total: 1 }, `${source} stats`)
    await dht.destroy()
  }
})

test('transport-only query shares one bounded candidate registry', async (t) => {
  const destinations = Array.from({ length: 21 }, (_, i) => ({ ref: `route-${i}` }))
  const ids = opaqueIds(destinations)
  const transport = createOpaqueTransport({
    closest: destinations.slice(1, 9),
    bootstrap: destinations.slice(9, 19),
    ids,
    request(message) {
      t.is(query._transportCandidates.size, 19, 'all initial sources admitted before request')
      return immediateOperation(validReply(destinations[19], { closerNodes: [destinations[20]] }))
    }
  })
  const dht = createTransportDHT(transport, { maxTransportCandidates: 20 })
  const query = dht.query(
    { target: b4a.alloc(32), command: 7 },
    { nodes: [destinations[0]], concurrency: 1 }
  )
  const error = await promiseError(query.finished())

  t.is(error && error.code, 'TRANSPORT_INVALID_RESPONSE')
  t.is(transport.calls.request.length, 1)
  t.is(query._transportCandidates.size, 0, 'overflow teardown clears registry')
  await dht.destroy()
})

test('transport-only auto commit stays in the query session and cancels on destroy', async (t) => {
  const destination = { ref: 'commit-route' }
  const token = b4a.alloc(32, 7)
  const ids = opaqueIds([destination])
  const transport = createOpaqueTransport({ ids })
  const dht = createTransportDHT(transport)
  const query = dht.query(
    { target: b4a.alloc(32), command: 7, value: b4a.from('value') },
    { nodes: [destination], commit: true, concurrency: 1 }
  )
  const finished = query.finished()
  await waitFor(() => transport.requests.length === 1)
  transport.requests[0].resolve(validReply(destination, { token }))
  await waitFor(() => transport.calls.request.length === 2)

  const commit = transport.calls.request[1][0]
  t.is(commit.to, destination)
  t.is(commit.token, token)
  t.is(commit.internal, false)
  t.ok(query._session.inflight.some((request) => request.to === destination))

  const terminal = new Error('query stopped')
  query.destroy(terminal)
  t.is(await promiseError(finished), terminal)
  t.is(transport.calls.cancel.length, 1)
  t.is(transport.calls.cancel[0][0].code, 'REQUEST_DESTROYED')
  t.is(query._session.inflight.length, 0)
  t.is(query._transportCandidates.size, 0)
  await dht.destroy()
})

test('transport-only query destroy cancels active traversal', async (t) => {
  const destination = { ref: 'traversal-route' }
  const transport = createOpaqueTransport({ ids: opaqueIds([destination]) })
  const dht = createTransportDHT(transport)
  const query = dht.query(
    { target: b4a.alloc(32), command: 7 },
    { nodes: [destination], concurrency: 1 }
  )
  const finished = query.finished()
  await waitFor(() => transport.requests.length === 1)

  const terminal = new Error('stop traversal')
  query.destroy(terminal)

  t.is(await promiseError(finished), terminal)
  t.is(transport.calls.cancel.length, 1)
  t.is(transport.calls.cancel[0][0].code, 'REQUEST_DESTROYED')
  t.is(query._session.inflight.length, 0)
  t.is(query._transportCandidates.size, 0)
  await dht.destroy()
})

test('transport-only query isolates traversal from a shared session', async (t) => {
  const queryDestination = { ref: 'query' }
  const unrelatedDestination = { ref: 'unrelated' }
  const destinations = [queryDestination, unrelatedDestination]
  const transport = createOpaqueTransport({ ids: opaqueIds(destinations) })
  const dht = createTransportDHT(transport)
  const shared = dht.session()
  const unrelated = shared.request({ command: 8 }, unrelatedDestination, { retry: false })
  await waitFor(() => transport.requests.length === 1)
  const query = dht.query(
    { target: b4a.alloc(32), command: 7 },
    { nodes: [queryDestination], concurrency: 1, session: shared }
  )
  const finished = query.finished()
  await waitFor(() => transport.requests.length === 2)

  query.destroy(new Error('stop query'))
  await promiseError(finished)
  t.is(transport.calls.cancel.length, 1)
  t.is(shared.inflight.length, 1, 'unrelated request remains attached')
  t.is(shared.inflight[0].to, unrelatedDestination)
  t.is(shared.children.size, 0, 'query child detaches without destroying parent')

  transport.requests[0].resolve(validReply(unrelatedDestination))
  t.is((await unrelated).from, unrelatedDestination)
  t.is(shared.inflight.length, 0)
  await dht.destroy()
})

test('transport-only query isolates auto commit from a shared session', async (t) => {
  const queryDestination = { ref: 'query' }
  const unrelatedDestination = { ref: 'unrelated' }
  const destinations = [queryDestination, unrelatedDestination]
  const transport = createOpaqueTransport({ ids: opaqueIds(destinations) })
  const dht = createTransportDHT(transport)
  const shared = dht.session()
  const unrelated = shared.request({ command: 8 }, unrelatedDestination, { retry: false })
  await waitFor(() => transport.requests.length === 1)
  const query = dht.query(
    { target: b4a.alloc(32), command: 7 },
    { nodes: [queryDestination], concurrency: 1, session: shared, commit: true }
  )
  const finished = query.finished()
  await waitFor(() => transport.requests.length === 2)
  transport.requests[1].resolve(validReply(queryDestination, { token: b4a.alloc(32, 7) }))
  await waitFor(() => transport.requests.length === 3)

  query.destroy(new Error('stop commit'))
  await promiseError(finished)
  t.is(transport.calls.cancel.length, 1)
  t.is(shared.inflight.length, 1, 'unrelated request survives commit cancellation')
  t.is(shared.inflight[0].to, unrelatedDestination)
  t.is(shared.children.size, 0, 'commit child detaches without destroying parent')

  transport.requests[0].resolve(validReply(unrelatedDestination))
  t.is((await unrelated).from, unrelatedDestination)
  t.is(shared.inflight.length, 0)
  await dht.destroy()
})

test('direct query candidates copy ids and freeze their dial descriptor', (t) => {
  const sourceId = b4a.alloc(32, 1)
  const source = { id: sourceId, host: '127.0.0.1', port: 1234 }
  const dht = Object.create(DHT.prototype)
  dht.outboundPolicy = 'direct'
  const candidate = dht._queryCandidate(source, new Map())

  sourceId.fill(9)
  source.id = b4a.alloc(32, 8)
  source.host = 'mutated.invalid'
  source.port = 9999

  t.ok(Object.isFrozen(candidate))
  t.ok(Object.isFrozen(candidate.destination))
  t.is(candidate.id === sourceId, false)
  t.is(candidate.id === candidate.destination.id, false)
  t.alike(candidate.id, b4a.alloc(32, 1))
  t.alike(candidate.destination.id, b4a.alloc(32, 1))
  t.is(candidate.key, '127.0.0.1:1234')
  t.alike(candidate.destination, {
    id: b4a.alloc(32, 1),
    host: '127.0.0.1',
    port: 1234
  })
})

test('direct query rejects ids that shrink while being copied', (t) => {
  const dht = Object.create(DHT.prototype)
  dht.outboundPolicy = 'direct'
  const registry = new Map()
  const source = {
    id: shrinkingId(),
    host: '127.0.0.1',
    port: 1234
  }
  const error = syncError(() => dht._queryCandidate(source, registry))

  t.ok(error)
  t.is(error && error.message, 'Invalid direct node id')
  t.is(registry.size, 0)
})

test('transport-only bounds and closes closest and bootstrap iterators', async (t) => {
  for (const [source, async, finite] of [
    ['closest', false, true],
    ['closest', false, false],
    ['bootstrap', true, true],
    ['bootstrap', true, false]
  ]) {
    const destination = { ref: `${source}-${finite ? 'finite' : 'infinite'}` }
    const bounded = boundedIterable(destination, { async, finite, length: 5, safety: 10 })
    const transport = createOpaqueTransport({ ids: opaqueIds([destination]) })
    if (source === 'closest') transport.localClosest = bounded.iterable
    else transport.remoteBootstrap = bounded.iterable
    const dht = createTransportDHT(transport)
    const registry = new Map()
    const added = []
    let candidates = null
    let error = null

    try {
      if (source === 'closest') {
        candidates = dht._closestQueryNodes(b4a.alloc(32), 3, registry, added)
      } else {
        candidates = []
        for await (const candidate of dht._resolveQueryBootstrap(
          b4a.alloc(32),
          3,
          registry,
          added
        )) {
          candidates.push(candidate)
        }
      }
    } catch (cause) {
      error = cause
    }

    t.is(error, null, `${source} ${finite ? 'finite' : 'infinite'} error`)
    t.is(candidates && candidates.length, 3, `${source} count`)
    t.is(bounded.state.next, 3, `${source} bounded next`)
    t.is(bounded.state.returned, 1, `${source} closes iterator`)
    t.is(registry.size, 1, `${source} repeated key capacity`)
    await dht.destroy()
  }
})

test('transport-only query reuses the initial candidate identity when dialing', async (t) => {
  const destination = { ref: 'seed' }
  let keys = 0
  let ids = 0
  const transport = createOpaqueTransport({
    bootstrap: [],
    ids: new Map(),
    key() {
      return ++keys === 1 ? 'seed' : 'changed-seed'
    },
    id() {
      return b4a.alloc(32, ++ids === 1 ? 1 : 9)
    }
  })
  const dht = createTransportDHT(transport)
  const query = dht.query(
    { target: b4a.alloc(32), command: 7 },
    { nodes: [destination], concurrency: 1 }
  )
  const finished = query.finished()
  await waitFor(() => transport.calls.request.length === 1)

  t.is(keys, 1)
  t.is(ids, 1)
  t.is(dht.io.inflight[0]._identity.destination, destination)
  t.is(dht.io.inflight[0]._identity.key, 'seed')
  t.alike(dht.io.inflight[0]._identity.id, b4a.alloc(32, 1))
  query.destroy(new Error('done'))
  await promiseError(finished)
  await dht.destroy()
})

test('transport-only query reuses validated reply identities when dialing closer nodes', async (t) => {
  const a = { ref: 'a' }
  const b = { ref: 'b' }
  const reads = new Map()
  let bReadsAtDial = null
  let bIdentityAtDial = null
  const transport = createOpaqueTransport({
    bootstrap: [a],
    ids: new Map(),
    key(destination) {
      const count = (reads.get(destination) || 0) + 1
      reads.set(destination, count)
      return destination.ref + (destination === b && count > 1 ? '-changed' : '')
    },
    id(destination) {
      const count = reads.get(destination)
      return b4a.alloc(32, destination === b && count > 1 ? 9 : destination === a ? 1 : 2)
    },
    request(message) {
      if (message.to === a) {
        return immediateOperation(validReply(a, { closerNodes: [b] }))
      }
      bReadsAtDial = reads.get(b)
      const request = dht.io.inflight.find((candidate) => candidate.to === b)
      bIdentityAtDial = request && request._identity
      return { promise: new Promise(() => {}), cancel() {} }
    }
  })
  const dht = createTransportDHT(transport)
  const query = dht.query({ target: b4a.alloc(32), command: 7 }, { concurrency: 1 })
  const finished = query.finished()
  await waitFor(() => transport.calls.request.length === 2)

  t.is(bReadsAtDial, 1)
  t.is(bIdentityAtDial.destination, b)
  t.is(bIdentityAtDial.key, 'b')
  t.alike(bIdentityAtDial.id, b4a.alloc(32, 2))
  query.destroy(new Error('done'))
  await promiseError(finished)
  await dht.destroy()
})

test('destroying a parent session terminates child queries without resurrection', async (t) => {
  const unrelatedDestination = { ref: 'unrelated' }
  const a = { ref: 'a' }
  const b = { ref: 'b' }
  const destinations = [unrelatedDestination, a, b]
  const transport = createOpaqueTransport({ ids: opaqueIds(destinations) })
  const dht = createTransportDHT(transport)
  const parent = dht.session()
  const unrelated = parent.request({ command: 8 }, unrelatedDestination, { retry: false })
  await waitFor(() => transport.calls.request.length === 1)
  const query = dht.query(
    { target: b4a.alloc(32), command: 7 },
    { nodes: [a, b], concurrency: 1, session: parent }
  )
  const finished = query.finished()
  await waitFor(() => transport.calls.request.length === 2)
  const terminal = new Error('parent closed')

  parent.destroy(terminal)
  await tick()
  if (transport.requests[2]) transport.requests[2].reject(terminal)
  t.is(await promiseError(unrelated), terminal)
  t.is(await promiseError(finished), terminal)
  await tick()
  t.is(transport.calls.request.length, 2, 'does not start the second query request')
  t.is(transport.calls.cancel.length, 2)
  t.is(parent.inflight.length, 0)
  t.is(parent.children.size, 0)
  t.is(parent.destroyed, true)

  parent.destroy(new Error('reentrant close'))
  t.is(transport.calls.cancel.length, 2, 'repeated destroy is clean')
  const late = await promiseError(parent.request({ command: 9 }, b, { retry: false }))
  t.is(late, terminal)
  t.is(transport.calls.request.length, 2, 'closed session cannot attach new requests')
  await dht.destroy()
})

test('destroying a parent immediately terminates an unopened child query', async (t) => {
  const transport = createOpaqueTransport()
  const dht = createTransportDHT(transport)
  const parent = dht.session()
  const query = dht.query(
    { target: b4a.alloc(32), command: 7 },
    { session: parent, concurrency: 1 }
  )
  const terminal = new Error('unopened parent closed')

  parent.destroy(terminal)
  const destroyingImmediately = query.destroying
  const error = await promiseError(query.finished())

  t.is(destroyingImmediately, true)
  t.is(error, terminal)
  t.is(dht.stats.queries.active, 0)
  t.is(parent.children.size, 0)
  t.is(transport.calls.closest.length, 0)
  t.is(transport.calls.bootstrap.length, 0)
  t.is(transport.calls.request.length, 0)
  await dht.destroy()
})

test('transport-only open stops before bootstrap when closest admission closes parent', async (t) => {
  const destination = { ref: 'closest-closes-parent' }
  const terminal = new Error('closest closed parent')
  let parent = null
  const transport = createOpaqueTransport({
    closest: [destination],
    ids: opaqueIds([destination]),
    key() {
      parent.destroy(terminal)
      return destination.ref
    }
  })
  const dht = createTransportDHT(transport)
  parent = dht.session()
  const query = parent.query({ target: b4a.alloc(32), command: 7 }, { concurrency: 1 })
  const error = await promiseError(query.finished())

  t.is(error, terminal)
  t.is(transport.calls.closest.length, 1)
  t.is(transport.calls.bootstrap.length, 0)
  t.is(transport.calls.request.length, 0)
  t.is(dht.stats.queries.active, 0)
  t.is(query._transportCandidates.size, 0)
  t.is(parent.children.size, 0)
  await dht.destroy()
})

test('transport-only open closes bootstrap iterator when admission closes parent', async (t) => {
  const destination = { ref: 'bootstrap-closes-parent' }
  const terminal = new Error('bootstrap closed parent')
  const bootstrap = boundedIterable(destination, {
    async: true,
    finite: false,
    length: 0,
    safety: 21
  })
  let parent = null
  const transport = createOpaqueTransport({
    bootstrap: bootstrap.iterable,
    ids: opaqueIds([destination]),
    key() {
      parent.destroy(terminal)
      return destination.ref
    }
  })
  const dht = createTransportDHT(transport)
  parent = dht.session()
  const query = parent.query({ target: b4a.alloc(32), command: 7 }, { concurrency: 1 })
  const error = await promiseError(query.finished())

  t.is(error, terminal)
  t.is(transport.calls.bootstrap.length, 1)
  t.is(bootstrap.state.next, 1)
  t.is(bootstrap.state.returned, 1)
  t.is(transport.calls.request.length, 0)
  t.is(dht.stats.queries.active, 0)
  t.is(query._transportCandidates.size, 0)
  t.is(parent.children.size, 0)
  await dht.destroy()
})

test('transport-only discards a queued reply after query teardown', async (t) => {
  const a = { ref: 'settled' }
  const b = { ref: 'late-closer' }
  const transport = createOpaqueTransport({ ids: opaqueIds([a, b]) })
  const dht = createTransportDHT(transport)
  let mapped = 0
  let pushed = 0
  const query = dht.query(
    { target: b4a.alloc(32), command: 7 },
    {
      nodes: [a],
      concurrency: 1,
      map(reply) {
        mapped++
        return reply
      }
    }
  )
  query.on('data', () => pushed++)
  const finished = query.finished()
  await waitFor(() => transport.calls.request.length === 1)
  const req = dht.io.inflight[0]
  const key = dht._nodeKey(query._requestCandidates.get(req))
  const seen = query._seen.get(key)
  const reply = dht.io._validateReply(validReply(a, { closerNodes: [b] }), req._identity)

  req._settle(null, reply)
  const terminal = new Error('destroy after settle')
  query.destroy(terminal)
  t.is(await promiseError(finished), terminal)
  await tick()

  t.is(mapped, 0)
  t.is(pushed, 0)
  t.is(query.successes, 0)
  t.is(query.errors, 0)
  t.is(query.closestReplies.length, 0)
  t.is(query._seen.size, 1)
  t.is(query._seen.get(key), seen)
  t.is(query._transportCandidates.size, 0)
  t.is(dht.io._replyCandidates.has(reply), false)
  t.is(query.inflight, 0)
  t.is(transport.calls.request.length, 1)
  await dht.destroy()
})

test('transport-only identity cannot resurrect a session during request creation', async (t) => {
  const destination = { ref: 'identity-destroys-session' }
  const terminal = new Error('identity closed session')
  let session = null
  const transport = createOpaqueTransport({
    ids: opaqueIds([destination]),
    key() {
      session.destroy(terminal)
      return destination.ref
    },
    request(message) {
      return immediateOperation(validReply(message.to))
    }
  })
  const dht = createTransportDHT(transport)
  session = dht.session()
  const error = await promiseError(session.request({ command: 7 }, destination, { retry: false }))

  t.is(error, terminal)
  t.is(session.inflight.length, 0)
  t.is(dht.io.inflight.length, 0)
  t.is(dht.stats.requests.active, 0)
  t.is(dht.stats.requests.total, 0)
  t.is(dht.io._destinationRegistry.size, 0)
  t.is(transport.calls.request.length, 0)
  await dht.destroy()
})

test('transport-only seed identity cannot activate a query after closing its parent', async (t) => {
  const destination = { ref: 'seed-destroys-parent' }
  const terminal = new Error('seed closed parent')
  let parent = null
  const transport = createOpaqueTransport({
    ids: opaqueIds([destination]),
    key() {
      parent.destroy(terminal)
      return destination.ref
    }
  })
  const dht = createTransportDHT(transport)
  parent = dht.session()
  let query = null
  const constructionError = syncError(() => {
    query = parent.query(
      { target: b4a.alloc(32), command: 7 },
      { nodes: [destination], concurrency: 1 }
    )
  })
  const activeAfterConstruction = dht.stats.queries.active
  const terminalError = query ? await promiseError(query.finished()) : constructionError

  t.is(constructionError, terminal)
  t.is(query, null)
  t.is(terminalError, terminal)
  t.is(activeAfterConstruction, 0)
  t.alike(dht.stats.queries, { active: 0, total: 0 })
  t.is(parent.children.size, 0)
  t.is(transport.calls.request.length, 0)
  await dht.destroy()
})

test('direct and transport public APIs preserve closed session terminals', async (t) => {
  const Session = require('../lib/session')
  for (const outboundPolicy of ['direct', 'transport-only']) {
    const terminal = new Error(`${outboundPolicy} closed`)
    const dht = Object.create(DHT.prototype)
    dht.destroyed = false
    dht.outboundPolicy = outboundPolicy
    const session = new Session(dht)
    session.destroy(terminal)
    const destination = { host: '127.0.0.1', port: 1 }

    t.is(
      await promiseError(dht.request({ command: 7 }, destination, { session })),
      terminal,
      `${outboundPolicy} request`
    )
    t.is(await promiseError(dht.ping(destination, { session })), terminal, `${outboundPolicy} ping`)
    t.is(
      await promiseError(dht.delayedPing(destination, 1, { session })),
      terminal,
      `${outboundPolicy} delayed ping`
    )
    t.is(
      syncError(() => dht.query({ target: b4a.alloc(32), command: 7 }, { session })),
      terminal,
      `${outboundPolicy} query`
    )
    t.is(
      syncError(() => dht.findNode(b4a.alloc(32), { session })),
      terminal,
      `${outboundPolicy} find node`
    )
  }
})

test('query configures retries and cycles before DHT sends', (t) => {
  const Query = require('../lib/query')
  const query = Object.create(Query.prototype)
  const req = { retries: 3, oncycle: null }
  let configure = null
  query.inflight = 0
  query.force = true
  query.internal = false
  query.command = 7
  query.target = b4a.alloc(32)
  query.value = null
  query.retries = 5
  query._session = null
  query._onvisitbound = () => {}
  query._onerrorbound = () => {}
  query._oncyclebound = () => {}
  query._requestCandidates = new WeakMap()
  const destination = { host: '127.0.0.1', port: 1 }
  const candidate = { destination, key: '127.0.0.1:1', id: b4a.alloc(32) }
  let requested = null
  query.dht = {
    _request(...args) {
      requested = args[0]
      configure = args[9]
      return req
    }
  }

  query._visit(candidate)
  t.is(typeof configure, 'function')
  if (configure) configure(req)
  t.is(requested, destination, 'passes the raw direct destination')
  t.is(req.retries, 0, 'force disables retries before send')
  t.is(req.oncycle, query._oncyclebound)
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

function createOpaqueTransport({ bootstrap = [], closest = [], ids, key, id, request } = {}) {
  const transport = createTransport()
  transport.localClosest = closest
  transport.remoteBootstrap = bootstrap
  transport.bootstrap = function (opts) {
    transport.calls.bootstrap.push([opts])
    return typeof this.remoteBootstrap === 'function'
      ? this.remoteBootstrap(opts)
      : Promise.resolve(this.remoteBootstrap)
  }
  transport.closest = function (opts) {
    transport.calls.closest.push([opts])
    return this.localClosest
  }
  transport.key = function (destination) {
    transport.calls.key.push([destination])
    return key ? key(destination) : destination.ref
  }
  transport.id = function (destination) {
    transport.calls.id.push([destination])
    return id ? id(destination) : ids && ids.get(destination)
  }
  if (request) {
    transport.request = function (message) {
      transport.calls.request.push([message])
      return request(message)
    }
  }
  return transport
}

function opaqueIds(destinations) {
  return new Map(destinations.map((destination, i) => [destination, b4a.alloc(32, i + 1)]))
}

function invalidBootstrapCases() {
  const valid = { ref: 'valid' }
  const invalid = { ref: 'invalid' }
  const first = { ref: 'same' }
  const conflict = { ref: 'same' }
  return [
    ['invalid identity', [valid, invalid], new Map([[valid, b4a.alloc(32, 1)]])],
    [
      'conflicting identity',
      [first, conflict],
      new Map([
        [first, b4a.alloc(32, 1)],
        [conflict, b4a.alloc(32, 2)]
      ])
    ]
  ]
}

function hostileCallerSeedCases() {
  const first = { ref: 'same' }
  const conflict = { ref: 'same' }
  const valid = { ref: 'valid' }
  const ids = new Map([
    [first, b4a.alloc(32, 1)],
    [conflict, b4a.alloc(32, 2)],
    [valid, b4a.alloc(32, 3)]
  ])
  const hostileNodes = [valid, valid]
  Object.defineProperty(hostileNodes, 0, {
    get() {
      throw new Error('node getter failed')
    }
  })
  const hostileReply = {}
  Object.defineProperty(hostileReply, 'from', {
    get() {
      throw new Error('from getter failed')
    }
  })
  const hostileLength = {}
  Object.defineProperty(hostileLength, 'length', {
    get() {
      throw new Error('length getter failed')
    }
  })
  return [
    { name: 'conflicting nodes', nodes: [first, conflict], ids },
    { name: 'conflicting replies', replies: [{ from: first }, { from: conflict }], ids },
    { name: 'hostile node getter', nodes: hostileNodes, ids },
    { name: 'hostile reply getter', replies: [{ from: valid }, hostileReply], ids },
    { name: 'hostile length getter', nodes: hostileLength, ids }
  ]
}

function throwingIterable(destination, error, async) {
  if (async) {
    return {
      async *[Symbol.asyncIterator]() {
        yield destination
        throw error
      }
    }
  }
  return {
    *[Symbol.iterator]() {
      yield destination
      throw error
    }
  }
}

function boundedIterable(destination, { async, finite, length, safety }) {
  const state = { next: 0, returned: 0 }
  const iterator = {
    next() {
      state.next++
      if (state.next > safety) throw new Error('iterator was not bounded')
      if (finite && state.next > length) return { done: true }
      return async
        ? Promise.resolve({ done: false, value: destination })
        : { done: false, value: destination }
    },
    return() {
      state.returned++
      return async ? Promise.resolve({ done: true }) : { done: true }
    }
  }
  const iterable = async
    ? { [Symbol.asyncIterator]: () => iterator }
    : { [Symbol.iterator]: () => iterator }
  return { iterable, state }
}

function immediateOperation(reply) {
  return { promise: Promise.resolve(reply), cancel() {} }
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

function createTransportDHT(transport, opts = {}) {
  return new DHT({
    outboundPolicy: 'transport-only',
    requestTransport: transport,
    ...opts
  })
}

function validReply(from, overrides = {}) {
  return { from, error: 0, rtt: 1, ...overrides }
}

function hostileIds() {
  return [
    [
      'byteLength',
      new Proxy(b4a.alloc(32), {
        get(target, property) {
          if (property === 'byteLength') throw new Error('hostile byteLength')
          return Reflect.get(target, property, target)
        }
      })
    ],
    [
      'copy',
      new Proxy(b4a.alloc(32), {
        get(target, property) {
          if (property === 'length') throw new Error('hostile copy')
          return Reflect.get(target, property, target)
        }
      })
    ]
  ]
}

function shrinkingId() {
  let byteLengths = 0
  return new Proxy(b4a.alloc(32), {
    get(target, property) {
      if (property === 'byteLength') return ++byteLengths === 1 ? 32 : 31
      if (property === 'length') return 31
      return Reflect.get(target, property, target)
    }
  })
}

async function invalidCloserReply(closerNodes) {
  const transport = createTransport()
  const dht = createTransportDHT(transport)
  const result = dht.request({ command: 7 }, transport.destinations[0], { retry: false })
  await tick()
  transport.requests[0].resolve(
    validReply(transport.destinations[1], {
      closerNodes
    })
  )
  return { dht, error: await promiseError(result) }
}

function manualTimer() {
  let now = 0
  let next = 0
  const pending = new Map()

  return {
    set(fn, ms) {
      const handle = { id: next++, at: now + ms, fn }
      pending.set(handle.id, handle)
      return handle
    },
    clear(handle) {
      if (handle) pending.delete(handle.id)
    },
    advance(ms) {
      now += ms
      const due = [...pending.values()]
        .filter((handle) => handle.at <= now)
        .sort((a, b) => a.at - b.at || a.id - b.id)
      for (const handle of due) {
        if (!pending.delete(handle.id)) continue
        handle.fn()
      }
    }
  }
}

function throwingClearTimer(error) {
  let callback = null
  let handle = null

  return {
    set(fn) {
      callback = fn
      handle = {}
      return handle
    },
    clear(candidate) {
      tSame(candidate, handle)
      throw error
    },
    fire() {
      callback()
    }
  }
}

function tSame(actual, expected) {
  if (actual !== expected) throw new Error('unexpected timer handle')
}

async function promiseError(promise) {
  try {
    await promise
    return null
  } catch (error) {
    return error
  }
}

async function waitFor(condition) {
  for (let i = 0; i < 20; i++) {
    if (condition()) return
    await tick()
  }
  throw new Error('condition not reached')
}

async function callError(fn) {
  try {
    await fn()
    return null
  } catch (error) {
    return error
  }
}

function syncError(fn) {
  try {
    fn()
    return null
  } catch (error) {
    return error
  }
}

function tick() {
  return Promise.resolve().then(() => Promise.resolve())
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
