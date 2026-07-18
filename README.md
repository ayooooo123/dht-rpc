# dht-rpc

Make RPC calls over a [Kademlia](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf) based DHT.

```
npm install dht-rpc
```

## Key Features

- Remote IP / firewall detection
- Easily add any command to your DHT
- Streaming queries and updates

Note that internally V5 of dht-rpc differs significantly from V4, due to a series
of improvements to NAT detection, secure routing IDs and more.

## Experimental request transports

The `private-routing-v1` branch has an experimental, generic request-transport
hook. Direct UDP remains the default. To make DHT-RPC fail closed instead, select
`transport-only` and provide the complete adapter:

```js
const dht = new DHT({
  outboundPolicy: 'transport-only',
  requestTransport,
  requestTimeout: 1000,
  maxTransportCandidates: 256
})
```

`transport-only` never constructs UDX, binds a UDP socket, watches network
interfaces, samples NAT state, or starts direct background traffic. Direct-only
constructor options and mutating or direct-I/O entrypoints fail with
`DIRECT_IO_FORBIDDEN`; there is no automatic downgrade to direct transport.
Inert state accessors do not attempt I/O: `id`, `host`, `port`, `socket`,
`address()`, `localAddress()`, and `remoteAddress()` return `null`, `randomized`
returns `false`, and `toArray()` returns `[]`.

The adapter contract is:

```text
RequestTransport {
  ready()
  suspend()
  resume()
  destroy()
  bootstrap({ target, limit })
  closest({ target, limit })
  key(destination)
  id(destination)
  request({ to, token, internal, command, target, value, attempt })
}
```

Lifecycle methods may return promises. `bootstrap()` returns a promise or async
iterable of destinations, while `closest()` returns an iterable of locally known
destinations. A destination is opaque to DHT-RPC: it may be `{ id, ref }`, and
does not need `host` or `port`. `key(destination)` must return a stable
adapter-scoped string and `id(destination)` must return a 32-byte,
`b4a`-compatible identifier.

Each `request()` call represents one attempt and must return:

```text
RequestOperation {
  promise
  cancel(reason)
}
```

The promise resolves to the existing logical reply shape:

```text
LogicalReply {
  rtt
  from
  to
  token
  closerNodes
  error
  value
}
```

DHT-RPC owns retries, per-attempt timeouts, cancellation, session/query
attachment, bootstrap readiness, lifecycle ordering, statistics, and
exactly-once terminal callbacks. Superseded, suspended, destroyed, and
session-cancelled attempts call `cancel(reason)` exactly once; late settlements
are ignored. Adapter request throws and rejections consume the retry budget and
finish as `TRANSPORT_UNAVAILABLE`; wrapper timeouts finish as `REQUEST_TIMEOUT`.
A malformed request operation finishes as `TRANSPORT_INVALID` and is not
retried.

Before query state changes, DHT-RPC validates and copies every adapter identity,
then rejects malformed replies, identity collisions, and capacity overflow with
`TRANSPORT_INVALID_RESPONSE`. A logical reply requires non-negative integer
`rtt` and `error` values plus an object `from`; omitted `to`, `token`,
`closerNodes`, and `value` fields normalize to `null`. Each query has a key-to-ID
registry capped by `maxTransportCandidates` (default `256`, valid range
`20..4096`) shared by caller seeds, `closest()`, `bootstrap()`, reply senders, and
closer nodes. A standalone request retains its destination identity and validates
each response atomically in a temporary registry capped at 21 identities: one
sender and at most 20 closer nodes. Registries are cleared when their operation
ends. Public results unwrap the original opaque destinations; copied IDs are used
only for Kademlia ordering and validation.

This seam does **not** provide anonymity by itself. The adapter must supply the
private routing, cryptography, traffic-analysis defenses, and destination
authentication. The planned HyperDHT consumer and its security boundary are
documented in [private-routing-v1](https://github.com/ayooooo123/hyperdht/blob/private-routing-v1/docs/private-routing-v1.md).

For local fork development, keep the npm dependency name and point it at the Git
branch:

```json
{
  "dependencies": {
    "dht-rpc": "github:ayooooo123/dht-rpc#private-routing-v1"
  }
}
```

Replace the branch with a reviewed commit SHA for reproducible builds. Do not
publish or import this fork under a different npm package name.

## Usage

Here is an example implementing a simple key value store

First spin up a bootstrap node. You can make multiple if you want for redundancy.
There is nothing special about a bootstrap node, except it needs to know it's own host and port,
since it knows no other nodes to infer it from.

```js
import DHT from 'dht-rpc'

const bootstrap = DHT.bootstrapper(10001, '127.0.0.1')
```

Now lets make some dht nodes that can store values in our key value store.

```js
import DHT from 'dht-rpc'
import crypto from 'crypto'

// Let's create 100 dht nodes for our example.
for (var i = 0; i < 100; i++) createNode()

function createNode() {
  const node = new DHT({
    bootstrap: ['localhost:10001']
  })

  const values = new Map()
  const VALUES = 0 // define a command enum

  node.on('request', function (req) {
    if (req.command === VALUES) {
      if (req.token) {
        // if we are the closest node store the value (ie the node sent a valid roundtrip token)
        const key = hash(req.value).toString('hex')
        values.set(key, req.value)
        console.log('Storing', key, '-->', req.value.toString())
        return req.reply(null)
      }

      const value = values.get(req.target.toString('hex'))
      req.reply(value)
    }
  })
}

function hash(value) {
  return crypto.createHash('sha256').update(value).digest()
}
```

To insert a value into this dht make another script that does this following

```js
const node = new DHT()

const q = node.query(
  {
    target: hash(val),
    command: VALUES,
    value
  },
  {
    // commit true will make the query re-request the 20 closest
    // nodes with a valid round trip token to update the values
    commit: true
  }
)

await q.finished()
```

Then after inserting run this script to query for a value

```js
const target = Buffer.from(hexFromAbove, 'hex')
for await (const data of node.query({ target, command: VALUES })) {
  if (data.value && hash(data.value).toString('hex') === hexFromAbove) {
    // We found the value! Destroy the query stream as there is no need to continue.
    console.log(val, '-->', data.value.toString())
    break
  }
}
console.log('(query finished)')
```

## API

#### `const node = new DHT([options])`

Create a new DHT node.

Options include:

```js
{
  // A list of bootstrap nodes. Optionally prefix a suggested-IP, i.e. '192.168.1.10@bootstrap-node.com:24242'
  bootstrap: [ 'bootstrap-node.com:24242', ... ],
  // Optionally pass in array of { host, port } to add to the routing table if you know any peers
  nodes: [{ host, port }, ...],
  // Optionally pass a port you prefer to bind to instead of a random one, or a range of ports it will try to bind to in order: [startPort, endPort], endPort exclusive
  port: 0,
  // Optionally pass a host you prefer to bind to instead of all networks
  host: '0.0.0.0',
  // Optionally pass a UDX instance on which sockets will be created.
  udx,
  // dht-rpc will automatically detect if you are firewalled. If you know that you are not set this to false
  firewalled: true,
  // dht-rpc will hint when a node is down if a request times out. Setting to false disables sending hints
  sendDownHints: true,
  // How many down hints can be sent per tick. -1 is unlimited
  downHintsRateLimit: 50
}
```

Nodes per default use something called adaptive mode to decide whether or not they want to join other nodes' routing table.
This includes things like node uptime, if the node is firewalled etc. Adaptive mode is conservative, so it might take ~20-30 mins for the node to turn persistent. If you are making a test case with your own bootstrap network you'd usually want to turn this off to make sure your test finishes in a timely manner. You can do this by passing `ephemeral: false` in the constructor.
For the vast majority of use-cases you should always use adaptive mode to ensure good DHT health, ie the defaults.

Your DHT routing id is `hash(publicIp + publicPort)` and will be autoconfigured internally.

#### `const node = DHT.bootstrapper(port, host, [options])`

Make a bootstrap node for your DHT. The port and host needs to be its globally accessible port and host.
Note: `port` and `host` parameters are used to create the node id. Use `options.host` if you want to bind to i.e. 127.0.0.1.
DHT nodes can use any other DHT node to bootstrap, but a bootstrap node can bootstrap itself, by itself.

#### `await node.fullyBootstrapped()`

Wait for the node to be fully bootstrapped etc.
You don't have to wait for this method, but can be useful during testing.

#### `node.id`

Get your own routing ID. Only available when the node is not ephemeral.

#### `node.ephemeral`

A boolean indicating if you are currently ephemeral or not

#### `node.on('bootstrap')`

Emitted when the routing table is fully bootstrapped. Emitted as a conveinience.

#### `node.on('listening')`

Emitted when the underlying UDX socket is listening. Emitted as a conveinience.

#### `node.on('ready')`

Emitted when the node is fully bootstrapped etc.

#### `node.on('persistent')`

Emitted when the node is no longer in ephemeral mode.
All nodes start in ephemeral mode, as they figure out their NAT settings.
If you set `ephemeral: false` then this is emitted during the bootstrap phase, assuming
you are on an open NAT.

#### `node.on('wake-up')`

Emitted when the node has detected that the computer has gone to sleep. If this happens,
it will switch from persistent mode to ephemeral again.

#### `node.on('network-change', interfaces)`

Emitted when the network interfaces of the computer change.

#### `node.on('nat-update', (host, port) => {})`

Emitted when `node.host` or `node.port` were changed.

#### `node.on('close')`

Will be emitted after `node.destroy()` is completed.

#### `node.refresh()`

Refresh the routing table by looking up a random node in the background.
This is called internally periodically, but exposed in-case you want to force a refresh.

#### `node.host`

Get your node's public ip, inferred from other nodes in the DHT.
If the ip cannot be determined, this is set to `null`.

#### `node.port`

Get your node's public port, inferred from other nodes in the DHT.
If your node does not have a consistent port, this is set to 0.

#### `node.firewalled`

Boolean indicated if your node is behind a firewall.

This is auto detected by having other nodes trying to do a PING to you
without you contacting them first.

#### `node.randomized`

Boolean indicating if your node is likely behind a randomizing NAT.

#### `const addr = node.address()`

Get the local address of the UDP socket bound.

Note that if you are in ephemeral mode, this will return a different
port than the one you provided in the constructor (under `port`), as ephemeral
mode always uses a random port.

#### `node.on('request', req)`

Emitted when an incoming DHT request is received. This is where you can add your own RPC methods.

- `req.target` - the dht target the peer is looking (routing is handled behind the scene)
- `req.command` - the RPC command enum
- `req.value` - the RPC value buffer
- `req.token` - If the remote peer echoed back a valid roundtrip token, proving their "from address" this is set
- `req.from` - who sent this request (host, port)

To reply to a request use the `req.reply(value)` method and to reply with an error code use `req.error(errorCode)`.

In general error codes are up to the user to define, with the general suggestion to start application specific errors
from error code `16` and up, to avoid future clashes with `dht-rpc` internals.

Currently dht-rpc defines the following errors

```js
DHT.OK = 0 // ie no error
DHT.ERROR_UNKNOWN_COMMAND = 1 // the command requested does not exist
DHT.ERROR_INVALID_TOKEN = 2 // the round trip token sent is invalid
```

#### `reply = await node.request({ token, target, command, value }, to, [options])`

Send a request to a specific node specified by the `to` address (`{ host, port }`).
See the query API for more info on the arguments.

Options include:

```js
{
  retry: true, // whether the request should retry on timeout
  socket: udxSocket // request on this specific socket
}
```

Normally you'd set the token when committing to the dht in the query's commit hook.

#### `reply = await node.ping(to, [options])`

Ping a remote node described by `to`. The return promise resolves if the remote node replies. `to` is an object with the shape `{ host, port }`.

Additional options include:

```js
{
  size: 0, // size of the value buffer, filled with zeroes
  ttl: 0 // Set the ttl for the packet sent. `0` is the default ttl.
}
```

#### `reply = await node.delayedPing(to, delayMs, [options])`

Send a ping that intentionally waits before replying. Useful for testing latency/timeout behavior and scheduling a delayed response.

- `to` - `{ host, port }` of the target peer
- `delayMs` - integer milliseconds to delay the reply (0–10_000). Values above 10_000 ms are rejected and no reply is sent (the request will time out).
- `options` - same options as `node.request()`:

Example:

```js
const start = Date.now()
await node.delayedPing({ host: peer.host, port: peer.port }, 1000)
console.log('elapsed >=', Date.now() - start, 'ms') // ~1s + network overhead
```

#### `stream = node.query({ target, command, value }, [options])`

Query the DHT. Will move as close as possible to the `target` provided, which should be a 32-byte uniformly distributed buffer (ie a hash).

- `target` - find nodes close to this (should be a 32 byte buffer like a hash)
- `command` - an enum (uint) indicating the method you want to invoke
- `value` - optional binary payload to send with it

If you want to modify state stored in the dht, you can use the commit flag to signal the closest
nodes.

```js
{
  // "commit" the query to the 20 closest nodes so they can modify/update their state
  commit: true
}
```

Commiting a query will just re-request your command to the closest nodes once those are verified.
If you want to do some more specific logic with the closest nodes you can specify a function instead,
that is called for each close reply.

```js
{
  async commit (reply, dht, query) {
    // normally you'd send back the roundtrip token here, to prove to the remote that you own
    // your ip/port
    await dht.request({ token: reply.token, target, command, value }, reply.from)
  }
}
```

Other options include:

```js
{
  nodes: [
    // start the query by querying these nodes
    // useful if you are re-doing a query from a set of closest nodes.
  ],
  replies: [
    // similar to nodes, but useful if you have an array of closest replies instead
    // from a previous query.
  ],
  map (reply) {
    // map the reply into what you want returned on the stream
    return { onlyValue: reply.value }
  }
}
```

The query method returns a stream encapsulating the query, that is also an async iterator. Each `data` event contain a DHT reply.
If you just want to wait for the query to finish, you can use the `await stream.finished()` helper. After completion the closest
nodes are stored in `stream.closestNodes` array.

If you want to access the closest replies to your provided target you can see those at `stream.closestReplies`.

#### `stream = node.findNode(target, [options])`

Find the node closest to the node with id `target`. Returns a stream encapsulating the query (see `node.query()`). `options` are the same as `node.query()`.

#### `node.destroy()`

Shutdown the DHT node.

#### `node.destroyed`

Boolean indicating if this has been destroyed.

#### `node.toArray([options])`

Get the routing table peers out as an array of `{ host, port }`. Use `options.limit` to get a subset.

#### `node.addNode({ host, port })`

Manually add a node to the routing table.

#### `await node.suspend()`

Tell the DHT you are going to background (ie suspend and allow it to make preperations for that)

#### `await node.resume()`

Tell the DHT you are resuming from suspension.

## License

MIT
