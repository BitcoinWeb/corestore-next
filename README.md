# Chainstore v2

Chainstore is a Unichain factory that makes it easier to manage large collections of named Unichains.

Chainstore provides:
1. __Key Derivation__ - All writable Unichain keys are derived from a single master key and a user-provided name.
2. __Session Handling__ - If a single Unichain is loaded multiple times through the `get` method, the underlying resources will only be opened once (using Unichain 10's new session feature). Once all sessions are closed, the resources will be released.
3. __Storage Management__ - Unichains can be stored in any random-access-storage instance, where they will be keyed by their discovery keys.
4. __Namespacing__ - You can share a single Chainstore instance between multiple applications or components without worrying about naming collisions by creating "namespaces" (e.g. `chainstore.namespace('my-app').get({ name: 'main' }) 

### Installation
`npm install @web4/chainstore@next`

### Usage
A chainstore instance can be constructed with a random-access-storage module, a function that returns a random-access-storage module given a path, or a string. If a string is specified, it will be assumed to be a path to a local storage directory:
```js
const Chainstore = require('@web4/chainstore')

const store = new Chainstore('./my-storage')
const chain1 = store.get({ name: 'chain-1' })
const chain2 = store.get({ name: 'chain-2' })
```

### API
#### `const store = new Chainstore(storage)`
Create a new Chainstore instance.

`storage` can be either a random-access-storage module, a string, or a function that takes a path and returns an random-access-storage instance.

#### `const chain = store.get(key | { name: 'a-name', ...unichainOpts})`
Loads a Unichain, either by name (if the `name` option is provided), or from the provided key (if the first argument is a Buffer, or if the `key` options is set).

If that Unichain has previously been loaded, subsequent calls to `get` will return a new Unichain session on the existing chain.

All other options besides `name` and `key` will be forwarded to the Unichain constructor.

#### `const stream = store.replicate(opts)`
Creates a replication stream that's capable of replicating all Unichains that are managed by the Chainstore, assuming the remote peer has the correct capabilities.

`opts` will be forwarded to Unichain's `replicate` function.

Chainstore replicates in an "all-to-all" fashion, meaning that when replication begins, it will attempt to replicate every Unichain that's currently loaded and in memory. These attempts will fail if the remote side doesn't have a Unichain's capability -- Chainstore replication does not exchange Unichain keys.

If the remote side dynamically adds a new Unichain to the replication stream, Chainstore will load and replicatethat chain if possible.

#### `const store = store.namespace(name)`
Create a new namespaced Chainstore. Namespacing is useful if you're going to be sharing a single Chainstore instance between many applications or components, as it prevents name collisions.

Namespaces can be chained:
```js
const ns1 = store.namespace('a')
const ns2 = ns1.namespace('b')
const chain1 = ns1.get({ name: 'main' }) // These will load different Unichains
const chain2 = ns2.get({ name: 'main' })
```

### License
MIT

