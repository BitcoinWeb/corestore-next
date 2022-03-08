const test = require('brittle')
const crypto = require('@web4/bitweb-crypto')
const ram = require('random-access-memory')
const tmp = require('tmp-promise')

const Chainstore = require('..')

test('basic get with caching', async function (t) {
  const store = new Chainstore(ram)
  const chain1a = store.get({ name: 'chain-1' })
  const chain1b = store.get({ name: 'chain-1' })
  const chain2 = store.get({ name: 'chain-2' })

  await Promise.all([chain1a.ready(), chain1b.ready(), chain2.ready()])

  t.alike(chain1a.key, chain1b.key)
  t.unlike(chain1a.key, chain2.key)

  t.ok(chain1a.writable)
  t.ok(chain1b.writable)

  t.is(store.chains.size, 2)
})

test('basic get with custom keypair', async function (t) {
  const store = new Chainstore(ram)
  const kp1 = crypto.keyPair()
  const kp2 = crypto.keyPair()

  const chain1 = store.get(kp1)
  const chain2 = store.get(kp2)
  await Promise.all([chain1.ready(), chain2.ready()])

  t.alike(chain1.key, kp1.publicKey)
  t.alike(chain2.key, kp2.publicKey)
  t.ok(chain1.writable)
  t.ok(chain2.writable)
})

test('basic namespaces', async function (t) {
  const store = new Chainstore(ram)
  const ns1 = store.namespace('ns1')
  const ns2 = store.namespace('ns2')
  const ns3 = store.namespace('ns1') // Duplicate namespace

  const chain1 = ns1.get({ name: 'main' })
  const chain2 = ns2.get({ name: 'main' })
  const chain3 = ns3.get({ name: 'main' })
  await Promise.all([chain1.ready(), chain2.ready(), chain3.ready()])

  t.absent(chain1.key.equals(chain2.key))
  t.ok(chain1.key.equals(chain3.key))
  t.ok(chain1.writable)
  t.ok(chain2.writable)
  t.ok(chain3.writable)
  t.is(store.chains.size, 2)

  t.end()
})

test('basic replication', async function (t) {
  const store1 = new Chainstore(ram)
  const store2 = new Chainstore(ram)

  const chain1 = store1.get({ name: 'chain-1' })
  const chain2 = store1.get({ name: 'chain-2' })
  await chain1.append('hello')
  await chain2.append('world')

  const chain3 = store2.get({ key: chain1.key })
  const chain4 = store2.get({ key: chain2.key })

  const s = store1.replicate(true)
  s.pipe(store2.replicate(false)).pipe(s)

  t.alike(await chain3.get(0), Buffer.from('hello'))
  t.alike(await chain4.get(0), Buffer.from('world'))
})

test('replicating chains created after replication begins', async function (t) {
  const store1 = new Chainstore(ram)
  const store2 = new Chainstore(ram)

  const s = store1.replicate(true, { live: true })
  s.pipe(store2.replicate(false, { live: true })).pipe(s)

  const chain1 = store1.get({ name: 'chain-1' })
  const chain2 = store1.get({ name: 'chain-2' })
  await chain1.append('hello')
  await chain2.append('world')

  const chain3 = store2.get({ key: chain1.key })
  const chain4 = store2.get({ key: chain2.key })

  t.alike(await chain3.get(0), Buffer.from('hello'))
  t.alike(await chain4.get(0), Buffer.from('world'))
})

test('replicating chains using discovery key hook', async function (t) {
  const dir = await tmp.dir({ unsafeCleanup: true })
  let store1 = new Chainstore(dir.path)
  const store2 = new Chainstore(ram)

  const chain = store1.get({ name: 'main' })
  await chain.append('hello')
  const key = chain.key

  await store1.close()
  store1 = new Chainstore(dir.path)

  const s = store1.replicate(true, { live: true })
  s.pipe(store2.replicate(false, { live: true })).pipe(s)

  const chain2 = store2.get(key)
  t.alike(await chain2.get(0), Buffer.from('hello'))

  await dir.cleanup()
})

test('nested namespaces', async function (t) {
  const store = new Chainstore(ram)
  const ns1a = store.namespace('ns1').namespace('a')
  const ns1b = store.namespace('ns1').namespace('b')

  const chain1 = ns1a.get({ name: 'main' })
  const chain2 = ns1b.get({ name: 'main' })
  await Promise.all([chain1.ready(), chain2.ready()])

  t.not(chain1.key.equals(chain2.key))
  t.ok(chain1.writable)
  t.ok(chain2.writable)
  t.is(store.chains.size, 2)
})

test('chain uncached when all sessions close', async function (t) {
  const store = new Chainstore(ram)
  const chain1 = store.get({ name: 'main' })
  await chain1.ready()
  t.is(store.chains.size, 1)
  await chain1.close()
  t.is(store.chains.size, 0)
})

test('writable chain loaded from name userData', async function (t) {
  const dir = await tmp.dir({ unsafeCleanup: true })

  let store = new Chainstore(dir.path)
  let chain = store.get({ name: 'main' })
  await chain.ready()
  const key = chain.key

  t.ok(chain.writable)
  await chain.append('hello')
  t.is(chain.length, 1)

  await store.close()
  store = new Chainstore(dir.path)
  chain = store.get(key)
  await chain.ready()

  t.ok(chain.writable)
  await chain.append('world')
  t.is(chain.length, 2)
  t.alike(await chain.get(0), Buffer.from('hello'))
  t.alike(await chain.get(1), Buffer.from('world'))

  await dir.cleanup()
})

test('storage locking', async function (t) {
  const dir = await tmp.dir({ unsafeCleanup: true })

  const store1 = new Chainstore(dir.path)
  await store1.ready()

  const store2 = new Chainstore(dir.path)
  try {
    await store2.ready()
    t.fail('dir should have been locked')
  } catch {
    t.pass('dir was locked')
  }

  await dir.cleanup()
})
