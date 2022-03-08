const p = require('path')
const fs = require('fs')

const test = require('brittle')
const ram = require('random-access-memory')
const raf = require('random-access-file')

const KeyManager = require('../lib/keys')

test('can create unichain keypairs', async t => {
  const keys = await KeyManager.fromStorage(ram)

  const kp1 = await keys.createUnichainKeyPair('chain1')
  const kp2 = await keys.createUnichainKeyPair('chain2')

  t.is(kp1.publicKey.length, 32)
  t.is(kp2.publicKey.length, 32)
  t.unlike(kp1.publicKey, kp2.publicKey)
})

test('distinct tokens create distinct unichain keypairs', async t => {
  const keys = await KeyManager.fromStorage(ram)
  const token1 = KeyManager.createToken()
  const token2 = KeyManager.createToken()

  const kp1 = await keys.createUnichainKeyPair('chain1', token1)
  const kp2 = await keys.createUnichainKeyPair('chain1', token2)

  t.unlike(kp1.publicKey, kp2.publicKey)
})

test('short user-provided token will throw', async t => {
  const keys = await KeyManager.fromStorage(ram)

  try {
    await keys.createUnichainKeyPair('chain1', Buffer.from('hello'))
    t.fail('did not throw')
  } catch {
    t.pass('threw correctly')
  }
})

test('persistent storage regenerates keys correctly', async t => {
  const testPath = p.resolve(__dirname, 'test-data')

  const keys1 = await KeyManager.fromStorage((name) => raf(testPath, { directory: testPath }))
  const kp1 = await keys1.createUnichainKeyPair('chain1')

  const keys2 = await KeyManager.fromStorage((name) => raf(testPath, { directory: testPath }))
  const kp2 = await keys2.createUnichainKeyPair('chain1')

  t.alike(kp1.publicKey, kp2.publicKey)

  await fs.promises.rm(testPath, { recursive: true })
})

test('different master keys -> different keys', async t => {
  const keys1 = await KeyManager.fromStorage(ram)
  const keys2 = await KeyManager.fromStorage(ram)

  const kp1 = await keys1.createUnichainKeyPair('chain1')
  const kp2 = await keys2.createUnichainKeyPair('chain1')

  t.unlike(kp1.publicKey, kp2.publicKey)
})
