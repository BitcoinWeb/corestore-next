const { EventEmitter } = require('events')
const safetyCatch = require('safety-catch')
const crypto = require('@web4/bitweb-crypto')
const sodium = require('sodium-universal')
const Unichain = require('@web4/unichain')

const KeyManager = require('./lib/keys')

const CHAINS_DIR = 'chains'
const PROFILES_DIR = 'profiles'
const USERDATA_NAME_KEY = '@chainstore/name'
const USERDATA_NAMESPACE_KEY = '@chainstore/namespace'
const DEFAULT_NAMESPACE = generateNamespace('@chainstore/default')

module.exports = class Chainstore extends EventEmitter {
  constructor (storage, opts = {}) {
    super()

    this.storage = Unichain.defaultStorage(storage, { lock: PROFILES_DIR + '/default' })

    this.chains = opts._chains || new Map()
    this.keys = opts.keys

    this._namespace = opts._namespace || DEFAULT_NAMESPACE
    this._replicationStreams = opts._streams || []

    this._opening = opts._opening ? opts._opening.then(() => this._open()) : this._open()
    this._opening.catch(noop)
    this.ready = () => this._opening
  }

  async _open () {
    if (this.keys) {
      this.keys = await this.keys // opts.keys can be a Promise that resolves to a KeyManager
    } else {
      this.keys = await KeyManager.fromStorage(p => this.storage(PROFILES_DIR + '/' + p))
    }
  }

  async _generateKeys (opts) {
    if (opts._discoveryKey) {
      return {
        keyPair: null,
        sign: null,
        discoveryKey: opts._discoveryKey
      }
    }
    if (!opts.name) {
      return {
        keyPair: {
          publicKey: opts.publicKey,
          secretKey: opts.secretKey
        },
        sign: opts.sign,
        discoveryKey: crypto.discoveryKey(opts.publicKey)
      }
    }
    const { publicKey, sign } = await this.keys.createUnichainKeyPair(opts.name, this._namespace)
    return {
      keyPair: {
        publicKey,
        secretKey: null
      },
      sign,
      discoveryKey: crypto.discoveryKey(publicKey)
    }
  }

  _getPrereadyUserData (chain, key) {
    for (const { key: savedKey, value } of chain.chain.header.userData) {
      if (key === savedKey) return value
    }
    return null
  }

  async _preready (chain) {
    const name = this._getPrereadyUserData(chain, USERDATA_NAME_KEY)
    if (!name) return

    const namespace = this._getPrereadyUserData(chain, USERDATA_NAMESPACE_KEY)
    const { publicKey, sign } = await this.keys.createUnichainKeyPair(name.toString(), namespace)
    if (!publicKey.equals(chain.key)) throw new Error('Stored chain key does not match the provided name')

    // TODO: Should Unichain expose a helper for this, or should preready return keypair/sign?
    chain.sign = sign
    chain.key = publicKey
    chain.writable = true
  }

  async _preload (opts) {
    await this.ready()

    const { discoveryKey, keyPair, sign } = await this._generateKeys(opts)
    const id = discoveryKey.toString('hex')

    while (this.chains.has(id)) {
      const existing = this.chains.get(id)
      if (existing.opened && !existing.closing) return { from: existing, keyPair, sign }
      if (!existing.opened) {
        await existing.ready().catch(safetyCatch)
      } else if (existing.closing) {
        await existing.close()
      }
    }

    const userData = {}
    if (opts.name) {
      userData[USERDATA_NAME_KEY] = Buffer.from(opts.name)
      userData[USERDATA_NAMESPACE_KEY] = this._namespace
    }

    // No more async ticks allowed after this point -- necessary for caching

    const storageRoot = [CHAINS_DIR, id.slice(0, 2), id.slice(2, 4), id].join('/')
    const chain = new Unichain(p => this.storage(storageRoot + '/' + p), {
      _preready: this._preready.bind(this),
      autoClose: true,
      encryptionKey: opts.encryptionKey || null,
      userData,
      sign: null,
      createIfMissing: !opts._discoveryKey,
      keyPair: keyPair && keyPair.publicKey
        ? {
            publicKey: keyPair.publicKey,
            secretKey: null
          }
        : null
    })

    this.chains.set(id, chain)
    chain.ready().then(() => {
      for (const { stream } of this._replicationStreams) {
        chain.replicate(stream)
      }
    }, () => {
      this.chains.delete(id)
    })
    chain.once('close', () => {
      this.chains.delete(id)
    })

    return { from: chain, keyPair, sign }
  }

  get (opts = {}) {
    opts = validateGetOptions(opts)
    const chain = new Unichain(null, {
      ...opts,
      name: null,
      preload: () => this._preload(opts)
    })
    return chain
  }

  replicate (isInitiator, opts) {
    const isExternal = isStream(isInitiator) || !!(opts && opts.stream)
    const stream = Unichain.createProtocolStream(isInitiator, {
      ...opts,
      ondiscoverykey: discoveryKey => {
        const chain = this.get({ _discoveryKey: discoveryKey })
        return chain.ready().catch(safetyCatch)
      }
    })
    for (const chain of this.chains.values()) {
      if (chain.opened) chain.replicate(stream) // If the chain is not opened, it will be replicated in preload.
    }
    const streamRecord = { stream, isExternal }
    this._replicationStreams.push(streamRecord)
    stream.once('close', () => {
      this._replicationStreams.splice(this._replicationStreams.indexOf(streamRecord), 1)
    })
    return stream
  }

  namespace (name) {
    if (!Buffer.isBuffer(name)) name = Buffer.from(name)
    return new Chainstore(this.storage, {
      _namespace: generateNamespace(this._namespace, name),
      _opening: this._opening,
      _chains: this.chains,
      _streams: this._replicationStreams,
      keys: this._opening.then(() => this.keys)
    })
  }

  async _close () {
    if (this._closing) return this._closing
    await this._opening
    const closePromises = []
    for (const chain of this.chains.values()) {
      closePromises.push(chain.close())
    }
    await Promise.allSettled(closePromises)
    for (const { stream, isExternal } of this._replicationStreams) {
      // Only close streams that were created by the Chainstore
      if (!isExternal) stream.destroy()
    }
    await this.keys.close()
  }

  close () {
    if (this._closing) return this._closing
    this._closing = this._close()
    this._closing.catch(noop)
    return this._closing
  }

  static createToken () {
    return KeyManager.createToken()
  }
}

function validateGetOptions (opts) {
  if (Buffer.isBuffer(opts)) return { key: opts, publicKey: opts }
  if (opts.key) {
    opts.publicKey = opts.key
  }
  if (opts.keyPair) {
    opts.publicKey = opts.keyPair.publicKey
    opts.secretKey = opts.keyPair.secretKey
  }
  if (opts.name && typeof opts.name !== 'string') throw new Error('name option must be a String')
  if (opts.name && opts.secretKey) throw new Error('Cannot provide both a name and a secret key')
  if (opts.publicKey && !Buffer.isBuffer(opts.publicKey)) throw new Error('publicKey option must be a Buffer')
  if (opts.secretKey && !Buffer.isBuffer(opts.secretKey)) throw new Error('secretKey option must be a Buffer')
  if (!opts._discoveryKey && (!opts.name && !opts.publicKey)) throw new Error('Must provide either a name or a publicKey')
  return opts
}

function generateNamespace (first, second) {
  if (!Buffer.isBuffer(first)) first = Buffer.from(first)
  if (second && !Buffer.isBuffer(second)) second = Buffer.from(second)
  const out = Buffer.allocUnsafe(32)
  sodium.crypto_generichash(out, second ? Buffer.concat([first, second]) : first)
  return out
}

function isStream (s) {
  return typeof s === 'object' && s && typeof s.pipe === 'function'
}

function noop () {}
