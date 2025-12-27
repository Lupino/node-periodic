var EventEmitter = require('events').EventEmitter
  , util = require('util')
  , net = require('net')
  , crypto = require('crypto')
  ;

// --- Base Transport ---
var Transport = function(options) {
  EventEmitter.call(this);
  this._socket = net.connect(options);
  // 禁用 Nagle 算法，确保小包实时发送
  this._socket.setNoDelay(true);

  this._socket.on('data', this.onData.bind(this));
  this._socket.on('close', this.onClose.bind(this));
  this._socket.on('connect', this.onConnect.bind(this));
  this._socket.on('end', this.onEnd.bind(this));
  this._socket.on('error', this.onError.bind(this));
  this._socket.on('timeout', this.onTimeout.bind(this));
  this._socket.on('drain', this.onDrain.bind(this));
};

util.inherits(Transport, EventEmitter);

Transport.prototype.onData = function(chunk) { this.emit('data', chunk); };
Transport.prototype.onClose = function(had_error) { this.emit('close', had_error); };
Transport.prototype.onConnect = function() { this.emit('connect'); };
Transport.prototype.onEnd = function() { this.emit('end'); };
Transport.prototype.onError = function(err) { this.emit('error', err); };
Transport.prototype.onTimeout = function() { this.emit('timeout'); };
Transport.prototype.onDrain = function() { this.emit('drain'); };

Transport.prototype.write = function(data, encoding, callback) {
  return this._socket.write(data, encoding, callback);
};
Transport.prototype.end = function(data, encoding) {
  this._socket.end(data, encoding);
};

exports.Transport = Transport;

// --- RSA Transport Implementation ---

const OAEP_HASH = 'sha256';
const AES_ALGORITHM = 'aes-256-ctr';
const AES_KEY_SIZE = 32;
const AES_IV_SIZE = 16;
const LENGTH_HEADER_SIZE = 8;

const MODE_PLAIN = 0;
const MODE_RSA = 1;
const MODE_AES = 2;

var RSATransport = function(options) {
  EventEmitter.call(this);

  if (!options.privateKey || !options.peerPublicKey) {
    throw new Error('RSATransport requires "privateKey" and "peerPublicKey" options (PEM format).');
  }

  this._mode = (options.mode !== undefined) ? options.mode : MODE_AES;
  this.privateKey = options.privateKey;
  this.peerPublicKey = options.peerPublicKey;

  // Key Sizes
  this.myKeyDetails = crypto.createPrivateKey(this.privateKey);
  this.myKeyBlockSize = this.myKeyDetails.asymmetricKeyDetails.modulusLength / 8;

  this.peerKeyDetails = crypto.createPublicKey(this.peerPublicKey);
  this.peerKeyBlockSize = this.peerKeyDetails.asymmetricKeyDetails.modulusLength / 8;

  // State
  this._buffer = Buffer.alloc(0);
  this._state = 'HANDSHAKE_SEND_FP';
  this._sessionKey = null;
  this._writeBuffer = [];

  const netOptions = { port: options.port, host: options.host, path: options.path };
  this._socket = net.connect(netOptions);

  // [重要] 禁用 Nagle 算法，这对握手协议至关重要
  this._socket.setNoDelay(true);

  this._socket.on('data', this._onSocketData.bind(this));
  this._socket.on('close', this.onClose.bind(this));
  this._socket.on('connect', this._startHandshake.bind(this));
  this._socket.on('end', this.onEnd.bind(this));
  this._socket.on('error', this.onError.bind(this));
  this._socket.on('timeout', this.onTimeout.bind(this));
  this._socket.on('drain', this.onDrain.bind(this));
};

util.inherits(RSATransport, EventEmitter);

RSATransport.MODE_PLAIN = MODE_PLAIN;
RSATransport.MODE_RSA = MODE_RSA;
RSATransport.MODE_AES = MODE_AES;

RSATransport.prototype._startHandshake = function() {
  try {
    const myPubKeyObject = crypto.createPublicKey(this.privateKey);
    const myDer = myPubKeyObject.export({ type: 'pkcs1', format: 'der' });
    const myFingerprint = crypto.createHash('sha256').update(myDer).digest();

    this._sendOAEP(myFingerprint);
    this._state = 'HANDSHAKE_WAIT_SERVER_FP';
  } catch (e) {
    this.onError(new Error("Handshake init failed: " + e.message));
  }
};

RSATransport.prototype._sendOAEP = function(data, callback) {
  const maxSize = this.peerKeyBlockSize - 66;

  let offset = 0;
  const chunks = [];

  while (offset < data.length) {
    const end = Math.min(offset + maxSize, data.length);
    const chunk = data.slice(offset, end);
    try {
      const encrypted = crypto.publicEncrypt({
        key: this.peerPublicKey,
        padding: crypto.constants.RSA_PKCS1_OAEP_PADDING,
        oaepHash: OAEP_HASH
      }, chunk);
      chunks.push(encrypted);
    } catch (e) {
      if (callback) callback(e);
      else this.onError(e);
      return false;
    }
    offset += maxSize;
  }

  const totalBuffer = Buffer.concat(chunks);
  return this._socket.write(totalBuffer, callback);
};

RSATransport.prototype._decryptOAEP = function(chunk) {
  return crypto.privateDecrypt({
    key: this.privateKey,
    padding: crypto.constants.RSA_PKCS1_OAEP_PADDING,
    oaepHash: OAEP_HASH
  }, chunk);
};

RSATransport.prototype._onSocketData = function(chunk) {
  this._buffer = Buffer.concat([this._buffer, chunk]);
  this._processBuffer();
};

RSATransport.prototype._processBuffer = function() {
  let proceed = true;
  while (proceed && this._buffer.length > 0) {
    proceed = false;

    if (this._state === 'HANDSHAKE_WAIT_SERVER_FP') {
      if (this._buffer.length >= this.myKeyBlockSize) {
        const encryptedFP = this._buffer.slice(0, this.myKeyBlockSize);
        this._buffer = this._buffer.slice(this.myKeyBlockSize);

        try {
          this._decryptOAEP(encryptedFP);
          this._performModeNegotiation();
        } catch (e) {
          this.onError(new Error("Handshake verification failed: " + e.message));
          this._socket.destroy();
          return;
        }
        proceed = true; // Loop again
      }
    }
    else if (this._state === 'ESTABLISHED') {
      if (this._mode === MODE_PLAIN) {
        // Plain Mode: Dump everything we have
        if (this._buffer.length > 0) {
          const payload = this._buffer;
          this._buffer = Buffer.alloc(0);
          this.emit('data', payload);
        }
      }
      else if (this._mode === MODE_RSA) {
        if (this._buffer.length >= this.myKeyBlockSize) {
          const encryptedBlock = this._buffer.slice(0, this.myKeyBlockSize);
          this._buffer = this._buffer.slice(this.myKeyBlockSize);
          try {
            const plaintext = this._decryptOAEP(encryptedBlock);
            this.emit('data', plaintext);
          } catch (e) {
            this.onError(new Error("RSA Decryption failed: " + e.message));
          }
          proceed = true;
        }
      }
      else if (this._mode === MODE_AES) {
        if (this._buffer.length >= LENGTH_HEADER_SIZE) {
          const packetLen = Number(this._buffer.readBigUInt64BE(0));
          if (this._buffer.length >= LENGTH_HEADER_SIZE + packetLen) {
            const payload = this._buffer.slice(LENGTH_HEADER_SIZE, LENGTH_HEADER_SIZE + packetLen);
            this._buffer = this._buffer.slice(LENGTH_HEADER_SIZE + packetLen);

            const iv = payload.slice(0, AES_IV_SIZE);
            const ciphertext = payload.slice(AES_IV_SIZE);
            try {
              const decipher = crypto.createDecipheriv(AES_ALGORITHM, this._sessionKey, iv);
              let decrypted = decipher.update(ciphertext);
              decrypted = Buffer.concat([decrypted, decipher.final()]);
              this.emit('data', decrypted);
            } catch (e) {
              this.onError(new Error("AES Decryption error: " + e.message));
            }
            proceed = true;
          }
        }
      }
    }
  }
};

RSATransport.prototype._performModeNegotiation = function() {
  try {
    const self = this;
    const modePayload = Buffer.from([this._mode]);

    // 1. 发送 Mode 包，并等待它完全写入内核
    this._sendOAEP(modePayload, function() {

      // 2. 如果是 AES 模式，发送 Session Key
      if (self._mode === MODE_AES) {
        self._sessionKey = crypto.randomBytes(AES_KEY_SIZE);
        self._sendOAEP(self._sessionKey, function() {
          self._finishHandshake();
        });
      } else {
        // 3. Plain 或 RSA 模式，直接完成握手
        self._finishHandshake();
      }
    });

  } catch (e) {
    this.onError(new Error("Mode negotiation failed: " + e.message));
  }
};

RSATransport.prototype._finishHandshake = function() {
  this._state = 'ESTABLISHED';
  this.emit('connect');

  // [关键修复]：刷新缓冲区
  if (this._writeBuffer.length > 0) {
    const flush = () => {
      const pending = this._writeBuffer;
      this._writeBuffer = [];
      pending.forEach((item) => {
        this.write(item.data, item.encoding, item.callback);
      });
    };

    // 如果是 Plain 模式，强制延时 50ms。
    // 这给服务端足够时间处理 "Mode Byte"，清空它的 recv buffer，并切换到 recvData tp 状态。
    // 避免 [Mode][PlainData] 粘包导致 PlainData 被服务端 swallow。
    if (this._mode === MODE_PLAIN) {
      setTimeout(flush, 50);
    } else {
      flush();
    }
  }
};

RSATransport.prototype.onDrain = function() {
  this.emit('drain');
};

RSATransport.prototype.write = function(data, encoding, callback) {
  if (typeof encoding === 'function') {
    callback = encoding;
    encoding = null;
  }
  if (typeof data === 'string') {
    data = Buffer.from(data, encoding || 'utf8');
  }

  if (this._state !== 'ESTABLISHED') {
    this._writeBuffer.push({ data: data, encoding: encoding, callback: callback });
    return false;
  }

  try {
    if (this._mode === MODE_PLAIN) {
      // Plain Mode: Direct
      return this._socket.write(data, callback);
    }
    else if (this._mode === MODE_RSA) {
      // RSA Mode: OAEP
      return this._sendOAEP(data, callback);
    }
    else if (this._mode === MODE_AES) {
      // AES Mode: Encrypt + Frame
      const iv = crypto.randomBytes(AES_IV_SIZE);
      const cipher = crypto.createCipheriv(AES_ALGORITHM, this._sessionKey, iv);
      const encrypted = Buffer.concat([cipher.update(data), cipher.final()]);
      const payload = Buffer.concat([iv, encrypted]);
      const header = Buffer.alloc(LENGTH_HEADER_SIZE);
      header.writeBigUInt64BE(BigInt(payload.length));
      return this._socket.write(Buffer.concat([header, payload]), callback);
    }
  } catch (e) {
    if (callback) callback(e);
    else this.onError(e);
    return false;
  }
};

RSATransport.prototype.end = function(data, encoding) {
  if (data) {
    this.write(data, encoding, () => {
      this._socket.end();
    });
  } else {
    this._socket.end();
  }
};

RSATransport.prototype.onClose = function(had_error) { this.emit('close', had_error); };
RSATransport.prototype.onEnd = function() { this.emit('end'); };
RSATransport.prototype.onError = function(err) { this.emit('error', err); };
RSATransport.prototype.onTimeout = function() { this.emit('timeout'); };

exports.RSATransport = RSATransport;
