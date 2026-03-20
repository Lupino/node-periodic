import { EventEmitter } from 'events';
import net from 'net';
import crypto from 'crypto';

// --- Base Transport ---
export class Transport extends EventEmitter {
  private _socket: net.Socket;

  constructor(options: net.NetConnectOpts) {
    super();
    this._socket = net.connect(options);
    // 禁用 Nagle 算法，确保小包实时发送
    // this._socket.setNoDelay(true);

    this._socket.on('data', this.onData.bind(this));
    this._socket.on('close', this.onClose.bind(this));
    this._socket.on('connect', this.onConnect.bind(this));
    this._socket.on('end', this.onEnd.bind(this));
    this._socket.on('error', this.onError.bind(this));
    this._socket.on('timeout', this.onTimeout.bind(this));
    this._socket.on('drain', this.onDrain.bind(this));
  }

  private onData(chunk: Buffer) {
    this.emit('data', chunk);
  }
  private onClose(had_error: boolean) {
    this.emit('close', had_error);
  }
  private onConnect() {
    this.emit('connect');
  }
  private onEnd() {
    this.emit('end');
  }
  private onError(err: Error) {
    this.emit('error', err);
  }
  private onTimeout() {
    this.emit('timeout');
  }
  private onDrain() {
    this.emit('drain');
  }

  write(
    data: string | Uint8Array,
    encoding?: BufferEncoding | ((err?: Error) => void) | null,
    callback?: (err?: Error) => void,
  ) {
    return (this._socket as any).write(data as any, encoding as any, callback as any);
  }

  end(data?: string | Uint8Array, encoding?: BufferEncoding) {
    this._socket.end(data as any, encoding as any);
  }
}

// --- RSA Transport Implementation ---

const OAEP_HASH = 'sha256';
const AES_ALGORITHM = 'aes-256-ctr';
const AES_KEY_SIZE = 32;
const AES_IV_SIZE = 16;
const LENGTH_HEADER_SIZE = 8;

const MODE_PLAIN = 0;
const MODE_RSA = 1;
const MODE_AES = 2;

export type RSATransportMode = typeof MODE_PLAIN | typeof MODE_RSA | typeof MODE_AES;

export type RSATransportOptions = {
  privateKey: string | Buffer;
  peerPublicKey: string | Buffer;
  mode?: RSATransportMode;
} & net.NetConnectOpts;

export class RSATransport extends EventEmitter {
  static MODE_PLAIN = MODE_PLAIN;
  static MODE_RSA = MODE_RSA;
  static MODE_AES = MODE_AES;

  private _socket: net.Socket;
  private _mode: RSATransportMode;

  private privateKey: string | Buffer;
  private peerPublicKey: string | Buffer;
  private _peerFingerprint: Buffer;

  private myKeyDetails: crypto.KeyObject;
  private myKeyBlockSize: number;

  private peerKeyDetails: crypto.KeyObject;
  private peerKeyBlockSize: number;

  private _buffer: Buffer;
  private _state: 'HANDSHAKE_SEND_FP' | 'HANDSHAKE_WAIT_SERVER_FP' | 'ESTABLISHED';
  private _sessionKey: Buffer | null;
  private _writeBuffer: Array<{ data: Buffer; encoding: BufferEncoding | null; callback?: (err?: Error) => void }>;

  constructor(options: RSATransportOptions) {
    super();

    if (!options.privateKey || !options.peerPublicKey) {
      throw new Error('RSATransport requires "privateKey" and "peerPublicKey" options (PEM format).');
    }

    this._mode = options.mode !== undefined ? options.mode : MODE_AES;
    this.privateKey = options.privateKey;
    this.peerPublicKey = options.peerPublicKey;

    // Key Sizes
    this.myKeyDetails = crypto.createPrivateKey(this.privateKey);
    // asymmetricKeyDetails may be undefined for some key types; assume RSA like original implementation
    this.myKeyBlockSize = ((this.myKeyDetails.asymmetricKeyDetails as any).modulusLength as number) / 8;

    this.peerKeyDetails = crypto.createPublicKey(this.peerPublicKey);
    this.peerKeyBlockSize = ((this.peerKeyDetails.asymmetricKeyDetails as any).modulusLength as number) / 8;
    const peerDer = this.peerKeyDetails.export({ type: 'pkcs1', format: 'der' });
    this._peerFingerprint = crypto.createHash('sha256').update(peerDer).digest();

    // State
    this._buffer = Buffer.alloc(0);
    this._state = 'HANDSHAKE_SEND_FP';
    this._sessionKey = null;
    this._writeBuffer = [];

    const netOptions: net.NetConnectOpts = {
      port: (options as any).port,
      host: (options as any).host,
      path: (options as any).path,
    };
    this._socket = net.connect(netOptions);

    // [重要] 禁用 Nagle 算法，这对握手协议至关重要
    // this._socket.setNoDelay(true);

    this._socket.on('data', this._onSocketData.bind(this));
    this._socket.on('close', this.onClose.bind(this));
    this._socket.on('connect', this._startHandshake.bind(this));
    this._socket.on('end', this.onEnd.bind(this));
    this._socket.on('error', this.onError.bind(this));
    this._socket.on('timeout', this.onTimeout.bind(this));
    this._socket.on('drain', this.onDrain.bind(this));
  }

  private _startHandshake() {
    try {
      const myPubKeyObject = crypto.createPublicKey(this.privateKey);
      const myDer = myPubKeyObject.export({ type: 'pkcs1', format: 'der' });
      const myFingerprint = crypto.createHash('sha256').update(myDer).digest();

      this._sendOAEP(myFingerprint);
      this._state = 'HANDSHAKE_WAIT_SERVER_FP';
    } catch (e: any) {
      this.onError(new Error('Handshake init failed: ' + e.message));
    }
  }

  private _sendOAEP(data: Buffer, callback?: (err?: Error) => void) {
    const maxSize = this.peerKeyBlockSize - 66;

    let offset = 0;
    const chunks: Buffer[] = [];

    while (offset < data.length) {
      const end = Math.min(offset + maxSize, data.length);
      const chunk = data.subarray(offset, end);
      try {
        const encrypted = crypto.publicEncrypt(
          {
            key: this.peerPublicKey,
            padding: crypto.constants.RSA_PKCS1_OAEP_PADDING,
            oaepHash: OAEP_HASH,
          },
          chunk,
        );
        chunks.push(encrypted);
      } catch (e: any) {
        if (callback) callback(e);
        else this.onError(e);
        return false;
      }
      offset += maxSize;
    }

    const totalBuffer = Buffer.concat(chunks);
    return this._socket.write(totalBuffer, callback);
  }

  private _decryptOAEP(chunk: Buffer) {
    return crypto.privateDecrypt(
      {
        key: this.privateKey,
        padding: crypto.constants.RSA_PKCS1_OAEP_PADDING,
        oaepHash: OAEP_HASH,
      },
      chunk,
    );
  }

  private _onSocketData(chunk: Buffer) {
    this._buffer = Buffer.concat([this._buffer, chunk]);
    this._processBuffer();
  }

  private _processBuffer() {
    let proceed = true;
    while (proceed && this._buffer.length > 0) {
      proceed = false;

      if (this._state === 'HANDSHAKE_WAIT_SERVER_FP') {
        if (this._buffer.length >= this.myKeyBlockSize) {
          const encryptedFP = this._buffer.subarray(0, this.myKeyBlockSize);
          this._buffer = this._buffer.subarray(this.myKeyBlockSize);

          try {
            const serverFingerprint = this._decryptOAEP(encryptedFP);
            if (!serverFingerprint.equals(this._peerFingerprint)) {
              throw new Error('Peer fingerprint mismatch');
            }
            this._performModeNegotiation();
          } catch (e: any) {
            this.onError(new Error('Handshake verification failed: ' + e.message));
            this._socket.destroy();
            return;
          }
          proceed = true;
        }
      } else if (this._state === 'ESTABLISHED') {
        if (this._mode === MODE_PLAIN) {
          if (this._buffer.length > 0) {
            const payload = this._buffer;
            this._buffer = Buffer.alloc(0);
            this.emit('data', payload);
          }
        } else if (this._mode === MODE_RSA) {
          if (this._buffer.length >= this.myKeyBlockSize) {
            const encryptedBlock = this._buffer.subarray(0, this.myKeyBlockSize);
            this._buffer = this._buffer.subarray(this.myKeyBlockSize);
            try {
              const plaintext = this._decryptOAEP(encryptedBlock);
              this.emit('data', plaintext);
            } catch (e: any) {
              this.onError(new Error('RSA Decryption failed: ' + e.message));
            }
            proceed = true;
          }
        } else if (this._mode === MODE_AES) {
          if (this._buffer.length >= LENGTH_HEADER_SIZE) {
            const packetLen = Number(this._buffer.readBigUInt64BE(0));
            if (this._buffer.length >= LENGTH_HEADER_SIZE + packetLen) {
              const payload = this._buffer.subarray(LENGTH_HEADER_SIZE, LENGTH_HEADER_SIZE + packetLen);
              this._buffer = this._buffer.subarray(LENGTH_HEADER_SIZE + packetLen);

              const iv = payload.subarray(0, AES_IV_SIZE);
              const ciphertext = payload.subarray(AES_IV_SIZE);
              try {
                const decipher = crypto.createDecipheriv(AES_ALGORITHM, this._sessionKey as Buffer, iv);
                let decrypted = decipher.update(ciphertext);
                decrypted = Buffer.concat([decrypted, decipher.final()]);
                this.emit('data', decrypted);
              } catch (e: any) {
                this.onError(new Error('AES Decryption error: ' + e.message));
              }
              proceed = true;
            }
          }
        }
      }
    }
  }

  private _performModeNegotiation() {
    try {
      const self = this;
      const modePayload = Buffer.from([this._mode]);

      this._sendOAEP(modePayload, function () {
        if (self._mode === MODE_AES) {
          self._sessionKey = crypto.randomBytes(AES_KEY_SIZE);
          self._sendOAEP(self._sessionKey, function () {
            self._finishHandshake();
          });
        } else {
          self._finishHandshake();
        }
      });
    } catch (e: any) {
      this.onError(new Error('Mode negotiation failed: ' + e.message));
    }
  }

  private _finishHandshake() {
    this._state = 'ESTABLISHED';
    this.emit('connect');

    if (this._writeBuffer.length > 0) {
      const flush = () => {
        const pending = this._writeBuffer;
        this._writeBuffer = [];
        pending.forEach((item) => {
          this.write(item.data, item.encoding, item.callback);
        });
      };

      if (this._mode === MODE_PLAIN) {
        setTimeout(flush, 50);
      } else {
        flush();
      }
    }
  }

  private onDrain() {
    this.emit('drain');
  }

  write(
    data: string | Buffer,
    encoding?: BufferEncoding | ((err?: Error) => void) | null,
    callback?: (err?: Error) => void,
  ) {
    if (typeof encoding === 'function') {
      callback = encoding;
      encoding = null;
    }
    if (typeof data === 'string') {
      data = Buffer.from(data, (encoding as BufferEncoding) || 'utf8');
    }

    if (this._state !== 'ESTABLISHED') {
      this._writeBuffer.push({ data, encoding: (encoding as BufferEncoding) || null, callback });
      return false;
    }

    try {
      if (this._mode === MODE_PLAIN) {
        return this._socket.write(data, callback);
      } else if (this._mode === MODE_RSA) {
        return this._sendOAEP(data, callback);
      } else if (this._mode === MODE_AES) {
        const iv = crypto.randomBytes(AES_IV_SIZE);
        const cipher = crypto.createCipheriv(AES_ALGORITHM, this._sessionKey as Buffer, iv);
        const encrypted = Buffer.concat([cipher.update(data), cipher.final()]);
        const payload = Buffer.concat([iv, encrypted]);
        const header = Buffer.alloc(LENGTH_HEADER_SIZE);
        header.writeBigUInt64BE(BigInt(payload.length));
        return this._socket.write(Buffer.concat([header, payload]), callback);
      }
    } catch (e: any) {
      if (callback) callback(e);
      else this.onError(e);
      return false;
    }
  }

  end(data?: string | Buffer, encoding?: BufferEncoding) {
    if (data) {
      this.write(data, encoding || null, () => {
        this._socket.end();
      });
    } else {
      this._socket.end();
    }
  }

  private onClose(had_error: boolean) {
    this.emit('close', had_error);
  }
  private onEnd() {
    this.emit('end');
  }
  private onError(err: Error) {
    this.emit('error', err);
  }
  private onTimeout() {
    this.emit('timeout');
  }
}
