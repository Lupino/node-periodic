'use strict';

import { EventEmitter } from 'events';
import util from 'util';
import bufferEqual from 'buffer-equal';
import { Transport, RSATransport } from './transport';
import genericPool from 'generic-pool';
import randomString from 'random-string';
import { Uint64BE } from 'int64-buffer';
import { crc32 as CRC32 } from 'js-crc';

// 0x00 SC.Noop
export const NOOP = Buffer.from('\x00');
// 0x01 WC.GrabJob
export const GRAB_JOB = Buffer.from('\x01');
// 0x02 WC.SchedLater
export const SCHED_LATER = Buffer.from('\x02');
// 0x03 WC.WorkDone
export const WORK_DONE = Buffer.from('\x03');
// 0x04 WC.WorkFail
export const WORK_FAIL = Buffer.from('\x04');
// 0x05 SC.JobAssign
export const JOB_ASSIGN = Buffer.from('\x05');
// 0x06 SC.NoJob
export const NO_JOB = Buffer.from('\x06');
// 0x07 WC.CanDo
export const CAN_DO = Buffer.from('\x07');
// 0x08 WC.CantDo
export const CANT_DO = Buffer.from('\x08');
// 0x09 WC.Ping
// 0x09 CC.Ping
export const PING = Buffer.from('\x09');
// 0x0A SC.Pong
export const PONG = Buffer.from('\x0A');
// 0x0B WC.Sleep
export const SLEEP = Buffer.from('\x0B');
// 0x0C SC.Unknown
export const UNKNOWN = Buffer.from('\x0C');
// 0x0D CC.SubmitJob
export const SUBMIT_JOB = Buffer.from('\x0D');
// 0x0E CC.Status
export const STATUS = Buffer.from('\x0E');
// 0x0F CC.DropFunc
export const DROP_FUNC = Buffer.from('\x0F');
// 0x10 SC.Success
export const SUCCESS = Buffer.from('\x10');
// 0x11 CC.RemoveJob
export const REMOVE_JOB = Buffer.from('\x11');
// 0x12 CC.Dump
// 0x13 CC.Load
// 0x14 CC.Shutdown
// 0x15 WC.Broadcast
export const BROADCAST = Buffer.from('\x15');
// 0x16 CC.ConfigGet
// 0x17 CC.ConfigSet
// 0x18 SC.Config
// 0x19 CC.RunJob
export const RUN_JOB = Buffer.from('\x19');
// 0x1A SC.Acquired
export const ACQUIRED = Buffer.from('\x1A');
// 0x1B WC.Acquire
export const ACQUIRE = Buffer.from('\x1B');
// 0x1C WC.Release
export const RELEASE = Buffer.from('\x1C');
// 0x1D SC.NoWorker
export const NO_WORKER = Buffer.from('\x1D');
// 0x1E SC.Data
export const DATA = Buffer.from('\x1E');
// 0x1F CC.RecvData
export const RECV_DATA = Buffer.from('\x1F');
// 0x20 WC.WorkData
export const WORK_DATA = Buffer.from('\x20');
// 0x21 WC.Assigned
export const JOB_ASSIGNED = Buffer.from('\x21');

const MAGIC_REQUEST = Buffer.from('\x00REQ');
const MAGIC_RESPONSE = Buffer.from('\x00RES');

// client type
const TYPE_CLIENT = Buffer.from('\x01');
const TYPE_WORKER = Buffer.from('\x02');

export type PeriodicJobSpec = {
  func: string;
  name: string;
  workload?: string | Buffer;
  sched_at?: number;
  count?: number;
  timeout?: number;
};

export type WorkerTaskDone = (err?: any, ret?: any, later?: number) => void;
export type WorkerTask = (job: PeriodicJob, done: WorkerTaskDone) => void;

export type StatusResult = Record<
  string,
  {
    func_name: string;
    worker_count: number;
    job_count: number;
    processing: number;
    schedat: number;
  }
>;

type TransportLike = {
  on(event: string, listener: (...args: any[]) => void): any;
  write(data: any, encoding?: any, callback?: any): any;
  end(data?: any, encoding?: any): any;
};

type ClientOptions = {
  rsa?: boolean;
} & any;

class BaseClient extends EventEmitter {
  public client_id?: Buffer;

  private _clientType: Buffer;
  public _transport: TransportLike;
  private _agents: Record<string, BaseAgent>;
  private _buffers: Buffer[];

  constructor(options: ClientOptions, clientType: Buffer, TransportClass?: any) {
    super();
    TransportClass = TransportClass === undefined ? Transport : TransportClass;
    this._clientType = clientType;

    const transport = new TransportClass(options) as TransportLike;
    this._transport = transport;
    this._agents = {};

    const agent = new BaseAgent(this, null);
    agent.send(clientType);

    this._buffers = [];
    const self = this;

    transport.on('data', function (chunk: Buffer) {
      self._buffers.push(chunk);
      let buffer = Buffer.concat(self._buffers);
      while (buffer.length >= 12) {
        const magic = buffer.subarray(0, 4);
        if (!bufferEqual(magic, MAGIC_RESPONSE)) {
          throw 'Magic not match.';
        }
        const header = buffer.subarray(4, 8);
        const length = header.readUInt32BE();
        if (buffer.length >= 12 + length) {
          const crc = buffer.subarray(8, 12);
          const payload = buffer.subarray(12, 12 + length);

          if (!bufferEqual(Buffer.from(CRC32(payload), 'hex'), crc)) {
            throw 'CRC not match.';
          }

          if (!self.client_id) {
            self.client_id = payload.subarray(1, 5);
          } else {
            const msgid = payload.subarray(0, 4).toString();
            self.emitAgent('data', msgid, payload.subarray(4));
            self.emitAgent('end', msgid);
          }

          buffer = buffer.subarray(12 + length);
        } else {
          break;
        }
      }
      self._buffers = [buffer];
    });
  }

  close() {
    this._transport.end();
  }

  agent(autoremove: boolean, cb?: (err?: any, payload?: Buffer) => void): BaseAgent;
  agent(cb?: (err?: any, payload?: Buffer) => void): BaseAgent;
  agent(autoremove: any, cb?: any) {
    if (typeof autoremove === 'function') {
      cb = autoremove;
      autoremove = true;
    }
    const agent = new BaseAgent(this, randomString({ length: 4 }), cb, autoremove);
    this._agents[(agent as any)._msgid] = agent;
    return agent;
  }

  removeAgent(agent: BaseAgent) {
    const msgid = (agent as any)._msgid;
    if (this._agents[msgid]) {
      delete this._agents[msgid];
    }
  }

  emitAgent(evt: string, msgid: string, data?: Buffer) {
    const agent = this._agents[msgid];
    if (agent) {
      agent.emit(evt, data);
    } else {
      if (data && data.length > 0) {
        throw 'Agent ' + msgid + ' not found.';
      }
    }
  }
}

class BaseAgent extends EventEmitter {
  private _client: BaseClient;
  private _msgid: string | null;
  private _cb: (err?: any, payload?: Buffer) => void;
  public _data: Buffer[];
  private _autoremove: boolean;

  constructor(client: BaseClient, msgid: string | null, cb?: (err?: any, payload?: Buffer) => void, autoremove?: boolean) {
    super();
    this._client = client;
    this._msgid = msgid;
    this._cb = cb || function () {};
    this._data = [];
    this._autoremove = autoremove === undefined ? true : autoremove;

    const self = this;
    this.on('data', function (data: Buffer) {
      self.onData(data);
    });

    this.on('end', function () {
      self.onEnd();
    });

    this.on('error', function (err: any) {
      self.onError(err);
    });
  }

  send(buf: Buffer) {
    if (this._msgid) {
      buf = Buffer.concat([Buffer.from(this._msgid + ''), buf]);
    }
    const size = encodeInt32(buf.length);
    const crc = Buffer.from(CRC32(buf), 'hex');
    this._client._transport.write(Buffer.concat([MAGIC_REQUEST, size, crc, buf]));
  }

  private onData(data: Buffer) {
    this._data.push(data);
  }

  private onEnd() {
    if (this._autoremove) {
      this._client.removeAgent(this);
    }
    const cb = this._cb;
    if (this._data && this._data.length > 0) {
      const data = this._data.shift() as Buffer;
      setImmediate(function () {
        cb(null, data);
      });
    } else {
      setImmediate(function () {
        cb();
      });
    }
  }

  private onError(err: any) {
    this._client.removeAgent(this);
    this._cb(err);
  }
}

export class PeriodicClient {
  private _client: BaseClient;

  constructor(options: ClientOptions) {
    const transportClass = options.rsa ? RSATransport : Transport;
    this._client = new BaseClient(options, TYPE_CLIENT, transportClass);
    checkAlive(this);
  }

  ping(cb: (err?: any, ok?: Buffer) => void) {
    const agent = this._client.agent(cb);
    agent.send(PING);
  }

  submitJob(job: PeriodicJobSpec, cb: (err?: any, ok?: Buffer) => void) {
    const agent = this._client.agent(cb);
    agent.send(Buffer.concat([SUBMIT_JOB, encodeJob(job)]));
  }

  runJob(job: PeriodicJobSpec, cb: (err?: any, data?: Buffer) => void) {
    const agent = this._client.agent(function (err?: any, payload?: Buffer) {
      if (err) return cb(err);
      payload = payload as Buffer;
      if (payload[0] === NO_WORKER[0]) {
        return cb(new Error('no worker'));
      }
      if (payload[0] == DATA[0]) {
        payload = payload.subarray(1);
        if (payload.toString() === 'failed') {
          return cb(new Error('failed'));
        }
        return cb(null, payload);
      }
      return cb(new Error('unknow error ' + payload));
    });
    agent.send(Buffer.concat([RUN_JOB, encodeJob(job)]));
  }

  status(cb: (err?: any, ok?: StatusResult) => void) {
    const agent = this._client.agent(function (err?: any, payload?: Buffer) {
      if (err) return cb(err);
      const retval: StatusResult = {} as any;
      (payload as Buffer)
        .subarray(1)
        .toString()
        .trim()
        .split('\n')
        .forEach(function (stat) {
          const parts = stat.split(',');
          retval[parts[0]] = {
            func_name: parts[0],
            worker_count: Number(parts[1]),
            job_count: Number(parts[2]),
            processing: Number(parts[3]),
            schedat: Number(parts[4]),
          };
        });
      cb(null, retval);
    });
    agent.send(STATUS);
  }

  dropFunc(func: string, cb: (err?: any, ok?: Buffer) => void) {
    const agent = this._client.agent(cb);
    agent.send(Buffer.concat([DROP_FUNC, encodeStr8(func)]));
  }

  removeJob(opts: { func: string; name: string }, cb: (err?: any, ok?: Buffer) => void) {
    const agent = this._client.agent(cb);
    agent.send(Buffer.concat([REMOVE_JOB, encodeStr8(opts.func), encodeStr8(opts.name)]));
  }

  close() {
    this._client.close();
  }
}

function checkAlive(client: { ping: (cb: (err?: any, ok?: Buffer) => void) => void }) {
  client.ping(() => {
    setTimeout(() => {
      checkAlive(client);
    }, 100000);
  });
}

export class PeriodicClientPool {
  ping: (...args: any[]) => any;
  submitJob: (...args: any[]) => any;
  status: (...args: any[]) => any;
  dropFunc: (...args: any[]) => any;
  removeJob: (...args: any[]) => any;
  close: () => any;

  constructor(options: ClientOptions, poolOpts: any) {
    const factory = {
      create: function () {
        return new PeriodicClient(options);
      },
      destroy: function (client: PeriodicClient) {
        client.close();
      },
    };
    const pool = (genericPool as any).createPool(factory, poolOpts);

    this.ping = withClient(pool, 'ping');
    this.submitJob = withClient(pool, 'submitJob');
    this.status = withClient(pool, 'status');
    this.dropFunc = withClient(pool, 'dropFunc');
    this.removeJob = withClient(pool, 'removeJob');
    this.close = pool.destroy.bind(pool);
  }
}

function withClient(pool: any, func: string) {
  return function () {
    const args: any[] = [];
    const argv = arguments.length;
    for (let i = 0; i < argv; i++) {
      args.push(arguments[i]);
    }
    let cb: any = false;
    if (argv > 0) {
      if (typeof args[argv - 1] === 'function') {
        cb = args.pop();
      }
    }

    const resourcePromise = pool.acquire();
    resourcePromise
      .then(function (client: any) {
        if (cb) {
          args.push(function () {
            pool.release(client);
            cb.apply(null, arguments);
          });
        }
        client[func].apply(client, args);
        if (!cb) pool.release(client);
      })
      .catch(function (err: any) {
        if (cb) cb(err);
      });
  };
}

export class PeriodicWorker {
  private _client: BaseClient;
  private _tasks: Record<string, WorkerTask | null>;

  constructor(options: ClientOptions) {
    const transportClass = options.rsa ? RSATransport : Transport;
    this._client = new BaseClient(options, TYPE_WORKER, transportClass);
    this._tasks = {};
    checkAlive(this);
  }

  ping(cb: (err?: any, ok?: Buffer) => void) {
    const agent = this._client.agent(cb);
    agent.send(PING);
  }

  work(size?: number) {
    size = Number(size) || 1;
    if (size < 0) {
      size = 1;
    }
    for (let i = 0; i < size; i++) {
      this._work();
    }
  }

  private _work() {
    const self = this;
    let timer: any = null;
    let waiting = false;
    let task: WorkerTask | null | undefined;
    let job: PeriodicJob | undefined;

    const agent = this._client.agent(false, function (err?: any, buf?: Buffer) {
      if (err) return sendGrabJob();
      buf = buf as Buffer;
      if (buf[0] === JOB_ASSIGN[0]) {
        waiting = true;

        agent.send(JOB_ASSIGNED);

        job = new PeriodicJob(buf.subarray(1), self._client, function () {
          waiting = false;
          sendGrabJob();
        });

        task = self._tasks[job.funcName];
        if (task) {
          try {
            task(job, function (err2?: any, ret?: any, later?: number) {
              if (err2) {
                return job!.fail(function (err3?: any) {
                  if (err3) {
                    console.error('job.fail error', err3);
                  }
                });
              }
              if (ret) {
                return job!.done(ret, function (err3?: any) {
                  if (err3) {
                    console.error('job.done error', err3);
                  }
                });
              }
              if (later) {
                return job!.schedLater(later, function (err3?: any) {
                  if (err3) {
                    console.error('job.schedLater error', err3);
                  }
                });
              }
              job!.done(function (err3?: any) {
                if (err3) {
                  console.error('job.done error', err3);
                }
              });
            });
          } catch (e) {
            console.error('process job fail', e);
            job.fail();
            sendGrabJob();
          }
        } else {
          self.removeFunc(job.funcName);
          job.fail();
          sendGrabJob();
        }
      } else {
        sendGrabJob();
      }
    });

    function sendGrabJob(delay?: number) {
      delay = delay || 0;
      if (timer) {
        clearTimeout(timer);
      }
      timer = setTimeout(function () {
        if (!waiting) {
          if (agent._data.length === 0) {
            agent.send(GRAB_JOB);
          }
        }
        sendGrabJob(1);
      }, delay * 1000);
    }
    sendGrabJob();
  }

  addFunc(func: string, task: WorkerTask, cb?: (err?: any, ok?: Buffer) => void) {
    const agent = this._client.agent(cb || function () {});
    agent.send(Buffer.concat([CAN_DO, encodeStr8(func)]));
    this._tasks[func] = task;
  }

  broadcast(func: string, task: WorkerTask, cb?: (err?: any, ok?: Buffer) => void) {
    const agent = this._client.agent(cb || function () {});
    agent.send(Buffer.concat([BROADCAST, encodeStr8(func)]));
    this._tasks[func] = task;
  }

  removeFunc(func: string, cb?: (err?: any, ok?: Buffer) => void) {
    const agent = this._client.agent(cb || function () {});
    agent.send(Buffer.concat([CANT_DO, encodeStr8(func)]));
    this._tasks[func] = null;
  }

  close() {
    this._client.close();
  }
}

export class PeriodicJob {
  private _buffer: Buffer;
  private _client: BaseClient;
  private _done: () => void;
  private _payload: any;

  public doneed: boolean;
  public funcName: string;
  public name: string;
  public jobHandle: Buffer;
  public schedAt: number;
  public runAt: number;
  public workload: any;

  constructor(buf: Buffer, client: BaseClient, done: () => void) {
    this._buffer = buf;
    this._client = client;
    this.doneed = false;

    const self = this;
    this._done = function () {
      if (self.doneed) {
        return;
      } else {
        self.doneed = true;
        done();
      }
    };

    this._payload = decodeJob(buf);

    this.funcName = this._payload.func;
    this.name = this._payload.name;

    this.jobHandle = Buffer.concat([encodeStr8(this.funcName), encodeStr8(this.name)]);

    this.schedAt = this._payload.sched_at;
    this.runAt = this._payload.run_at || this.schedAt;
    this.workload = this._payload.workload;
  }

  done(data?: any, cb?: (err?: any, ok?: Buffer) => void): void {
    if (typeof data === 'function') {
      cb = data;
      data = '';
    }
    const self = this;
    const agent = this._client.agent(function (err?: any, payload?: Buffer) {
      (cb || function () {})(err, payload);
      self._done();
    });
    data = data || '';
    agent.send(Buffer.concat([WORK_DONE, this.jobHandle, Buffer.from(data)]));
  }

  data(data?: any, cb?: (err?: any, ok?: Buffer) => void) {
    if (typeof data === 'function') {
      cb = data;
      data = '';
    }
    const agent = this._client.agent(cb || function () {});
    data = data || '';
    agent.send(Buffer.concat([WORK_DATA, this.jobHandle, Buffer.from(data)]));
  }

  fail(cb?: (err?: any, ok?: Buffer) => void) {
    const self = this;
    const agent = this._client.agent(function (err?: any, payload?: Buffer) {
      (cb || function () {})(err, payload);
      self._done();
    });
    agent.send(Buffer.concat([WORK_FAIL, this.jobHandle]));
  }

  schedLater(delay: number, cb?: (err?: any, ok?: Buffer) => void) {
    const self = this;
    const agent = this._client.agent(function (err?: any, payload?: Buffer) {
      (cb || function () {})(err, payload);
      self._done();
    });
    agent.send(Buffer.concat([SCHED_LATER, this.jobHandle, encodeInt64(delay), encodeInt16(0)]));
  }

  acquireLock(name: string, maxCount: number, cb: (release: () => void) => void) {
    const self = this;
    const agent = this._client.agent(function (_err?: any, buf?: Buffer) {
      buf = buf as Buffer;
      if (buf[0] === ACQUIRED[0]) {
        if (buf[1] === 1) {
          cb(function () {
            self.releaseLock(name);
            self._done();
          });
        } else {
          self._done();
        }
      } else {
        self._done();
      }
    });
    agent.send(Buffer.concat([ACQUIRE, encodeStr8(name), encodeInt16(maxCount), this.jobHandle]));
  }

  releaseLock(name: string, cb?: (err?: any, ok?: Buffer) => void) {
    const self = this;
    const agent = this._client.agent(function (err?: any, payload?: Buffer) {
      (cb || function () {})(err, payload);
      self._done();
    });
    agent.send(Buffer.concat([RELEASE, encodeStr8(name), this.jobHandle]));
  }
}

function encodeStr8(dat: any) {
  dat = Buffer.from(dat || '');
  return Buffer.concat([encodeInt8(dat.length), dat]);
}

function encodeStr32(dat: any) {
  dat = Buffer.from(dat || '');
  return Buffer.concat([encodeInt32(dat.length), dat]);
}

function encodeInt8(n: number) {
  if (n > 0xff) {
    throw new Error('Data to large 0xFF');
  }
  const h = Buffer.alloc(1);
  h.writeUInt8(n);
  return h;
}

function encodeInt16(n: number) {
  if (n > 0xffff) {
    throw new Error('Data to large 0xFFFF');
  }
  n = n || 0;
  const h = Buffer.alloc(2);
  h.writeUInt16BE(n);
  return h;
}

function encodeInt32(n: number) {
  if (n > 0xffffffff) {
    throw new Error('Data to large 0xFFFFFFFF');
  }
  n = n || 0;
  const h = Buffer.alloc(4);
  h.writeUInt32BE(n);
  return h;
}

function encodeInt64(n: number) {
  return (Uint64BE as any)(n || 0).toBuffer();
}

function encodeJob(job: PeriodicJobSpec) {
  let ver = 0;
  if ((job.count || 0) > 0 && (job.timeout || 0) > 0) {
    ver = 3;
  } else if ((job.timeout || 0) > 0) {
    ver = 2;
  } else if ((job.count || 0) > 0) {
    ver = 1;
  }

  let ext = encodeInt8(ver);

  if (ver === 1) {
    ext = Buffer.concat([ext, encodeInt32(job.count || 0)]);
  } else if (ver === 2) {
    ext = Buffer.concat([ext, encodeInt32(job.timeout || 0)]);
  } else if (ver === 3) {
    ext = Buffer.concat([ext, encodeInt32(job.count || 0), encodeInt32(job.timeout || 0)]);
  }

  return Buffer.concat([
    encodeStr8(job.func),
    encodeStr8(job.name),
    encodeStr32((job as any).workload),
    encodeInt64((job as any).sched_at),
    ext,
  ]);
}

function decodeJob(payload: Buffer) {
  const job: any = {};
  let h = 0;
  h = payload.subarray(0, 1).readUInt8();
  payload = payload.subarray(1);
  job.func = payload.subarray(0, h).toString();
  payload = payload.subarray(h);

  h = payload.subarray(0, 1).readUInt8();
  payload = payload.subarray(1);
  job.name = payload.subarray(0, h).toString();
  payload = payload.subarray(h);

  h = payload.subarray(0, 4).readUInt32BE();
  payload = payload.subarray(4);
  job.workload = payload.subarray(0, h).toString();
  payload = payload.subarray(h);

  job.sched_at = (Uint64BE as any)(payload.subarray(0, 8)).toNumber();
  payload = payload.subarray(8);

  const ver = payload.subarray(0, 1).readUInt8();
  payload = payload.subarray(1);
  if (ver === 1) {
    job.count = payload.subarray(0, 4).readUInt32BE();
  } else if (ver === 2) {
    job.timeout = payload.subarray(0, 4).readUInt32BE();
  } else if (ver === 3) {
    job.count = payload.subarray(0, 4).readUInt32BE();
    job.timeout = payload.subarray(4, 8).readUInt32BE();
  }

  return job;
}

// preserve runtime shape by attaching to exports
// (TypeScript will emit these named exports on module.exports)
void util;
