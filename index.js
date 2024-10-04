'use strict';

var EventEmitter = require('events').EventEmitter
  , util = require('util')
  , bufferEqual = require('buffer-equal')
  , Transport = require('./transport').Transport
  , TLSTransport = require('./transport').TLSTransport
  , genericPool = require('generic-pool')
  , randomString = require('random-string')
  , Uint64BE = require('int64-buffer').Uint64BE
  , CRC32 = require('js-crc').crc32
  ;

// 0x00 SC.Noop
var NOOP = exports.NOOP = Buffer.from('\x00');
// 0x01 WC.GrabJob
var GRAB_JOB = exports.GRAB_JOB = Buffer.from('\x01');
// 0x02 WC.SchedLater
var SCHED_LATER = exports.SCHED_LATER = Buffer.from('\x02');
// 0x03 WC.WorkDone
var WORK_DONE = exports.WORK_DONE = Buffer.from('\x03');
// 0x04 WC.WorkFail
var WORK_FAIL = exports.WORK_FAIL = Buffer.from('\x04');
// 0x05 SC.JobAssign
var JOB_ASSIGN = exports.JOB_ASSIGN = Buffer.from('\x05');
// 0x06 SC.NoJob
var NO_JOB = exports.NO_JOB = Buffer.from('\x06');
// 0x07 WC.CanDo
var CAN_DO = exports.CAN_DO = Buffer.from('\x07');
// 0x08 WC.CantDo
var CANT_DO = exports.CANT_DO = Buffer.from('\x08');
// 0x09 WC.Ping
// 0x09 CC.Ping
var PING = exports.PING = Buffer.from('\x09');
// 0x0A SC.Pong
var PONG = exports.PONG = Buffer.from('\x0A');
// 0x0B WC.Sleep
var SLEEP = exports.SLEEP = Buffer.from('\x0B');
// 0x0C SC.Unknown
var UNKNOWN = exports.UNKNOWN = Buffer.from('\x0C');
// 0x0D CC.SubmitJob
var SUBMIT_JOB = exports.SUBMIT_JOB = Buffer.from('\x0D');
// 0x0E CC.Status
var STATUS = exports.STATUS = Buffer.from('\x0E');
// 0x0F CC.DropFunc
var DROP_FUNC = exports.DROP_FUNC = Buffer.from('\x0F');
// 0x10 SC.Success
var SUCCESS = exports.SUCCESS = Buffer.from('\x10');
// 0x11 CC.RemoveJob
var REMOVE_JOB = exports.REMOVE_JOB = Buffer.from('\x11');
// 0x12 CC.Dump
// 0x13 CC.Load
// 0x14 CC.Shutdown
// 0x15 WC.Broadcast
var BROADCAST = exports.BROADCAST = Buffer.from('\x15');
// 0x16 CC.ConfigGet
// 0x17 CC.ConfigSet
// 0x18 SC.Config
// 0x19 CC.RunJob
var RUN_JOB = exports.RUN_JOB = Buffer.from('\x19');
// 0x1A SC.Acquired
var ACQUIRED = exports.ACQUIRED = Buffer.from('\x1A');
// 0x1B WC.Acquire
var ACQUIRE = exports.ACQUIRE = Buffer.from('\x1B');
// 0x1C WC.Release
var RELEASE = exports.RELEASE = Buffer.from('\x1C');
// 0x1D SC.NoWorker
var NO_WORKER = exports.NO_WORKER = Buffer.from('\x1D');
// 0x1E SC.Data
var DATA = exports.DATA = Buffer.from('\x1E');
// 0x1F CC.RecvData
var RECV_DATA = exports.RECV_DATA = Buffer.from('\x1F');
// 0x20 WC.WorkData
var WORK_DATA = exports.WORK_DATA = Buffer.from('\x20');

var MAGIC_REQUEST   = Buffer.from('\x00REQ');
var MAGIC_RESPONSE  = Buffer.from('\x00RES');


// client type

var TYPE_CLIENT = Buffer.from('\x01');
var TYPE_WORKER = Buffer.from('\x02');


var BaseClient = function(options, clientType, TransportClass) {
  EventEmitter.call(this);
  TransportClass = TransportClass === undefined ? Transport : TransportClass;
  this._clientType = clientType;
  var transport = new TransportClass(options) ;
  this._transport = transport;
  this._agents = {};
  var agent = new BaseAgent(this, null);
  agent.send(clientType);
  this._buffers = [];
  var self = this;
  transport.on('data', function(chunk) {
    self._buffers.push(chunk);
    var buffer = Buffer.concat(self._buffers);
    while (buffer.length >= 12) {
      var magic = buffer.slice(0, 4);
      if (!bufferEqual(magic, MAGIC_RESPONSE)) {
        throw 'Magic not match.';
      }
      var header = buffer.slice(4, 8);
      var length = header.readUInt32BE();
      if (buffer.length >=  12 + length) {
        var crc = buffer.slice(8, 12);
        var payload = buffer.slice(12, 12 + length);

        if (Buffer.from(CRC32(payload), 'hex') === crc) {
          throw 'CRC not match.'
        }

        if (!self.client_id) {
          self.client_id = payload.slice(1,5);
        } else {
          var msgid = payload.slice(0, 4).toString();
          self.emitAgent('data', msgid, payload.slice(4));
          self.emitAgent('end',  msgid);
        }

        buffer = buffer.slice(12 + length, buffer.length);

      } else {
        break;
      }
    }
    self._buffers = [buffer];
  });
};


var BaseAgent = function(client, msgid, cb, autotemove) {
  this._client = client;
  this._msgid = msgid;
  this._cb = cb || function() {};
  this._data = [];
  this._autoremove = autotemove === undefined ? true : autotemove;

  var self = this;

  this.on('data', function(data) {
    self.onData(data);
  });

  this.on('end', function() {
    self.onEnd();
  });
  this.on('error', function(err) {
    self.onError(err);
  });
};


util.inherits(BaseAgent, EventEmitter);

BaseAgent.prototype.send = function(buf) {
  if (this._msgid) {
    buf = Buffer.concat([Buffer.from(this._msgid + ''), buf]);
  }
  var size = encodeInt32(buf.length);
  var crc = Buffer.from(CRC32(buf), 'hex');
  this._client._transport.write(Buffer.concat([MAGIC_REQUEST, size, crc, buf]));
};


BaseAgent.prototype.onData = function (data) {
  this._data.push(data);
};


BaseAgent.prototype.onEnd = function () {
  if (this._autoremove) {
    this._client.removeAgent(this);
  }
  var cb = this._cb;
  if (this._data && this._data.length > 0) {
    var data = this._data.shift();
    setImmediate(function() {
      cb(null, data);
    });
  } else {
    setImmediate(function() {
      cb();
    });
  }
};


BaseAgent.prototype.onError = function (err) {
  this._client.removeAgent(this);
  this._cb(err);
};


BaseClient.prototype.close = function() {
  this._transport.end();
};


BaseClient.prototype.agent = function(autotemove, cb) {
  if (typeof autotemove === 'function') {
    cb = autotemove;
    autotemove = true;
  }
  var agent = new BaseAgent(this, randomString({length: 4}), cb, autotemove);
  this._agents[agent._msgid] = agent;
  return agent;
};


BaseClient.prototype.removeAgent = function(agent) {
  if (this._agents[agent._msgid]) {
    delete this._agents[agent._msgid];
  }
};


BaseClient.prototype.emitAgent = function(evt, msgid, data) {
  var agent = this._agents[msgid];
  if (agent) {
    agent.emit(evt, data);
  } else {
    if (data && data.length > 0) {
      throw 'Agent ' + msgid + ' not found.';
    }
  }
};


var PeriodicClient = exports.PeriodicClient = function(options) {
  var transportClass;
  if (options.tls) {
    transportClass = TLSTransport;
  } else {
    transportClass = Transport;
  }
  this._client = new BaseClient(options, TYPE_CLIENT, transportClass);
  checkAlive(this);
};

function checkAlive(client) {
  client.ping(() => {
    setTimeout(() => { checkAlive(client); }, 100000);
  });
}

PeriodicClient.prototype.ping = function(cb) {
  var agent = this._client.agent(cb);
  agent.send(PING);
};


PeriodicClient.prototype.submitJob = function(job, cb) {
  var agent = this._client.agent(cb);
  agent.send(Buffer.concat([SUBMIT_JOB, encodeJob(job)]));
};

// runJob and return the result
PeriodicClient.prototype.runJob = function(job, cb) {
  var agent = this._client.agent(function(err, payload) {
    if (err) return cb(err);
    if (payload[0] === NO_WORKER[0]) {
      return cb(new Error('no worker'))
    }
    if (payload[0] == DATA[0]) {
      payload = payload.slice(1)
      if (payload.toString() === 'failed') {
        return cb(new Error('failed'))
      }
      return cb(null, payload);
    }
    return cb(new Error('unknow error ' + payload));
  });
  agent.send(Buffer.concat([RUN_JOB, encodeJob(job)]));
};


PeriodicClient.prototype.status = function(cb) {
  var agent = this._client.agent(function(err, payload) {
    if (err) return cb(err);
    var retval = {};
    payload.slice(1).toString().trim().split('\n').forEach(function(stat) {
      stat = stat.split(',');
      retval[stat[0]] = {
        'func_name': stat[0],
        'worker_count': Number(stat[1]),
        'job_count': Number(stat[2]),
        'processing': Number(stat[3]),
        'schedat': Number(stat[4])
      };

    });
    cb(null, retval);
  });
  agent.send(STATUS);
};


PeriodicClient.prototype.dropFunc = function(func, cb) {
  var agent = this._client.agent(cb);
  agent.send(Buffer.concat([DROP_FUNC, encodeStr8(func)]));
};


PeriodicClient.prototype.removeJob = function(opts, cb) {
  var agent = this._client.agent(cb);
  agent.send(Buffer.concat([
    REMOVE_JOB,
    encodeStr8(opts.func),
    encodeStr8(opts.name)
  ]));
};


PeriodicClient.prototype.close = function() {
  this._client.close();
};


exports.PeriodicClientPool = function(options, poolOpts) {
  var factory = {
    create: function() {
      return new PeriodicClient(options);
    },
    destroy: function(client) {
      client.close();
    }
  };
  var pool = genericPool.createPool(factory, poolOpts);

  this.ping = withClient(pool, 'ping');
  this.submitJob = withClient(pool, 'submitJob');
  this.status = withClient(pool, 'status');
  this.dropFunc = withClient(pool, 'dropFunc');
  this.removeJob = withClient(pool, 'removeJob');
  this.close = pool.destroy.bind(pool);

};

function withClient(pool, func) {
  return function() {
    var args = [];
    var argv = arguments.length;
    for (var i=0;i<argv;i++) {
      args.push(arguments[i]);
    }
    var cb = false;
    if (argv > 0) {
      if (typeof args[argv - 1] === 'funciton') {
        cb = args.pop();
      }
    }

    var resourcePromise = pool.acquire();
    resourcePromise
      .then(function(client) {
        if (cb) {
          args.push(function() {
            pool.release(client);
            cb.apply(null, arguments);
          });
        }
        client[func].apply(client, args);
        if (!cb) pool.release(client);
      })
      .catch(function(err) {
        if (cb) cb(err);
      });
  };
}


var PeriodicWorker = exports.PeriodicWorker = function(options) {
  var transportClass;
  if (options.tls) {
    transportClass = TLSTransport;
  } else {
    transportClass = Transport;
  }
  this._client = new BaseClient(options, TYPE_WORKER, transportClass);
  this._tasks = {};
  checkAlive(this);
};


PeriodicWorker.prototype.ping = function(cb) {
  var agent = this._client.agent(cb);
  agent.send(PING);
};

PeriodicWorker.prototype.work = function(size) {
  size = Number(size) || 1;
  if (size<0) {
    size = 1;
  }
  for (var i=0; i<size; i++) {
    this._work();
  }
};

PeriodicWorker.prototype._work = function() {
  var self = this;
  var timer = null;
  var waiting = false;
  var task;
  var job;
  var agent = this._client.agent(false, function(err, buf) {
    if (err) return sendGrabJob();
    if (buf[0] === JOB_ASSIGN[0]) {
      waiting = true;

      job = new PeriodicJob(buf.slice(1), self._client, function() {
        waiting = false;
        sendGrabJob();
      });

      task = self._tasks[job.funcName];
      if (task) {
        try {
          task(job, function(err, ret, later) {
            if (err) {
              return job.fail();
            }
            if (ret) {
              return job.data(ret);
            }
            if (later) {
              return job.schedLater(later);
            }
            job.done();
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

  function sendGrabJob(delay) {
    delay = delay || 0;
    if (timer) {
      clearTimeout(timer);
    }
    timer = setTimeout(function() {
      if (!waiting) {
        if (agent._data.length === 0) {
          agent.send(GRAB_JOB);
        }
      }
      sendGrabJob(1);
    }, delay * 1000);
  }
  sendGrabJob();
};


// addFunc to periodic server.
// eg.
//        worker.addFunc(funcName, function(job, done) {
//          // you code here
//          // done the job
//          done()
//          // fail the job
//          done(someError)
//          // send data to cient
//          done(null, someData)
//          // sched later
//          done(null, null, later);
//        })
PeriodicWorker.prototype.addFunc = function(func, task) {
  var agent = this._client.agent();
  agent.send(Buffer.concat([CAN_DO, encodeStr8(func)]));
  agent.emit('end');
  this._tasks[func] = task;
};


// set the func is a broadcast func.
// eg.
//        worker.broadcast(funcName, function(job, done) {
//          // you code here
//          done()
//        })
PeriodicWorker.prototype.broadcast = function(func, task) {
  var agent = this._client.agent();
  agent.send(Buffer.concat([BROADCAST, encodeStr8(func)]));
  agent.emit('end');
  this._tasks[func] = task;
};


PeriodicWorker.prototype.removeFunc = function(func) {
  var agent = this._client.agent();
  agent.send(Buffer.concat([CANT_DO, encodeStr8(func)]));
  agent.emit('end');
  this._tasks[func] = null;
};


PeriodicWorker.prototype.close = function() {
  this._client.close();
};


var PeriodicJob = function(buf, client, done) {
  this._buffer = buf;
  this._client = client;
  this.doneed = false;
  var self = this;
  this._done = function() {
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
};


PeriodicJob.prototype.done = function(data) {
  var agent = this._client.agent();
  data = data || '';
  agent.send(Buffer.concat([WORK_DONE, this.jobHandle, Buffer.from(data)]));
  agent.emit('end');
  this._done();
};


PeriodicJob.prototype.fail = function() {
  var agent = this._client.agent();
  agent.send(Buffer.concat([WORK_FAIL, this.jobHandle]));
  agent.emit('end');
  this._done();
};


PeriodicJob.prototype.schedLater = function(delay) {
  var agent = this._client.agent();
  agent.send(Buffer.concat([SCHED_LATER, this.jobHandle, encodeInt64(delay),
    encodeInt16(0)]));
  agent.emit('end');
  this._done();
};


PeriodicJob.prototype.acquireLock = function(name, maxCount, cb) {
  var self = this;
  var agent = this._client.agent(function(err, buf) {
    if (buf[0] === ACQUIRED[0]) {
      if (buf[1] === 1) {
        cb(function() {
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
  agent.send(Buffer.concat([ACQUIRE, encodeStr8(name), encodeInt16(maxCount),
    this.jobHandle]));
};

PeriodicJob.prototype.releaseLock = function(name) {
  var agent = this._client.agent();
  agent.send(Buffer.concat[RELEASE, encodeStr8(name), this.jobHandle]);
  agent.emit('end');
}

function encodeStr8(dat) {
  dat = Buffer.from(dat || '');
  return Buffer.concat([encodeInt8(dat.length), dat]);
}

function encodeStr32(dat) {
  dat = Buffer.from(dat || '');
  return Buffer.concat([encodeInt32(dat.length), dat]);
}

function encodeInt8(n) {
  if (n > 0xFF) {
    throw new Error('Data to large 0xFF');
  }
  var h = Buffer.alloc(1);
  h.writeUInt8(n);
  return h;
}

function encodeInt16(n) {
  if (n > 0xFFFF) {
    throw new Error('Data to large 0xFFFF');
  }
  n = n || 0;
  var h = Buffer.alloc(2);
  h.writeUInt16BE(n);
  return h;
}

function encodeInt32(n) {
  if (n > 0xFFFFFFFF) {
    throw new Error('Data to large 0xFFFFFFFF');
  }
  n = n || 0;
  var h = Buffer.alloc(4);
  h.writeUInt32BE(n);
  return h;
}

function encodeInt64(n) {
  return Uint64BE(n || 0).toBuffer();
}

function encodeJob(job) {
  var ver = 0;
  if (job.count > 0 && job.timeout > 0) {
    ver = 3;
  } else if (job.timeout > 0) {
    ver = 2;
  } else if (job.count > 0) {
    ver = 1;
  }

  var ext = encodeInt8(ver);

  if (ver === 1) {
    ext = Buffer.concat([ext, encodeInt32(job.count)]);
  } else if (ver === 2) {
    ext = Buffer.concat([ext, encodeInt32(job.timeout)]);
  } else if (ver === 3) {
    ext = Buffer.concat([ext, encodeInt32(job.count), encodeInt32(job.timeout)]);
  }

  return Buffer.concat([
    encodeStr8(job.func),
    encodeStr8(job.name),
    encodeStr32(job.workload),
    encodeInt64(job.sched_at),
    ext,
  ]);
}

function decodeJob(payload) {
  var job = {};
  var h = 0;
  h = payload.slice(0, 1).readUInt8();
  payload = payload.slice(1);
  job.func = payload.slice(0, h).toString();
  payload = payload.slice(h);

  h = payload.slice(0, 1).readUInt8();
  payload = payload.slice(1);
  job.name = payload.slice(0, h).toString();
  payload = payload.slice(h);

  h = payload.slice(0, 4).readUInt32BE();
  payload = payload.slice(4);
  job.workload = payload.slice(0, h).toString();
  payload = payload.slice(h);

  job.sched_at = Uint64BE(payload.slice(0, 8)).toNumber();
  payload = payload.slice(8);

  var ver = payload.slice(0, 1).readUInt8();
  payload = payload.slice(1);
  if (ver === 1) {
    job.count = payload.slice(0, 4).readUInt32BE();
  } else if (ver === 2) {
    job.timeout = payload.slice(0, 4).readUInt32BE();
  } else if (ver === 3) {
    job.count = payload.slice(0, 4).readUInt32BE();
    job.timeout = payload.slice(4, 8).readUInt32BE();
  }

  return job;
}
