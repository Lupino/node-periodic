'use strict';

var EventEmitter = require('events').EventEmitter
  , util = require('util')
  , bufferEqual = require('buffer-equal')
  , Transport = require('./transport').Transport
  , TLSTransport = require('./transport').TLSTransport
  , genericPool = require('generic-pool')
  , randomString = require('random-string')
  , Uint64BE = require('int64-buffer').Uint64BE
  ;

var NOOP        = exports.NOOP        = Buffer.from('\x00');
// for job
var GRAB_JOB    = exports.GRAB_JOB    = Buffer.from('\x01');
var SCHED_LATER = exports.SCHED_LATER = Buffer.from('\x02');
var WORK_DONE   = exports.WORK_DONE   = Buffer.from('\x03');
var WORK_FAIL   = exports.WORK_FAIL   = Buffer.from('\x04');
var JOB_ASSIGN  = exports.JOB_ASSIGN  = Buffer.from('\x05');
var NO_JOB      = exports.NO_JOB      = Buffer.from('\x06');
// for func
var CAN_DO      = exports.CAN_DO      = Buffer.from('\x07');
var BROADCAST   = exports.BROADCAST   = Buffer.from('\x15');
var CANT_DO     = exports.CANT_DO     = Buffer.from('\x08');
// for test
var PING        = exports.PING        = Buffer.from('\x09');
var PONG        = exports.PONG        = Buffer.from('\x0A');
// other
var SLEEP       = exports.SLEEP       = Buffer.from('\x0B');
var UNKNOWN     = exports.UNKNOWN     = Buffer.from('\x0C');
// client command
var SUBMIT_JOB  = exports.SUBMIT_JOB  = Buffer.from('\x0D');
var STATUS      = exports.STATUS      = Buffer.from('\x0E');
var DROP_FUNC   = exports.DROP_FUNC   = Buffer.from('\x0F');
var SUCCESS     = exports.SUCCESS     = Buffer.from('\x10');
var REMOVE_JOB  = exports.REMOVE_JOB  = Buffer.from('\x11');

var RUN_JOB     = exports.RUN_JOB     = Buffer.from('\x19');
var WORK_DATA   = exports.WORK_DATA   = Buffer.from('\x1A');

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
    while (buffer.length >= 8) {
      var magic = buffer.slice(0, 4);
      if (!bufferEqual(magic, MAGIC_RESPONSE)) {
        throw 'Magic not match.';
      }
      var header = buffer.slice(4, 8);
      var length = header.readUInt32BE();
      if (buffer.length >=  8 + length) {
        var payload = buffer.slice(8, 8 + length);
        var msgid = payload.slice(0, 4).toString();
        self.emitAgent('data', msgid, payload.slice(4));
        self.emitAgent('end',  msgid);

        buffer = buffer.slice(8 + length, buffer.length);
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
  this._client._transport.write(Buffer.concat([MAGIC_REQUEST, encodeStr32(buf)]));
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
  var agent = this._client.agent(cb);
  agent.send(Buffer.concat([RUN_JOB, encodeJob(job)]));
};


PeriodicClient.prototype.status = function(cb) {
  var agent = this._client.agent(function(err, payload) {
    if (err) return cb(err);
    var retval = {};
    payload.toString().trim().split('\n').forEach(function(stat) {
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


PeriodicClient.prototype.removeJob = function(job, cb) {
  var agent = this._client.agent(cb);
  agent.send(Buffer.concat([REMOVE_JOB, encodeJob(job)]));
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
  this._done = done;
  var h = buf.slice(0, 1).readUInt8();
  this.jobHandle = buf.slice(0, h + 1);
  buf = buf.slice(h + 1);

  this._payload = decodeJob(buf);
  this.funcName = this._payload.func;
  this.name = this._payload.name;
  this.schedAt = this._payload.sched_at;
  this.runAt = this._payload.run_at || this.schedAt;
  this.workload = this._payload.workload;
};


PeriodicJob.prototype.done = function() {
  var agent = this._client.agent();
  agent.send(Buffer.concat([WORK_DONE, this.jobHandle]));
  agent.emit('end');
  this._done();
};


PeriodicJob.prototype.data = function(data) {
  var agent = this._client.agent();
  agent.send(Buffer.concat([WORK_DATA, this.jobHandle, Buffer.from(data)]));
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

function encodeStr8(dat) {
  dat = Buffer.from(dat || '');
  var h = Buffer.alloc(1);
  h.writeUInt8(dat.length);
  return Buffer.concat([h, dat]);
}

function encodeStr32(dat) {
  dat = Buffer.from(dat || '');
  var h = Buffer.alloc(4);
  h.writeUInt32BE(dat.length);
  return Buffer.concat([h, dat]);
}

function encodeInt16(n) {
  n = n || 0;
  var h = Buffer.alloc(2);
  h.writeUInt16BE(n);
  return h;
}

function encodeInt32(n) {
  n = n || 0;
  var h = Buffer.alloc(4);
  h.writeUInt32BE(n);
  return h;
}

function encodeInt64(n) {
  return Uint64BE(n || 0).toBuffer();
}

function encodeJob(job) {
  return Buffer.concat([
    encodeStr8(job.func),
    encodeStr8(job.name),
    encodeStr32(job.workload),
    encodeInt64(job.sched_at),
    encodeInt32(job.count),
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

  job.count = payload.slice(0, 4).readUInt32BE();
  return job;
}
