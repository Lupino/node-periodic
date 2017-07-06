"use strict";

var net = require('net')
  , EventEmitter = require('events').EventEmitter
  , util = require('util')
  , bufferIndexOf = require('buffer-indexof')
  , bufferEqual = require('buffer-equal')
  , uuid = require('uuid')
  ;

var NOOP        = exports.NOOP        = new Buffer("\x00");
// for job
var GRAB_JOB    = exports.GRAB_JOB    = new Buffer("\x01");
var SCHED_LATER = exports.SCHED_LATER = new Buffer("\x02");
var WORK_DONE   = exports.WORK_DONE   = new Buffer("\x03");
var WORK_FAIL   = exports.WORK_FAIL   = new Buffer("\x04");
var JOB_ASSIGN  = exports.JOB_ASSIGN  = new Buffer("\x05");
var NO_JOB      = exports.NO_JOB      = new Buffer("\x06");
// for func
var CAN_DO      = exports.CAN_DO      = new Buffer("\x07");
var CANT_DO     = exports.CANT_DO     = new Buffer("\x08");
// for test
var PING        = exports.PING        = new Buffer("\x09");
var PONG        = exports.PONG        = new Buffer("\x0A");
// other
var SLEEP       = exports.SLEEP       = new Buffer("\x0B");
var UNKNOWN     = exports.UNKNOWN     = new Buffer("\x0C");
// client command
var SUBMIT_JOB  = exports.SUBMIT_JOB  = new Buffer("\x0D");
var STATUS      = exports.STATUS      = new Buffer("\x0E");
var DROP_FUNC   = exports.DROP_FUNC   = new Buffer("\x0F");
var SUCCESS     = exports.SUCCESS     = new Buffer("\x10");
var REMOVE_JOB  = exports.REMOVE_JOB  = new Buffer("\x11");

var NULL_CHAR       = new Buffer("\x00\x01");
var MAGIC_REQUEST   = new Buffer("\x00REQ");
var MAGIC_RESPONSE  = new Buffer("\x00RES");


// client type

var TYPE_CLIENT = new Buffer("\x01");
var TYPE_WORKER = new Buffer("\x02");


var BaseClient = function(options, clientType) {
    EventEmitter.call(this);
    this._clientType = clientType;
    var socket = net.connect(options) ;
    this._socket = socket;
    this._agents = {};
    var agent = new BaseAgent(this, null);
    agent.send(clientType);
    this._buffers = [];
    var self = this;
    socket.on("data", function(chunk) {
        self._buffers.push(chunk);
        var buffer = Buffer.concat(self._buffers);
        while (buffer.length >= 8) {
            var magic = buffer.slice(0, 4);
            if (!bufferEqual(magic, MAGIC_RESPONSE)) {
                throw "Magic not match.";
            }
            var header = buffer.slice(4, 8);
            var length = parseHeader(header);
            if (buffer.length >=  8 + length) {
                var payload = buffer.slice(8, 8 + length);
                var parts = bsplit(payload, NULL_CHAR, 2);
                var uuid = parts[0].toString();
                self.emitAgent('data', uuid, parts[1]);
                self.emitAgent('end',  uuid);

                buffer = buffer.slice(8 + length, buffer.length);
            } else {
                break;
            }
        }
        self._buffers = [buffer];
    });
};


var BaseAgent = function(client, uuid, cb) {
    this._client = client;
    this._uuid = uuid;
    this._cb = cb || function() {};
    this._data = [];

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
    if (this._uuid) {
        buf = Buffer.concat([new Buffer(this._uuid + ""), NULL_CHAR, buf]);
    }
    var header = makeHeader(buf);
    this._client._socket.write(MAGIC_REQUEST);
    this._client._socket.write(header);
    this._client._socket.write(buf);
};


BaseAgent.prototype.onData = function (data) {
    this._data.push(data);
};


BaseAgent.prototype.onEnd = function () {
    this._client.removeAgent(this);
    if (this._data && this._data.length > 0) {
        var data = Buffer.concat(this._data);
        this._cb(null, data);
    } else {
        this._cb();
    }
};


BaseAgent.prototype.onError = function (err) {
    this._client.removeAgent(this);
    this._cb(err);
};


BaseClient.prototype.close = function() {
    this._socket.end();
};


BaseClient.prototype.agent = function(cb) {
    var agent = new BaseAgent(this, uuid.v1().toString(), cb);
    this._agents[agent._uuid] = agent;
    return agent;
};


BaseClient.prototype.removeAgent = function(agent) {
    if (this._agents[agent._uuid]) {
        delete this._agents[agent._uuid];
    }
};


BaseClient.prototype.emitAgent = function(evt, uuid, data) {
    var agent = this._agents[uuid];
    if (agent) {
        agent.emit(evt, data);
    } else {
      if (data && data.length > 0) {
        throw 'Agent ' + uuid + ' not found.';
      }
    }
};


function parseHeader(head){
    var length = head[0] << 24 | head[1] << 16 | head[2] << 8 | head[3];
    length = length & ~0x80000000;
    return length;
}


function makeHeader(data) {
    var buf = new Buffer(4);
    var length = data.length;
    buf[0] = length >> 24 & 0xff;
    buf[1] = length >> 16 & 0xff;
    buf[2] = length >> 8 & 0xff;
    buf[3] = length >> 0 & 0xff;
    return buf;
}


var PeriodicClient = exports.PeriodicClient = function(options) {
    this._client = new BaseClient(options, TYPE_CLIENT);
};


PeriodicClient.prototype.ping = function(cb) {
    var agent = this._client.agent(cb);
    agent.send(PING);
};


PeriodicClient.prototype.submitJob = function(job, cb) {
    var agent = this._client.agent(cb);
    agent.send(Buffer.concat([SUBMIT_JOB, NULL_CHAR, encodeJob(job)]));
};


PeriodicClient.prototype.status = function(cb) {
    var agent = this._client.agent(function(err, payload) {
        if (err) return cb(err);
        var retval = {};
        var stats = payload.toString().trim().split('\n').forEach(function(stat) {
            stat = stat.split(",");
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
    agent.send(Buffer.concat([DROP_FUNC, NULL_CHAR, new Buffer(func)]));
};


PeriodicClient.prototype.removeJob = function(job, cb) {
    var agent = this._client.agent(cb);
    agent.send(Buffer.concat([REMOVE_JOB, NULL_CHAR, encodeJob(job)]));
};


PeriodicClient.prototype.close = function() {
    this._client.close();
};


var PeriodicWorker = exports.PeriodicWorker = function(options) {
    this._client = new BaseClient(options, TYPE_WORKER);
};


PeriodicWorker.prototype.ping = function(cb) {
    var agent = this._client.agent(cb);
    agent.send(PING);
};


PeriodicWorker.prototype.grabJob = function(cb) {
    var self = this;
    var agent = this._client.agent(function(err, buf) {
        if (err) return cb(err);
        var cmd = buf.slice(0,1);
        if (buf[0] === NO_JOB[0] || cmd[0] !== JOB_ASSIGN[0]) {
            return cb(null, null);
        }
        cb(null, new PeriodicJob(buf.slice(3, buf.length), self._client));
    });
    agent.send(GRAB_JOB);
};


PeriodicWorker.prototype.addFunc = function(func, cb) {
    var agent = this._client.agent(cb);
    agent.send(Buffer.concat([CAN_DO, NULL_CHAR, new Buffer(func)]));
    agent.emit('end');
};


PeriodicWorker.prototype.removeFunc = function(func, cb) {
    var agent = this._client.agent(cb);
    agent.send(Buffer.concat([CANT_DO, NULL_CHAR, new Buffer(func)]));
};


PeriodicWorker.prototype.close = function() {
    this._client.close();
};


var PeriodicJob = function(buf, client) {
    this._buffer = buf;
    this._client = client;
    var payload = bsplit(buf, NULL_CHAR, 2);
    this.jobHandle = payload[0];
    this._payload = decodeJob(payload[1]);
    this.funcName = this._payload.func;
    this.name = this._payload.name;
    this.schedAt = this._payload.sched_at;
    this.runAt = this._payload.run_at || this.schedAt;
    this.workload = this._payload.workload;
};


PeriodicJob.prototype.done = function(cb) {
    var agent = this._client.agent(cb);
    agent.send(Buffer.concat([WORK_DONE, NULL_CHAR, this.jobHandle]));
    agent.emit('end');
};


PeriodicJob.prototype.fail = function(cb) {
    var agent = this._client.agent(cb);
    agent.send(Buffer.concat([WORK_FAIL, NULL_CHAR, this.jobHandle]));
    agent.emit('end');
};


PeriodicJob.prototype.schedLater = function(delay, cb) {
    var agent = this._client.agent(cb);
    agent.send(Buffer.concat([SCHED_LATER, NULL_CHAR, this.jobHandle,
                NULL_CHAR, new Buffer("" + delay)]));
    agent.emit('end');
};

function encodeJob(job) {
  var ret = [new Buffer(job.func), NULL_CHAR, new Buffer(job.name)];
  if (job.workload || job.count > 0 || job.sched_at > 0) {
    ret.push(NULL_CHAR);
    ret.push(new Buffer("" + job.sched_at));
  }
  if (job.workload || job.count > 0) {
    ret.push(NULL_CHAR);
    ret.push(new Buffer("" + job.count));
  }
  if (job.workload) {
    ret.push(NULL_CHAR);
    ret.push(job.workload);
  }
  return Buffer.concat(ret);
}

function decodeJob(payload) {
  var parts = bsplit(payload, NULL_CHAR, 5)
  var size = parts.length;

  var job = {};
  job.func = parts[0].toString();
  job.name = parts[1].toString();
  if (size > 2) {
    job.sched_at = Number(parts[2]);
  }
  if (size > 3) {
    job.count = Number(parts[3]);
  }
  if (size > 4) {
    job.workload = parts[4];
  }
  return job;
}

function bsplit(buf, splitBuf, total){

  var search = -1
  , lines = []
  , count = 1
  , total = total || -1;
  ;

  if (total === 1) {
    lines.push(total);
    return lines;
  }

  while((search = bufferIndexOf(buf,splitBuf)) > -1){
    lines.push(buf.slice(0,search));
    buf = buf.slice(search+splitBuf.length,buf.length);
    count += 1;
    if (total > -1 && count >= total) {
      break;
    }
  }

  if(buf.length) lines.push(buf);

  return lines;
}
