"use strict";

var net = require('net')
  , EventEmitter = require('events').EventEmitter
  , util = require('util')
  , bsplit = require('buffer-split')
  ;

var NOOP        = exports.NOOP        = new Buffer("\x00");
// for job
var GRAB_JOB    = exports.GRAB_JOB    = new Buffer("\x01");
var SCHED_LATER = exports.SCHED_LATER = new Buffer("\x02");
var JOB_DONE    = exports.JOB_DONE    = new Buffer("\x03");
var JOB_FAIL    = exports.JOB_FAIL    = new Buffer("\x04");
var WAIT_JOB    = exports.WAIT_JOB    = new Buffer("\x05");
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

var NULL_CHAR = new Buffer("\x00\x01");


// client type

var TYPE_CLIENT = new Buffer("\x01");
var TYPE_WORKER = new Buffer("\x02");


var BaseClient = function(options, clientType) {
    EventEmitter.call(this);
    this._clientType = clientType;
    var socket = net.connect(options) ;
    this._socket = socket;
    this._msgId = 0;
    this._maxMsgId = options.maxMsgId || 100;
    var agent = new BaseAgent(this, 0);
    agent.send(clientType);
    this._buffers = [];
    var self = this;
    socket.on("data", function(chunk) {
        self._buffers.push(chunk);
        var buffer = Buffer.concat(self._buffers);
        if (buffer.length >= 4) {
            var header = buffer.slice(0, 4);
            var length = parseHeader(header);
            if (buffer.length >=  4 + length) {
                self._buffers = [buffer.slice(4 + length, buffer.length)];
                var payload = buffer.slice(4, 4 + length);
                var msgId = bsplit(payload, NULL_CHAR)[0];
                payload = payload.slice(msgId.length + NULL_CHAR.length, payload.length);
                self.emit(msgId + "-data", payload);
            }
        }
    });
};


util.inherits(BaseClient, EventEmitter);


var BaseAgent = function(client, msgId) {
    this._client = client;
    this._msgId = msgId;
};

BaseAgent.prototype.send = function(buf) {
    if (this._msgId > 0) {
        buf = Buffer.concat([new Buffer(this._msgId + ""), NULL_CHAR, buf]);
    }
    var header = makeHeader(buf);
    this._client._socket.write(header);
    this._client._socket.write(buf);
};


BaseAgent.prototype.recive = function(cb) {
    var self = this;
    var events = [this._msgId + "-data", this._msgId + "-error"];
    this._client.once(this._msgId + "-data", function(data) {
        self._client.removeAllListeners(events);
        cb(null, data);
    });
    this._client.once(this._msgId + "-error", function(err) {
        self._client.removeAllListeners(events);
        cb(err);
    });
};


BaseClient.prototype.close = function() {
    this._socket.end();
};


BaseClient.prototype.agent = function() {
    this._msgId += 1;
    if (this._msgId > this._maxMsgId) {
        this._msgId = 1;
    }
    return new BaseAgent(this, this._msgId);
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
    var agent = this._client.agent();
    agent.send(PING);
    agent.recive(cb);
};


PeriodicClient.prototype.submitJob = function(job, cb) {
    var agent = this._client.agent();
    agent.send(Buffer.concat([SUBMIT_JOB, NULL_CHAR,
                new Buffer(JSON.stringify(job))]));
    agent.recive(cb);
};


PeriodicClient.prototype.status = function(cb) {
    var agent = this._client.agent();
    agent.send(STATUS);
    agent.recive(function(err, payload) {
        if (err) return cb(err);
        var retval = {};
        var stats = payload.toString().trim().split('\n').forEach(function(stat) {
            stat = stat.split(",");
            retval[stat[0]] = {
                'func_name': stat[0],
                'worker_count': Number(stat[1]),
                'job_count': Number(stat[2]),
                'processing': Number(stat[3])
            };

        });
        cb(null, retval);
    });
};


PeriodicClient.prototype.dropFunc = function(func, cb) {
    var agent = this._client.agent();
    agent.send(Buffer.concat([DROP_FUNC, NULL_CHAR, new Buffer(func)]));
    agent.recive(cb);
};


PeriodicClient.prototype.close = function() {
    this._client.close();
};


var PeriodicWorker = exports.PeriodicWorker = function(options) {
    this._client = new BaseClient(options, TYPE_WORKER);
};


PeriodicWorker.prototype.ping = function(cb) {
    var agent = this._client.agent();
    agent.send(PING);
    agent.recive(cb);
};


PeriodicWorker.prototype.grabJob = function(cb) {
    var agent = this._client.agent();
    agent.send(GRAB_JOB);
    agent.recive(function(err, buf) {
        if (err) return cb(err);
        if (buf === NO_JOB || buf === WAIT_JOB) {
            return cb(null, null);
        }
        cb(null, new PeriodicJob(buf, agent));
    });
};


PeriodicWorker.prototype.addFunc = function(func, cb) {
    var agent = this._client.agent();
    agent.send(Buffer.concat([CAN_DO, NULL_CHAR, new Buffer(func)]));
    cb();
};


PeriodicWorker.prototype.removeFunc = function(func, cb) {
    var agent = this._client.agent();
    agent.send(Buffer.concat([CANT_DO, NULL_CHAR, new Buffer(func)]));
    cb();
};


PeriodicWorker.prototype.close = function() {
    this._client.close();
};


var PeriodicJob = function(buf, agent) {
    this._buffer = buf;
    this._agent = agent;
    var payload = bsplit(buf, NULL_CHAR);
    this.jobHandle = payload[0];
    this._payload = JSON.parse(payload[1].toString());
    this.funcName = this._payload.func;
    this.name = this._payload.name;
    this.schedAt = this._payload.sched_at;
    this.runAt = this._payload.run_at || this.schedAt;
    this.workload = this._payload.workload;
};


PeriodicJob.prototype.done = function(cb) {
    this._agent.send(Buffer.concat([JOB_DONE, NULL_CHAR, this.jobHandle]));
    cb();
};


PeriodicJob.prototype.fail = function(cb) {
    this._agent.send(Buffer.concat([JOB_FAIL, NULL_CHAR, this.jobHandle]));
    cb();
};


PeriodicJob.prototype.schedLater = function(delay, cb) {
    this._agent.send(Buffer.concat([SCHED_LATER, NULL_CHAR, this.jobHandle,
                NULL_CHAR, new Buffer("" + delay)]));
    cb();
};
