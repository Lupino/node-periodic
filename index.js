"use strict";

var net = require('net')
  , EventEmitter = require('events').EventEmitter
  , util = require('util')
  , bsplit = require('buffer-split')
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
    var agent = new BaseAgent(this, null);
    agent.send(clientType);
    this._buffers = [];
    var self = this;
    socket.on("data", function(chunk) {
        self._buffers.push(chunk);
        var buffer = Buffer.concat(self._buffers);
        if (buffer.length >= 8) {
            var magic = buffer.slice(0, 4);
            if (!bufferEqual(magic, MAGIC_RESPONSE)) {
                throw "Magic not match.";
            }
            var header = buffer.slice(4, 8);
            var length = parseHeader(header);
            if (buffer.length >=  8 + length) {
                self._buffers = [buffer.slice(8 + length, buffer.length)];
                var payload = buffer.slice(8, 8 + length);
                var uuid = bsplit(payload, NULL_CHAR)[0];
                payload = payload.slice(uuid.length + NULL_CHAR.length, payload.length);
                self.emit(uuid + "-data", payload);
            }
        }
    });
};


util.inherits(BaseClient, EventEmitter);


var BaseAgent = function(client, uuid) {
    this._client = client;
    this._uuid = uuid;
};

BaseAgent.prototype.send = function(buf) {
    if (this._uuid) {
        buf = Buffer.concat([new Buffer(this._uuid + ""), NULL_CHAR, buf]);
    }
    var header = makeHeader(buf);
    this._client._socket.write(MAGIC_REQUEST);
    this._client._socket.write(header);
    this._client._socket.write(buf);
};


BaseAgent.prototype.recive = function(cb) {
    var self = this;
    var events = [this._uuid + "-data", this._uuid + "-error"];
    this._client.once(this._uuid + "-data", function(data) {
        self._client.removeAllListeners(events);
        cb(null, data);
    });
    this._client.once(this._uuid + "-error", function(err) {
        self._client.removeAllListeners(events);
        cb(err);
    });
};


BaseClient.prototype.close = function() {
    this._socket.end();
};


BaseClient.prototype.agent = function() {
    return new BaseAgent(this, uuid.v1());
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


PeriodicClient.prototype.removeJob = function(job, cb) {
    var agent = this._client.agent();
    agent.send(Buffer.concat([REMOVE_JOB, NULL_CHAR, new Buffer(JSON.stringify(job))]));
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
        var cmd = buf.slice(0,1);
        if (buf[0] === NO_JOB[0] || cmd[0] !== JOB_ASSIGN[0]) {
            return cb(null, null);
        }
        cb(null, new PeriodicJob(buf.slice(3, buf.length), agent));
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
    this._agent.send(Buffer.concat([WORK_DONE, NULL_CHAR, this.jobHandle]));
    cb();
};


PeriodicJob.prototype.fail = function(cb) {
    this._agent.send(Buffer.concat([WORK_FAIL, NULL_CHAR, this.jobHandle]));
    cb();
};


PeriodicJob.prototype.schedLater = function(delay, cb) {
    this._agent.send(Buffer.concat([SCHED_LATER, NULL_CHAR, this.jobHandle,
                NULL_CHAR, new Buffer("" + delay)]));
    cb();
};
