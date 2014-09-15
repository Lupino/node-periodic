"use strict"

var net = require('net')
  , EventEmitter = require('events').EventEmitter
  , util = require('util')
  ;

var NOOP        = exports.NOOP        = 0;
// for job
var GRAB_JOB    = exports.GRAB_JOB    = 1;
var SCHED_LATER = exports.SCHED_LATER = 2;
var JOB_DONE    = exports.JOB_DONE    = 3;
var JOB_FAIL    = exports.JOB_FAIL    = 4;
var WAIT_JOB    = exports.WAIT_JOB    = 5;
var NO_JOB      = exports.NO_JOB      = 6;
// for func
var CAN_DO      = exports.CAN_DO      = 7;
var CANT_DO     = exports.CANT_DO     = 8;
// for test
var PING        = exports.PING        = 9;
var PONG        = exports.PONG        = 10;
// other
var SLEEP       = exports.SLEEP       = 11;
var UNKNOWN     = exports.UNKNOWN     = 12;
// client command
var SUBMIT_JOB  = exports.SUBMIT_JOB  = 13;
var STATUS      = exports.STATUS      = 14;
var DROP_FUNC   = exports.DROP_FUNC   = 15;

var NULL_CHAR = 1;


// client type

var TYPE_CLIENT = 1;
var TYPE_WORKER = 2;


var BaseClient = function(options, clientType) {
    EventEmitter.call(this);
    this._clientType = clientType;
    var socket = net.connect(options) ;
    this._socket = socket;
    var buf = new Buffer(1);
    buf[0] = clientType;
    this.send(buf);
    this._buffers = [];
    var self = this;
    socket.on("data", function(chunk) {
        self._buffers.push(chunk);
        var buffer = Buffer.concat(self._buffers);
        if (buffer.length >= 4) {
            var header = buffer.slice(0, 4);
            var length = parseHeader(header);
            if (buffer.length >=  4 + length) {
                self.emit("data", buffer.slice(4, 4 + length));
                self._buffers = [buffer.slice(4 + length, buffer.length)];
            }
        }
    });
};


util.inherits(BaseClient, EventEmitter);


BaseClient.prototype.send = function(buf) {
    var header = makeHeader(buf);
    this._socket.write(header);
    this._socket.write(buf);
};


BaseClient.prototype.recive = function(cb) {
    this.once("data", function(data) {
        cb(null, data);
    });
    this.once("error", function(err) {
        cb(err);
    });
};


BaseClient.prototype.close = function() {
    this._socket.end();
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
    this._agent = new BaseClient(options, TYPE_CLIENT);
};


PeriodicClient.prototype.ping = function(cb) {
    var buf = new Buffer(1);
    buf[0] = PING;
    this._agent.send(buf);
    this._agent.recive(cb);
};


PeriodicClient.prototype.submitJob = function(job, cb) {
    var buf = new Buffer(2);
    buf[0] = SUBMIT_JOB;
    buf[1] = NULL_CHAR;
    buf.write(JSON.stringify(job));
    this._agent.send(buf);
    this._agent.recive(cb);
};


PeriodicClient.prototype.status = function(cb) {
    var buf = new Buffer(1);
    buf[0] = STATUS;
    this._agent.send(buf);
    this._agent.recive(function(err, rsp) {
        if (err) return cb(err);
        cb(null, JSON.parse(rsp));
    });
};


PeriodicClient.prototype.dropFunc = function(func, cb) {
    var buf = new Buffer(2);
    buf[0] = DROP_FUNC;
    buf[1] = NULL_CHAR;
    buf.write(func);
    this._agent.send(buf);
    this._agent.recive(cb);
};


PeriodicClient.prototype.close = function() {
    this._agent.close();
};


var PeriodicWorker = exports.PeriodicWorker = function(options) {
    this._agent = new BaseClient(options, TYPE_WORKER);
};


PeriodicWorker.prototype.ping = function(cb) {
    var buf = new Buffer(1);
    buf[0] = PING;
    this._agent.send(buf);
    this._agent.recive(cb);
};


PeriodicWorker.prototype.grabJob = function(cb) {
    var buf = new Buffer(1);
    buf[0] = GRAB_JOB;
    this._agent.send(buf);
    this._agent.recive(function(err, buf) {
        if (err) return cb(err);
        if (buf[0] === NO_JOB || buf[0] === WAIT_JOB) {
            return cb(null, null);
        }
        cb(null, new PeriodicJob(buf, this._agent));
    });
};


PeriodicWorker.prototype.addFunc = function(func, cb) {
    var buf = new Buffer(2);
    buf[0] = CAN_DO;
    buf[1] = NULL_CHAR;
    buf.write(func);
    this._agent.send(buf);
    cb();
};


PeriodicWorker.prototype.removeFunc = function(func, cb) {
    var buf = new Buffer(2);
    buf[0] = CANT_DO;
    buf[1] = NULL_CHAR;
    buf.write(func);
    this._agent.send(buf);
    cb();
};


PeriodicWorker.prototype.close = function() {
    this._agent.close();
};


var PeriodicJob = function(buf, agent) {
    this._buffer = buf;
    this._agent = agent;
    var len = this._buffer.length;
    var splitIdx = 0;
    for (var idx = len - 1; idx >= 0; idx --) {
        if (buf[idx] === NULL_CHAR) {
            splitIdx = idx;
            break;
        }
    }
    this.jobHandle = this._buffer.slice(splitIdx + 1, len);
    this._payload = JSON.parse(this._buffer.slice(0, splitIdx));
    this.funcName = this._payload.func;
    this.name = this._payload.name;
    this.schedAt = this._payload.sched_at;
    this.runAt = this._payload.run_at || this.schedAt;
    this.workload = this._payload.workload;
};


PeriodicJob.prototype.done = function(cb) {
    var buf = new Buffer(2);
    buf[0] = JOB_DONE;
    buf[1] = NULL_CHAR;
    this._agent.send(Buffer.concat([buf, this.jobHandle]));
    cb();
};


PeriodicJob.prototype.fail = function(cb) {
    var buf = new Buffer(2);
    buf[0] = JOB_FAIL;
    buf[1] = NULL_CHAR;
    this._agent.send(Buffer.concat([buf, this.jobHandle]));
    cb();
};


PeriodicJob.prototype.schedLater = function(delay, cb) {
    var buf = new Buffer(2);
    buf[0] = SCHED_LATER;
    buf[1] = NULL_CHAR;
    var nulbuf = new Buffer(1);
    nulbuf[0] = NULL_CHAR;
    this._agent.send(Buffer.concat([buf, this.jobHandle, nulbuf, "" + delay]));
    cb();
};
