var EventEmitter = require('events').EventEmitter
  , util = require('util')
  , net = require('net')
  , tls = require('tls')
  ;

var Transport = function(options) {
  EventEmitter.call(this);
  this._socket = net.connect(options) ;
  this._socket.on('data', this.onData.bind(this));
  this._socket.on('close', this.onClose.bind(this));
  this._socket.on('connect', this.onConnect.bind(this));
  this._socket.on('end', this.onEnd.bind(this));
  this._socket.on('error', this.onError.bind(this));
  this._socket.on('timeout', this.onTimeout.bind(this));
};

util.inherits(Transport, EventEmitter);

Transport.prototype.onData = function(chunk) {
  this.emit('data', chunk);
};

Transport.prototype.onClose = function(had_error) {
  this.emit('close', had_error);
};

Transport.prototype.onConnect = function() {
  this.emit('connect');
};

Transport.prototype.onEnd = function() {
  this.emit('end');
};

Transport.prototype.onError = function(err) {
  this.emit('error', err);
};

Transport.prototype.onTimeout = function() {
  this.emit('timeout');
};

Transport.prototype.write = function(data, encoding, callback) {
  this._socket.write(data, encoding, callback);
};

Transport.prototype.end = function(data, encoding) {
  this._socket.end(data, encoding);
};

exports.Transport = Transport;

var TLSTransport = function(options) {
  EventEmitter.call(this);
  this._socket = tls.connect(options) ;
  this._socket.on('data', this.onData.bind(this));
  this._socket.on('close', this.onClose.bind(this));
  this._socket.on('connect', this.onConnect.bind(this));
  this._socket.on('end', this.onEnd.bind(this));
  this._socket.on('error', this.onError.bind(this));
  this._socket.on('timeout', this.onTimeout.bind(this));
};

util.inherits(TLSTransport, EventEmitter);

Transport.prototype.onData = function(chunk) {
  this.emit('data', chunk);
};

Transport.prototype.onClose = function(had_error) {
  this.emit('close', had_error);
};

Transport.prototype.onConnect = function() {
  this.emit('connect');
};

Transport.prototype.onEnd = function() {
  this.emit('end');
};

Transport.prototype.onError = function(err) {
  this.emit('error', err);
};

Transport.prototype.onTimeout = function() {
  this.emit('timeout');
};

Transport.prototype.write = function(data, encoding, callback) {
  this._socket.write(data, encoding, callback);
};

Transport.prototype.end = function(data, encoding) {
  this._socket.end(data, encoding);
};

exports.TLSTransport = TLSTransport;
