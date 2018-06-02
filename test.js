var periodic = require('./index');
var test = require('tape');
var async = require('async');
var transport = require('./transport');
// var fs = require('fs');

var options = {
  port: 5000,
  // tls: true,
  // // Necessary only if using the client certificate authentication
  // key: fs.readFileSync('client-key.pem'),
  // cert: fs.readFileSync('client.pem'),

  // // Necessary only if the server uses the self-signed certificate
  // ca: [ fs.readFileSync('ca.pem') ]
};

test('ping for worker', function(t) {
  var worker = new periodic.PeriodicWorker(options);
  worker.ping(function(err, ok) {
    t.equal(ok[0], periodic.PONG[0]);
    worker.close();
    t.end();
  });
});


test('ping for client', function(t) {
  var client = new periodic.PeriodicClient(options);
  client.ping(function(err, ok) {
    t.equal(ok[0], periodic.PONG[0]);
    client.close();
    t.end();
  });
});


test('submitJob', function(t) {
  var client = new periodic.PeriodicClient(options);
  var job = {
    func: 'test',
    name: 'haha',
    sched_at: Math.floor(new Date() / 1000)
  };
  client.submitJob(job, function(err, ok) {
    t.equal(ok[0], periodic.SUCCESS[0]);
    client.close();
    t.end();
  });
});


test('status', function(t) {
  var client = new periodic.PeriodicClient(options);
  client.status(function(err, ok) {
    t.pass(JSON.stringify(ok));
    client.close();
    t.end();
  });
});


test('dropFunc', function(t) {
  var client = new periodic.PeriodicClient(options);
  client.dropFunc('test', function(err, ok) {
    t.equal(ok[0], periodic.SUCCESS[0]);
    client.close();
    t.end();
  });
});


test('worker', function(t) {
  var worker = new periodic.PeriodicWorker(options);
  var client = new periodic.PeriodicClient(options);
  var func = 'test_worker';
  var job = {
    func: func,
    name: 'haha',
    sched_at: Math.floor(Number(new Date()) / 1000)
  };
  async.waterfall([
    function(next) {
      t.pass('start submitJob');
      client.submitJob(job, next);
    },
    function(ok, next) {
      t.pass('client submitJob');
      var count = 1;
      worker.addFunc(func, function(job) {
        t.equal(job.funcName, func);
        t.equal(job.name, 'haha');
        t.pass('schedAt: ' + job.schedAt);
        if (count > 1) {
          job.done();
          next();
        } else {
          job.schedLater(5);
        }
        count = count + 1;
      });
      worker.work()
    },
    function(next) {
      t.pass('Job Done');
      client.dropFunc(func);
      next();
    }
  ], function() {
    client.close();
    // worker.close();
    t.end();
    process.exit();
  });
});
