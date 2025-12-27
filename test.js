var periodic = require('./index');
var RSATransport = require('./transport').RSATransport;
var test = require('tape');
var async = require('async');
var fs = require('fs');

var options = {
  port: 5000,
  rsa: true,
  // // Necessary only if using the client certificate authentication
  // privateKey : fs.readFileSync('private_key.pem'),
  // peerPublicKey: fs.readFileSync('server_public_key.pem'),
  // mode: RSATransport.MODE_AES,
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

test('runJob', function(t) {
  var client = new periodic.PeriodicClient(options);
  var job = {
    func: 'test',
    name: 'haha',
    sched_at: Math.floor(new Date() / 1000)
  };
  client.runJob(job, function(err, data) {
    t.equal(err.message, 'no worker');
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
      t.equal(ok[0], periodic.SUCCESS[0]);
      t.pass('client submitJob');
      var count = 1;
      worker.addFunc(func, function(job) {
        t.equal(job.funcName, func);
        t.equal(job.name, 'haha');
        t.pass('schedAt: ' + job.schedAt);
        if (count > 1) {
          job.done(function(err, ok) {
            t.equal(ok[0], periodic.SUCCESS[0]);
            console.log(err, ok);
            next();
          });
        } else {
          job.schedLater(1, function(err, ok) {
            t.equal(ok[0], periodic.SUCCESS[0]);
            console.log(err, ok);
          });
        }
        count = count + 1;
      }, function(err, ok) {
        t.equal(ok[0], periodic.SUCCESS[0]);
        worker.work();
      });
    },
    function(next) {
      t.pass('Job Done');
      client.dropFunc(func, function(err, ok) {
        t.equal(ok[0], periodic.SUCCESS[0]);
        next();
      });
    }
  ], function(err) {
    if (err) {
      t.error(err);
    }
    client.close();
    // worker.close();
    t.end();
    // process.exit();
  });
});

test('run-job', function(t) {
  var worker = new periodic.PeriodicWorker(options);
  var client = new periodic.PeriodicClient(options);
  var func = 'test_run_job_worker';
  var func1 = 'test_run_job_worker_sched_later';
  var func2 = 'test_run_job_worker_fail';
  var job = {
    func: func,
    name: 'haha',
    sched_at: Math.floor(Number(new Date()) / 1000)
  };
  worker.addFunc(func, function(job) {
    t.equal(job.funcName, func);
    t.equal(job.name, 'haha');
    t.pass('schedAt: ' + job.schedAt);
    job.done(job.name, function(err, ok) {
      t.equal(ok[0], periodic.SUCCESS[0]);
    });
  }, function(err, ok) {
    t.equal(ok[0], periodic.SUCCESS[0]);
  });
  worker.addFunc(func1, function(job) {
    t.equal(job.funcName, func1);
    t.equal(job.name, 'haha');
    t.pass('schedAt: ' + job.schedAt);
    job.schedLater(1, function(err, ok) {
      t.equal(ok[0], periodic.SUCCESS[0]);
    })
  }, function(err, ok) {
    t.equal(ok[0], periodic.SUCCESS[0]);
  });
  worker.addFunc(func2, function(job) {
    t.equal(job.funcName, func2);
    t.equal(job.name, 'haha');
    t.pass('schedAt: ' + job.schedAt);
    job.fail(function(err, ok) {
      t.equal(ok[0], periodic.SUCCESS[0]);
    })
  }, function(err, ok) {
    t.equal(ok[0], periodic.SUCCESS[0]);
  });
  worker.work();
  async.waterfall([
    function(next) {
      setTimeout(next, 1000);
    },
    function(next) {
      t.pass('start runJob');
      client.runJob(job, next);
    },
    function(payload, next) {
      t.equal(job.name, payload.toString());
      job.func = func1;
      client.runJob(job, function(err) {
        t.equal(err.message, 'failed');
        next()
      });
    },
    function(next) {
      job.func = func2;
      client.runJob(job, function(err) {
        t.equal(err.message, 'failed');
        next()
      });
    },
    function(next) {
      t.pass('Job Done');
      client.dropFunc(func, function(err, ok) {
        t.equal(ok[0], periodic.SUCCESS[0]);
      });
      client.dropFunc(func1, function(err, ok) {
        t.equal(ok[0], periodic.SUCCESS[0]);
      });
      next();
    }
  ], function(err) {
    if (err) {
      t.error(err);
    }
    client.close();
    // worker.close();
    t.end();
    process.exit();
  });
});
