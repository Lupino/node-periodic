var periodic = require('./index');
var test = require('tape');
var async = require('async');
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
      worker.work();
    },
    function(next) {
      t.pass('Job Done');
      client.dropFunc(func);
      next();
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
    job.done(job.name);
  });
  worker.addFunc(func1, function(job) {
    t.equal(job.funcName, func1);
    t.equal(job.name, 'haha');
    t.pass('schedAt: ' + job.schedAt);
    job.schedLater(1)
  });
  worker.addFunc(func2, function(job) {
    t.equal(job.funcName, func2);
    t.equal(job.name, 'haha');
    t.pass('schedAt: ' + job.schedAt);
    job.fail()
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
      client.dropFunc(func);
      client.dropFunc(func1);
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
