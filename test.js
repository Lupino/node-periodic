var periodic = require('./dist/index');
var RSATransport = require('./dist/transport').RSATransport;
var test = require('tape');
var async = require('async');
var fs = require('fs');

function resolveRSAMode(name) {
  name = (name || '').toUpperCase();
  if (name === 'PLAIN') return RSATransport.MODE_PLAIN;
  if (name === 'RSA') return RSATransport.MODE_RSA;
  if (name === 'AES') return RSATransport.MODE_AES;
  return null;
}

function makeTestContext() {
  var rsaModeName = process.env.PERIODIC_RSA_MODE;
  var rsaMode = resolveRSAMode(rsaModeName);

  var options = {
    port: 5000,
  };

  if (rsaMode !== null) {
    options.rsa = true;
    options.privateKey = fs.readFileSync(process.env.PERIODIC_CLIENT_PRIVATE_KEY || 'private_key.pem');
    options.peerPublicKey = fs.readFileSync(process.env.PERIODIC_SERVER_PUBLIC_KEY || 'server_public_key.pem');
    options.mode = rsaMode;
  }

  var prefix = rsaMode !== null ? '[rsa ' + String(rsaModeName || '').toLowerCase() + '] ' : '';

  return {
    rsaMode: rsaMode,
    options: options,
    name: function(name) {
      return prefix + name;
    },
  };
}

var ctx = makeTestContext();
var options = ctx.options;

function nowSec() {
  return Math.floor(new Date() / 1000);
}

function mkJob(func, name) {
  return {
    func: func,
    name: name,
    sched_at: nowSec(),
  };
}

test(ctx.name('ping for worker'), function(t) {
  var worker = new periodic.PeriodicWorker(options);
  worker.ping(function(err, ok) {
    t.error(err);
    t.equal(ok[0], periodic.PONG[0]);
    worker.close();
    t.end();
  });
});

test(ctx.name('ping for client'), function(t) {
  var client = new periodic.PeriodicClient(options);
  client.ping(function(err, ok) {
    t.error(err);
    t.equal(ok[0], periodic.PONG[0]);
    client.close();
    t.end();
  });
});

test(ctx.name('submitJob'), function(t) {
  var client = new periodic.PeriodicClient(options);
  client.submitJob(mkJob('test', 'haha'), function(err, ok) {
    t.error(err);
    t.equal(ok[0], periodic.SUCCESS[0]);
    client.close();
    t.end();
  });
});

test(ctx.name('runJob'), function(t) {
  var client = new periodic.PeriodicClient(options);
  client.runJob(mkJob('test', 'haha'), function(err) {
    t.ok(err);
    t.equal(err.message, 'no worker');
    client.close();
    t.end();
  });
});

test(ctx.name('status'), function(t) {
  var client = new periodic.PeriodicClient(options);
  client.status(function(err, ok) {
    t.error(err);
    t.pass(JSON.stringify(ok));
    client.close();
    t.end();
  });
});

test(ctx.name('dropFunc'), function(t) {
  var client = new periodic.PeriodicClient(options);
  client.dropFunc('test', function(err, ok) {
    t.error(err);
    t.equal(ok[0], periodic.SUCCESS[0]);
    client.close();
    t.end();
  });
});

test(ctx.name('worker'), function(t) {
  var worker = new periodic.PeriodicWorker(options);
  var client = new periodic.PeriodicClient(options);

  var func = 'test_worker';
  var job = mkJob(func, 'haha');

  async.waterfall(
    [
      function(next) {
        t.pass('start submitJob');
        client.submitJob(job, next);
      },
      function(ok, next) {
        t.error(null);
        t.equal(ok[0], periodic.SUCCESS[0]);
        t.pass('client submitJob');

        var count = 1;
        worker.addFunc(
          func,
          function(job) {
            t.equal(job.funcName, func);
            t.equal(job.name, 'haha');
            t.pass('schedAt: ' + job.schedAt);

            if (count > 1) {
              job.done(function(err, ok) {
                t.error(err);
                t.equal(ok[0], periodic.SUCCESS[0]);
                next();
              });
            } else {
              job.schedLater(1, function(err, ok) {
                t.error(err);
                t.equal(ok[0], periodic.SUCCESS[0]);
              });
            }
            count = count + 1;
          },
          function(err, ok) {
            t.error(err);
            t.equal(ok[0], periodic.SUCCESS[0]);
            worker.work();
          },
        );
      },
      function(next) {
        t.pass('Job Done');
        client.dropFunc(func, function(err, ok) {
          t.error(err);
          t.equal(ok[0], periodic.SUCCESS[0]);
          next();
        });
      },
    ],
    function(err) {
      if (err) {
        t.error(err);
      }
      client.close();
      // worker.close();
      t.end();
    },
  );
});

test(ctx.name('run-job'), function(t) {
  var worker = new periodic.PeriodicWorker(options);
  var client = new periodic.PeriodicClient(options);

  var func = 'test_run_job_worker';
  var func1 = 'test_run_job_worker_sched_later';
  var func2 = 'test_run_job_worker_fail';

  var job = mkJob(func, 'haha');

  worker.addFunc(
    func,
    function(job) {
      t.equal(job.funcName, func);
      t.equal(job.name, 'haha');
      t.pass('schedAt: ' + job.schedAt);
      job.done(job.name, function(err, ok) {
        t.error(err);
        t.equal(ok[0], periodic.SUCCESS[0]);
      });
    },
    function(err, ok) {
      t.error(err);
      t.equal(ok[0], periodic.SUCCESS[0]);
    },
  );

  worker.addFunc(
    func1,
    function(job) {
      t.equal(job.funcName, func1);
      t.equal(job.name, 'haha');
      t.pass('schedAt: ' + job.schedAt);
      job.schedLater(1, function(err, ok) {
        t.error(err);
        t.equal(ok[0], periodic.SUCCESS[0]);
      });
    },
    function(err, ok) {
      t.error(err);
      t.equal(ok[0], periodic.SUCCESS[0]);
    },
  );

  worker.addFunc(
    func2,
    function(job) {
      t.equal(job.funcName, func2);
      t.equal(job.name, 'haha');
      t.pass('schedAt: ' + job.schedAt);
      job.fail(function(err, ok) {
        t.error(err);
        t.equal(ok[0], periodic.SUCCESS[0]);
      });
    },
    function(err, ok) {
      t.error(err);
      t.equal(ok[0], periodic.SUCCESS[0]);
    },
  );

  worker.work();

  async.waterfall(
    [
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
          t.ok(err);
          t.equal(err.message, 'failed');
          next();
        });
      },
      function(next) {
        job.func = func2;
        client.runJob(job, function(err) {
          t.ok(err);
          t.equal(err.message, 'failed');
          next();
        });
      },
      function(next) {
        t.pass('Job Done');
        client.dropFunc(func, function(err, ok) {
          t.error(err);
          t.equal(ok[0], periodic.SUCCESS[0]);
        });
        client.dropFunc(func1, function(err, ok) {
          t.error(err);
          t.equal(ok[0], periodic.SUCCESS[0]);
        });
        next();
      },
    ],
    function(err) {
      if (err) {
        t.error(err);
      }
      client.close();
      // worker.close();
      t.end();
      process.exit();
    },
  );
});
