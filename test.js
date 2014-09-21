var periodic = require("./index");
var test = require('tape');
var async = require("async");


test('ping for worker', function(t) {
    var worker = new periodic.PeriodicWorker({port: 5000});
    worker.ping(function(err, ok) {
        t.equal(ok[0], periodic.PONG[0]);
        worker.close();
        t.end();
    });
});


test('ping for client', function(t) {
    var client = new periodic.PeriodicClient({port: 5000});
    client.ping(function(err, ok) {
        t.equal(ok[0], periodic.PONG[0]);
        client.close();
        t.end();
    });
});


test('submitJob', function(t) {
    var client = new periodic.PeriodicClient({port: 5000});
    var job = {
        func: "test",
        name: "haha",
        sched_at: Number(new Date())
    };
    client.submitJob(job, function(err, ok) {
        t.equal(ok[0], periodic.SUCCESS[0]);
        client.close();
        t.end();
    });
});


test('status', function(t) {
    var client = new periodic.PeriodicClient({port: 5000});
    client.status(function(err, ok) {
        t.pass(JSON.stringify(ok));
        client.close();
        t.end();
    });
});


test('dropFunc', function(t) {
    var client = new periodic.PeriodicClient({port: 5000});
    client.dropFunc("test", function(err, ok) {
        t.equal(ok[0], periodic.SUCCESS[0]);
        client.close();
        t.end();
    });
});


test('worker', function(t) {
    var worker = new periodic.PeriodicWorker({port: 5000});
    var client = new periodic.PeriodicClient({port: 5000});
    var func = "test_worker";
    var job = {
        func: func,
        name: "haha",
        sched_at: Math.floor(Number(new Date()) / 1000)
    };
    async.waterfall([
        function(next) {
            t.pass("start submitJob");
            client.submitJob(job, next);
        },
        function(ok, next) {
            t.pass("client submitJob");
            worker.addFunc(func, function() {
                setTimeout(function() {
                    // body...
                    next();
                }, 1000);
            });
        },
        function(next) {
            t.pass("worker addFunc");
            worker.grabJob(next);
        },
        function(job, next) {
            t.pass("worker grabJob");
            t.equal(job.funcName, func);
            t.equal(job.name, "haha");
            t.pass("schedAt: " + job.schedAt);
            job.schedLater(5, next);
        },
        function(next) {
            t.pass("Job schedLater");
            worker.grabJob(next);
        },
        function(job, next) {
            t.pass("worker grabJob");
            t.equal(job.funcName, func);
            t.equal(job.name, "haha");
            t.pass("schedAt: " + job.schedAt);
            job.done(next);
        },
        function(next) {
            t.pass("Job Done");
            client.dropFunc(func, next);
        }
    ], function(err, result) {
        client.close();
        worker.close();
        t.end();
    });
});
