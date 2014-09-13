var periodic = require("./index");
var test = require('tape');


test('ping for worker', function(t) {
    var worker = new periodic.PeriodicWorker({port: 5000});
    worker.ping(function(err, ok) {
        t.equal(ok[0], periodic.PONG);
        worker.close();
        t.end();
    });
});


test('ping for client', function(t) {
    var client = new periodic.PeriodicClient({port: 5000});
    client.ping(function(err, ok) {
        t.equal(ok[0], periodic.PONG);
        client.close();
        t.end();
    });
});
