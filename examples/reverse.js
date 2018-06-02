var periodic = require('../index');
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

var worker = new periodic.PeriodicWorker(options);

worker.addFunc('reverse', function(job) {
  job.data(Buffer.from(job.name).reverse())
});
worker.work();
