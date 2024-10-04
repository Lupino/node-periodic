var periodic = require('../index');
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

worker.addFunc('reverse', function(job, done) {
  done(null, Buffer.from(job.name).reverse());
}, function(err, ok) {
  worker.work();
});
