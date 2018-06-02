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

var client = new periodic.PeriodicClient(options);

var job = {
  func: 'reverse',
  name: 'Hello World!',
};

client.runJob(job, function(err, ok) {
  console.log(err, ok.toString());
  client.close();
  process.exit();
});
