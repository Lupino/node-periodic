var periodic = require('../index');
// var fs = require('fs');

var options = {
  port: 5000,
  // rsa: true,
  // // Necessary only if using the client certificate authentication
  // privateKey : fs.readFileSync('private_key.pem'),
  // peerPublicKey: fs.readFileSync('server_public_key.pem'),
  // mode: RSATransport.MODE_PLAIN,
  // // mode: RSATransport.MODE_RSA,
  // // mode: RSATransport.MODE_AES,
};

var worker = new periodic.PeriodicWorker(options);

worker.addFunc('reverse', function(job, done) {
  done(null, Buffer.from(job.name).reverse());
}, function(err, ok) {
  worker.work();
});
