The periodic task system client for nodejs.

[![NPM](https://nodei.co/npm/node-periodic.png)](https://nodei.co/npm/node-periodic/)

Install
-------

    npm install node-periodic


Test
----

    npm install
    npm install tape
    npm test


Authenticated clients
---------------------

When `periodicd` runs with an auth file, pass the matching client identity:

```js
var periodic = require('node-periodic');

var client = new periodic.PeriodicClient({
  port: 5000,
  clientName: 'client-a',
  clientToken: 'token-a',
});

var worker = new periodic.PeriodicWorker({
  port: 5000,
  clientName: 'client-a',
  clientToken: 'token-a',
});

worker.addFunc('func1', handle);
```

Example server auth file line:

```text
client client-a token-a func1,func2
worker worker-a token-worker-a func1,func2
```
