'use strict';
const util = require('util');

module.exports = (registry, topic, schemaString, type = 'value') => new Promise((resolve, reject) => {
  const { protocol, host, port, path, username, password } = registry;
  // console.log(`Step 11, reqId: ${requestId} in pushSchema with topic: ${topic}, schemaString: ${schemaString}, type: ${type}`);
  const body = JSON.stringify({schema: schemaString});
  // console.log(`Step 12, reqId: ${requestId} in pushSchema with topic: ${topic}, body: ${body}, type: ${type}`);
  const requestOptions = {
    host: host,
    port: port,
    method: 'POST',
    path: `${path}subjects/${topic}-${type}/versions`,
    headers: {
      'Content-Type': 'application/vnd.schemaregistry.v1+json',
      'Content-Length': Buffer.byteLength(body),
    },
    auth: username && password ? `${username}:${password}` : null,
  };

  // console.log(`Step 13, reqId: ${requestId} in pushSchema about to send with options: ${util.inspect(requestOptions)}`);

  const req = protocol.request(requestOptions, (res) => {
    let data = '';
    res.on('data', (d) => {
      data += d;
    });
    res.on('error', (e) => {
      reject(e);
    });
    res.on('end', () => {
      if (res.statusCode !== 200) {
        const error = JSON.parse(data);
        return reject(new Error(`Schema registry error: code: ${error.error_code} - ${error.message}`));
      }

      const resp = JSON.parse(data);

      resolve(resp.id);
    });
  }).on('error', (e) => {
    reject(e);
  });
  req.write(body);
  req.end();
});
