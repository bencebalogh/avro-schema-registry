'use strict';

module.exports = (protocol, host, port, path, topic, schemaString, type = 'value') => new Promise((resolve, reject) => {
  const body = JSON.stringify({schema: schemaString});
  const reqestOptions = {
    host: `${host}`,
    port: port,
    method: 'POST',
    path: `${path}subjects/${topic}-${type}/versions`,
    headers: {
      'Content-Type': 'application/vnd.schemaregistry.v1+json',
      'Content-Length': Buffer.byteLength(body),
    }
  };

  const req = protocol.request(reqestOptions, (res) => {
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
        return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`));
      }

      const resp = JSON.parse(data);

      resolve(resp.id);
    });
  });
  req.write(body);
  req.end();
});