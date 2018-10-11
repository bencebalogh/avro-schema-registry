'use strict';

module.exports = (protocol, host, port, schemaId) => new Promise((resolve, reject) => {
  const requestOptions = {
    host,
    port,
    path: `/schemas/ids/${schemaId}`
  };
  protocol.get(requestOptions, (res) => {
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

      const schema = JSON.parse(data).schema;
      resolve(schema);
    });
  });
});
