'use strict';

const getSchemaById = (registry, schemaId) => new Promise((resolve, reject) => {
  const {protocol, host, port, username, password, path} = registry;
  const requestOptions = {
    host,
    port,
    path: `${path}schemas/ids/${schemaId}`,
    auth: username && password ? `${username}:${password}` : null,
  };
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
        return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`));
      }

      const schema = JSON.parse(data).schema;

      resolve(schema);
    });
  }).on('error', (e) => {
    reject(e);
  });
  req.end();
});

const pushSchema = (registry, subject, schema) => new Promise((resolve, reject) => {
  const {protocol, host, port, username, password, path} = registry;
  const body = JSON.stringify({schema: schema});
  const requestOptions = {
    method: 'POST',
    host,
    port,
    path: `${path}subjects/${subject}/versions`,
    auth: username && password ? `${username}:${password}` : null,
  };

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
        return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`));
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

const getLatestVersionForSubject = (registry, subject) => new Promise((resolve, reject) => {
  const {protocol, host, port, username, password, path} = registry;
  const requestOptions = {
    host,
    port,
    path: `${path}subjects/${subject}/versions`,
    auth: username && password ? `${username}:${password}` : null,
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

      const versions = JSON.parse(data);

      const requestOptions2 = Object.assign(requestOptions, {path: `${path}subjects/${subject}/versions/${versions.pop()}`});

      protocol.get(requestOptions2, (res2) => {
        let data = '';
        res2.on('data', (d) => {
          data += d;
        });
        res2.on('error', (e) => {
          reject(e);
        });
        res2.on('end', () => {
          if (res2.statusCode !== 200) {
            const error = JSON.parse(data);
            return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`));
          }

          const responseBody = JSON.parse(data);

          resolve({schema: responseBody.schema, id: responseBody.id});
        });
      }).on('error', (e) => reject(e));
    });
  }).on('error', (e) => {
    reject(e);
  });
});

module.exports = {getSchemaById, pushSchema, getLatestVersionForSubject};
