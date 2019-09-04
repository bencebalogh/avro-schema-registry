'use strict';

const avsc = require('avsc');

const request = (registry, path, method = 'GET', body = null) => {
  return new Promise((resolve, reject) => {
    const { protocol, host, port, path: basePath, auth } = registry;

    const requestOptions = {
      host,
      port,
      method,
      path: `${basePath}${path}`,
      headers: {
        'Content-Type': 'application/vnd.schemaregistry.v1+json',
      },
      auth,
    };

    if (method !== 'GET' && body) {
      requestOptions.headers['Content-Length'] = Buffer.byteLength(body);
    }

    const req = protocol
      .request(requestOptions, res => {
        let data = '';
        res.on('data', d => {
          data += d;
        });
        res.on('error', e => {
          reject(e);
        });
        res.on('end', () => {
          if (res.statusCode !== 200) {
            const error = JSON.parse(data);
            return reject(
              new Error(
                `Schema registry error: ${error.error_code} - ${error.message}`,
              ),
            );
          }

          const resp = JSON.parse(data);

          resolve(resp);
        });
      })
      .on('error', e => {
        reject(e);
      });

    if (method !== 'GET' && body) {
      req.send(JSON.stringify(body));
    }

    req.end();
  });
};

module.exports = (registry, topic, parseOptions = null, type = 'value') => {
  function resolveVersion(versions) {
    return request(
      registry,
      `subjects/${topic}-${type}/versions/${versions.pop()}`,
    );
  }

  return request(registry, `subjects/${topic}-${type}/versions`)
    .then(resolveVersion)
    .then(({ id, schema }) => {
      return {
        id,
        parsedSchema: avsc.parse(schema, parseOptions),
      };
    });
};
