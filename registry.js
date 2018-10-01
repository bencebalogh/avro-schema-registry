'use strict';

const url = require('url');
const http = require('http');
const https = require('https');

const avsc = require('avsc');

function fetchSchema(transport, protocol, host, port, schemaId) {
  return new Promise((resolve, reject) => {
    const requestOptions = {
      host,
      port,
      path: `/schemas/ids/${schemaId}`
    };
    transport.get(requestOptions, (res) => {
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
}

function pushSchema(transport, protocol, host, port, path, topic, schemaString, type = 'value') {
  return new Promise((resolve, reject) => {
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
      const req = transport.request(reqestOptions, (res) => {
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
        }
      );
      req.write(body);
      req.end();
    }
  )
    ;
}

class SchemaCache {
  constructor() {
    this.schemasById = new Map();
    this.schemasBySchema = new Map();
  }

  set(schemaId, schema) {
    this.schemasById.set(schemaId, schema);
    if (!(schema instanceof Promise)) {
      this.schemasBySchema.set(JSON.stringify(schema), schemaId);
    }
  }

  getById(schemaId) {
    return this.schemasById.get(schemaId);
  }

  getBySchema(schema) {
    return this.schemasBySchema.get(JSON.stringify(schema));
  }
}

function schemas(registryUrl) {
  const parsed = url.parse(registryUrl);
  const schemas = new SchemaCache();
  const registry = {
    protocol: parsed.protocol,
    host: parsed.hostname,
    port: parsed.port,
    path: parsed.path,
  };
  const protocol = registry.protocol === 'https:' ? https : http;

  const decode = (obj) => {
    let schemaId;

    return new Promise((resolve, reject) => {
      if (obj.readUInt8(0) !== 0) {
        return reject(new Error(`Message doesn't contain schema identifier byte.`));
      }
      schemaId = obj.readUInt32BE(1);
      let promise = schemas.getById(schemaId);

      if (promise) {
        return resolve(promise);
      }

      promise = fetchSchema(protocol, registry.protocol, registry.host, registry.port, schemaId);
      schemas.set(schemaId, promise);
      promise.then(result => {
        schemas.set(schemaId, result);
      }).catch(reject);
      return resolve(promise);
    })
      .then((schema) => {
        return avsc.parse(schema).fromBuffer(obj.slice(5));
      });
  };

  const encodeFunction = (type) => (topic, schema, msg) => (() => new Promise((resolve, reject) => {
    const schemaString = JSON.stringify(schema);
    let id = schemas.getBySchema(schema);
    if (id) {
      return resolve(id)
    }

    const promise = pushSchema(protocol, registry.protocol, registry.host, registry.port, parsed.path, topic, schemaString, type);
    promise.then(id => {
      schemas.set(id, schema);
      return schema;
    }).catch(reject);
    
    return resolve(promise);
  }))()
  .then((schemaId) => {
    const encodedMessage = avsc.parse(schema).toBuffer(msg);

    const message = Buffer.alloc(encodedMessage.length + 5);
    message.writeUInt8(0);
    message.writeUInt32BE(schemaId, 1);
    encodedMessage.copy(message, 5);
    return message;
  });

  const encodeKey = encodeFunction('key');
  const encodeMessage = encodeFunction('value');

  return {
    encodeKey,
    encodeMessage,
    decode,
    decodeMessage: decode,
  };
};

module.exports = schemas;
