'use strict';

const url = require('url');
const http = require('http');
const https = require('http');

const avsc = require('avsc');

function schemas(registryUrl) {
  const parsed = url.parse(registryUrl);
  const schemas = {};
  const registry = {
    protocol: parsed.protocol,
    host: parsed.hostname,
    port: parsed.port,
    path: parsed.path,
  };
  const protocol = registry.protocol === 'https:' ? https : http;
  
  const decodeMessage = (msg) => new Promise((resolve, reject) => {
      if (msg.readUInt8(0) !== 0) {
        return reject(new Error(`Message doesn't contain schema identifier byte.`));
      }

      const schemaId = msg.readUInt32BE(1);
      for (let [id, schema] of Object.entries(schemas)) {
        if (parseInt(id, 10) === schemaId) {
          return resolve(schema);
        }
      }

      protocol.get(`${parsed.protocol}//${parsed.host}/schemas/ids/${schemaId}`, (res) => {
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
          schemas[schemaId] = schema;
          resolve(schema);
        });

      });
    })
    .then((schema) => {
      return avsc.parse(schema).fromBuffer(msg.slice(5));
    });
    
  const encodeMessage = (topic, schema, msg) => (() => new Promise((resolve, reject) => {
      const schemaString = JSON.stringify(schema);
      for (let [id, cSchema] of Object.entries(schemas)) {
        if (schema === cSchema) {
          return resolve(id);
        }
      }

      const req = protocol.request({
        host: `${registry.host}`,
        port: registry.port,
        method: 'POST',
        path: `${parsed.path}subjects/${topic}-value/versions`,
        headers: {
            'Content-Type': 'application/vnd.schemaregistry.v1+json',
            'Content-Length': Buffer.byteLength(JSON.stringify({schema: schemaString})),
        },
      }, (res) => {
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

          schemas[resp.id] = schema;

          resolve(resp.id);
        });
      });
      req.write(JSON.stringify({schema: schemaString}));
      req.end();
    }))()
    .then((schemaId) => {
      const encodedMessage = avsc.parse(schema).toBuffer(msg);

      const message = new Buffer(encodedMessage.length + 5);
      message.writeUInt8(0);
      message.writeUInt32BE(schemaId, 1);
      encodedMessage.copy(message, 5);
      return message;
    });
    
  return {encodeMessage, decodeMessage};
};

module.exports = schemas;
