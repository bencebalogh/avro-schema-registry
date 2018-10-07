'use strict';

const avsc = require('avsc');

const fetchSchema = require('./fetch-schema');
const pushSchema = require('./push-schema');

const byId = (registry) => (schemaId, msg) => (() => new Promise((resolve, reject) => {
  let promise = registry.cache.getById(schemaId);

  if (promise) {
    return resolve(promise);
  }

  promise = fetchSchema(registry.protocol, registry.host, registry.port, schemaId);
  
  registry.cache.set(schemaId, promise);
  
  promise
    .then((result) => registry.cache.set(schemaId, result))
    .catch(reject);

  return resolve(promise);
}))()
.then((schema) => {
  const encodedMessage = avsc.parse(schema).toBuffer(msg);

  const message = Buffer.alloc(encodedMessage.length + 5);
  message.writeUInt8(0);
  message.writeUInt32BE(schemaId, 1);
  encodedMessage.copy(message, 5);
  return message;
});

const bySchema = (type, registry) => (topic, schema, msg) => (() => {
  const schemaString = JSON.stringify(schema);
  let id = registry.cache.getBySchema(schema);
  if (id) {
    return Promise.resolve(id)
  }

  return pushSchema(registry.protocol, registry.host, registry.port, registry.path, topic, schemaString, type)
    .then(id => registry.cache.set(id, schema));
})()
.then((schemaId) => {
  const encodedMessage = avsc.parse(schema).toBuffer(msg);

  const message = Buffer.alloc(encodedMessage.length + 5);
  message.writeUInt8(0);
  message.writeUInt32BE(schemaId, 1);
  encodedMessage.copy(message, 5);
  return message;
});

module.exports = {
  bySchema,
  byId,
}
