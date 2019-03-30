'use strict';

const avsc = require('avsc');

const fetchSchema = require('./fetch-schema');
const pushSchema = require('./push-schema');

const byId = (registry) => (schemaId, msg, parseOptions = null) => (() => new Promise((resolve, reject) => {
  let promise = registry.cache.getById(schemaId);

  if (promise) {
    return resolve(promise);
  }

  promise = fetchSchema(registry, schemaId, parseOptions);
  
  registry.cache.set(schemaId, promise);
  
  promise
    .then((result) => registry.cache.set(schemaId, result))
    .catch(reject);

  return resolve(promise);
}))()
.then((schema) => {
  const encodedMessage = schema.toBuffer(msg);

  const message = Buffer.alloc(encodedMessage.length + 5);
  message.writeUInt8(0);
  message.writeUInt32BE(schemaId, 1);
  encodedMessage.copy(message, 5);
  return message;
});

const bySchema = (type, registry) => (topic, schema, msg, parseOptions = null) => (() => {
  const schemaString = JSON.stringify(schema);
  const parsedSchema = avsc.parse(schema, parseOptions);
  let id = registry.cache.getBySchema(parsedSchema);
  if (id) {
    return Promise.resolve(id)
  }

  return pushSchema(registry, topic, schemaString, type)
    .then(id => registry.cache.set(id, parsedSchema));
})()
.then((schemaId) => {
  const encodedMessage = registry.cache.getById(schemaId).toBuffer(msg);

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
