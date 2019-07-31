'use strict';

const avsc = require('avsc');

const fetch = require('./fetch');
const pushSchema = require('./push-schema');

const byId = (registry) => (schemaId, msg, parseOptions = null) => (() => new Promise((resolve, reject) => {
  let promise = registry.cache.getById(schemaId);

  if (promise) {
    return resolve(promise);
  }

  promise = fetch.schema(registry, schemaId, parseOptions);

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

const bySchema = (type, registry, pushNewSchemas = true) => (topic, schema, msg, parseOptions = null) => (async () => {
  const schemaString = JSON.stringify(schema);
  const parsedSchema = avsc.parse(schema, parseOptions);
  let id = registry.cache.getBySchema(parsedSchema);
  if (id) {
    return Promise.resolve(id)
  }

  if (pushNewSchemas) {
    return pushSchema(registry, topic, schemaString, type)
      .then(id => registry.cache.set(id, parsedSchema));
  } else {
    const subject = `${topic}-${type}`;
    const versions = await fetch.versionsBySubject(registry, subject, parseOptions);
    for(let i = versions.length - 1; i >= 0; i--) {
      const schemaInfo = await fetch.schemaBySubjectAndVersion(registry, subject, versions[i], parseOptions);

      if (schemaInfo.schema && JSON.stringify(schemaInfo.schema) === schemaString) {
        registry.cache.set(schemaInfo.id, parsedSchema);
        console.log('id', schemaInfo.id);
        return Promise.resolve(schemaInfo.id);
      }
    }

    return Promise.reject(new Error('Unable to locate schema in the registry'));
  }
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
