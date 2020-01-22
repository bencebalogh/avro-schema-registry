'use strict';

const avsc = require('avsc');

const fetchSchema = require('./fetch-schema');
const pushSchema = require('./push-schema');
const getLatestSchema = require('./get-latest-schema-version');
const { Strategy, getSubject } = require('./strategy');

const encodeMessage = (msg, schemaId) => schema => {
  const encodedMessage = schema.toBuffer(msg);

  const message = Buffer.alloc(encodedMessage.length + 5);
  message.writeUInt8(0);
  message.writeUInt32BE(schemaId, 1);
  encodedMessage.copy(message, 5);

  return message;
}

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
.then(encodeMessage(msg, schemaId));

const bySchema = (strategy, isKey, registry) => (topic, schema, msg, parseOptions = null) => (() => {
  const parsedSchema = avsc.parse(schema, parseOptions);
  let id = registry.cache.getBySchema(parsedSchema);
  if (id) {
    return Promise.resolve(id)
  }

  return pushSchema(strategy, registry, topic, parsedSchema, isKey)
    .then(id => registry.cache.set(id, parsedSchema));
})()
.then(schemaId => {
  const schema = registry.cache.getById(schemaId)

  return encodeMessage(msg, schemaId)(schema)
});

const getSchemaByName = (registry, topic, parseOptions, isKey, recordName, strategy) => {
  const subject = getSubject(strategy, topic, isKey, recordName);
  const schemaId = registry.cache.getByName(subject);

  if(schemaId) {
    return new Promise(resolve =>
      resolve({ id: schemaId, parsedSchema: registry.cache.getById(schemaId) })
    );
  }

  return getLatestSchema(registry, topic, parseOptions, isKey, recordName, strategy).then(({parsedSchema, id}) => {
    registry.cache.set(id, parsedSchema);

    return {id, parsedSchema};
  })
}

const getSchemaByTopicName = (registry) => (topic, parseOptions) => {
  return getSchemaByName(registry, topic, parseOptions, false, null, Strategy.TopicNameStrategy);
}

const getSchemaByTopicRecordName = (registry) => (topic, recordName, parseOptions) => {
  return getSchemaByName(registry, topic, parseOptions, false, recordName, Strategy.TopicRecordNameStrategy);
}

const getSchemaByRecordName = (registry) => (recordName, parseOptions) => {
  return getSchemaByName(registry, null, parseOptions, false, recordName, Strategy.RecordNameStrategy);
}

const byTopicName = (registry) => (topic, msg, parseOptions = null) => getSchemaByTopicName(registry)(topic, parseOptions).then(({id, parsedSchema}) => encodeMessage(msg, id)(parsedSchema))
const byTopicRecordName = (registry) => (topic, recordName, msg, parseOptions = null) => getSchemaByTopicRecordName(registry)(topic, recordName, parseOptions).then(({id, parsedSchema}) => encodeMessage(msg, id)(parsedSchema))
const byRecordName = (registry) => (recordName, msg, parseOptions = null) => getSchemaByRecordName(registry)(recordName, parseOptions).then(({id, parsedSchema}) => encodeMessage(msg, id)(parsedSchema))

module.exports = {
  bySchema,
  byId,
  byTopicName,
  byTopicRecordName,
  byRecordName,
  getSchemaByTopicName,
  getSchemaByTopicRecordName,
  getSchemaByRecordName
}
