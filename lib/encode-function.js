'use strict';

const avsc = require('avsc');
const uuid4 = require('uuid').v4;
const util = require('util');
const fetchSchema = require('./fetch-schema');
const pushSchema = require('./push-schema');
const getLatestSchema = require('./get-latest-schema-version');
// const MsgTypeResolver = require('./MsgTypeResolver');

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

const bySchema = (type, registry) => (topic, schema, msg, parseOptions = null) => (() => {
  // const defaultParseOptions = {
  //   namespace: MsgTypeResolver.NAMESPACE,
  //   logicalTypes: {
  //     resolveMsgType: MsgTypeResolver
  //   }
  // };
  // parseOptions = Object.assign({}, defaultParseOptions, parseOptions);

  // const requestid = uuid4();
  // console.log(`Step 1, reqId: ${requestid}, in bySchema with topic: ${topic}, schema: ${schema}, msg: ${util.inspect(msg)}`);
  const schemaString = JSON.stringify(schema);
  // console.log(`Step 2, reqId: ${requestid}, in bySchema with topic: ${topic}, schema: ${schema}, msg: ${util.inspect(msg)}, schemaString: ${schemaString}`);
  const parsedSchema = avsc.parse(schema, parseOptions);
  // console.log(`Step 3, reqId: ${requestid}, in bySchema with topic: ${topic}, schema: ${schema}, msg: ${util.inspect(msg)}, schemaString: ${schemaString}, parsedSchema: ${util.inspect(parsedSchema)}`);
  let id = registry.cache.getBySchema(parsedSchema);
  // console.log(`Step 4, reqId: ${requestid} after registry.cache.getBySchema, id: ${id}`);
  if (id) {
    // console.log(`Step 4.1, reqId: ${requestid} registry.cache.getBySchema returned id: ${id}, resolving`);
    return Promise.resolve(id)
  }
  // console.log(`Step 5, reqId: ${requestid} registry.cache.getBySchema didn't find id`);

  let typeName;

  if (type === "multi") {
    if (schema.namespace){
      // console.log(`Step 6, reqId: ${requestid} in type multi with namespace: ${schema.namespace}, name: ${schema.name}`);
      typeName = `${schema.namespace}.${schema.name}`;
      // console.log(`Step 7, reqId: ${requestid} in type multi set type to ${topic}`);
    } else{
      // console.log(`Step 8, reqId: ${requestid} in type multi without namespace. name: ${schema.name}`);
      typeName = `${schema.name}`;
      // console.log(`Step 9, reqId: ${requestid} in type multi set type to ${topic}`);
    }
  } else {
    typeName = type;
  }

  // console.log(`Step 10, reqId: ${requestid} going to pushSchema with topic: ${topic}, schemaString: ${schemaString}, type: ${type}`);
  return pushSchema(registry, topic, schemaString, typeName)
    .then(id => registry.cache.set(id, parsedSchema));
})()
.then(schemaId => {
  const schema = registry.cache.getById(schemaId)

  return encodeMessage(msg, schemaId)(schema)
});

const getSchemaByTopicName = (registry) => (topic, parseOptions) => {
  const schemaId = registry.cache.getByName(topic);

  if (schemaId) {
    return registry.cache.getById(schemaId);
  }

  return getLatestSchema(registry, topic, parseOptions).then(({parsedSchema, id}) => {
    registry.cache.set(id, parsedSchema);

    return {id, parsedSchema};
  })
}

const byTopicName = (registry) => (topic, msg, parseOptions = null) => getSchemaByTopicName(registry)(topic, parseOptions).then(({id, parsedSchema}) => encodeMessage(msg, id)(parsedSchema))

module.exports = {
  bySchema,
  byId,
  byTopicName,
  getSchemaByTopicName
}
