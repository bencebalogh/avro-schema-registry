'use strict';

const { URL } = require('url');
const http = require('http');
const https = require('https');

const avsc = require('avsc');

const SchemaCache = require('./lib/schema-cache');
const {pushSchema, getSchemaById, getLatestVersionForSubject} = require('./lib/http-calls');

function schemas(registryUrl, auth = null) {
  const parsed = new URL(registryUrl);
  const registry = {
    cache: new SchemaCache(),
    protocol: parsed.protocol.startsWith('https') ? https : http,
    host: parsed.hostname,
    port: parsed.port,
    path: parsed.path != null ? parsed.path : '/',
    username: parsed.username,
    password: parsed.password,
  };

  if(auth != null && (typeof auth === 'object')) {
    registry.username = auth.username;
    registry.password = auth.password;
  }

  const encodeFunction = (msg, schemaId, schema) => {
    const encodedMessage = schema.toBuffer(msg);

    const message = Buffer.alloc(encodedMessage.length + 5);
    message.writeUInt8(0);
    message.writeUInt32BE(schemaId, 1);
    encodedMessage.copy(message, 5);

    return message;
  };

  const getId = (subject, schema, parsedSchema) => {
    let schemaId = registry.cache.getBySchema(schema);
    if (!schemaId) {
      schemaId = pushSchema(registry, subject, schema);
      registry.cache.setBySchema(schema, schemaId);
    }

    return schemaId.then((id) => {
      registry.cache.setById(id, Promise.resolve(parsedSchema));
      registry.cache.setBySchema(schema, Promise.resolve(id));

      return id;
    });
  };

const getSchema = (id, parseOptions) => {
    let schemaPromise = registry.cache.getById(id);
    if (!schemaPromise) {
      schemaPromise = getSchemaById(registry, id);
      registry.cache.setById(schemaPromise);
    }

    return schemaPromise.then((schema) => {
      const parsedSchema = avsc.parse(schema, parseOptions);
      registry.cache.setById(id, Promise.resolve(parsedSchema));
      registry.cache.setBySchema(schema, Promise.resolve(id));

      return parsedSchema;
    });
  };

  const getSchemaAndId = (topic, parseOptions) => {
    let promise = registry.cache.getByName(topic);
    if (!promise) {
      promise = getLatestVersionForSubject(registry, topic);
      registry.cache.setByName(topic, promise);
    }

    return promise.then(({schema, id}) => {
      const parsedSchema = avsc.parse(schema, parseOptions);
      registry.cache.setByName(topic, Promise.resolve({schema, id}));
      registry.cache.setById(id, Promise.resolve(parsedSchema));
      registry.cache.setBySchema(schema, Promise.resolve(id));
      return {parsedSchema, id};
    });
  };

  const decode = (msg, parseOptions) => {
    if (msg.readUInt8(0) !== 0) {
      return Promise.reject(new Error(`Message doesn't contain schema identifier byte.`));
    }
    const id = msg.readUInt32BE(1);
    const buffer = msg.slice(5);

    const schema = registry.cache.getById(id);

    let schemaPromise = registry.cache.getById(id);
    if (!schemaPromise) {
      schemaPromise = getSchemaById(registry, id);
      registry.cache.setById(schemaPromise);
    }

    return schemaPromise.then((schema) => {
      const parsedSchema = avsc.parse(schema, parseOptions);
      registry.cache.setById(id, Promise.resolve(parsedSchema));
      registry.cache.setBySchema(JSON.stringify(schema), Promise.resolve(id));

      return parsedSchema.fromBuffer(buffer);
    });
  };

  const encodeKey = (topic, schema, msg, parseOptions = null) => {
    try {
      const parsedSchema = avsc.parse(schema, parseOptions);
      return getId(`${topic}-key`, schema, parsedSchema).then((id) => encodeFunction(msg, id, parsedSchema));
    } catch (e) {
      return Promise.reject(e);
    }
  };

  const encodeMessage = (topic, schema, msg, parseOptions = null) => {
    try {
      const parsedSchema = avsc.parse(schema, parseOptions);
      return getId(`${topic}-value`, schema, parsedSchema).then((id) => encodeFunction(msg, id, parsedSchema));
    } catch (e) {
      return Promise.reject(e);
    }
  };

  const encodeById = (id, msg, parseOptions = null) => {
    return getSchema(id, parseOptions).then((schema) => encodeFunction(msg, id, schema));
  };

  const encodeMessageByTopicName = (topic, msg, parseOptions = null) => {
    return getSchemaAndId(topic, parseOptions).then(({parsedSchema, id}) => encodeFunction(msg, id, parsedSchema));
  };

  const getSchemaByTopicName = (topic, parseOptions = null) => {
    return getSchemaAndId(topic, parseOptions);
  };

  return {
    decode,
    decodeMessage: decode,
    encodeById,
    encodeKey,
    encodeMessage,
    encodeMessageByTopicName,
    getSchemaByTopicName,
  };
}

module.exports = schemas;
