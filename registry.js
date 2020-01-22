'use strict';

const url = require('url');
const http = require('http');
const https = require('https');

const SchemaCache = require('./lib/schema-cache');
const decodeFunction = require('./lib/decode-function');
const encodeFunction = require('./lib/encode-function');
const { Strategy } = require('./lib/strategy');

function schemas(registryUrl) {
  const parsed = url.parse(registryUrl);
  const registry = {
    cache: new SchemaCache(),
    protocol: parsed.protocol.startsWith('https') ? https : http,
    host: parsed.hostname,
    port: parsed.port,
    path: parsed.path,
  };

  if(parsed.auth) {
    registry.auth = parsed.auth;
  }

  const decode = decodeFunction(registry);

  const encodeKey = encodeFunction.bySchema(Strategy.TopicNameStrategy, true, registry);
  const encodeMessage = encodeFunction.bySchema(Strategy.TopicNameStrategy, false, registry);
  const encodeKeyBySchema = (strategy, topic, schema, msg, parseOptions = null) =>
    encodeFunction.bySchema(strategy, true, registry)(topic, schema, msg, parseOptions);
  const encodeMessageBySchema = (strategy, topic, schema, msg, parseOptions = null) =>
    encodeFunction.bySchema(strategy, false, registry)(topic, schema, msg, parseOptions);
  const encodeById = encodeFunction.byId(registry);
  const encodeMessageByTopicName = encodeFunction.byTopicName(registry);
  const encodeMessageByTopicRecordName = encodeFunction.byTopicRecordName(registry);
  const encodeMessageByRecordName = encodeFunction.byRecordName(registry);
  const getSchemaByTopicName = encodeFunction.getSchemaByTopicName(registry);
  const getSchemaByTopicRecordName = encodeFunction.getSchemaByTopicRecordName(registry);
  const getSchemaByRecordName = encodeFunction.getSchemaByRecordName(registry);

  return {
    decode,
    decodeMessage: decode,
    encodeById,
    encodeKey,
    encodeMessage,
    encodeKeyBySchema,
    encodeMessageBySchema,
    encodeMessageByTopicName,
    encodeMessageByTopicRecordName,
    encodeMessageByRecordName,
    getSchemaByTopicName,
    getSchemaByTopicRecordName,
    getSchemaByRecordName,
    Strategy
  };
}

module.exports = schemas;
