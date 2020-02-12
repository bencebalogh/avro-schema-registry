'use strict';

const { URL } = require('url');
const http = require('http');
const https = require('https');

const SchemaCache = require('./lib/schema-cache');
const decodeFunction = require('./lib/decode-function');
const encodeFunction = require('./lib/encode-function');

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

  const decode = decodeFunction(registry);
  const encodeKey = encodeFunction.bySchema('key', registry);
  const encodeMessage = encodeFunction.bySchema('value', registry);
  const encodeById = encodeFunction.byId(registry);
  const encodeMessageByTopicName = encodeFunction.byTopicName(registry);
  const getSchemaByTopicName = encodeFunction.getSchemaByTopicName(registry);

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
