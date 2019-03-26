'use strict';

const url = require('url');
const http = require('http');
const https = require('https');

const SchemaCache = require('./lib/schema-cache');
const decodeFunction = require('./lib/decode-function');
const encodeFunction = require('./lib/encode-function');

function schemas(registryUrl, avscOptions) {
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

  const opts = avscOptions || {}
  
  const decode = decodeFunction(registry, opts)
  const encodeKey = encodeFunction.bySchema('key', registry, opts);
  const encodeMessage = encodeFunction.bySchema('value', registry, opts);
  const encodeById = encodeFunction.byId(registry, opts);

  return {
    decode,
    decodeMessage: decode,
    encodeById,
    encodeKey,
    encodeMessage,
  };
};

module.exports = schemas;
