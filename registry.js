'use strict';

const url = require('url');
const http = require('http');
const https = require('https');
const avsc = require('avsc');

const SchemaCache = require('./lib/schema-cache');
const decodeFunction = require('./lib/decode-function');
const encodeFunction = require('./lib/encode-function');


function schemas(registryUrl) {
  const parsed = url.parse(registryUrl);
  const registry = {
    cache: new SchemaCache(),
    protocol: parsed.protocol === 'https' ? https : http,
    host: parsed.hostname,
    port: parsed.port,
    path: parsed.path,
  };

  const decode = decodeFunction(registry)
  const getSchemabyMessage = decodeFunction(registry, true)
  const encodeKey = encodeFunction.bySchema('key', registry);
  const encodeMessage = encodeFunction.bySchema('value', registry);
  const encodeById = encodeFunction.byId(registry);

  return {
    decode,
    decodeMessage: decode,
    encodeById,
    encodeKey,
    encodeMessage,
    getSchemabyMessage
  };
};

module.exports = schemas;
