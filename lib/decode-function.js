'use strict';

const avsc = require('avsc');

const fetchSchema = require('./fetch-schema');

module.exports = (registry, opts) => (obj) => new Promise((resolve, reject) => {
  let schemaId;
  
  if (obj.readUInt8(0) !== 0) {
      return reject(new Error(`Message doesn't contain schema identifier byte.`));
    }
    schemaId = obj.readUInt32BE(1);
    let promise = registry.cache.getById(schemaId);

    if (promise) {
      return resolve(promise);
    }

    promise = fetchSchema(registry, schemaId);
    
    registry.cache.set(schemaId, promise);
    
    promise
      .then((result) => registry.cache.set(schemaId, result))
      .catch(reject);

    return resolve(promise);
}).then((schema) => avsc.parse(schema, opts).fromBuffer(obj.slice(5)));
