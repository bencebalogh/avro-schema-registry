'use strict';

const avsc = require('avsc');

const makeRequest = (registry, endpoint, success, ) => new Promise((resolve, reject) => {
  const {protocol, host, port, auth, path} = registry;
  const requestOptions = {
    host,
    port,
    path: path + endpoint,
    auth
  };

  const responseHandler = (res) => {
    let data = '';
    res.on('data', (d) => { data += d });
    res.on('error', (e) => { reject(e) });
    res.on('end', () => {
      if (res.statusCode !== 200) {
        const error = JSON.parse(data);
        return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`));
      }
      return resolve(success(data));
    });
  };

  protocol.get(requestOptions, responseHandler).on('error', (e) => { reject(e) });
});

const schema = async (registry, schemaId, parseOptions) => {
  const success = (data) => {
    const schema = JSON.parse(data).schema;
    return avsc.parse(schema, parseOptions);
  };
  return await makeRequest(registry, `schemas/ids/${schemaId}`, success);
};

const versionsBySubject = async (registry, subject) =>
  await makeRequest(registry, `subjects/${subject}/versions`, (data) => JSON.parse(data) );

const schemaBySubjectAndVersion = async (registry, subject, version) =>
  await makeRequest(registry, `subjects/${subject}/versions/${version}`, (data) => JSON.parse(data) );


module.exports = {
  schema,
  versionsBySubject,
  schemaBySubjectAndVersion
};

