'use strict';

const http = require('http');
const chai = require('chai');
const expect = require('chai').expect;
const nock = require('nock');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);

const fetchSchema = require('./../../lib/fetch-schema');

describe('fetchSchema', () => {
  const registry = {
    protocol: http,
    host: 'test.com',
    port: null,
    path: '/',
  };

  it('should reject a promise if the request fails', () => {
    nock('http://test.com')
      .get('/schemas/ids/1')
      .reply(500, {error_code: 40403, message: 'Schema not found'});

    const uut = fetchSchema(registry, 1);
    return uut.catch((error) => {
      expect(error).to.exist
        .and.be.instanceof(Error)
        .and.have.property('message', 'Schema registry error: 40403 - Schema not found');
    });
  });

  it('should resolve a promise with the schema if the request succeeds', () => {
    const schema = {type: 'string'};
    nock('http://test.com')
      .get('/schemas/ids/1')
      .reply(200, {schema});

    const uut = fetchSchema(registry, 1);
    return uut.then((schema) => {
      expect(schema).to.eql(schema);
    });
  }).on('error', (e) => {
    reject(e);
  });
});
