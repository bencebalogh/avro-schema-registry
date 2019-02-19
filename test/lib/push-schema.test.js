'use strict';

const http = require('http');
const chai = require('chai');
const expect = require('chai').expect;
const nock = require('nock');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);

const pushSchema = require('./../../lib/push-schema');

describe('pushSchema', () => {
  const registry = {
    protocol: http,
    host: 'test.com',
    port: null,
    path: '/',
  };

  it('should reject a promise if push fails', () => {
    const schema = {type: 'string'};
    nock('http://test.com')
      .post('/subjects/test-value/versions')
      .reply(500, {error_code: 42201, message: 'Invalid Avro schema'});

    const uut = pushSchema(registry, 'test', schema);
    return uut.catch((error) => {
      expect(error).to.exist
        .and.be.instanceof(Error)
        .and.have.property('message', 'Schema registry error: 42201 - Invalid Avro schema');
    });
  });

  it('should resolve a promise with the schema id if the post works', () => {
    const schema = {type: 'string'};
    nock('http://test.com')
      .post('/subjects/test-value/versions')
      .reply(200, {id: 1});

    const uut = pushSchema(registry, 'test', schema);
    return uut.then((id) => {
      expect(id).to.eql(1);
    });
  });

  it ('should call the correct path for key type', () => {
    const schema = {type: 'string'};
    nock('http://test.com')
      .post('/subjects/test-key/versions')
      .reply(200, {id: 1});

    const uut = pushSchema(registry, 'test', schema, 'key');
    return uut.then((id) => {
      expect(id).to.eql(1);
    });
  });

  it ('should call the correct path for value type', () => {
    const schema = {type: 'string'};
    nock('http://test.com')
      .post('/subjects/test-value/versions')
      .reply(200, {id: 1});

    const uut = pushSchema(registry, 'test', schema, 'value');
    return uut.then((id) => {
      expect(id).to.eql(1);
    });
  });

});
  