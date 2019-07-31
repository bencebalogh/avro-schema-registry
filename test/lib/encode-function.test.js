'use strict';

const http = require('http');
const chai = require('chai');
const expect = require('chai').expect;
const nock = require('nock');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);

const SchemaCache = require('./../../lib/schema-cache');
const encodeFunction = require('./../../lib/encode-function');

describe('encodeFunction', () => {
  let registry;

  beforeEach(() => {
    registry = {
      cache: new SchemaCache(),
      protocol: http,
      host: 'test.com',
      port: null,
      path: '/',
    };
  });

  describe('byId', () => {
    it('rejects with an error if schema registry call returns with an error', () => {
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(500, {error_code: 40403, message: 'Schema not found'});

      const uut = encodeFunction.byId(registry);
      return uut(1, 'test message').catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 40403 - Schema not found');
      });
    });

    it('encodes message by retrieving schema from the schema registry', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(200, {schema});

      const uut = encodeFunction.byId(registry);
      return uut(1, message).then((encoded) => {
        expect(encoded).to.eql(buffer);
      });
    });

    it('encodes message by retrieving schema from cache if schema has been retrieved once', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(200, {schema});

      const uut = encodeFunction.byId(registry);
      return uut(1, message).then((encoded1) => {
        expect(encoded1).to.eql(buffer);
        return uut(1, message).then((encoded2) => {
          // there is no nock call for second call so it must have come from cache
          expect(encoded2).to.eql(buffer);
        });
      });
    });
  });

  describe('bySchema', () => {
    it('calls the correct path for key type', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);
      nock('http://test.com')
        .post('/subjects/test-key/versions')
        .reply(200, {id: 1});

      const uut = encodeFunction.bySchema('key', registry);
      return uut('test', schema, message).then((encoded) => {
        expect(encoded).to.eql(buffer);
      });
    });

    it('calls the correct path for value type', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);
      nock('http://test.com')
        .post('/subjects/test-value/versions')
        .reply(200, {id: 1});

      const uut = encodeFunction.bySchema('value', registry);
      return uut('test', schema, message).then((encoded) => {
        expect(encoded).to.eql(buffer);
      });
    });

    it('rejects with an error if schema registry call returns with an error', () => {
      nock('http://test.com')
        .post('/subjects/test-key/versions')
        .reply(500, {error_code: 42201, message: 'Invalid Avro schema'});

      const uut = encodeFunction.bySchema('key', registry);
      return uut('test', {type: 'string'}, 'test message').catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 42201 - Invalid Avro schema');
      });
    });

    it('encodes message by retrieving schema from the schema registry', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);
      nock('http://test.com')
        .post('/subjects/test-key/versions')
        .reply(200, {id: 1});

      const uut = encodeFunction.bySchema('key', registry);
      return uut('test', schema, message).then((encoded) => {
        expect(encoded).to.eql(buffer);
      });
    });

    it('encodes message by retrieving schema from cache if schema has been retrieved once', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);

      nock('http://test.com')
        .post('/subjects/test-key/versions')
        .reply(200, {id: 1});

      const uut = encodeFunction.bySchema('key', registry);
      return uut('test', schema, message).then((encoded1) => {
        expect(encoded1).to.eql(buffer);
        return uut('test', schema, message).then((encoded2) => {
          // there is no nock call for second call so it must have come from cache
          expect(encoded2).to.eql(buffer);
        });
      });
    });

    it('encodes message when schema exists in the registry and pushNewSchemas is false', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);
      const versions = [1, 2, 3];

      nock('http://test.com')
        .get('/subjects/test-key/versions')
        .reply(200, versions);

      versions.map(version => {
        const body = version === 2 ? {schema: schema, id: 1} : {};
        nock('http://test.com')
          .get('/subjects/test-key/versions/' + version)
          .reply(200, body);
      });

      const uut = encodeFunction.bySchema('key', registry, false);
      return uut('test', schema, message).then((encoded) => {
        expect(encoded).to.eql(buffer);
      });
    });

    it('throws error if schema is different than latest version not exist and pushNewSchemas is false', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const versions = [1, 2];
      nock('http://test.com')
        .get('/subjects/test-key/versions')
        .reply(200, versions);

      versions.map(version => {
        nock('http://test.com')
          .get('/subjects/test-key/versions/' + version)
          .reply(200, {});
      });

      const uut = encodeFunction.bySchema('key', registry, false);
      return uut('test', schema, message).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Unable to locate schema in the registry');
      });
    });
  });
});
