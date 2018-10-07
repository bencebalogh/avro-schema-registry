'use strict';

const http = require('http');
const chai = require('chai');
const expect = require('chai').expect;
const nock = require('nock');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);

const SchemaCache = require('./../../lib/schema-cache');
const decodeFunction = require('./../../lib/decode-function');

describe('decodeFunction', () => {
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

  it('rejects with an error if there is no schema identifier in the message', () => {
    const uut = decodeFunction(registry);
    return uut(new Buffer('test')).catch((error) => {
      expect(error).to.exist
        .and.be.instanceof(Error)
        .and.have.property('message', `Message doesn't contain schema identifier byte.`);
    });
  });

  it('rejects with an error if schema registry call returns with an error', () => {
    nock('http://test.com')
      .get('/schemas/ids/1')
      .reply(500, {error_code: 40403, message: 'Schema not found'});
    const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);

    const uut = decodeFunction(registry);
    return uut(buffer).catch((error) => {
      expect(error).to.exist
        .and.be.instanceof(Error)
        .and.have.property('message', 'Schema registry error: 40403 - Schema not found');
    });
  });

  it('decodes message by retrieving schema from the schema registry', () => {
    const schema = {type: 'string'};
    const message = 'test message';
    const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);
    nock('http://test.com')
      .get('/schemas/ids/1')
      .reply(200, {schema});

    const uut = decodeFunction(registry);
    return uut(buffer).then((msg) => {
      expect(msg).to.eql(message);
    });
  });

  it('decodes message by retrieving schema from cache if schema has been retrieved once', () => {
    const schema = {type: 'string'};
    const message = 'test message';
    const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);
    nock('http://test.com')
      .get('/schemas/ids/1')
      .reply(200, {schema});

    const uut = decodeFunction(registry);
    return uut(buffer).then((msg1) => {
      expect(msg1).to.eql(message);
      uut(buffer).then((msg2) => {
        // there is no nock call for second call so it must have come from cache
        expect(msg2).to.eql(message);
      });
    });
  });

  it('ask for schema only once if `decode` called simultaneously', () => {
    const schema = {type: 'string'};
    const message = 'test message';
    const buffer = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x01, 0x18, 0x74, 0x65, 0x73, 0x74, 0x20, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65]);
    nock('http://test.com')
      .get('/schemas/ids/1')
      .reply(200, {schema});

    const uut = decodeFunction(registry);
    return Promise.all([
      uut(buffer),
      uut(buffer)
    ]).then(([msg1, msg2]) => {
      expect(msg1).to.eql(message);
      expect(msg2).to.eql(message);
    });
  });

});