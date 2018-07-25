'use strict';

const chai = require('chai');
const expect = require('chai').expect;
const nock = require('nock');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);

const registry = require('./../registry');

describe('registry', () => {
  describe('export', () => {
    it('returns a function', () => {
      expect(registry).to.be.instanceOf(Function);
    });

    it('returns an object with encode and decode methods', () => {
      const uut = registry('https://test.com');
      expect(uut).to.be.instanceOf(Object);
      expect(uut.encodeMessage).to.exist;
      expect(uut.encodeMessage).to.be.instanceOf(Function);
      expect(uut.decodeMessage).to.exist;
      expect(uut.decodeMessage).to.be.instanceOf(Function);
    });
  });

  describe('decodeMessage', () => {
    it('rejects with an error if there is no schema identifier in the message', () => {
      const uut = registry('http://test.com');
      return uut.decodeMessage(new Buffer('test')).catch((error) => {
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

      const uut = registry('http://test.com');
      return uut.decodeMessage(buffer).catch((error) => {
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

      const uut = registry('https://test.com');
      return uut.decodeMessage(buffer).then((msg) => {
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

      const uut = registry('https://test.com');
      return uut.decodeMessage(buffer).then((msg1) => {
        expect(msg1).to.eql(message);
        uut.decodeMessage(buffer).then((msg2) => {
          // there is no nock call for second call so it must have come from cache
          expect(msg2).to.eql(message);
        });
      });
    });

    it('ask for schema only once if `decodeMessage` called simultaneously', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const buffer = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x01, 0x18, 0x74, 0x65, 0x73, 0x74, 0x20, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65]);
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(200, {schema});

      const uut = registry('https://test.com');
      return Promise.all([
        uut.decodeMessage(buffer),
        uut.decodeMessage(buffer)
      ]).then(([msg1, msg2]) => {
        expect(msg1).to.eql(message);
        expect(msg2).to.eql(message);
      });
    });

    it('decodes message by retrieving schema form cache if schema has been retrieved by `encodeMessage`', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const buffer = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x01, 0x18, 0x74, 0x65, 0x73, 0x74, 0x20, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65]);
      nock('http://test.com')
        .post('/subjects/test-value/versions')
        .reply(200, {id: 1});

      const uut = registry('https://test.com');
      return uut.encodeMessage('test', schema, message)
        .then(buff => {
          expect(buff).to.eql(buffer);
          return uut.decodeMessage(buff);
        })
        .then(msg => {
          expect(msg).to.eql(message);
        });
    });
  });

  describe('encodeMessage', () => {
    it('rejects with an error if schema registry call returns with an error', () => {
      nock('http://test.com')
        .post('/subjects/test-value/versions')
        .reply(500, {error_code: 42201, message: 'Invalid Avro schema'});

      const uut = registry('http://test.com');
      return uut.encodeMessage('test', {type: 'string'}, 'test message').catch((error) => {
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
        .post('/subjects/test-value/versions')
        .reply(200, {id: 1});

      const uut = registry('https://test.com');
      return uut.encodeMessage('test', schema, message).then((encoded) => {
        expect(encoded).to.eql(buffer);
      });
    });

    it('encodes message by retrieving schema from cache if schema has been retrieved once', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);
      nock('http://test.com')
        .post('/subjects/test-value/versions')
        .reply(200, {id: 1});

      const uut = registry('https://test.com');
      return uut.encodeMessage('test', schema, message).then((encoded1) => {
        expect(encoded1).to.eql(buffer);
        return uut.encodeMessage('test', schema, message).then((encoded2) => {
          // there is no nock call for second call so it must have come from cache
          expect(encoded2).to.eql(buffer);
        });
      });
    });

    it('encodes message by retrieving schema form cache if schema has been retrieved by `decodeMessage`', () => {
      const schema = {type: 'string'};
      const message = 'test message';
      const buffer = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x01, 0x18, 0x74, 0x65, 0x73, 0x74, 0x20, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65]);
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(200, {schema});

      const uut = registry('https://test.com');
      return uut.decodeMessage(buffer)
        .then(msg => {
          expect(msg).to.eql(message);
          return uut.encodeMessage('test', schema, msg);
        })
        .then(buff => {
          expect(buff).to.eql(buffer);
        });
    });
  });
});