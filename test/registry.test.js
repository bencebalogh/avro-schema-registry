'use strict';

const chai = require('chai');
const expect = require('chai').expect;
const nock = require('nock');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);

const registry = require('./../registry');

describe('registry', () => {
  const schema = {type: 'string'};
  const message = 'test message';
  const buffer = Buffer.from([0x00,0x00,0x00,0x00,0x01,0x18,0x74,0x65,0x73,0x74,0x20,0x6d,0x65,0x73,0x73,0x61,0x67,0x65]);

  afterEach(() => {
    nock.cleanAll();
  });

  describe('export', () => {
    it('returns an object with encode and decode methods', () => {
      const uut = registry('http://test.com');
      expect(uut).to.be.instanceOf(Object);
      expect(uut.encodeKey).to.exist;
      expect(uut.encodeKey).to.be.instanceOf(Function);
      expect(uut.encodeMessage).to.exist;
      expect(uut.encodeMessage).to.be.instanceOf(Function);
      expect(uut.encodeById).to.exist;
      expect(uut.encodeById).to.be.instanceOf(Function);
      expect(uut.decode).to.exist;
      expect(uut.decode).to.be.instanceOf(Function);
      expect(uut.decodeMessage).to.exist;
      expect(uut.decodeMessage).to.be.instanceOf(Function);
      expect(uut.decode).to.equal(uut.decodeMessage);
    });

    it('selects https transport', () => {
      const uut = registry('https://test.com');

      const schema = {type: 'string'};
      nock('https://test.com')
        .post('/subjects/test-value/versions')
        .reply(200, {id: 1});

      return uut.encodeMessage('test', schema, 'some string');
    });

    it('respects basic auth credentials from the url', () => {
      const uut = registry('https://username:password@test.com');

      const schema = {type: 'string'};
      nock('https://test.com')
        .post('/subjects/test-value/versions')
        .basicAuth({ user: 'username', pass: 'password' })
        .reply(200, {id: 1});

      return uut.encodeMessage('test', schema, 'some string');
    });

    it('respects basic auth credentials from the auth object', () => {
      const uut = registry('https://@test.com', {username: 'username', password: 'password'});

      const schema = {type: 'string'};
      nock('https://test.com')
        .post('/subjects/test-value/versions')
        .basicAuth({ user: 'username', pass: 'password' })
        .reply(200, {id: 1});

      return uut.encodeMessage('test', schema, 'some string');
    });

    it('handles connection error', () => {
      const uut = registry('https://not-good-url');

      const schema = {type: 'string'};

      const result = uut.encodeMessage('test', schema, 'some string');
      expect(result).to.eventually.be.rejectedWith('getaddrinfo ENOTFOUND not-good-url');
    });
  });

  describe('decode', () => {
    it(`rejects if the message doesn't contain a magic byte`, () => {
      const uut = registry('http://test.com');
      uut.decode(Buffer.from('test')).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', `Message doesn't contain schema identifier byte.`);
      });
    });

    it('rejects if the registry call fails', () => {
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(500, {error_code: 40403, message: 'Schema not found'});

      const uut = registry('http://test.com');

      return uut.decode(buffer).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 40403 - Schema not found');
      });
    });

    it('uses the registry for the first call and cache for the second call for the same id', () => {
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(200, {schema});

      const uut = registry('http://test.com');

      return uut.decode(buffer).then((msg) => {
        expect(msg).to.eql(message);

        // no mocked http call for this call, must came from the cache
        return uut.decode(buffer).then((msg2) => {
          expect(msg2).to.eql(message);
        });
      });
    });
  });

  describe('encodeKey', () => {
    it('rejects if the schema parse fails', () => {
      const uut = registry('http://test.com');
      return uut.encodeKey('topic', {invalid: 'schema'}, message).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'unknown type: undefined');
      });
    });

    it('rejects if the schema registry call fails', () => {
      nock('http://test.com')
        .post('/subjects/topic-key/versions')
        .reply(500, {error_code: 1, message: 'failed request'});

      const uut = registry('http://test.com');

      return uut.encodeKey('topic', schema, message).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 1 - failed request');
      });
    });

    it('uses the registry for the first call to register schema and return id and cache for the second call for the same schema', () => {
      nock('http://test.com')
        .post('/subjects/topic-key/versions')
        .reply(200, {id: 1});

      const uut = registry('http://test.com');

      return uut.encodeKey('topic', schema, message).then((msg) => {
        expect(msg).to.eql(buffer);

        // no mocked http call for this call, must came from the cache
        return uut.encodeKey('topic', schema, message).then((msg2) => {
          expect(msg2).to.eql(buffer);
        });
      });
    });
  });

  describe('encodeMessage', () => {
    it('rejects if the schema parse fails', () => {
      const uut = registry('http://test.com');
      return uut.encodeMessage('topic', {invalid: 'schema'}, message).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'unknown type: undefined');
      });
    });

    it('rejects if the schema registry call fails', () => {
      nock('http://test.com')
        .post('/subjects/topic-value/versions')
        .reply(500, {error_code: 1, message: 'failed request'});

      const uut = registry('http://test.com');

      return uut.encodeMessage('topic', schema, message).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 1 - failed request');
      });
    });

    it('uses the registry for the first call to register schema and return id and cache for the second call for the same schema', () => {
      nock('http://test.com')
        .post('/subjects/topic-value/versions')
        .reply(200, {id: 1});

      const uut = registry('http://test.com');

      return uut.encodeMessage('topic', schema, message).then((msg) => {
        expect(msg).to.eql(buffer);

        // no mocked http call for this call, must came from the cache
        return uut.encodeMessage('topic', schema, message).then((msg2) => {
          expect(msg2).to.eql(buffer);
        });
      });
    });
  });

  describe('encodeById', () => {
    it('rejects if the schema registry call fails', () => {
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(500, {error_code: 1, message: 'failed request'});

      const uut = registry('http://test.com');

      return uut.encodeById(1, message).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 1 - failed request');
      });
    });

    it('uses the registry for the first call and cache for the second call for the same id', () => {
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(200, {schema});

      const uut = registry('http://test.com');

      uut.encodeById(1, message).then((msg) => {
        expect(msg).to.eql(buffer);

        // no mocked http call for this call, must came from the cache
        return uut.encodeById(1, message).then((msg2) => {
          expect(msg2).to.eql(buffer);
        });
      });
    });
  });

  describe('encodeMessageByTopicName', () => {
    it('rejects if the schema registry call fails', () => {
      nock('http://test.com')
        .get('/subjects/topic/versions')
        .reply(500, {error_code: 1, message: 'failed request'});

      const uut = registry('http://test.com');

      return uut.encodeMessageByTopicName('topic', message).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 1 - failed request');
      });
    });

    it('uses the registry for the first call and cache for the second call for the same topic name', () => {
      nock('http://test.com')
        .get('/subjects/topic/versions')
        .reply(200, [1,2]);
      nock('http://test.com')
        .get('/subjects/topic/versions/2')
        .reply(200, {schema, id: 1});

      const uut = registry('http://test.com');

      return uut.encodeMessageByTopicName('topic', message).then((msg) => {
        expect(msg).to.eql(buffer);

        // no mocked http call for this call, must came from the cache
        return uut.encodeMessageByTopicName('topic', message).then((msg2) => {
          expect(msg2).to.eql(buffer);
        });
      });
    });
  });

  describe('getSchemaByTopicName', () => {
    it('rejects if the schema registry call fails', () => {
      nock('http://test.com')
        .get('/subjects/topic/versions')
        .reply(500, {error_code: 1, message: 'failed request'});

      const uut = registry('http://test.com');

      return uut.getSchemaByTopicName('topic').catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 1 - failed request');
      });
    });

    it('uses the registry for the first call and cache for the second call for the same topic name', () => {
      nock('http://test.com')
        .get('/subjects/topic/versions')
        .reply(200, [1,2]);
      nock('http://test.com')
        .get('/subjects/topic/versions/2')
        .reply(200, {schema, id: 1});

      const uut = registry('http://test.com');

      return uut.getSchemaByTopicName('topic').then(({id, parsedSchema}) => {
        expect(id).to.eql(1);
        expect(parsedSchema).to.exist;

        // no mocked http call for this call, must came from the cache
        return uut.getSchemaByTopicName('topic').then(({id, parsedSchema}) => {
          expect(id).to.eql(1);
          expect(parsedSchema).to.exist;
        });
      });
    });
  });

});
