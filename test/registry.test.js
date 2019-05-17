'use strict';

const chai = require('chai');
const expect = require('chai').expect;
const nock = require('nock');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);

const registry = require('./../registry');

describe('registry', () => {
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
    })

    it('respects basic auth credentials', () => {
      const uut = registry('https://username:password@test.com');

      const schema = {type: 'string'};
      nock('https://test.com')
        .post('/subjects/test-value/versions')
        .basicAuth({ user: 'username', pass: 'password' })
        .reply(200, {id: 1});

      return uut.encodeMessage('test', schema, 'some string');
    })

    it('handles connection error', () => {
      const uut = registry('https://not-good-url');

      const schema = {type: 'string'};

      const result = uut.encodeMessage('test', schema, 'some string');
      expect(result).to.eventually.be.rejectedWith('getaddrinfo ENOTFOUND not-good-url not-good-url:443');
    })
  });

});
