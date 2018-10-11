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
  });

});