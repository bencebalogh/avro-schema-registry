'use strict';

const expect = require('chai').expect;
const schemaCache = require('../../lib/schema-cache');

describe('schema-cache', () => {
  const schema = {type: 'string'};

  describe('setById', () => {
    it('updates the cache', () => {
      const expected = new Map();
      expected.set(1, schema);

      const uut = new schemaCache();
      uut.setById(1, schema);
      expect(uut.schemasById).to.eql(expected);
    });
  });

  describe('setByName', () => {
    it('updates the cache', () => {
      const expected = new Map();
      expected.set('topic', schema);

      const uut = new schemaCache();
      uut.setByName('topic', schema);
      expect(uut.schemasByName).to.eql(expected);
    });
  });

  describe('setBySchema', () => {
    it('updates the cache', () => {
      const expected = new Map();
      expected.set(JSON.stringify(schema), 1);

      const uut = new schemaCache();
      uut.setBySchema(schema, 1);
      expect(uut.schemasBySchema).to.eql(expected);
    });
  });

  describe('getById', () => {});
  describe('getByName', () => {});
  describe('getBySchema', () => {});
});
