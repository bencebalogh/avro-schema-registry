'use strict';

const http = require('http');
const chai = require('chai');
const expect = require('chai').expect;
const nock = require('nock');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);

const SchemaCache = require('./../../lib/schema-cache');

describe('SchemaCache', () => {
  const schema = {type: 'string'};
  let uut;
  beforeEach(() => {
    uut = new SchemaCache();
  })
  
  describe('set', () => {
    it('should update the schemasById cache with the schema', () => {
      uut.set(1, schema);
      expect(uut.schemasById.get(1)).to.eql(schema);
    });
    
    it('should not update the schemasBySchema cache with the schema when called with a Promise', () => {
      const promise = new Promise((res, rej) => {});
      uut.set(1, promise);
      expect(uut.schemasBySchema.get(1)).to.be.undefined;
    });
    
    it('should update the schemaBySchema cache with the schema when called with a schema', () => {
      uut.set(1, schema);
      expect(uut.schemasBySchema.get(JSON.stringify(schema))).to.eql(1);
    });
    
    it('should return the schema id', () => {
      expect(uut.set(1, schema)).to.eql(1);
    });
  });
  
  describe('getById', () => {
    it('should return undefined from the schemasById cache if not found', () => {
      expect(uut.schemasById).to.eql(new Map());
      expect(uut.getById(1)).to.be.undefined;
    });
    
    it('should return the schema from the schemasById cache if found', () => {
      uut.schemasById = new Map([[1, schema]]);
      expect(uut.getById(1)).to.eql(schema);
    });
  });
  
  describe('getBySchema', () => {
    it('should return undefined from the schemasBySchema cache if not found', () => {
      expect(uut.schemasBySchema).to.eql(new Map());
      expect(uut.getBySchema(schema)).to.be.undefined;
    });

    it('should return the schema from the schemasBySchema cache if found', () => {
      uut.schemasBySchema = new Map([[JSON.stringify(schema), 1]]);
      expect(uut.getBySchema(schema)).to.eql(1);
    });
  });
});
  