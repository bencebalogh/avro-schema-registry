'use strict';

const http = require('http');
const nock = require('nock');
const expect = require('chai').expect;

const {pushSchema, getSchemaById, getLatestVersionForSubject} = require('../../lib/http-calls');

describe('http-calls', () => {
  const schema = {type: 'string'};
  const registry = {
    protocol: http,
    host: 'test.com',
    username: null,
    password: null,
    path: '/'
  };

  afterEach(() => {
    nock.cleanAll();
  });

  const nonStandardErrorPayload = `
  <html>
  <head><title>401 Authorization Required</title></head>
  <body>
  <center><h1>401 Authorization Required</h1></center>
  <hr><center>nginx</center>
  </body>
  </html>`

  describe('pushSchema', () => {
    it('reject if post request fails', () => {
      const requestError = new Error("ECONNREFUSED");
      nock('http://test.com')
        .post('/subjects/topic/versions')
        .replyWithError(requestError);

      return pushSchema(registry, 'topic', schema).catch((error) => {
        expect(error).to.equal(requestError);
      });
    });

    it('reject if post request returns with not 200', () => {
      nock('http://test.com')
        .post('/subjects/topic/versions')
        .reply(500, {error_code: 1, message: "failed request"});

      return pushSchema(registry, 'topic', schema).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 1 - failed request');
      });
    });

    it('resolve with schema id if post request returns with 200', () => {
      nock('http://test.com')
        .post('/subjects/topic/versions')
        .reply(200, {id: 1});

      return pushSchema(registry, 'topic', schema).then((id) => {
        expect(id).to.equal(1);
      });
    });

    it('reject if post request returns with with 401 and without registry error object', () => {
      nock('http://test.com')
        .post('/subjects/topic/versions')
        .reply(401, nonStandardErrorPayload);

      return pushSchema(registry, 'topic', schema).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', `Schema registry error: no error in response; httpStatus is 401`);
      });
    });
  });

  describe('getSchemaById', () => {
    it('reject if get request fails', () => {
      const requestError = new Error("ECONNREFUSED");
      nock('http://test.com')
        .get('/schemas/ids/1')
        .replyWithError(requestError);

      return getSchemaById(registry, 1).catch((error) => {
        expect(error).to.equal(requestError);
      });
    });

    it('reject if get request returns with not 200', () => {
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(500, {error_code: 1, message: "failed request"});

      return getSchemaById(registry, 1).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 1 - failed request');
      });
    });

    it('resolve with schema if get request returns with 200', () => {
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(200, {id: 1, schema});

      return getSchemaById(registry, 1).then((returnedSchema) => {
        expect(returnedSchema).to.eql(schema);
      });
    });

    it('reject if post request returns with with 401 and without registry error object', () => {
      nock('http://test.com')
        .get('/schemas/ids/1')
        .reply(401, nonStandardErrorPayload);

      return getSchemaById(registry, 1).catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', `Schema registry error: no error in response; httpStatus is 401`);
      });
    });
  });

  describe('getLatestVersionForSubject', () => {
    it('reject if first get request fails', () => {
      const requestError = new Error("ECONNREFUSED");
      nock('http://test.com')
        .get('/subjects/topic/versions')
        .replyWithError(requestError);

      return getLatestVersionForSubject(registry, 'topic').catch((error) => {
        expect(error).to.equal(requestError);
      });
    });

    it('reject if first get request returns with not 200', () => {
      const requestError = new Error("ECONNREFUSED");
      nock('http://test.com')
        .get('/subjects/topic/versions')
        .reply(500, {error_code: 1, message: "failed request"});

      return getLatestVersionForSubject(registry, 'topic').catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 1 - failed request');
      });
    });

    it('reject if second get request fails', () => {
      const requestError = new Error("ECONNREFUSED");
      nock('http://test.com')
        .get('/subjects/topic/versions')
        .reply(200, [1,2]);
      nock('http://test.com')
        .get('/subjects/topic/versions/2')
        .replyWithError(requestError);

      return getLatestVersionForSubject(registry, 'topic').catch((error) => {
        expect(error).to.equal(requestError);
      });
    });

    it('reject if second get request returns with not 200', () => {
      const requestError = new Error("ECONNREFUSED");
      nock('http://test.com')
        .get('/subjects/topic/versions')
        .reply(200, [1,2]);
      nock('http://test.com')
        .get('/subjects/topic/versions/2')
        .reply(500, {error_code: 1, message: "failed request"});

      return getLatestVersionForSubject(registry, 'topic').catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', 'Schema registry error: 1 - failed request');
      });
    });

    it('resolve with schema and id if both get requests return with 200', () => {
      const requestError = new Error("ECONNREFUSED");
      nock('http://test.com')
        .get('/subjects/topic/versions')
        .reply(200, [1,2]);
      nock('http://test.com')
        .get('/subjects/topic/versions/2')
        .reply(200, {id: 1, schema});

      return getLatestVersionForSubject(registry, 'topic').then(({schema: returnedSchema, id}) => {
        expect(schema).to.eql(returnedSchema);
        expect(id).to.equal(1);
      });
    });

    it('reject if first get request returns with 401 and without registry error object', () => {
      nock('http://test.com')
        .get('/subjects/topic/versions')
        .reply(401, nonStandardErrorPayload);

      return getLatestVersionForSubject(registry, 'topic').catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', `Schema registry error: no error in response; httpStatus is 401`);
      });
    });

    it('reject if second get request returns with 401 and without registry error object', () => {
      nock('http://test.com')
        .get('/subjects/topic/versions')
        .reply(200, [1,2]);
      nock('http://test.com')
        .get('/subjects/topic/versions/2')
        .reply(500, {error_code: 1, message: "failed request"});

      return getLatestVersionForSubject(registry, 'topic').catch((error) => {
        expect(error).to.exist
          .and.be.instanceof(Error)
          .and.have.property('message', `Schema registry error: no error in response; httpStatus is 401`);
      });
    });
  });
});
