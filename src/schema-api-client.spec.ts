import * as http from "http"
import * as nock from "nock"

import { SchemaApiClientConfiguration, SchemaApiClient } from "./schema-api-client"

describe("http-calls", () => {
  const schema = { type: "string" }
  const apiClientOptions: SchemaApiClientConfiguration = {
    baseUrl: "http://test.com/",
  }

  const schemaApi = new SchemaApiClient(apiClientOptions)

  afterEach(() => {
    nock.cleanAll()
  })

  describe("pushSchema", () => {
    it("reject if post request fails", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").post("/subjects/topic/versions").replyWithError(requestError)

      return schemaApi.pushSchema("topic", schema).catch((error) => {
        expect(error).toEqual(requestError)
      })
    })

    it("reject if post request returns with not 200", () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").post("/subjects/topic/versions").reply(500, mockError)

      return schemaApi.pushSchema("topic", schema).catch((error) => {
        expect(error).toBeInstanceOf(Error)
        expect(error).toEqual(
          expect.objectContaining({ message: `Schema registry error: ${JSON.stringify(mockError)}` })
        )
      })
    })

    it("resolve with schema id if post request returns with 200", () => {
      nock("http://test.com").post("/subjects/topic/versions").reply(200, { id: 1 })

      return schemaApi.pushSchema("topic", schema).then((id) => {
        expect(id).toEqual(1)
      })
    })
  })

  describe("getSchemaById", () => {
    it("reject if get request fails", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/schemas/ids/1").replyWithError(requestError)

      return schemaApi.getSchemaById(1).catch((error) => {
        expect(error).toEqual(requestError)
      })
    })

    it("reject if get request returns with not 200", () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").get("/schemas/ids/1").reply(500, mockError)

      return schemaApi.getSchemaById(1).catch((error) => {
        expect(error).toBeInstanceOf(Error)
        expect(error).toEqual(
          expect.objectContaining({ message: `Schema registry error: ${JSON.stringify(mockError)}` })
        )
      })
    })

    it("resolve with schema if get request returns with 200", () => {
      nock("http://test.com").get("/schemas/ids/1").reply(200, { id: 1, schema })

      return schemaApi.getSchemaById(1).then((returnedSchema) => {
        expect(returnedSchema).toEqual(schema)
      })
    })
  })

  describe("getLatestVersionForSubject", () => {
    it("reject if first get request fails", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/subjects/topic/versions").replyWithError(requestError)

      return schemaApi.getLatestVersionForSubject("topic").catch((error) => {
        expect(error).toEqual(requestError)
      })
    })

    it("reject if first get request returns with not 200", () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").get("/subjects/topic/versions").reply(500, mockError)

      return schemaApi.getLatestVersionForSubject("topic").catch((error) => {
        expect(error).toBeInstanceOf(Error)
        expect(error).toEqual(
          expect.objectContaining({ message: `Schema registry error: ${JSON.stringify(mockError)}` })
        )
      })
    })

    it("reject if second get request fails", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/subjects/topic/versions").reply(200, [1, 2])
      nock("http://test.com").get("/subjects/topic/versions/2").replyWithError(requestError)

      return schemaApi.getLatestVersionForSubject("topic").catch((error) => {
        expect(error).toEqual(requestError)
      })
    })

    it("reject if second get request returns with not 200", () => {
      const requestError = new Error("ECONNREFUSED")
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").get("/subjects/topic/versions").reply(200, [1, 2])
      nock("http://test.com").get("/subjects/topic/versions/2").reply(500, mockError)

      return schemaApi.getLatestVersionForSubject("topic").catch((error) => {
        expect(error).toBeInstanceOf(Error)
        expect(error).toEqual(
          expect.objectContaining({ message: `Schema registry error: ${JSON.stringify(mockError)}` })
        )
      })
    })

    it("resolve with schema and id if both get requests return with 200", () => {
      nock("http://test.com").get("/subjects/topic/versions").reply(200, [1, 2])
      nock("http://test.com").get("/subjects/topic/versions/2").reply(200, { id: 1, schema })

      //@ts-ignore
      return schemaApi.getLatestVersionForSubject("topic").then(({ schema: returnedSchema, id }) => {
        expect(schema).toEqual(returnedSchema)
        expect(id).toEqual(1)
      })
    })
  })
})
