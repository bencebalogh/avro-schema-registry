import * as nock from "nock"

import {
  SchemaApiClientConfiguration,
  SchemaRegistryClient,
  SchemaRegistryError,
  SchemaType,
} from "./schema-registry-client"

describe("SchemaRegistryClient (Integration Tests)", () => {
  const schema = { type: "string" }
  const schemaPayload = { schemaType: "AVRO", schema: JSON.stringify(schema) }
  const apiClientOptions: SchemaApiClientConfiguration = {
    baseUrl: "http://test.com/",
  }

  const schemaApi = new SchemaRegistryClient(apiClientOptions)

  afterEach(() => {
    nock.cleanAll()
  })

  describe("registerSchema", () => {
    it("reject if post request fails", async () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").post("/subjects/topic/versions").replyWithError(requestError)

      const result = schemaApi.registerSchema("topic", schemaPayload)
      await expect(result).rejects.toEqual(requestError)
    })

    it("reject if post request returns with not 200", async () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").post("/subjects/topic/versions").reply(500, mockError)

      const result = schemaApi.registerSchema("topic", schemaPayload)
      await expect(result).rejects.toThrowError(new SchemaRegistryError(mockError.error_code, mockError.message))
    })

    it("resolve with schema id if post request returns with 200", async () => {
      nock("http://test.com").post("/subjects/topic/versions").reply(200, { id: 1 })

      const result = schemaApi.registerSchema("topic", schemaPayload)
      await expect(result).resolves.toEqual({ id: 1 })
    })
  })

  describe("getSchemaById", () => {
    it("reject if get request fails", async () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/schemas/ids/1").replyWithError(requestError)

      const result = schemaApi.getSchemaById(1)
      await expect(result).rejects.toBeInstanceOf(Error)
      await expect(result).rejects.toEqual(requestError)
    })

    it("reject if get request returns with not 200", async () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").get("/schemas/ids/1").reply(500, mockError)

      const result = schemaApi.getSchemaById(1)
      await expect(result).rejects.toThrowError(new SchemaRegistryError(mockError.error_code, mockError.message))
    })

    it("resolve with schema if get request returns with 200", async () => {
      const mockPayload = { id: 1, schema }
      nock("http://test.com").get("/schemas/ids/1").reply(200, mockPayload)

      const result = schemaApi.getSchemaById(1)
      await expect(result).resolves.toEqual(mockPayload)
    })
  })

  describe("getSchemaTypes", () => {
    // TODO
  })

  describe("listSubjects", () => {
    // TODO
  })

  describe("listVersionsForId", () => {
    // TODO
  })

  describe("listVersionsForSubject", () => {
    // TODO
  })

  describe("deleteSubject", () => {
    // TODO
  })

  describe("getSchemaForSubjectAndVersion", () => {
    // TODO
  })

  describe("getSchemaForSubjectAndId", () => {
    // TODO
  })

  describe("checkSchema", () => {
    // TODO
  })

  describe("getLatestVersionForSubject", () => {
    it("reject if first get request fails", async () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/subjects/topic/versions/latest").replyWithError(requestError)

      const result = schemaApi.getLatestVersionForSubject("topic")
      await expect(result).rejects.toEqual(requestError)
    })

    it("reject if first get request returns with not 200", async () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").get("/subjects/topic/versions/latest").reply(500, mockError)

      const result = schemaApi.getLatestVersionForSubject("topic")
      await expect(result).rejects.toThrowError(new SchemaRegistryError(mockError.error_code, mockError.message))
    })

    it("resolve with schema and id if both get requests return with 200", async () => {
      nock("http://test.com").get("/subjects/topic/versions/latest").reply(200, { id: 1, schema })

      const result = schemaApi.getLatestVersionForSubject("topic")
      await expect(result).resolves.toEqual({ schema, id: 1 })
    })
  })
})
