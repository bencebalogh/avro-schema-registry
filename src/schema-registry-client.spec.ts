import { readFileSync } from "fs"
import * as http from "http"
import { Agent } from "https"
import * as nock from "nock"
import { version } from "typescript"

import { SchemaApiClientConfiguration, SchemaRegistryClient } from "./schema-registry-client"

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
      expect(result).rejects.toEqual(requestError)
    })

    it("reject if post request returns with not 200", async () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").post("/subjects/topic/versions").reply(500, mockError)

      const result = schemaApi.registerSchema("topic", schemaPayload)
      expect(result).rejects.toEqual(
        expect.objectContaining({ message: `Schema registry error: ${JSON.stringify(mockError)}` })
      )
    })

    it("resolve with schema id if post request returns with 200", async () => {
      nock("http://test.com").post("/subjects/topic/versions").reply(200, { id: 1 })

      const result = schemaApi.registerSchema("topic", schemaPayload)
      expect(result).resolves.toEqual(1)
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

      const result = schemaApi.getSchemaById(1)
      expect(result).rejects.toBeInstanceOf(Error)
      expect(result).rejects.toEqual(
        expect.objectContaining({ message: `Schema registry error: ${JSON.stringify(mockError)}` })
      )
    })

    it("resolve with schema if get request returns with 200", () => {
      nock("http://test.com").get("/schemas/ids/1").reply(200, { id: 1, schema })

      const result = schemaApi.getSchemaById(1)
      expect(result).resolves.toEqual(schema)
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
    // TODO: Add testcontainers
    it("reject if first get request fails", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/subjects/topic/versions").replyWithError(requestError)

      const result = schemaApi.getLatestVersionForSubject("topic")
      expect(result).rejects.toEqual(requestError)
    })

    it("reject if first get request returns with not 200", () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").get("/subjects/topic/versions").reply(500, mockError)

      const result = schemaApi.getLatestVersionForSubject("topic")
      expect(result).rejects.toBeInstanceOf(Error)
      expect(result).rejects.toEqual(
        expect.objectContaining({ message: `Schema registry error: ${JSON.stringify(mockError)}` })
      )
    })

    it("reject if second get request fails", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/subjects/topic/versions").reply(200, [1, 2])
      nock("http://test.com").get("/subjects/topic/versions/2").replyWithError(requestError)

      const result = schemaApi.getLatestVersionForSubject("topic")
      expect(result).rejects.toEqual(requestError)
    })

    it("reject if second get request returns with not 200", () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").get("/subjects/topic/versions").reply(200, [1, 2])
      nock("http://test.com").get("/subjects/topic/versions/2").reply(500, mockError)

      const result = schemaApi.getLatestVersionForSubject("topic")
      expect(result).rejects.toBeInstanceOf(Error)
      expect(result).rejects.toEqual(
        expect.objectContaining({ message: `Schema registry error: ${JSON.stringify(mockError)}` })
      )
    })

    it("resolve with schema and id if both get requests return with 200", () => {
      nock("http://test.com").get("/subjects/topic/versions").reply(200, [1, 2])
      nock("http://test.com").get("/subjects/topic/versions/2").reply(200, { id: 1, schema })

      const result = schemaApi.getLatestVersionForSubject("topic")
      expect(result).resolves.toEqual({ schema, id: 1 })
    })
  })
})

xdescribe("SchemaRegistryClient (Black-Box Tests)", () => {
  const client = new SchemaRegistryClient({
    baseUrl: "http://localhost:8081",
  })
  const subject = "TEST_TOPIC-value"
  let testSchemaId: number

  beforeEach(async () => {
    const result = await client.registerSchema(subject, { schemaType: "AVRO", schema: `{"type":"string"}` })
    testSchemaId = result.id
    expect(testSchemaId).toBeGreaterThan(0)
  })

  afterEach(async () => {
    // soft delete
    const softDeletedIds = await client.deleteSubject(subject)

    // perma delete
    const permanentDeletedIds = await client.deleteSubject(subject, true)
    expect(softDeletedIds).toEqual(permanentDeletedIds)
  })

  it("should get schema type", async () => {
    const result = await client.getSchemaTypes()
    expect(result).toEqual(["JSON", "PROTOBUF", "AVRO"])
  })

  it("schema by id", async () => {
    const schema = await client.getSchemaById(testSchemaId)
    expect(schema.length).toBeGreaterThan(0)
  })

  it("list subjects", async () => {
    const availableSubjects = await client.listSubjects()
    expect(availableSubjects).toEqual([subject])
  })

  it("versions by id", async () => {
    const versions = await client.listVersionsForId(testSchemaId)
    expect(versions).toHaveLength(1)
  })

  it("versions by subject & get schema for subject and version", async () => {
    const versions = await client.listVersionsForSubject(subject)
    expect(versions).toHaveLength(1)

    const schema = await client.getSchemaForSubjectAndVersion(subject, versions[0])
    expect(schema.id).toEqual(testSchemaId)
    expect(schema.version).toEqual(versions[0])
    expect(schema.subject).toEqual(subject)
    expect(schema.schema.length).toBeGreaterThan(0)

    const rawSchema = await client.getRawSchemaForSubjectAndVersion(subject, versions[0])
    expect(rawSchema).toEqual(schema.schema)
  })

  it("should get the latest schema version for a subject", async () => {
    const schema = await client.getLatestVersionForSubject(subject)
    expect(schema.id).toEqual(testSchemaId)
    expect(schema.version).toBeGreaterThan(0)
    expect(schema.subject).toEqual(subject)
    expect(schema.schema.length).toBeGreaterThan(0)
  })
})
