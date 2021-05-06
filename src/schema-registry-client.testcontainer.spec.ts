import { SchemaRegistryClient, SchemaRegistryError, SchemaType } from "./schema-registry-client"

// TODO: Add testcontainers

describe("SchemaRegistryClient AVRO (Black-Box Tests)", () => {
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
    expect(schema.schema.length).toBeGreaterThan(0)
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

  it("can check a schema", async () => {
    const result = await client.checkSchema(subject, { schemaType: SchemaType.AVRO, schema: `{"type":"string"}` })
    // this should return the id for the existing schema
    expect(result.id).toEqual(testSchemaId)
  })

  it("returns error for unknown schema during check", async () => {
    const result = await client.checkSchema("unknown_subject", {
      schemaType: SchemaType.AVRO,
      schema: `{"type":"string"}`,
    })
    expect(result).toBeUndefined()
  })
})

describe("SchemaRegistryClient PROTOBUF (Black-Box Tests)", () => {
  const client = new SchemaRegistryClient({
    baseUrl: "http://localhost:8081",
  })
  const subject = "TEST_PROTOBUF_TOPIC-value"
  let testSchemaId: number
  const testSchema = `syntax = "proto3";
  package com.acme;
  
  
  message MyRecord {
    string f1 = 1;
  }`

  beforeEach(async () => {
    const result = await client.registerSchema(subject, {
      schemaType: SchemaType.PROTOBUF,
      schema: testSchema,
    })
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

  it("should return schemaType for schemas that don't use AVRO", async () => {
    const result = await client.getLatestVersionForSubject(subject)
    expect(result.schemaType).toEqual(SchemaType.PROTOBUF)
  })

  it("has no clue about the schemaType", async () => {
    const result = await client.getSchemaById(testSchemaId)
    expect(result.schemaType).toEqual(SchemaType.PROTOBUF)
  })
})
