import { SchemaRegistryClient, SchemaRegistryError, SchemaType } from "./schema-registry-client"
import { GenericContainer, Network, StartedNetwork, StartedTestContainer } from "testcontainers"

let zookeeperContainer: StartedTestContainer
let kafkaContainer: StartedTestContainer
let schemaRegistryContainer: StartedTestContainer
let network: StartedNetwork
let registryPort: number

beforeAll(async () => {
  const TAG = "5.5.4"

  // increase timeout to 10 minutes (docker compose from scratch will probably take longer)
  try {
    jest.setTimeout(1000 * 60 * 60 * 10)
    network = await new Network().start()

    const ZOOKEEPER_CLIENT_PORT = 2181
    zookeeperContainer = await new GenericContainer(`confluentinc/cp-zookeeper:${TAG}`)
      .withName("zookeeper")
      .withEnv("ZOOKEEPER_CLIENT_PORT", `${ZOOKEEPER_CLIENT_PORT}`)
      .withNetworkMode(network.getName())
      .start()

    const zookeeperHost = `zookeeper:${ZOOKEEPER_CLIENT_PORT}`
    kafkaContainer = await new GenericContainer(`confluentinc/cp-kafka:${TAG}`)
      .withName("kafka")
      .withNetworkMode(network.getName())
      .withEnv("KAFKA_ZOOKEEPER_CONNECT", zookeeperHost)
      .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT")
      .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://:9092")
      .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
      .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
      .withEnv("KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR", "1")
      .withExposedPorts(9092)
      .start()

    schemaRegistryContainer = await new GenericContainer(`confluentinc/cp-schema-registry:${TAG}`)
      .withNetworkMode(network.getName())
      .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
      .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", `kafka:9092`)
      .withExposedPorts(8081)
      .start()

    registryPort = schemaRegistryContainer.getMappedPort(8081)
    jest.setTimeout(15000)
  } catch (e) {
    if (zookeeperContainer) await zookeeperContainer.stop()
    if (kafkaContainer) await kafkaContainer.stop()
    if (schemaRegistryContainer) await schemaRegistryContainer.stop()
    if (network) await network.stop()

    throw e
  }
})

afterAll(async () => {
  jest.setTimeout(1000 * 60 * 60 * 10)
  if (zookeeperContainer) await zookeeperContainer.stop()
  if (kafkaContainer) await kafkaContainer.stop()
  if (schemaRegistryContainer) await schemaRegistryContainer.stop()
  if (network) await network.stop()
})

describe("SchemaRegistryClient AVRO (Black-Box Tests)", () => {
  // registryPort = 8081
  let client: SchemaRegistryClient
  const subject = "TEST_TOPIC-value"
  let testSchemaId: number

  beforeAll(async () => {
    client = new SchemaRegistryClient({
      baseUrl: `http://localhost:${registryPort}`,
    })

    const result = await client.registerSchema(subject, { schemaType: SchemaType.AVRO, schema: `{"type":"string"}` })
    testSchemaId = result.id
    expect(testSchemaId).toBeGreaterThan(0)
  })

  afterAll(async () => {
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
    await expect(result).rejects.toThrowError(new SchemaRegistryError(40403, "Subject 'unknown_subject' not found"))
  })
})

describe("SchemaRegistryClient PROTOBUF (Black-Box Tests)", () => {
  let client: SchemaRegistryClient
  const subject = "TEST_PROTOBUF_TOPIC-value"
  let testSchemaId: number
  const testSchema = `syntax = "proto3";
  package com.acme;
  
  
  message MyRecord {
    string f1 = 1;
  }`

  beforeAll(async () => {
    client = new SchemaRegistryClient({
      baseUrl: `http://localhost:${registryPort}`,
    })

    const result = await client.registerSchema(subject, {
      schemaType: SchemaType.PROTOBUF,
      schema: testSchema,
    })
    testSchemaId = result.id
    expect(testSchemaId).toBeGreaterThan(0)
  })

  afterAll(async () => {
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
