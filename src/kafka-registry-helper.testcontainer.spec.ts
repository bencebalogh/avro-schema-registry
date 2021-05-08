import { parse, Type as AVSCInstance } from "avsc"
import { Field, Type } from "protobufjs"
import { StartedTestContainer, StartedNetwork, Network, GenericContainer } from "testcontainers"

import { KafkaRegistryHelper } from "./kafka-registry-helper"
import { SchemaType } from "./schema-registry-client"

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

describe("KafkaRegistryHelper (AVRO)", () => {
  const subject = "REGISTRY_TEST_SUBJECT"
  const schema = { type: "string" }
  const message = "test message"

  const registry = new KafkaRegistryHelper({ baseUrl: "http://localhost:8081" })
  registry.schemaHandlers[SchemaType.AVRO] = (schema: string) => {
    const avsc: AVSCInstance = parse(schema) // cound add all kinds of configurations here
    return {
      encode: (message: string) => {
        return avsc.toBuffer(message)
      },
      decode: (message: Buffer) => {
        return avsc.fromBuffer(message)
      },
    }
  }

  it("encodes and decodes AVRO message", async () => {
    const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.AVRO, JSON.stringify(schema))
    console.log(encodeResult)
    const decodeResult = await registry.decode(encodeResult)
    console.log(decodeResult)
    expect(decodeResult.toString()).toEqual(message)
  })
})

describe("KafkaRegistryHelper (PROTOBUF)", () => {
  const subject = "REGISTRY_TEST_PROTOBUF_SUBJECT-value"
  const schema = `syntax = "proto3";
  package com.example;

  message KafkaTestMessage {
      string is_cereal_soup = 1;
  }`
  const message = JSON.stringify({
    isCerealSoup: "yes",
  })
  const protobufType = new Type("KafkaTestMessage").add(new Field("isCerealSoup", 1, "string"))

  const registry = new KafkaRegistryHelper({ baseUrl: "http://localhost:8081" })

  registry.schemaHandlers[SchemaType.PROTOBUF] = (schema: string) => {
    // TODO this is where the schema would be used to construct the protobuf type
    return {
      encode: (message: string) => {
        return protobufType.encode(protobufType.fromObject(JSON.parse(message))).finish() as Buffer
      },
      decode: (message: Buffer) => {
        return JSON.stringify(protobufType.decode(message).toJSON())
      },
    }
  }

  afterEach(async () => {
    await registry.schemaRegistryClient.deleteSubject(subject)
    await registry.schemaRegistryClient.deleteSubject(subject, true)
  })

  it("encodes and decodes message", async () => {
    const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.PROTOBUF, schema)
    console.log(encodeResult)
    const decodeResult = await registry.decode(encodeResult)
    console.log(decodeResult)
    expect(decodeResult.toString()).toEqual(message)
  })
})
