import { parse, Type as AVSCInstance } from "avsc"
import { Field, Root, Type } from "protobufjs"

import { KafkaRegistryHelper } from "./kafka-registry-helper"
import { SchemaRegistryClient, SchemaType } from "./schema-registry-client"

const message = "test message"

describe("KafkaRegistryHelper (AVRO)", () => {
  const subject = "REGISTRY_TEST_SUBJECT"
  const schema = { type: "string" }
  const message = "test message"

  const registry = new KafkaRegistryHelper(new SchemaRegistryClient({ baseUrl: "http://localhost:8081" }))
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

  const registryClient = new SchemaRegistryClient({ baseUrl: "http://localhost:8081" })
  const registry = new KafkaRegistryHelper(registryClient)

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
    await registryClient.deleteSubject(subject)
    await registryClient.deleteSubject(subject, true)
  })

  it("encodes and decodes message", async () => {
    const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.PROTOBUF, schema)
    console.log(encodeResult)
    const decodeResult = await registry.decode(encodeResult)
    console.log(decodeResult)
    expect(decodeResult.toString()).toEqual(message)
  })
})
