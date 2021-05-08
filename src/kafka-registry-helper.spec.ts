import * as nock from "nock"
import { parse, Type as AVSCInstance, Type } from "avsc"

import { KafkaRegistryHelper } from "./kafka-registry-helper"
import { SchemaType } from "./schema-registry-client"

describe("KafkaRegistryHelper with AVRO", () => {
  const host = "http://localhost:8081"
  const subject = "REGISTRY_TEST_SUBJECT"
  let registry: KafkaRegistryHelper = undefined
  const message = { hello: "world" }
  const type = Type.forValue(message)
  const schema = type.toString()
  parse(Type.forValue(message).toString())

  beforeAll(() => {
    // Create registry helper instance and attach avro schema handler
    registry = new KafkaRegistryHelper({ baseUrl: host }).withSchemaHandler(SchemaType.AVRO, (schema: string) => {
      const avsc: AVSCInstance = parse(schema) // could add all kinds of configurations here
      return {
        encode: (message: any) => {
          return avsc.toBuffer(message)
        },
        decode: (message: Buffer) => {
          return avsc.fromBuffer(message)
        },
      }
    })
  })

  it("encodes and decodes AVRO message", async () => {
    const schemaId = 1

    nock(host).post(`/subjects/${subject}`).once().reply(404, { error_code: 404, message: "no" })
    nock(host)
      .post(`/subjects/${subject}/versions`)
      .once()
      .reply(200, (_uri: string, request: string) => {
        return { id: schemaId, ...JSON.parse(request) }
      })
    nock(host).get(`/schemas/ids/${schemaId}`).once().reply(200, { schema: schema })

    // nothing is cached here
    const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
    const decodeResult = await registry.decode(encodeResult)
    expect(decodeResult).toEqual(message)

    // now that the schema is registered the registry would return the schema for check
    nock(host).post(`/subjects/${subject}`).once().reply(200, { schema, id: schemaId })
    const encodeResultB = await registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
    const decodeResultB = await registry.decode(encodeResultB)
    expect(decodeResultB).toEqual(message)

    // lastly, everything should be cached 🤙
    const encodeResultC = await registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
    const decodeResultC = await registry.decode(encodeResultC)
    expect(decodeResultC).toEqual(message)
  })
})

describe("KafkaRegistryHelper with PROTOBUF", () => {
  const host = "http://localhost:8081"
  const subject = "REGISTRY_TEST_SUBJECT_PROTOBUF"
  let registry: KafkaRegistryHelper = undefined
  const message = { hello: "world" }
  const schema = "ADD PROTOBUF SCHEMA HERE"

  beforeAll(() => {
    // Create registry helper instance and attach avro schema handler
    registry = new KafkaRegistryHelper({ baseUrl: host }).withSchemaHandler(SchemaType.PROTOBUF, (schema: string) => {
      // TODO: this is where some PROTOBUF magic would happen
      return {
        encode: (message: any) => {
          // TODO: this is really not PROTOBUF
          return Buffer.from(JSON.stringify(message))
        },
        decode: (message: Buffer) => {
          // TODO: more like PROTOBUG
          return JSON.parse(message.toString())
        },
      }
    })
  })

  it("encodes and decodes PROTOBUF message", async () => {
    const schemaId = 1

    nock(host).post(`/subjects/${subject}`).once().reply(404, { error_code: 404, message: "no" })
    nock(host)
      .post(`/subjects/${subject}/versions`)
      .once()
      .reply(200, (_uri: string, request: string) => {
        return { id: schemaId, ...JSON.parse(request) }
      })
    nock(host).get(`/schemas/ids/${schemaId}`).once().reply(200, { schema: schema, schemaType: SchemaType.PROTOBUF })

    // nothing is cached here
    const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.PROTOBUF, schema)
    const decodeResult = await registry.decode(encodeResult)
    expect(decodeResult).toEqual(message)

    // now that the schema is registered the registry would return the schema for check
    nock(host).post(`/subjects/${subject}`).once().reply(200, { schema, id: schemaId })
    const encodeResultB = await registry.encodeForSubject(subject, message, SchemaType.PROTOBUF, schema)
    const decodeResultB = await registry.decode(encodeResultB)
    expect(decodeResultB).toEqual(message)

    // lastly, everything should be cached 🤙
    const encodeResultC = await registry.encodeForSubject(subject, message, SchemaType.PROTOBUF, schema)
    const decodeResultC = await registry.decode(encodeResultC)
    expect(decodeResultC).toEqual(message)
  })
})
