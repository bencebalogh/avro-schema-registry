import * as avsc from "avsc"

import { SchemaCache } from "./schema-cache"
import { SchemaApiClient } from "./schema-api-client"

export class Schemas {
  private cache: SchemaCache

  constructor(private readonly api: SchemaApiClient) {
    this.cache = new SchemaCache()
  }

  async getId(subject, schema, parsedSchema): Promise<string> {
    let schemaId = this.cache.getBySchema(schema)
    if (!schemaId) {
      schemaId = await this.api.pushSchema(subject, schema)
      this.cache.setBySchema(schema, schemaId)
    }

    this.cache.setById(schemaId, Promise.resolve(parsedSchema))
    this.cache.setBySchema(schema, Promise.resolve(schemaId))

    return schemaId
  }

  async getSchema(id, parseOptions) {
    let schemaPromise = this.cache.getById(id)
    if (!schemaPromise) {
      schemaPromise = this.api.getSchemaById(id)
      this.cache.setById(schemaPromise, undefined)
    }

    return schemaPromise.then((schema) => {
      const parsedSchema = avsc.parse(schema, parseOptions)
      if (schemaPromise != Promise.resolve(parsedSchema)) {
        this.cache.setById(id, Promise.resolve(parsedSchema))
        this.cache.setBySchema(schema, Promise.resolve(id))
      }

      return parsedSchema
    })
  }

  async getSchemaAndId(topic, parseOptions) {
    let promise = this.cache.getByName(topic)
    if (!promise) {
      promise = this.api.getLatestVersionForSubject(topic)
      this.cache.setByName(topic, promise)
    }

    return promise.then(({ schema, id }) => {
      const parsedSchema = avsc.parse(schema, parseOptions)
      if (promise != Promise.resolve({ schema, id })) {
        this.cache.setByName(topic, Promise.resolve({ schema, id }))
        this.cache.setById(id, Promise.resolve(parsedSchema))
        this.cache.setBySchema(schema, Promise.resolve(id))
      }
      return { parsedSchema, id }
    })
  }

  async decodeMessage(msg, parseOptions) {
    if (msg.readUInt8(0) !== 0) {
      return Promise.reject(new Error(`Message doesn't contain schema identifier byte.`))
    }
    const id = msg.readUInt32BE(1)
    const buffer = msg.slice(5)

    let schemaPromise = this.cache.getById(id)

    if (!schemaPromise) {
      schemaPromise = this.api.getSchemaById(id)
      this.cache.setById(schemaPromise, undefined)
    }

    return schemaPromise.then((schema) => {
      // if schema returned from a cached parsedSchema already don't parse it again
      // if it's not cached parse and cache the parsed schema
      if (!(schema instanceof avsc.Type)) {
        const parsedSchema = avsc.parse(schema, parseOptions)
        this.cache.setById(id, Promise.resolve(parsedSchema))
        this.cache.setBySchema(JSON.stringify(schema), Promise.resolve(id))
        return parsedSchema.fromBuffer(buffer)
      }

      return schema.fromBuffer(buffer)
    })
  }

  async encodeKey(topic, schema, msg, parseOptions = null) {
    try {
      const parsedSchema = avsc.parse(schema, parseOptions)
      const id = await this.getId(`${topic}-key`, schema, parsedSchema)
      return this.encodeFunction(msg, id, parsedSchema)
    } catch (e) {
      return Promise.reject(e)
    }
  }

  async encodeMessage(topic, schema, msg, parseOptions = null): Promise<unknown> {
    const parsedSchema = avsc.parse(schema, parseOptions)
    const id = await this.getId(`${topic}-value`, schema, parsedSchema)
    return this.encodeFunction(msg, id, parsedSchema)
  }

  async encodeById(id, msg, parseOptions = null) {
    const schema = await this.getSchema(id, parseOptions)
    return this.encodeFunction(msg, id, schema)
  }

  async encodeMessageByTopicName(topic, msg, parseOptions = null) {
    const { parsedSchema, id } = await this.getSchemaAndId(topic, parseOptions)
    return this.encodeFunction(msg, id, parsedSchema)
  }

  async getSchemaByTopicName(topic, parseOptions = null) {
    return this.getSchemaAndId(topic, parseOptions)
  }

  encodeFunction(msg, schemaId, schema) {
    const encodedMessage = schema.toBuffer(msg)

    const message = Buffer.alloc(encodedMessage.length + 5)
    message.writeUInt8(0)
    message.writeUInt32BE(schemaId, 1)
    encodedMessage.copy(message, 5)

    return message
  }
}
