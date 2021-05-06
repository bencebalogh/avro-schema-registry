import { kafkaEncode, kafkaDecode } from "./kafka-helper"
import { SchemaRegistryClient, SchemaRegistryError, SchemaType } from "./schema-registry-client"

/* 
TODO:
- caching of schemas
*/

/**
 * This is a "convenient" method to provide different schema
 * handlers (i.e. AVRO, JSON, PROTOBUF) into the ??
 */
type SchemaHandlerFactory = (
  schema: string
) => {
  decode(message: Buffer): any
  encode(message: any): Buffer
}

export class KafkaRegistryHelper {
  schemaHandlers: Partial<Record<SchemaType, SchemaHandlerFactory>> = {}

  constructor(private readonly schemaRegistry: SchemaRegistryClient) {}

  /**
   * Checks whether a schema is available in the schema registry, if not tries to register it
   * @param subject
   * @param schemaType
   * @param schema
   * @param references
   * @returns schema string and schemaId
   */
  private async makeSureSchemaIsRegistered(
    subject: string,
    schemaType: SchemaType = SchemaType.AVRO,
    schema?: string,
    references?: any
  ): Promise<{ id: number; schema: string }> {
    // register schema or fetch schema
    let registrySchemaId = undefined
    let registrySchema = undefined
    if (schema) {
      try {
        const checkSchemaResult = await this.schemaRegistry.checkSchema(subject, { schemaType, schema, references })
        registrySchemaId = checkSchemaResult.id
        registrySchema = checkSchemaResult.schema
      } catch (e) {
        if (e instanceof SchemaRegistryError && e.errorCode === 404) {
          // schema does not exist, need to create it
          const registerSchemaResult = await this.schemaRegistry.registerSchema(subject, {
            schemaType,
            schema,
            references,
          })
          registrySchemaId = registerSchemaResult.id
          registrySchema = registerSchemaResult.schema
        } else {
          throw e
        }
      }
    } else {
      const { id, schema } = await this.schemaRegistry.getLatestVersionForSubject(subject)
      registrySchemaId = id
      registrySchema = schema
    }

    return { id: registrySchemaId, schema: registrySchema }
  }

  async encodeForId(schemaId: number, message: string, schemaType: SchemaType = SchemaType.AVRO) {
    if (!this.schemaHandlers[schemaType]) {
      throw new Error(`No protocol handler for protocol ${schemaType}`)
    }

    const { schema: registrySchema, schemaType: registrySchemaType } = await this.schemaRegistry.getSchemaById(schemaId)

    if (schemaType !== registrySchemaType) {
      throw new Error("Mismatch between schemaType argument and schema registry schemaType")
    }

    return this.encodeMessage(schemaId, message, schemaType, registrySchema)
  }

  async encodeForSubject(
    subject: string,
    message: string,
    schemaType: SchemaType = SchemaType.AVRO,
    schema?: string,
    references?: any
  ) {
    if (!this.schemaHandlers[schemaType]) {
      throw new Error(`No protocol handler for protocol ${schemaType}`)
    }

    const { id, schema: registrySchema } = await this.makeSureSchemaIsRegistered(
      subject,
      schemaType,
      schema,
      references
    )

    return this.encodeMessage(id, message, schemaType, registrySchema)
  }

  private encodeMessage(schemaId: number, message: string, schemaType: SchemaType, registrySchema: string) {
    const schemaHandler = this.schemaHandlers[schemaType](registrySchema)

    const encodedMessage = schemaHandler.encode(message)

    return kafkaEncode(schemaId, encodedMessage)
  }

  async decode(message: Buffer) {
    const { schemaId, payload } = kafkaDecode(message)
    if (schemaId) {
      const { schema, schemaType } = await this.schemaRegistry.getSchemaById(schemaId)
      // if schemaType isn't provided, use avro (it's the default)
      return this.schemaHandlers[schemaType ?? SchemaType.AVRO](schema).decode(payload)
    } else {
      return payload
    }
  }
}
