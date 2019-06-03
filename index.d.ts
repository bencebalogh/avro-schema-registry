type DecodeMessage = (obj: Buffer, parseOptions?: any) => Promise<any>;
type EncodeMessage = (
  topic: string,
  schema: any,
  msg: any,
  parseOptions?: any,
) => Promise<any>;

export interface ISchemaRegistry {
  decode: DecodeMessage;
  decodeMessage: DecodeMessage;
  encodeById: (schemaId: string, msg: any, parseOptions?: any) => Promise<any>;
  encodeKey: EncodeMessage;
  encodeMessage: EncodeMessage;
}

export declare function schema(registryUrl: string): ISchemaRegistry;

schema;
