declare namespace schema {
    export type DecodeMessage = <T>(obj: Buffer, parseOptions?: any) => Promise<T>;
    export type EncodeById = (schemaId: number, msg: any, parseOptions?: any) => Promise<Buffer>;
    export type EncodeBySchema = (topic: string, schema: any, msg: any, parseOptions?: any) => Promise<Buffer>;

    export interface ISchemaRegistry {
        decode: DecodeMessage;
        decodeMessage: DecodeMessage;
        encodeById: EncodeById;
        encodeKey: EncodeBySchema;
        encodeMessage: EncodeBySchema;
    }
}

export = schema;
declare function schema(registryUrl: string) : schema.ISchemaRegistry;
