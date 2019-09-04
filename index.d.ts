declare namespace schema {
    export type DecodeMessage = <T>(obj: Buffer, parseOptions?: any) => Promise<T>;
    export type EncodeById = (schemaId: number, msg: any, parseOptions?: any) => Promise<Buffer>;
    export type EncodeBySchema = (topic: string, schema: any, msg: any, parseOptions?: any) => Promise<Buffer>;
    export type EncodeByTopicName = (topic: string, msg: any, parseOptions?: any) => Promise<Buffer>;
    export type GetSchemaByTopicName = (topic: string, parseOptions?: any) => Promise<{id: number, parsedSchema: any}>;

    export interface ISchemaRegistry {
        decode: DecodeMessage;
        decodeMessage: DecodeMessage;
        encodeById: EncodeById;
        encodeKey: EncodeBySchema;
        encodeMessage: EncodeBySchema;
        encodeMessageByTopicName: EncodeByTopicName
        getSchemaByTopicName: GetSchemaByTopicName
    }
}

export = schema;
declare function schema(registryUrl: string) : schema.ISchemaRegistry;
