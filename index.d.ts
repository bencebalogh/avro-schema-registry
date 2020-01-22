declare namespace schema {
    export type DecodeMessage = <T>(obj: Buffer, parseOptions?: any) => Promise<T>;
    export type EncodeById = (schemaId: number, msg: any, parseOptions?: any) => Promise<Buffer>;
    export type EncodeBySchema = (topic: string, schema: any, msg: any, parseOptions?: any) => Promise<Buffer>;
    export type EncodeKeyBySchema = (strategy: string, schema: any, msg: any, parseOptions?: any) => Promise<Buffer>;
    export type EncodeMessageBySchema = (strategy: string, schema: any, msg: any, parseOptions?: any) => Promise<Buffer>;
    export type EncodeByTopicName = (topic: string, msg: any, parseOptions?: any) => Promise<Buffer>;
    export type EncodeByTopicRecordName = (topic: string, recordName: string, msg: any, parseOptions?: any) => Promise<Buffer>;
    export type EncodeByRecordName = (recordName: string, msg: any, parseOptions?: any) => Promise<Buffer>;
    export type GetSchemaByTopicName = (topic: string, parseOptions?: any) => Promise<{ id: number, parsedSchema: any }>;
    export type GetSchemaByTopicRecordName = (topic: string, recordName: string, parseOptions?: any) => Promise<{ id: number, parsedSchema: any }>;
    export type GetSchemaByRecordName = (recordName: string, parseOptions?: any) => Promise<{ id: number, parsedSchema: any }>;

    export interface ISchemaRegistry {
        decode: DecodeMessage;
        decodeMessage: DecodeMessage;
        encodeById: EncodeById;
        encodeKey: EncodeBySchema;
        encodeMessage: EncodeBySchema;
        encodeKeyBySchema: EncodeKeyBySchema;
        encodeMessageBySchema: EncodeMessageBySchema;
        encodeMessageByTopicName: EncodeByTopicName;
        encodeMessageByTopicRecordName: EncodeByTopicRecordName;
        encodeMessageByRecordName: EncodeByRecordName;
        getSchemaByTopicName: GetSchemaByTopicName;
        getSchemaByTopicRecordName: GetSchemaByTopicRecordName;
        getSchemaByRecordName: GetSchemaByRecordName;
        Strategy: {
            TopicNameStrategy: string;
            TopicRecordNameStrategy: string;
            RecordNameStrategy: string;
        };
    }
}

export = schema;
declare function schema(registryUrl: string) : schema.ISchemaRegistry;
