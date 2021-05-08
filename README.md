# schematic-kafka

This work is based on [avro-schema-registry](https://github.com/bencebalogh/avro-schema-registry).

There are a couple of differences, the obvious being a pure typescript implementation.

This lib is **schema type agnostic**. It works fine with whatever protocol you may want to use, but it doesn't take care of this aspect.

## Quickstart

### Install

```
npm install avsc schematic-kafka
# or
yarn add avsc schematic-kafka
```

### Use

```
import { KafkaRegistryHelper, SchemaType } from "schematic-kafka"
import { parse, Type as AVSCInstance } from "avsc"

// create instance
const registry = new KafkaRegistryHelper({ baseUrl: "https://schemaRegistryHost:8081" })
  .withSchemaHandler(SchemaType.AVRO, (schema: string) => {
    // if you want to customize your encoder, this is where you'd do it
    const avsc: AVSCInstance = parse(schema)
    return {
      encode: (message: any) => {
        return avsc.toBuffer(message)
      },
      decode: (message: Buffer) => {
        return avsc.fromBuffer(message)
      },
    }
  })

// how to decode a message from kafka
// AVSC return parsed json, so decodedMessage this is an already object, ready to use
const decodedMessage = await registry.decode(rawMessageFromKafka)

// how to encode a message with a schema
// where
// - subject    is the kafka topic plus the (-key, -value) postfix
// - message    the actual message to send (this has to be in whatever format
//              the schema handler defined above expects in the encode-function)
// - schemaType (optional) AVRO/PROTOBUF/JSON
// - schema     (optional) serialized schema to be used
// returns      a Buffer that you can send to the kafka broker
const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
```

For more examples, take a look at `src/kafka-registry-helper.testcontainer.spec.ts`.

## How this library works

This is how a kafka message looks like when you send or receive it.

```
[ 1 byte  | 0      | 0 indicates this message is schema encoded ]
[ 4 bytes | number | schema id                                  ]
[ n bytes | msg    | protocol encoded message                   ]
```

The first byte being a zero tells us that the following four bytes contain the schema id. With this schema id we can request the schema type (AVRO, PROTOBUF or JSON) and schema (serialized representation of the schema for the respective schema type) from the schema registry.

This library can decodes whole kafka message header and then calls the appropriate decoder that you provide with the schema as argument.

## Documentation

### Client SSL authentication

This library uses node's http/https request. As such you can provide an Agent to modify your requests.

```
import { Agent } from "https"

const agent = new Agent({
  key: readFileSync("./client.key"),
  cert: readFileSync("./client.cert"),
})
new KafkaRegistryHelper({ baseUrl: "https://schemaRegistryHost:8081", agent })
...
```

### Basic authentication

```
new KafkaRegistryHelper({ baseUrl: "https://schemaRegistryHost:8081", username: "username", password: "password })

// OR

new KafkaRegistryHelper({ baseUrl: "https://username:password@schemaRegistryHost:8081" })
```

# Doc

The module exports one function only, which expects a `url` parameter, which is a Confluent Schema Registry endpoint and an optional auth object. The function returns an object .

Every method returns a Promise. Every method uses an internal cache to store already retrieved schemas and if the same id or schema is used again it won't perform another network call. Schemas are cached with their parsing options.

## Authentication with the Schema Registry

You can set username and password in the url object:

```
require('avro-schema-registry')('https://username:password@host.com:8081');
```

You can pass in an optional second parameter for the registry, with the username and password:

```
require('avro-schema-registry')('https://host.com:8081', {username: 'username', password: 'password'});
```

If both the url contains the authencation information and there's an authentication object parameter then the object takes precedence.

## feature x

TODO - document things hint: Most methods have jsdoc comments on them. Have a look.

# Dependencies

The module has just the tslib as dependency.
