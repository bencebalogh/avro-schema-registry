# avro-schema-registry

Confluent Schema Registry implementation to easily serialize and deserialize kafka messages with only one peer depencency.

# Quickstart

```
const registry = require('avro-schema-registry')('https://host.com:8081');

const schema = {type: 'string'};
const message = 'test message';

registry.encodeMessage('topic', schema, message)
  .then((msg) => {
    console.log(msg);   // <Buffer 00 00 00 00 01 18 74 65 73 74 20 6d 65 73 73 61 67 65>

    return registry.decodeMessage(msg);
  })
  .then((msg) => {
    console.log(msg);  // test message
  });
```

# Install

```
npm install avsc // if not already installed
npm install avro-schema-registry
```

# Doc

The module exports one function only, which expects a `url` parameter, which is a Confluent Schema Registry endpoint. The function returns an object with two methods.

Both methods return a Promise.

Both methods use an internal cache to store already retrieved schemas and if the same schema id is requested again it won't perform another network call to retrieve the schema.

## decodeMessage
Parameters:
- msg: message object to decode

Decodes an avro encoded buffer into a javascript object.

## encodeMessage
Parameters:
- topic: the topic to register the schema, if it doesn't exist already in the registry. The schema will be put under the subject `${topic}-value`
- schema: object representing an avro schema
- msg: message object to be encoded

Decodes a message object into an avro encoded buffer.


# Peer dependency

The module has no dependency, only one peer dependency: [avsc](https://github.com/mtth/avsc)
