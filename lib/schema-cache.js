'use strict';

class SchemaCache {
  constructor() {
    this.schemasById = new Map();
    this.schemasBySchema = new Map();
    this.schemasByName = new Map();
  }

  set(schemaId, schema) {
    this.schemasById.set(schemaId, schema);
    if (!(schema instanceof Promise)) {
      this.schemasBySchema.set(JSON.stringify(schema), schemaId);
      this.schemasByName.set(schema.name, schemaId);
    }
    return schemaId;
  }

  getById(schemaId) {
    return this.schemasById.get(schemaId);
  }

  getBySchema(schema) {
    return this.schemasBySchema.get(JSON.stringify(schema));
  }

  getByName(name) {
    return this.schemasByName.get(name);
  }
}

module.exports = SchemaCache;
