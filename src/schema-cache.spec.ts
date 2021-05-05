"use strict"

import { SchemaCache } from "./schema-cache"

describe("schema-cache", () => {
  const schema = { type: "string" }

  describe("setById", () => {
    it("updates the cache", () => {
      const expected = new Map()
      expected.set(1, schema)

      const uut = new SchemaCache()
      uut.setById(1, schema)
      expect(uut.schemasById).toEqual(expected)
    })
  })

  describe("setByName", () => {
    it("updates the cache", () => {
      const expected = new Map()
      expected.set("topic", schema)

      const uut = new SchemaCache()
      uut.setByName("topic", schema)
      expect(uut.schemasByName).toEqual(expected)
    })
  })

  describe("setBySchema", () => {
    it("updates the cache", () => {
      const expected = new Map()
      expected.set(JSON.stringify(schema), 1)

      const uut = new SchemaCache()
      uut.setBySchema(schema, 1)
      expect(uut.schemasBySchema).toEqual(expected)
    })
  })

  describe("getById", () => {})
  describe("getByName", () => {})
  describe("getBySchema", () => {})
})
