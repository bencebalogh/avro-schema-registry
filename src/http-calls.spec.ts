import * as http from "http"
import * as nock from "nock"

import { pushSchema, getSchemaById, getLatestVersionForSubject } from "./http-calls"

describe("http-calls", () => {
  const schema = { type: "string" }
  const registry = {
    protocol: http,
    host: "test.com",
    username: null,
    password: null,
    path: "/",
  }

  afterEach(() => {
    nock.cleanAll()
  })

  describe("pushSchema", () => {
    it("reject if post request fails", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").post("/subjects/topic/versions").replyWithError(requestError)

      return pushSchema(registry, "topic", schema).catch((error) => {
        expect(error).toEqual(requestError)
      })
    })

    it("reject if post request returns with not 200", () => {
      nock("http://test.com").post("/subjects/topic/versions").reply(500, { error_code: 1, message: "failed request" })

      return pushSchema(registry, "topic", schema).catch((error) => {
        expect(error).toBeInstanceOf(Error)
        expect(error).toEqual(expect.objectContaining({ message: "Schema registry error: 1 - failed request" }))
      })
    })

    it("resolve with schema id if post request returns with 200", () => {
      nock("http://test.com").post("/subjects/topic/versions").reply(200, { id: 1 })

      return pushSchema(registry, "topic", schema).then((id) => {
        expect(id).toEqual(1)
      })
    })
  })

  describe("getSchemaById", () => {
    it("reject if get request fails", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/schemas/ids/1").replyWithError(requestError)

      return getSchemaById(registry, 1).catch((error) => {
        expect(error).toEqual(requestError)
      })
    })

    it("reject if get request returns with not 200", () => {
      nock("http://test.com").get("/schemas/ids/1").reply(500, { error_code: 1, message: "failed request" })

      return getSchemaById(registry, 1).catch((error) => {
        expect(error).toBeInstanceOf(Error)
        expect(error).toEqual(expect.objectContaining({ message: "Schema registry error: 1 - failed request" }))
      })
    })

    it("resolve with schema if get request returns with 200", () => {
      nock("http://test.com").get("/schemas/ids/1").reply(200, { id: 1, schema })

      return getSchemaById(registry, 1).then((returnedSchema) => {
        expect(returnedSchema).toEqual(schema)
      })
    })
  })

  describe("getLatestVersionForSubject", () => {
    it("reject if first get request fails", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/subjects/topic/versions").replyWithError(requestError)

      return getLatestVersionForSubject(registry, "topic").catch((error) => {
        expect(error).toEqual(requestError)
      })
    })

    it("reject if first get request returns with not 200", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/subjects/topic/versions").reply(500, { error_code: 1, message: "failed request" })

      return getLatestVersionForSubject(registry, "topic").catch((error) => {
        expect(error).toBeInstanceOf(Error)
        expect(error).toEqual(expect.objectContaining({ message: "Schema registry error: 1 - failed request" }))
      })
    })

    it("reject if second get request fails", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/subjects/topic/versions").reply(200, [1, 2])
      nock("http://test.com").get("/subjects/topic/versions/2").replyWithError(requestError)

      return getLatestVersionForSubject(registry, "topic").catch((error) => {
        expect(error).toEqual(requestError)
      })
    })

    it("reject if second get request returns with not 200", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/subjects/topic/versions").reply(200, [1, 2])
      nock("http://test.com").get("/subjects/topic/versions/2").reply(500, { error_code: 1, message: "failed request" })

      return getLatestVersionForSubject(registry, "topic").catch((error) => {
        expect(error).toBeInstanceOf(Error)
        expect(error).toEqual(expect.objectContaining({ message: "Schema registry error: 1 - failed request" }))
      })
    })

    it("resolve with schema and id if both get requests return with 200", () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/subjects/topic/versions").reply(200, [1, 2])
      nock("http://test.com").get("/subjects/topic/versions/2").reply(200, { id: 1, schema })

      //@ts-ignore
      return getLatestVersionForSubject(registry, "topic").then(({ schema: returnedSchema, id }) => {
        expect(schema).toEqual(returnedSchema)
        expect(id).toEqual(1)
      })
    })
  })
})
