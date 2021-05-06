import * as httpsRequest from "https"
import * as httpRequest from "http"
import { URL } from "url"

export class SchemaRegistryError extends Error {
  constructor(readonly errorCode: number, readonly message: string) {
    super(`Schema registry error: code: ${errorCode} - ${message}`)
  }
}

export interface SchemaApiClientConfiguration {
  baseUrl: string
  username?: string
  password?: string
  agent?: httpRequest.Agent | httpsRequest.Agent
}

type RequestOptions = httpsRequest.RequestOptions | httpRequest.RequestOptions

export class SchemaRegistryClient {
  baseRequestOptions: RequestOptions
  requester: typeof httpRequest | typeof httpsRequest
  basePath: string

  constructor(options: SchemaApiClientConfiguration) {
    const parsed = new URL(options.baseUrl)

    this.requester = parsed.protocol.startsWith("https") ? httpsRequest : httpRequest
    this.basePath = parsed.pathname != null ? parsed.pathname : "/"

    const username = options.username ?? parsed.username
    const password = options.password ?? parsed.password

    this.baseRequestOptions = {
      host: parsed.hostname,
      port: parsed.port,
      headers: {
        Accept: "application/vnd.schemaregistry.v1+json",
        "Content-Type": "application/vnd.schemaregistry.v1+json",
      },
      agent: options.agent,
      auth: username && password ? `${username}:${password}` : null,
    }
  }

  // schemas section
  /**
   * Get a schema by its id
   * @param schemaId
   * @returns
   */
  async getSchemaById(schemaId: number): Promise<string> {
    const path = `${this.basePath}schemas/ids/${schemaId}`

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return JSON.parse(await this.request(requestOptions)).schema
  }

  /**
   * Get types of registered schemas
   * @returns
   */
  async getSchemaTypes(): Promise<Array<string>> {
    const path = `${this.basePath}schemas/types`

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return JSON.parse(await this.request(requestOptions))
  }

  /**
   * Get subject/version pairs for given id
   * @param id version of schema registered
   */
  async listVersionsForId(id: number): Promise<Array<{ subject: string; version: number }>> {
    const path = `${this.basePath}schemas/ids/${id}/versions`

    const versionListRequestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return JSON.parse(await this.request(versionListRequestOptions))
  }

  // subject section
  /**
   * Get list of available subjects
   * @returns
   */
  async listSubjects(): Promise<Array<string>> {
    const path = `${this.basePath}subjects`

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return JSON.parse(await this.request(requestOptions))
  }

  /**
   * GET /subjects/(string: subject)/versions
   * @param subject
   * @returns
   */
  async listVersionsForSubject(subject: string): Promise<Array<number>> {
    const versionListRequestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path: `${this.basePath}subjects/${subject}/versions`,
    }

    return JSON.parse(await this.request(versionListRequestOptions))
  }

  /**
   * Deletes a schema.
   * Hint: Should only be used in development mode.
   * @param subject subject name
   * @returns list of deleted schema versions
   */
  async deleteSubject(subject: string, permanent: boolean = false): Promise<Array<number>> {
    const path = `${this.basePath}subjects/${subject}${permanent ? "?permanent=true" : ""}`

    const versionListRequestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      method: "DELETE",
      path,
    }

    return JSON.parse(await this.request(versionListRequestOptions))
  }

  /**
   * Get schema for subject and version
   * @param subject
   * @param version
   * @returns
   */
  async getSchemaForSubjectAndVersion(
    subject: string,
    version: number
  ): Promise<{ subject: string; id: number; version: number; schema: string }> {
    // TODO: There is also GET /subjects/(string: subject)/versions/(versionId: version)/schema which would return only the schema as response
    const path = `${this.basePath}subjects/${subject}/versions/${version}`

    const schemaInfoRequestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return JSON.parse(await this.request(schemaInfoRequestOptions))
  }

  /**
   * Get the latest schema version for a subject
   * @param subject
   * @returns
   */
  async getLatestVersionForSubject(subject: string): ReturnType<SchemaRegistryClient["getSchemaForSubjectAndVersion"]> {
    const schemaVersionList = await this.listVersionsForSubject(subject)
    return await this.getSchemaForSubjectAndVersion(subject, schemaVersionList.pop())
  }

  /**
   * Get schema for subject and version
   * @param subject
   * @param schema
   * @returns serialized schema
   */
  async getRawSchemaForSubjectAndVersion(subject: string, version: number): Promise<string> {
    const path = `${this.basePath}subjects/${subject}/versions/${version}/schema`

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return await this.request(requestOptions)
  }

  /**
   * Register schema in registry
   * @param subject
   * @param schema
   * @returns
   */
  async registerSchema(
    subject: string,
    schema: { schema: string; schemaType: string; references?: any }
  ): Promise<{ subject: string; id: number; version: number; schema: string }> {
    const path = `${this.basePath}subjects/${subject}/versions`

    const body = JSON.stringify(schema)
    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      method: "POST",
      headers: { ...this.baseRequestOptions.headers },
      path,
    }

    return JSON.parse(await this.request(requestOptions, body))
  }

  /**
   * Check if a schema has already been registered under the specified subject.
   * If so, this returns the schema string along with its globally unique identifier, its version under this subject and the subject name.
   * @param subject
   * @param schema
   * @returns
   */
  async checkSchema(
    subject: string,
    schema: { schema: string; schemaType: string; references?: any }
  ): Promise<{ subject: string; id: number; version: number; schema: string }> {
    const path = `${this.basePath}subjects/${subject}`

    const body = JSON.stringify(schema)
    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      method: "POST",
      headers: { ...this.baseRequestOptions.headers },
      path,
    }

    return JSON.parse(await this.request(requestOptions, body))
  }

  // TODO: DELETE /subjects/(string: subject)/versions/(versionId: version)

  // TODO: GET /subjects/(string: subject)/versions/{versionId: version}/referencedby

  // TODO: mode section
  // TODO: compatibility section
  // TODO: config section

  private request(requestOptions: RequestOptions, requestBody?: string) {
    if (requestBody && requestBody.length > 0) {
      requestOptions.headers = { ...requestOptions.headers, "Content-Length": Buffer.byteLength(requestBody) }
    }

    return new Promise<string>((resolve, reject) => {
      const req = this.requester
        .request(requestOptions, (res) => {
          let data = ""
          res.on("data", (d) => {
            data += d
          })
          res.on("error", (e) => {
            reject(e)
          })
          res.on("end", () => {
            if (res.statusCode !== 200) {
              const { error_code, message } = JSON.parse(data)
              return reject(new SchemaRegistryError(error_code, message))
            } else {
              return resolve(data)
            }
          })
        })
        .on("error", (e) => {
          reject(e)
        })
      if (requestBody) {
        req.write(requestBody)
      }
      req.end()
    })
  }
}
