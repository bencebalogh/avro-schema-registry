import * as httpsRequest from "https"
import * as httpRequest from "http"
import { URL } from "url"

export interface SchemaApiClientConfiguration {
  baseUrl: string
  username?: string
  password?: string
  agent?: httpRequest.Agent | httpsRequest.Agent
}

type RequestOptions = httpsRequest.RequestOptions | httpRequest.RequestOptions

export class SchemaApiClient {
  baseRequestOptions: RequestOptions
  requester: typeof httpRequest | typeof httpsRequest
  basePath: string

  constructor(private readonly options: SchemaApiClientConfiguration) {
    const parsed = new URL(options.baseUrl)

    this.requester = parsed.protocol.startsWith("https") ? httpsRequest : httpRequest
    this.basePath = parsed.pathname != null ? parsed.pathname : "/"

    const username = options.username ?? parsed.username
    const password = options.password ?? parsed.password

    this.baseRequestOptions = {
      host: parsed.hostname,
      port: parsed.port,
      headers: {
        "Content-Type": "application/vnd.schemaregistry.v1+json",
      },
      agent: options.agent,
      auth: username && password ? `${username}:${password}` : null,
    }
  }

  async getSchemaById(schemaId) {
    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path: `${this.basePath}schemas/ids/${schemaId}`,
    }

    const data = await this.request(requestOptions)

    return JSON.parse(data).schema
  }

  async pushSchema(subject, schema) {
    const body = JSON.stringify({ schema: JSON.stringify(schema) })
    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      method: "POST",
      path: `${this.basePath}subjects/${subject}/versions`,
    }

    const data = await this.request(requestOptions, body)

    return JSON.parse(data).id
  }

  async getLatestVersionForSubject(subject) {
    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path: `${this.basePath}subjects/${subject}/versions`,
    }

    const data = await this.request(requestOptions)

    const versions = JSON.parse(data)

    const requestOptions2: RequestOptions = {
      ...requestOptions,
      path: `${this.basePath}subjects/${subject}/versions/${versions.pop()}`,
    }

    const data2 = await this.request(requestOptions2)

    const responseBody2 = JSON.parse(data2)

    return { schema: responseBody2.schema, id: responseBody2.id }
  }

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
              return reject(new Error(`Schema registry error: ${data}`))
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
