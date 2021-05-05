import * as httpsRequest from "https"
import * as httpRequest from "http"

export interface SchemaApiClientConfiguration {
  protocol: typeof httpRequest | typeof httpsRequest
  host: string
  port?: string
  username?: string
  password?: string
  path: string
}

type RequestOptions = httpsRequest.RequestOptions | httpRequest.RequestOptions

export class SchemaApiClient {
  constructor(private readonly registry: SchemaApiClientConfiguration) {}

  async getSchemaById(schemaId) {
    const { protocol, host, port, username, password, path } = this.registry
    const requestOptions: RequestOptions = {
      host,
      port,
      headers: {
        "Content-Type": "application/vnd.schemaregistry.v1+json",
      },
      path: `${path}schemas/ids/${schemaId}`,
      auth: username && password ? `${username}:${password}` : null,
    }

    const data = await this.request(protocol, requestOptions)

    return JSON.parse(data).schema
  }

  async pushSchema(subject, schema) {
    const { protocol, host, port, username, password, path } = this.registry
    const body = JSON.stringify({ schema: JSON.stringify(schema) })
    const requestOptions: RequestOptions = {
      method: "POST",
      host,
      port,
      headers: {
        "Content-Type": "application/vnd.schemaregistry.v1+json",
        "Content-Length": Buffer.byteLength(body),
      },
      path: `${path}subjects/${subject}/versions`,
      auth: username && password ? `${username}:${password}` : null,
    }

    const data = await this.request(protocol, requestOptions, body)

    return JSON.parse(data).id
  }

  async getLatestVersionForSubject(subject) {
    const { protocol, host, port, username, password, path } = this.registry
    const requestOptions: RequestOptions = {
      host,
      port,
      headers: {
        "Content-Type": "application/vnd.schemaregistry.v1+json",
      },
      path: `${path}subjects/${subject}/versions`,
      auth: username && password ? `${username}:${password}` : null,
    }

    const data = await this.request(protocol, requestOptions)

    const versions = JSON.parse(data)

    const requestOptions2: RequestOptions = {
      ...requestOptions,
      path: `${path}subjects/${subject}/versions/${versions.pop()}`,
    }

    const data2 = await this.request(protocol, requestOptions2)

    const responseBody2 = JSON.parse(data2)

    return { schema: responseBody2.schema, id: responseBody2.id }
  }

  private request(
    protocol: SchemaApiClientConfiguration["protocol"],
    requestOptions: RequestOptions,
    requestBody?: string
  ) {
    return new Promise<string>((resolve, reject) => {
      const req = protocol
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
