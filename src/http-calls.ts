export const getSchemaById = (registry, schemaId) =>
  new Promise((resolve, reject) => {
    const { protocol, host, port, username, password, path } = registry
    const requestOptions = {
      host,
      port,
      headers: {
        "Content-Type": "application/vnd.schemaregistry.v1+json",
      },
      path: `${path}schemas/ids/${schemaId}`,
      auth: username && password ? `${username}:${password}` : null,
    }
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
            const error = JSON.parse(data)
            return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`))
          }

          const schema = JSON.parse(data).schema

          resolve(schema)
        })
      })
      .on("error", (e) => {
        reject(e)
      })
    req.end()
  })

export const pushSchema = (registry, subject, schema) =>
  new Promise((resolve, reject) => {
    const { protocol, host, port, username, password, path } = registry
    const body = JSON.stringify({ schema: JSON.stringify(schema) })
    const requestOptions = {
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
            const error = JSON.parse(data)
            return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`))
          }

          const resp = JSON.parse(data)

          resolve(resp.id)
        })
      })
      .on("error", (e) => {
        reject(e)
      })
    req.write(body)
    req.end()
  })

export const getLatestVersionForSubject = (registry, subject) =>
  new Promise((resolve, reject) => {
    const { protocol, host, port, username, password, path } = registry
    const requestOptions = {
      host,
      port,
      headers: {
        "Content-Type": "application/vnd.schemaregistry.v1+json",
      },
      path: `${path}subjects/${subject}/versions`,
      auth: username && password ? `${username}:${password}` : null,
    }
    protocol
      .get(requestOptions, (res) => {
        let data = ""
        res.on("data", (d) => {
          data += d
        })
        res.on("error", (e) => {
          reject(e)
        })
        res.on("end", () => {
          if (res.statusCode !== 200) {
            const error = JSON.parse(data)
            return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`))
          }

          const versions = JSON.parse(data)

          const requestOptions2 = Object.assign(requestOptions, {
            path: `${path}subjects/${subject}/versions/${versions.pop()}`,
          })

          protocol
            .get(requestOptions2, (res2) => {
              let data = ""
              res2.on("data", (d) => {
                data += d
              })
              res2.on("error", (e) => {
                reject(e)
              })
              res2.on("end", () => {
                if (res2.statusCode !== 200) {
                  const error = JSON.parse(data)
                  return reject(new Error(`Schema registry error: ${error.error_code} - ${error.message}`))
                }

                const responseBody = JSON.parse(data)

                resolve({ schema: responseBody.schema, id: responseBody.id })
              })
            })
            .on("error", (e) => reject(e))
        })
      })
      .on("error", (e) => {
        reject(e)
      })
  })
