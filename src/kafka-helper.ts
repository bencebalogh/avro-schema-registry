/**
 *
 * @param encodedMessage
 * @param schemaId
 * @returns
 */
export const kafkaEncode = (schemaId: number, encodedMessage: Buffer): Buffer => {
  if (!(encodedMessage instanceof Buffer)) {
    throw new Error("encoded message must be of type Buffer")
  }

  // Allocate buffer for encoded kafka event (1 byte preable + 4 byte messageid + sizeof(encodedMessage))
  const message = Buffer.alloc(encodedMessage.length + 5)

  message.writeUInt8(0)
  message.writeUInt32BE(schemaId, 1)
  encodedMessage.copy(message, 5)

  return message
}

/**
 *
 * @param msg
 * @returns
 */
export const kafkaDecode = (msg: Buffer): { schemaId?: number; payload: Buffer } => {
  if (msg.readUInt8(0) !== 0) {
    // throw new Error(`Missing schema preamble.`)
    return { payload: msg }
  }

  const schemaId = msg.readUInt32BE(1)
  const payload = msg.slice(5)

  return { schemaId, payload }
}
