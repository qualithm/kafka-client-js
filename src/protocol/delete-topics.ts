/**
 * DeleteTopics request/response encoding and decoding.
 *
 * The DeleteTopics API (key 20) deletes one or more topics from the cluster.
 *
 * **Request versions:**
 * - v0–v3: non-flexible, topic names array + timeout
 * - v4+: flexible encoding (KIP-482)
 * - v6+: adds topic_id (UUID) for deletion by ID
 *
 * **Response versions:**
 * - v0: topic name + error code
 * - v1+: adds throttle_time_ms
 * - v4+: flexible encoding
 * - v5+: adds error_message
 * - v6+: adds topic_id
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DeleteTopics
 *
 * @packageDocumentation
 */

import { ApiKey } from "../codec/api-keys.js"
import type { BinaryReader, TaggedField } from "../codec/binary-reader.js"
import type { BinaryWriter } from "../codec/binary-writer.js"
import { frameRequest, type RequestHeader } from "../codec/protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "../result.js"

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * A topic to delete (v6+ supports deletion by either name or ID).
 */
export type DeleteTopicState = {
  /** Topic name. Null if deleting by ID only (v6+). */
  readonly name: string | null
  /** Topic ID (v6+, UUID as 16 bytes). */
  readonly topicId?: Uint8Array
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * DeleteTopics request payload.
 */
export type DeleteTopicsRequest = {
  /**
   * Topic names to delete (v0–v5).
   * In v6+, use `topics` array instead.
   */
  readonly topicNames?: readonly string[]
  /**
   * Topics to delete (v6+, supports name or ID).
   */
  readonly topics?: readonly DeleteTopicState[]
  /** Timeout in milliseconds. */
  readonly timeoutMs: number
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-topic result from a DeleteTopics response.
 */
export type DeleteTopicsTopicResponse = {
  /** The topic name. Null if deleted by ID (v6+). */
  readonly name: string | null
  /** Topic ID (v6+, UUID as 16 bytes). */
  readonly topicId?: Uint8Array
  /** Error code (0 = success). */
  readonly errorCode: number
  /** Error message (v5+). */
  readonly errorMessage: string | null
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DeleteTopics response payload.
 */
export type DeleteTopicsResponse = {
  /** Time the request was throttled in milliseconds (v1+). */
  readonly throttleTimeMs: number
  /** Per-topic results. */
  readonly topics: readonly DeleteTopicsTopicResponse[]
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DeleteTopics request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–6).
 */
export function encodeDeleteTopicsRequest(
  writer: BinaryWriter,
  request: DeleteTopicsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 4

  if (apiVersion >= 6) {
    // v6+: topics array with name + topic_id
    const topics: readonly DeleteTopicState[] =
      request.topics ?? (request.topicNames ?? []).map((n): DeleteTopicState => ({ name: n }))
    if (isFlexible) {
      writer.writeUnsignedVarInt(topics.length + 1)
    } else {
      writer.writeInt32(topics.length)
    }
    for (const topic of topics) {
      // name (compact nullable string)
      writer.writeCompactString(topic.name)
      // topic_id (UUID, 16 bytes)
      writer.writeRawBytes(topic.topicId ?? new Uint8Array(16))
      // tagged fields
      writer.writeTaggedFields(topic.taggedFields ?? [])
    }
  } else {
    // v0–v5: simple topic names array
    const names = request.topicNames ?? (request.topics ?? []).map((t) => t.name ?? "")
    if (isFlexible) {
      writer.writeUnsignedVarInt(names.length + 1)
    } else {
      writer.writeInt32(names.length)
    }
    for (const name of names) {
      if (isFlexible) {
        writer.writeCompactString(name)
      } else {
        writer.writeString(name)
      }
    }
  }

  // timeout_ms (INT32)
  writer.writeInt32(request.timeoutMs)

  // Tagged fields (v4+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed DeleteTopics request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–6).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildDeleteTopicsRequest(
  correlationId: number,
  apiVersion: number,
  request: DeleteTopicsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DeleteTopics,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDeleteTopicsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DeleteTopics response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–6).
 * @returns The decoded response or a failure.
 */
export function decodeDeleteTopicsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DeleteTopicsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 4

  // Throttle time (v1+)
  let throttleTimeMs = 0
  if (apiVersion >= 1) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  // Topics array
  let arrayLength: number
  if (isFlexible) {
    const lengthResult = reader.readUnsignedVarInt()
    if (!lengthResult.ok) {
      return lengthResult
    }
    arrayLength = lengthResult.value - 1
  } else {
    const lengthResult = reader.readInt32()
    if (!lengthResult.ok) {
      return lengthResult
    }
    arrayLength = lengthResult.value
  }

  const topics: DeleteTopicsTopicResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    // name
    const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!nameResult.ok) {
      return nameResult
    }

    // topic_id (v6+, UUID 16 bytes)
    let topicId: Uint8Array | undefined
    if (apiVersion >= 6) {
      const uuidResult = reader.readRawBytes(16)
      if (!uuidResult.ok) {
        return uuidResult
      }
      topicId = uuidResult.value
    }

    // error_code
    const errorCodeResult = reader.readInt16()
    if (!errorCodeResult.ok) {
      return errorCodeResult
    }

    // error_message (v5+)
    let errorMessage: string | null = null
    if (apiVersion >= 5) {
      const errMsgResult = isFlexible ? reader.readCompactString() : reader.readString()
      if (!errMsgResult.ok) {
        return errMsgResult
      }
      errorMessage = errMsgResult.value
    }

    // Tagged fields (v4+)
    let taggedFields: readonly TaggedField[] = []
    if (isFlexible) {
      const tagResult = reader.readTaggedFields()
      if (!tagResult.ok) {
        return tagResult
      }
      taggedFields = tagResult.value
    }

    topics.push({
      name: nameResult.value,
      topicId,
      errorCode: errorCodeResult.value,
      errorMessage,
      taggedFields
    })
  }

  // Tagged fields (v4+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess({ throttleTimeMs, topics, taggedFields }, reader.offset - startOffset)
}
