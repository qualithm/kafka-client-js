/**
 * CreatePartitions request/response encoding and decoding.
 *
 * The CreatePartitions API (key 37) increases the number of partitions for
 * existing topics.
 *
 * **Request versions:**
 * - v0–v1: non-flexible encoding
 * - v2+: flexible encoding (KIP-482)
 * - v1+: adds validate_only
 *
 * **Response versions:**
 * - v0: per-topic error codes
 * - v1+: adds error_message
 * - v2+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_CreatePartitions
 *
 * @packageDocumentation
 */

import { ApiKey } from "./api-keys.js"
import type { BinaryReader, TaggedField } from "./binary-reader.js"
import type { BinaryWriter } from "./binary-writer.js"
import { frameRequest, type RequestHeader } from "./protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "./result.js"

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * Assignment for new partitions.
 */
export type CreatePartitionsAssignment = {
  /** The broker IDs for the new partition's replicas. */
  readonly brokerIds: readonly number[]
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Per-topic configuration for creating partitions.
 */
export type CreatePartitionsTopicRequest = {
  /** The topic name. */
  readonly name: string
  /** The new total number of partitions. */
  readonly count: number
  /** Manual replica assignments for the new partitions. Null for automatic. */
  readonly assignments: readonly CreatePartitionsAssignment[] | null
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * CreatePartitions request payload.
 */
export type CreatePartitionsRequest = {
  /** Per-topic partition creation specs. */
  readonly topics: readonly CreatePartitionsTopicRequest[]
  /** Timeout in milliseconds. */
  readonly timeoutMs: number
  /** If true, validate the request without creating partitions (v1+). */
  readonly validateOnly?: boolean
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-topic result from a CreatePartitions response.
 */
export type CreatePartitionsTopicResponse = {
  /** The topic name. */
  readonly name: string
  /** Error code (0 = success). */
  readonly errorCode: number
  /** Error message. Null if no error. */
  readonly errorMessage: string | null
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * CreatePartitions response payload.
 */
export type CreatePartitionsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-topic results. */
  readonly topics: readonly CreatePartitionsTopicResponse[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a CreatePartitions request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–3).
 */
export function encodeCreatePartitionsRequest(
  writer: BinaryWriter,
  request: CreatePartitionsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // Topics array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.topics.length + 1)
  } else {
    writer.writeInt32(request.topics.length)
  }

  for (const topic of request.topics) {
    // name
    if (isFlexible) {
      writer.writeCompactString(topic.name)
    } else {
      writer.writeString(topic.name)
    }

    // count (INT32)
    writer.writeInt32(topic.count)

    // assignments (nullable array)
    if (topic.assignments === null) {
      if (isFlexible) {
        writer.writeUnsignedVarInt(0) // null compact array
      } else {
        writer.writeInt32(-1)
      }
    } else {
      if (isFlexible) {
        writer.writeUnsignedVarInt(topic.assignments.length + 1)
      } else {
        writer.writeInt32(topic.assignments.length)
      }
      for (const assignment of topic.assignments) {
        // broker_ids array
        if (isFlexible) {
          writer.writeUnsignedVarInt(assignment.brokerIds.length + 1)
        } else {
          writer.writeInt32(assignment.brokerIds.length)
        }
        for (const brokerId of assignment.brokerIds) {
          writer.writeInt32(brokerId)
        }
        if (isFlexible) {
          writer.writeTaggedFields(assignment.taggedFields ?? [])
        }
      }
    }

    if (isFlexible) {
      writer.writeTaggedFields(topic.taggedFields ?? [])
    }
  }

  // timeout_ms (INT32)
  writer.writeInt32(request.timeoutMs)

  // validate_only (v1+)
  if (apiVersion >= 1) {
    writer.writeBoolean(request.validateOnly ?? false)
  }

  // Tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed CreatePartitions request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–3).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildCreatePartitionsRequest(
  correlationId: number,
  apiVersion: number,
  request: CreatePartitionsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.CreatePartitions,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeCreatePartitionsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a CreatePartitions response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–3).
 * @returns The decoded response or a failure.
 */
export function decodeCreatePartitionsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<CreatePartitionsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }
  const throttleTimeMs = throttleResult.value

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

  const topics: CreatePartitionsTopicResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    // name
    const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!nameResult.ok) {
      return nameResult
    }

    // error_code
    const errorCodeResult = reader.readInt16()
    if (!errorCodeResult.ok) {
      return errorCodeResult
    }

    // error_message
    const errMsgResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!errMsgResult.ok) {
      return errMsgResult
    }

    // Tagged fields (v2+)
    let taggedFields: readonly TaggedField[] = []
    if (isFlexible) {
      const tagResult = reader.readTaggedFields()
      if (!tagResult.ok) {
        return tagResult
      }
      taggedFields = tagResult.value
    }

    topics.push({
      name: nameResult.value ?? "",
      errorCode: errorCodeResult.value,
      errorMessage: errMsgResult.value,
      taggedFields
    })
  }

  // Tagged fields (v2+)
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
