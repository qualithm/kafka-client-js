/**
 * OffsetFetch request/response encoding and decoding.
 *
 * The OffsetFetch API (key 9) retrieves committed offsets for a consumer
 * group's topic partitions. This is used when a consumer joins a group and
 * needs to resume from the last committed position.
 *
 * **Request versions:**
 * - v0–v1: group_id, topics[] (topic, partitions[])
 * - v2–v5: same as v1 (v2 adds retention, v3–v5 unchanged semantically)
 * - v6+: flexible encoding (KIP-482)
 * - v7: all topic partitions returned when topics=null
 * - v8+: groups[] (batched multi-group fetch)
 *
 * **Response versions:**
 * - v0: topics[] (partitions[] with offset, metadata, error_code)
 * - v2+: top-level error_code
 * - v3+: throttle_time_ms
 * - v5+: leader epoch per partition
 * - v6+: flexible encoding
 * - v8+: groups[] (batched response)
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_OffsetFetch
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
 * OffsetFetch per-topic request data (v0–v7).
 */
export type OffsetFetchTopicRequest = {
  /** The topic name. */
  readonly name: string
  /** The partition indices to fetch offsets for. */
  readonly partitionIndexes: readonly number[]
  /** Tagged fields (v6+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * OffsetFetch request payload.
 *
 * For v0–v7, use `groupId` and `topics`.
 * For v8+, use `groups` for batched multi-group fetch.
 */
export type OffsetFetchRequest = {
  /** The consumer group ID (v0–v7). */
  readonly groupId: string
  /** Topics to fetch offsets for. Null fetches all (v7+). */
  readonly topics: readonly OffsetFetchTopicRequest[] | null
  /** Whether to require stable offsets (v7+). */
  readonly requireStable?: boolean
  /** Tagged fields (v6+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * OffsetFetch per-partition response data.
 */
export type OffsetFetchPartitionResponse = {
  /** The partition index. */
  readonly partitionIndex: number
  /** The committed offset. -1 if no offset committed. */
  readonly committedOffset: bigint
  /** The leader epoch at commit time (v5+). -1 if unknown. */
  readonly committedLeaderEpoch: number
  /** Metadata string. Null if not set. */
  readonly metadata: string | null
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Tagged fields (v6+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * OffsetFetch per-topic response data.
 */
export type OffsetFetchTopicResponse = {
  /** The topic name. */
  readonly name: string
  /** Per-partition responses. */
  readonly partitions: readonly OffsetFetchPartitionResponse[]
  /** Tagged fields (v6+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * OffsetFetch response payload.
 */
export type OffsetFetchResponse = {
  /** Time the request was throttled in milliseconds (v3+). */
  readonly throttleTimeMs: number
  /** Per-topic responses. */
  readonly topics: readonly OffsetFetchTopicResponse[]
  /** Top-level error code (v2+). 0 = no error. */
  readonly errorCode: number
  /** Tagged fields (v6+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an OffsetFetch request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–8).
 */
export function encodeOffsetFetchRequest(
  writer: BinaryWriter,
  request: OffsetFetchRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 6

  // group_id
  if (isFlexible) {
    writer.writeCompactString(request.groupId)
  } else {
    writer.writeString(request.groupId)
  }

  // topics (nullable in v7+)
  if (request.topics === null) {
    if (isFlexible) {
      writer.writeUnsignedVarInt(0) // compact null array = 0
    } else {
      writer.writeInt32(-1) // nullable array marker
    }
  } else {
    if (isFlexible) {
      writer.writeUnsignedVarInt(request.topics.length + 1)
    } else {
      writer.writeInt32(request.topics.length)
    }

    for (const topic of request.topics) {
      encodeOffsetFetchTopic(writer, topic, isFlexible)
    }
  }

  // require_stable (v7+)
  if (apiVersion >= 7) {
    writer.writeBoolean(request.requireStable ?? false)
  }

  // Tagged fields (v6+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

function encodeOffsetFetchTopic(
  writer: BinaryWriter,
  topic: OffsetFetchTopicRequest,
  isFlexible: boolean
): void {
  // name
  if (isFlexible) {
    writer.writeCompactString(topic.name)
  } else {
    writer.writeString(topic.name)
  }

  // partition_indexes
  if (isFlexible) {
    writer.writeUnsignedVarInt(topic.partitionIndexes.length + 1)
  } else {
    writer.writeInt32(topic.partitionIndexes.length)
  }

  for (const idx of topic.partitionIndexes) {
    writer.writeInt32(idx)
  }

  // Tagged fields
  if (isFlexible) {
    writer.writeTaggedFields(topic.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed OffsetFetch request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–8).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildOffsetFetchRequest(
  correlationId: number,
  apiVersion: number,
  request: OffsetFetchRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.OffsetFetch,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeOffsetFetchRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an OffsetFetch response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–8).
 * @returns The decoded response or a failure.
 */
export function decodeOffsetFetchResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<OffsetFetchResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 6

  // throttle_time_ms (v3+)
  let throttleTimeMs = 0
  if (apiVersion >= 3) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  // topics array
  const topicsResult = decodeOffsetFetchTopicResponses(reader, apiVersion, isFlexible)
  if (!topicsResult.ok) {
    return topicsResult
  }

  // error_code (v2+)
  let errorCode = 0
  if (apiVersion >= 2) {
    const errorResult = reader.readInt16()
    if (!errorResult.ok) {
      return errorResult
    }
    errorCode = errorResult.value
  }

  // Tagged fields (v6+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess(
    { throttleTimeMs, topics: topicsResult.value, errorCode, taggedFields },
    reader.offset - startOffset
  )
}

function decodeOffsetFetchTopicResponses(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<OffsetFetchTopicResponse[]> {
  const startOffset = reader.offset
  let count: number

  if (isFlexible) {
    const countResult = reader.readUnsignedVarInt()
    if (!countResult.ok) {
      return countResult
    }
    count = countResult.value - 1
  } else {
    const countResult = reader.readInt32()
    if (!countResult.ok) {
      return countResult
    }
    count = countResult.value
  }

  const topics: OffsetFetchTopicResponse[] = []
  for (let i = 0; i < count; i++) {
    const topicResult = decodeOffsetFetchTopicResponse(reader, apiVersion, isFlexible)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

function decodeOffsetFetchTopicResponse(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<OffsetFetchTopicResponse> {
  const startOffset = reader.offset

  // name
  const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!nameResult.ok) {
    return nameResult
  }

  // partitions
  const partitionsResult = decodeOffsetFetchPartitionResponses(reader, apiVersion, isFlexible)
  if (!partitionsResult.ok) {
    return partitionsResult
  }

  // Tagged fields
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess(
    { name: nameResult.value ?? "", partitions: partitionsResult.value, taggedFields },
    reader.offset - startOffset
  )
}

function decodeOffsetFetchPartitionResponses(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<OffsetFetchPartitionResponse[]> {
  const startOffset = reader.offset
  let count: number

  if (isFlexible) {
    const countResult = reader.readUnsignedVarInt()
    if (!countResult.ok) {
      return countResult
    }
    count = countResult.value - 1
  } else {
    const countResult = reader.readInt32()
    if (!countResult.ok) {
      return countResult
    }
    count = countResult.value
  }

  const partitions: OffsetFetchPartitionResponse[] = []
  for (let i = 0; i < count; i++) {
    const partResult = decodeOffsetFetchPartitionResponse(reader, apiVersion, isFlexible)
    if (!partResult.ok) {
      return partResult
    }
    partitions.push(partResult.value)
  }

  return decodeSuccess(partitions, reader.offset - startOffset)
}

function decodeOffsetFetchPartitionResponse(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<OffsetFetchPartitionResponse> {
  const startOffset = reader.offset

  // partition_index (INT32)
  const partitionResult = reader.readInt32()
  if (!partitionResult.ok) {
    return partitionResult
  }

  // committed_offset (INT64)
  const offsetResult = reader.readInt64()
  if (!offsetResult.ok) {
    return offsetResult
  }

  // committed_leader_epoch (INT32, v5+)
  let committedLeaderEpoch = -1
  if (apiVersion >= 5) {
    const epochResult = reader.readInt32()
    if (!epochResult.ok) {
      return epochResult
    }
    committedLeaderEpoch = epochResult.value
  }

  // metadata (nullable string)
  const metadataResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!metadataResult.ok) {
    return metadataResult
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // Tagged fields
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess(
    {
      partitionIndex: partitionResult.value,
      committedOffset: offsetResult.value,
      committedLeaderEpoch,
      metadata: metadataResult.value,
      errorCode: errorCodeResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}
