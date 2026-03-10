/**
 * OffsetCommit request/response encoding and decoding.
 *
 * The OffsetCommit API (key 8) commits consumer group offsets for a set of
 * topic partitions. The group coordinator stores these offsets and returns
 * them via OffsetFetch when consumers rejoin.
 *
 * **Request versions:**
 * - v0: group_id, topics[] (partition, offset, metadata)
 * - v1: adds generation_id, member_id, retention_time (deprecated v2+)
 * - v2: adds retention_time_ms (-1 = broker default)
 * - v3–v4: retention_time_ms removed (broker uses offsets.retention.minutes)
 * - v5: adds group_instance_id (KIP-345 static membership)
 * - v6: adds leader_epoch per partition
 * - v7: adds leader_epoch (same as v6)
 * - v8+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: topics[] (partition, error_code)
 * - v3+: throttle_time_ms
 * - v8+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_OffsetCommit
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
 * OffsetCommit per-partition request data.
 */
export type OffsetCommitPartitionRequest = {
  /** The partition index. */
  readonly partitionIndex: number
  /** The committed offset. */
  readonly committedOffset: bigint
  /** The leader epoch of the last consumed record (v6+). -1 if unknown. */
  readonly committedLeaderEpoch?: number
  /** Metadata string (nullable). */
  readonly committedMetadata: string | null
  /** Tagged fields (v8+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * OffsetCommit per-topic request data.
 */
export type OffsetCommitTopicRequest = {
  /** The topic name. */
  readonly name: string
  /** Per-partition data. */
  readonly partitions: readonly OffsetCommitPartitionRequest[]
  /** Tagged fields (v8+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * OffsetCommit request payload.
 */
export type OffsetCommitRequest = {
  /** The consumer group ID. */
  readonly groupId: string
  /** The generation ID from the JoinGroup response (v1+). -1 if not in a group. */
  readonly generationIdOrMemberEpoch?: number
  /** The member ID assigned by the group coordinator (v1+). */
  readonly memberId?: string
  /** The group instance ID for static membership (v5+). Null if not static. */
  readonly groupInstanceId?: string | null
  /** Retention time in milliseconds (v2–v4, -1 = broker default). */
  readonly retentionTimeMs?: bigint
  /** Per-topic data. */
  readonly topics: readonly OffsetCommitTopicRequest[]
  /** Tagged fields (v8+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * OffsetCommit per-partition response data.
 */
export type OffsetCommitPartitionResponse = {
  /** The partition index. */
  readonly partitionIndex: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Tagged fields (v8+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * OffsetCommit per-topic response data.
 */
export type OffsetCommitTopicResponse = {
  /** The topic name. */
  readonly name: string
  /** Per-partition responses. */
  readonly partitions: readonly OffsetCommitPartitionResponse[]
  /** Tagged fields (v8+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * OffsetCommit response payload.
 */
export type OffsetCommitResponse = {
  /** Time the request was throttled in milliseconds (v3+). */
  readonly throttleTimeMs: number
  /** Per-topic responses. */
  readonly topics: readonly OffsetCommitTopicResponse[]
  /** Tagged fields (v8+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an OffsetCommit request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–8).
 */
export function encodeOffsetCommitRequest(
  writer: BinaryWriter,
  request: OffsetCommitRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 8

  // group_id
  if (isFlexible) {
    writer.writeCompactString(request.groupId)
  } else {
    writer.writeString(request.groupId)
  }

  // generation_id (v1+)
  if (apiVersion >= 1) {
    writer.writeInt32(request.generationIdOrMemberEpoch ?? -1)
  }

  // member_id (v1+)
  if (apiVersion >= 1) {
    if (isFlexible) {
      writer.writeCompactString(request.memberId ?? "")
    } else {
      writer.writeString(request.memberId ?? "")
    }
  }

  // group_instance_id (v7+, nullable)
  if (apiVersion >= 7) {
    if (isFlexible) {
      writer.writeCompactString(request.groupInstanceId ?? null)
    } else {
      writer.writeString(request.groupInstanceId ?? null)
    }
  }

  // retention_time_ms (v2–v4)
  if (apiVersion >= 2 && apiVersion <= 4) {
    writer.writeInt64(request.retentionTimeMs ?? -1n)
  }

  // topics array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.topics.length + 1)
  } else {
    writer.writeInt32(request.topics.length)
  }

  for (const topic of request.topics) {
    encodeOffsetCommitTopic(writer, topic, apiVersion, isFlexible)
  }

  // Tagged fields (v8+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

function encodeOffsetCommitTopic(
  writer: BinaryWriter,
  topic: OffsetCommitTopicRequest,
  apiVersion: number,
  isFlexible: boolean
): void {
  // name
  if (isFlexible) {
    writer.writeCompactString(topic.name)
  } else {
    writer.writeString(topic.name)
  }

  // partitions array
  if (isFlexible) {
    writer.writeUnsignedVarInt(topic.partitions.length + 1)
  } else {
    writer.writeInt32(topic.partitions.length)
  }

  for (const partition of topic.partitions) {
    encodeOffsetCommitPartition(writer, partition, apiVersion, isFlexible)
  }

  // Tagged fields
  if (isFlexible) {
    writer.writeTaggedFields(topic.taggedFields ?? [])
  }
}

function encodeOffsetCommitPartition(
  writer: BinaryWriter,
  partition: OffsetCommitPartitionRequest,
  apiVersion: number,
  isFlexible: boolean
): void {
  // partition_index (INT32)
  writer.writeInt32(partition.partitionIndex)

  // committed_offset (INT64)
  writer.writeInt64(partition.committedOffset)

  // committed_leader_epoch (INT32, v6+)
  if (apiVersion >= 6) {
    writer.writeInt32(partition.committedLeaderEpoch ?? -1)
  }

  // committed_metadata (nullable string)
  if (isFlexible) {
    writer.writeCompactString(partition.committedMetadata)
  } else {
    writer.writeString(partition.committedMetadata)
  }

  // Tagged fields
  if (isFlexible) {
    writer.writeTaggedFields(partition.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed OffsetCommit request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–8).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildOffsetCommitRequest(
  correlationId: number,
  apiVersion: number,
  request: OffsetCommitRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.OffsetCommit,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeOffsetCommitRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an OffsetCommit response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–8).
 * @returns The decoded response or a failure.
 */
export function decodeOffsetCommitResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<OffsetCommitResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 8

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
  const topicsResult = decodeOffsetCommitTopicResponses(reader, isFlexible)
  if (!topicsResult.ok) {
    return topicsResult
  }

  // Tagged fields (v8+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess(
    { throttleTimeMs, topics: topicsResult.value, taggedFields },
    reader.offset - startOffset
  )
}

function decodeOffsetCommitTopicResponses(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<OffsetCommitTopicResponse[]> {
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

  const topics: OffsetCommitTopicResponse[] = []
  for (let i = 0; i < count; i++) {
    const topicResult = decodeOffsetCommitTopicResponse(reader, isFlexible)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

function decodeOffsetCommitTopicResponse(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<OffsetCommitTopicResponse> {
  const startOffset = reader.offset

  // name
  const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!nameResult.ok) {
    return nameResult
  }

  // partitions array
  const partitionsResult = decodeOffsetCommitPartitionResponses(reader, isFlexible)
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

function decodeOffsetCommitPartitionResponses(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<OffsetCommitPartitionResponse[]> {
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

  const partitions: OffsetCommitPartitionResponse[] = []
  for (let i = 0; i < count; i++) {
    const partResult = decodeOffsetCommitPartitionResponse(reader, isFlexible)
    if (!partResult.ok) {
      return partResult
    }
    partitions.push(partResult.value)
  }

  return decodeSuccess(partitions, reader.offset - startOffset)
}

function decodeOffsetCommitPartitionResponse(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<OffsetCommitPartitionResponse> {
  const startOffset = reader.offset

  // partition_index (INT32)
  const partitionResult = reader.readInt32()
  if (!partitionResult.ok) {
    return partitionResult
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
      errorCode: errorCodeResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}
