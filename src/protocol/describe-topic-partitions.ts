/**
 * DescribeTopicPartitions request/response encoding and decoding.
 *
 * The DescribeTopicPartitions API (key 75) provides paginated topic
 * partition metadata, designed for large clusters.
 *
 * **Request versions:**
 * - v0: topics array, response_partition_limit, cursor (nullable)
 * - All versions use flexible encoding
 *
 * **Response versions:**
 * - v0: throttle_time_ms, topics array, next_cursor (nullable)
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeTopicPartitions
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
 * A topic to describe partitions for.
 */
export type DescribeTopicPartitionsTopicRequest = {
  /** The topic name. */
  readonly name: string
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Cursor for pagination.
 */
export type TopicPartitionsCursor = {
  /** The topic name. */
  readonly topicName: string
  /** The partition index to start from. */
  readonly partitionIndex: number
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * DescribeTopicPartitions request payload.
 */
export type DescribeTopicPartitionsRequest = {
  /** Topics to describe. */
  readonly topics: readonly DescribeTopicPartitionsTopicRequest[]
  /** Maximum number of partitions to return per response. */
  readonly responsePartitionLimit: number
  /** Cursor for pagination, or null for the first request. */
  readonly cursor?: TopicPartitionsCursor | null
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A partition in the response.
 */
export type DescribeTopicPartitionsPartitionResponse = {
  /** Error code for this partition (0 = no error). */
  readonly errorCode: number
  /** The partition index. */
  readonly partitionIndex: number
  /** The leader broker ID. */
  readonly leaderId: number
  /** The leader epoch. */
  readonly leaderEpoch: number
  /** The replica broker IDs. */
  readonly replicaNodes: readonly number[]
  /** The in-sync replica broker IDs. */
  readonly isrNodes: readonly number[]
  /** The eligible leader replica broker IDs. */
  readonly eligibleLeaderReplicas: readonly number[]
  /** The last known eligible leader replicas. */
  readonly lastKnownElr: readonly number[]
  /** The offline replica broker IDs. */
  readonly offlineReplicas: readonly number[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * A topic in the response.
 */
export type DescribeTopicPartitionsTopicResponse = {
  /** Error code for this topic (0 = no error). */
  readonly errorCode: number
  /** The topic name. */
  readonly name: string | null
  /** The topic ID (UUID as hex string). */
  readonly topicId: Uint8Array
  /** Whether this is an internal topic. */
  readonly isInternal: boolean
  /** The partitions. */
  readonly partitions: readonly DescribeTopicPartitionsPartitionResponse[]
  /** Bitfield of topic authorised operations. */
  readonly topicAuthorizedOperations: number
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeTopicPartitions response payload.
 */
export type DescribeTopicPartitionsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-topic results. */
  readonly topics: readonly DescribeTopicPartitionsTopicResponse[]
  /** Cursor for the next page, or null if complete. */
  readonly nextCursor: TopicPartitionsCursor | null
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeTopicPartitions request body into the given writer.
 */
export function encodeDescribeTopicPartitionsRequest(
  writer: BinaryWriter,
  request: DescribeTopicPartitionsRequest,
  _apiVersion: number
): void {
  // topics (compact array)
  writer.writeUnsignedVarInt(request.topics.length + 1)
  for (const topic of request.topics) {
    writer.writeCompactString(topic.name)
    writer.writeTaggedFields(topic.taggedFields ?? [])
  }

  // response_partition_limit (INT32)
  writer.writeInt32(request.responsePartitionLimit)

  // cursor (nullable struct)
  const cursor = request.cursor ?? null
  if (cursor === null) {
    // Null indicator for tagged version struct: 0xff byte
    writer.writeInt8(-1)
  } else {
    // Non-null indicator
    writer.writeInt8(0)
    writer.writeCompactString(cursor.topicName)
    writer.writeInt32(cursor.partitionIndex)
    writer.writeTaggedFields(cursor.taggedFields ?? [])
  }

  // tagged fields
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed DescribeTopicPartitions request ready to send to a broker.
 */
export function buildDescribeTopicPartitionsRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeTopicPartitionsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeTopicPartitions,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeTopicPartitionsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeTopicPartitions response body.
 */
export function decodeDescribeTopicPartitionsResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<DescribeTopicPartitionsResponse> {
  const startOffset = reader.offset

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // topics array
  const topicsResult = decodeTopicsArray(reader)
  if (!topicsResult.ok) {
    return topicsResult
  }

  // next_cursor (nullable struct)
  const cursorResult = decodeCursor(reader)
  if (!cursorResult.ok) {
    return cursorResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      throttleTimeMs: throttleResult.value,
      topics: topicsResult.value,
      nextCursor: cursorResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeTopicsArray(
  reader: BinaryReader
): DecodeResult<DescribeTopicPartitionsTopicResponse[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const topics: DescribeTopicPartitionsTopicResponse[] = []
  for (let i = 0; i < count; i++) {
    const topicResult = decodeTopicEntry(reader)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

function decodeTopicEntry(
  reader: BinaryReader
): DecodeResult<DescribeTopicPartitionsTopicResponse> {
  const startOffset = reader.offset

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // name (COMPACT_NULLABLE_STRING)
  const nameResult = reader.readCompactString()
  if (!nameResult.ok) {
    return nameResult
  }

  // topic_id (UUID)
  const topicIdResult = reader.readUuid()
  if (!topicIdResult.ok) {
    return topicIdResult
  }

  // is_internal (BOOLEAN)
  const isInternalResult = reader.readBoolean()
  if (!isInternalResult.ok) {
    return isInternalResult
  }

  // partitions array
  const partitionsResult = decodePartitionsArray(reader)
  if (!partitionsResult.ok) {
    return partitionsResult
  }

  // topic_authorized_operations (INT32)
  const authOpsResult = reader.readInt32()
  if (!authOpsResult.ok) {
    return authOpsResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      errorCode: errorCodeResult.value,
      name: nameResult.value,
      topicId: topicIdResult.value,
      isInternal: isInternalResult.value,
      partitions: partitionsResult.value,
      topicAuthorizedOperations: authOpsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodePartitionsArray(
  reader: BinaryReader
): DecodeResult<DescribeTopicPartitionsPartitionResponse[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const partitions: DescribeTopicPartitionsPartitionResponse[] = []
  for (let i = 0; i < count; i++) {
    const partResult = decodePartitionEntry(reader)
    if (!partResult.ok) {
      return partResult
    }
    partitions.push(partResult.value)
  }

  return decodeSuccess(partitions, reader.offset - startOffset)
}

function decodePartitionEntry(
  reader: BinaryReader
): DecodeResult<DescribeTopicPartitionsPartitionResponse> {
  const startOffset = reader.offset

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // partition_index (INT32)
  const partIdxResult = reader.readInt32()
  if (!partIdxResult.ok) {
    return partIdxResult
  }

  // leader_id (INT32)
  const leaderResult = reader.readInt32()
  if (!leaderResult.ok) {
    return leaderResult
  }

  // leader_epoch (INT32)
  const leaderEpochResult = reader.readInt32()
  if (!leaderEpochResult.ok) {
    return leaderEpochResult
  }

  // replica_nodes (compact array of INT32)
  const replicasResult = decodeInt32CompactArray(reader)
  if (!replicasResult.ok) {
    return replicasResult
  }

  // isr_nodes (compact array of INT32)
  const isrResult = decodeInt32CompactArray(reader)
  if (!isrResult.ok) {
    return isrResult
  }

  // eligible_leader_replicas (compact array of INT32)
  const elrResult = decodeInt32CompactArray(reader)
  if (!elrResult.ok) {
    return elrResult
  }

  // last_known_elr (compact array of INT32)
  const lastElrResult = decodeInt32CompactArray(reader)
  if (!lastElrResult.ok) {
    return lastElrResult
  }

  // offline_replicas (compact array of INT32)
  const offlineResult = decodeInt32CompactArray(reader)
  if (!offlineResult.ok) {
    return offlineResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      errorCode: errorCodeResult.value,
      partitionIndex: partIdxResult.value,
      leaderId: leaderResult.value,
      leaderEpoch: leaderEpochResult.value,
      replicaNodes: replicasResult.value,
      isrNodes: isrResult.value,
      eligibleLeaderReplicas: elrResult.value,
      lastKnownElr: lastElrResult.value,
      offlineReplicas: offlineResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeInt32CompactArray(reader: BinaryReader): DecodeResult<number[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const values: number[] = []
  for (let i = 0; i < count; i++) {
    const valResult = reader.readInt32()
    if (!valResult.ok) {
      return valResult
    }
    values.push(valResult.value)
  }

  return decodeSuccess(values, reader.offset - startOffset)
}

function decodeCursor(reader: BinaryReader): DecodeResult<TopicPartitionsCursor | null> {
  const startOffset = reader.offset

  // Nullable struct: -1 = null
  const indicatorResult = reader.readInt8()
  if (!indicatorResult.ok) {
    return indicatorResult
  }

  if (indicatorResult.value === -1) {
    return decodeSuccess(null, reader.offset - startOffset)
  }

  // topic_name (COMPACT_STRING)
  const topicResult = reader.readCompactString()
  if (!topicResult.ok) {
    return topicResult
  }

  // partition_index (INT32)
  const partResult = reader.readInt32()
  if (!partResult.ok) {
    return partResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      topicName: topicResult.value ?? "",
      partitionIndex: partResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
