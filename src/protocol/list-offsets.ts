/**
 * ListOffsets request/response encoding and decoding.
 *
 * The ListOffsets API (key 2) retrieves offsets for topic partitions based on
 * timestamps. This is essential for consumer offset reset (earliest/latest)
 * and seeking to specific timestamps.
 *
 * **Request versions:**
 * - v0: replica ID, topics with partitions and timestamp
 * - v1: adds timestamp -1 (latest) vs -2 (earliest) semantics
 * - v2+: adds isolation level (read_uncommitted=0, read_committed=1)
 * - v4+: adds current_leader_epoch to partitions
 * - v6+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: topics with partitions, error code, and offsets array
 * - v1+: single offset instead of offsets array, adds timestamp
 * - v4+: adds leader_epoch
 * - v6+: flexible encoding
 *
 * **Special timestamps:**
 * - `-1` (LATEST): Get the offset of the next message to be written
 * - `-2` (EARLIEST): Get the earliest available offset
 * - `-3` (MAX_TIMESTAMP, v7+): Get the offset with the largest timestamp
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_ListOffsets
 *
 * @packageDocumentation
 */

import { ApiKey } from "../codec/api-keys.js"
import type { BinaryReader, TaggedField } from "../codec/binary-reader.js"
import type { BinaryWriter } from "../codec/binary-writer.js"
import { frameRequest, type RequestHeader } from "../codec/protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "../result.js"

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/**
 * Special timestamp values for offset lookups.
 */
export const OffsetTimestamp = {
  /** Get the offset of the next message to be written (end of partition). */
  Latest: -1n,
  /** Get the earliest available offset (beginning of partition). */
  Earliest: -2n,
  /** Get the offset with the largest timestamp (v7+). */
  MaxTimestamp: -3n
} as const

export type OffsetTimestamp = (typeof OffsetTimestamp)[keyof typeof OffsetTimestamp]

/**
 * Isolation level for transactional reads.
 */
export const IsolationLevel = {
  /** Read uncommitted messages (default, includes transactional messages). */
  ReadUncommitted: 0,
  /** Read committed messages only (excludes in-flight transactional messages). */
  ReadCommitted: 1
} as const

export type IsolationLevel = (typeof IsolationLevel)[keyof typeof IsolationLevel]

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * Partition in a ListOffsets request.
 */
export type ListOffsetsPartitionRequest = {
  /** The partition index. */
  readonly partitionIndex: number
  /**
   * The current leader epoch, if known (v4+).
   * Use -1 if unknown or if version < 4.
   */
  readonly currentLeaderEpoch?: number
  /**
   * The timestamp to query for.
   * Use OffsetTimestamp.Latest (-1) or OffsetTimestamp.Earliest (-2),
   * or a specific timestamp in milliseconds.
   */
  readonly timestamp: bigint
  /** Tagged fields (v6+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Topic in a ListOffsets request.
 */
export type ListOffsetsTopicRequest = {
  /** The topic name. */
  readonly name: string
  /** The partitions to query. */
  readonly partitions: readonly ListOffsetsPartitionRequest[]
  /** Tagged fields (v6+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * ListOffsets request payload.
 */
export type ListOffsetsRequest = {
  /**
   * The broker ID of the requester, or -1 for clients.
   * Used for debugging on the broker side.
   */
  readonly replicaId?: number
  /**
   * Isolation level for transactional reads (v2+).
   * 0 = READ_UNCOMMITTED (default), 1 = READ_COMMITTED.
   */
  readonly isolationLevel?: IsolationLevel
  /** The topics to query. */
  readonly topics: readonly ListOffsetsTopicRequest[]
  /** Tagged fields (v6+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Partition response in a ListOffsets response.
 */
export type ListOffsetsPartitionResponse = {
  /** The partition index. */
  readonly partitionIndex: number
  /** The error code (0 = no error). */
  readonly errorCode: number
  /**
   * The timestamp associated with the offset (v1+).
   * -1 if not available.
   */
  readonly timestamp: bigint
  /**
   * The offset (v1+).
   * In v0, this comes from the old_style_offsets array instead.
   */
  readonly offset: bigint
  /**
   * The leader epoch of the returned offset (v4+).
   * -1 if not available.
   */
  readonly leaderEpoch: number
  /** Tagged fields (v6+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Topic response in a ListOffsets response.
 */
export type ListOffsetsTopicResponse = {
  /** The topic name. */
  readonly name: string
  /** The partition responses. */
  readonly partitions: readonly ListOffsetsPartitionResponse[]
  /** Tagged fields (v6+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * ListOffsets response payload.
 */
export type ListOffsetsResponse = {
  /** Time the request was throttled in milliseconds (v2+). */
  readonly throttleTimeMs: number
  /** The topic responses. */
  readonly topics: readonly ListOffsetsTopicResponse[]
  /** Tagged fields (v6+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a ListOffsets request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–7).
 */
export function encodeListOffsetsRequest(
  writer: BinaryWriter,
  request: ListOffsetsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 6

  // replica_id (INT32)
  writer.writeInt32(request.replicaId ?? -1)

  // isolation_level (INT8, v2+)
  if (apiVersion >= 2) {
    writer.writeInt8(request.isolationLevel ?? IsolationLevel.ReadUncommitted)
  }

  // topics array
  if (isFlexible) {
    // Compact array: length + 1
    writer.writeUnsignedVarInt(request.topics.length + 1)
  } else {
    writer.writeInt32(request.topics.length)
  }

  for (const topic of request.topics) {
    encodeTopicRequest(writer, topic, apiVersion, isFlexible)
  }

  // Tagged fields (v6+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Encode a topic in the request.
 */
function encodeTopicRequest(
  writer: BinaryWriter,
  topic: ListOffsetsTopicRequest,
  apiVersion: number,
  isFlexible: boolean
): void {
  // topic name
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
    encodePartitionRequest(writer, partition, apiVersion, isFlexible)
  }

  // Tagged fields (v6+)
  if (isFlexible) {
    writer.writeTaggedFields(topic.taggedFields ?? [])
  }
}

/**
 * Encode a partition in the request.
 */
function encodePartitionRequest(
  writer: BinaryWriter,
  partition: ListOffsetsPartitionRequest,
  apiVersion: number,
  isFlexible: boolean
): void {
  // partition_index (INT32)
  writer.writeInt32(partition.partitionIndex)

  // current_leader_epoch (INT32, v4+)
  if (apiVersion >= 4) {
    writer.writeInt32(partition.currentLeaderEpoch ?? -1)
  }

  // timestamp (INT64)
  writer.writeInt64(partition.timestamp)

  // v0 only: max_num_offsets (INT32) - deprecated, always 1
  if (apiVersion === 0) {
    writer.writeInt32(1)
  }

  // Tagged fields (v6+)
  if (isFlexible) {
    writer.writeTaggedFields(partition.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed ListOffsets request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–7).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildListOffsetsRequest(
  correlationId: number,
  apiVersion: number,
  request: ListOffsetsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.ListOffsets,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeListOffsetsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a ListOffsets response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–7).
 * @returns The decoded response or a failure.
 */
export function decodeListOffsetsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<ListOffsetsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 6

  // throttle_time_ms (INT32, v2+)
  let throttleTimeMs = 0
  if (apiVersion >= 2) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  // topics array
  const topicsResult = decodeTopicsArray(reader, apiVersion, isFlexible)
  if (!topicsResult.ok) {
    return topicsResult
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
    {
      throttleTimeMs,
      topics: topicsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

/**
 * Decode the topics array from a ListOffsets response.
 */
function decodeTopicsArray(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<ListOffsetsTopicResponse[]> {
  const startOffset = reader.offset

  // Array length
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

  const topics: ListOffsetsTopicResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    const topicResult = decodeTopicResponse(reader, apiVersion, isFlexible)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

/**
 * Decode a single topic from the response.
 */
function decodeTopicResponse(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<ListOffsetsTopicResponse> {
  const startOffset = reader.offset

  // topic name
  let name: string
  if (isFlexible) {
    const nameResult = reader.readCompactString()
    if (!nameResult.ok) {
      return nameResult
    }
    name = nameResult.value ?? ""
  } else {
    const nameResult = reader.readString()
    if (!nameResult.ok) {
      return nameResult
    }
    name = nameResult.value ?? ""
  }

  // partitions array
  const partitionsResult = decodePartitionsArray(reader, apiVersion, isFlexible)
  if (!partitionsResult.ok) {
    return partitionsResult
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
    {
      name,
      partitions: partitionsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

/**
 * Decode the partitions array from a topic response.
 */
function decodePartitionsArray(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<ListOffsetsPartitionResponse[]> {
  const startOffset = reader.offset

  // Array length
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

  const partitions: ListOffsetsPartitionResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    const partitionResult = decodePartitionResponse(reader, apiVersion, isFlexible)
    if (!partitionResult.ok) {
      return partitionResult
    }
    partitions.push(partitionResult.value)
  }

  return decodeSuccess(partitions, reader.offset - startOffset)
}

/**
 * Decode a single partition from the response.
 */
function decodePartitionResponse(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<ListOffsetsPartitionResponse> {
  const startOffset = reader.offset

  // partition_index (INT32)
  const partitionIndexResult = reader.readInt32()
  if (!partitionIndexResult.ok) {
    return partitionIndexResult
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  let timestamp = -1n
  let offset = -1n
  let leaderEpoch = -1

  if (apiVersion === 0) {
    // v0: old_style_offsets array (deprecated)
    // Array of INT64, typically just one element
    const oldOffsetsLengthResult = reader.readInt32()
    if (!oldOffsetsLengthResult.ok) {
      return oldOffsetsLengthResult
    }
    const offsetsLength = oldOffsetsLengthResult.value

    if (offsetsLength > 0) {
      const offsetResult = reader.readInt64()
      if (!offsetResult.ok) {
        return offsetResult
      }
      offset = offsetResult.value

      // Skip remaining offsets if any
      for (let i = 1; i < offsetsLength; i++) {
        const skipResult = reader.readInt64()
        if (!skipResult.ok) {
          return skipResult
        }
      }
    }
  } else {
    // v1+: timestamp and single offset
    const timestampResult = reader.readInt64()
    if (!timestampResult.ok) {
      return timestampResult
    }
    timestamp = timestampResult.value

    const offsetResult = reader.readInt64()
    if (!offsetResult.ok) {
      return offsetResult
    }
    offset = offsetResult.value

    // leader_epoch (INT32, v4+)
    if (apiVersion >= 4) {
      const epochResult = reader.readInt32()
      if (!epochResult.ok) {
        return epochResult
      }
      leaderEpoch = epochResult.value
    }
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
    {
      partitionIndex: partitionIndexResult.value,
      errorCode: errorCodeResult.value,
      timestamp,
      offset,
      leaderEpoch,
      taggedFields
    },
    reader.offset - startOffset
  )
}
