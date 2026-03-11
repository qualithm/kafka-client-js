/**
 * Fetch request/response encoding and decoding.
 *
 * The Fetch API (key 1) retrieves record batches from topic partitions.
 * It is the primary API used by consumers to read messages from Kafka.
 *
 * **Request versions:**
 * - v0: replica_id, max_wait_ms, min_bytes, topics with partition fetch data
 * - v3+: max_bytes at request level
 * - v4+: isolation_level
 * - v7+: session_id, session_epoch, forgotten_topics_data
 * - v9+: current_leader_epoch per partition
 * - v11+: rack_id
 * - v12+: flexible encoding (KIP-482)
 * - v13+: topic_id (UUID) replaces topic name in topics and forgotten topics
 *
 * **Response versions:**
 * - v0: per-partition high_watermark, error_code, and record set
 * - v1+: throttle_time_ms
 * - v4+: aborted_transactions per partition
 * - v5+: log_start_offset per partition
 * - v7+: top-level error_code and session_id
 * - v11+: preferred_read_replica per partition
 * - v12+: flexible encoding
 * - v13+: topic_id (UUID) replaces topic name
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_Fetch
 *
 * @packageDocumentation
 */

import { ApiKey } from "./api-keys.js"
import type { BinaryReader, TaggedField } from "./binary-reader.js"
import type { BinaryWriter } from "./binary-writer.js"
import { frameRequest, type RequestHeader } from "./protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "./result.js"

// ---------------------------------------------------------------------------
// IsolationLevel (shared with ListOffsets, re-defined here for independence)
// ---------------------------------------------------------------------------

/**
 * Isolation level for fetch requests (v4+).
 *
 * - `ReadUncommitted` (0): Read all records including uncommitted transactional records.
 * - `ReadCommitted` (1): Only read committed transactional records and all non-transactional records.
 */
export const FetchIsolationLevel = {
  ReadUncommitted: 0,
  ReadCommitted: 1
} as const

export type FetchIsolationLevel = (typeof FetchIsolationLevel)[keyof typeof FetchIsolationLevel]

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * Per-partition fetch data in a Fetch request.
 */
export type FetchPartitionRequest = {
  /** The partition index. */
  readonly partitionIndex: number
  /**
   * The current leader epoch of the partition (v9+).
   * Use -1 if unknown.
   */
  readonly currentLeaderEpoch?: number
  /** The offset to fetch from. */
  readonly fetchOffset: bigint
  /**
   * The last fetched epoch of the partition (v12+).
   * Use -1 if unknown.
   */
  readonly lastFetchedEpoch?: number
  /**
   * The earliest available offset of the follower replica (v5+).
   * Used only when the request is from a follower. Consumers should set -1.
   */
  readonly logStartOffset?: bigint
  /** Maximum bytes to fetch from this partition. */
  readonly partitionMaxBytes: number
  /** Tagged fields (v12+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Per-topic fetch data in a Fetch request.
 */
export type FetchTopicRequest = {
  /**
   * The topic name (v0–v12).
   * Ignored in v13+ when topic_id is used.
   */
  readonly name?: string
  /**
   * The topic ID (v13+). A 16-byte UUID.
   */
  readonly topicId?: Uint8Array
  /** Partition data for this topic. */
  readonly partitions: readonly FetchPartitionRequest[]
  /** Tagged fields (v12+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A topic to forget from a fetch session (v7+).
 */
export type ForgottenTopic = {
  /**
   * The topic name (v7–v12).
   */
  readonly name?: string
  /**
   * The topic ID (v13+). A 16-byte UUID.
   */
  readonly topicId?: Uint8Array
  /** The partitions to forget. */
  readonly partitions: readonly number[]
  /** Tagged fields (v12+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Fetch request payload.
 */
export type FetchRequest = {
  /**
   * The broker ID of the follower (-1 for consumers).
   */
  readonly replicaId?: number
  /**
   * Maximum time in milliseconds to wait for data.
   */
  readonly maxWaitMs: number
  /**
   * Minimum bytes to accumulate before responding.
   */
  readonly minBytes: number
  /**
   * Maximum bytes to return at request level (v3+).
   * Default: 0x7fffffff (MAX_INT32).
   */
  readonly maxBytes?: number
  /**
   * Isolation level for transactional reads (v4+).
   */
  readonly isolationLevel?: FetchIsolationLevel
  /**
   * The fetch session ID (v7+). 0 for full fetches.
   */
  readonly sessionId?: number
  /**
   * The fetch session epoch (v7+). -1 for initial requests.
   */
  readonly sessionEpoch?: number
  /** Topics to fetch. */
  readonly topics: readonly FetchTopicRequest[]
  /**
   * Topics to remove from an incremental fetch session (v7+).
   */
  readonly forgottenTopics?: readonly ForgottenTopic[]
  /**
   * The consumer's rack ID (v11+).
   */
  readonly rackId?: string
  /** Tagged fields (v12+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * An aborted transaction in a Fetch response (v4+).
 */
export type FetchAbortedTransaction = {
  /** The producer ID of the aborted transaction. */
  readonly producerId: bigint
  /** The first offset in the aborted transaction. */
  readonly firstOffset: bigint
  /** Tagged fields (v12+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-partition response in a Fetch response.
 */
export type FetchPartitionResponse = {
  /** The partition index. */
  readonly partitionIndex: number
  /** The error code (0 = no error). */
  readonly errorCode: number
  /** The last stable offset of the partition. */
  readonly highWatermark: bigint
  /**
   * The last stable offset (v4+).
   * -1 if not available.
   */
  readonly lastStableOffset: bigint
  /**
   * The log start offset of the partition (v5+).
   * -1 if not available.
   */
  readonly logStartOffset: bigint
  /**
   * Aborted transactions in this partition (v4+).
   */
  readonly abortedTransactions: readonly FetchAbortedTransaction[]
  /**
   * The preferred read replica for the consumer (v11+).
   * -1 if not set.
   */
  readonly preferredReadReplica: number
  /**
   * The raw record batch data for this partition (nullable).
   * Must be decoded with {@link decodeRecordBatch}.
   */
  readonly records: Uint8Array | null
  /** Tagged fields (v12+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-topic response in a Fetch response.
 */
export type FetchTopicResponse = {
  /**
   * The topic name (v0–v12). Empty string in v13+ when topic_id is used.
   */
  readonly name: string
  /**
   * The topic ID (v13+). A 16-byte UUID, or empty for earlier versions.
   */
  readonly topicId: Uint8Array
  /** Partition responses. */
  readonly partitions: readonly FetchPartitionResponse[]
  /** Tagged fields (v12+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Fetch response payload.
 */
export type FetchResponse = {
  /**
   * Time the request was throttled in milliseconds (v1+).
   */
  readonly throttleTimeMs: number
  /**
   * Top-level error code (v7+). 0 = no error.
   */
  readonly errorCode: number
  /**
   * The fetch session ID (v7+). 0 if not using sessions.
   */
  readonly sessionId: number
  /** Topic responses. */
  readonly topics: readonly FetchTopicResponse[]
  /** Tagged fields (v12+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Null UUID constant
// ---------------------------------------------------------------------------

const NULL_UUID = new Uint8Array(16)

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a Fetch request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–13).
 */
export function encodeFetchRequest(
  writer: BinaryWriter,
  request: FetchRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 12

  // replica_id (INT32)
  writer.writeInt32(request.replicaId ?? -1)

  // max_wait_ms (INT32)
  writer.writeInt32(request.maxWaitMs)

  // min_bytes (INT32)
  writer.writeInt32(request.minBytes)

  // max_bytes (INT32, v3+)
  if (apiVersion >= 3) {
    writer.writeInt32(request.maxBytes ?? 0x7fffffff)
  }

  // isolation_level (INT8, v4+)
  if (apiVersion >= 4) {
    writer.writeInt8(request.isolationLevel ?? FetchIsolationLevel.ReadUncommitted)
  }

  // session_id (INT32, v7+)
  if (apiVersion >= 7) {
    writer.writeInt32(request.sessionId ?? 0)
  }

  // session_epoch (INT32, v7+)
  if (apiVersion >= 7) {
    writer.writeInt32(request.sessionEpoch ?? -1)
  }

  // topics array
  const { topics } = request
  if (isFlexible) {
    writer.writeUnsignedVarInt(topics.length + 1)
  } else {
    writer.writeInt32(topics.length)
  }

  for (const topic of topics) {
    encodeTopicRequest(writer, topic, apiVersion, isFlexible)
  }

  // forgotten_topics_data, rack_id, tagged fields (v7+)
  encodeFetchRequestTrailer(writer, request, apiVersion, isFlexible)
}

/**
 * Encode forgotten topics, rack ID, and trailing tagged fields.
 */
function encodeFetchRequestTrailer(
  writer: BinaryWriter,
  request: FetchRequest,
  apiVersion: number,
  isFlexible: boolean
): void {
  // forgotten_topics_data (v7+)
  if (apiVersion >= 7) {
    const forgotten = request.forgottenTopics ?? []
    if (isFlexible) {
      writer.writeUnsignedVarInt(forgotten.length + 1)
    } else {
      writer.writeInt32(forgotten.length)
    }
    for (const ft of forgotten) {
      encodeForgottenTopic(writer, ft, apiVersion, isFlexible)
    }
  }

  // rack_id (v11+)
  if (apiVersion >= 11) {
    if (isFlexible) {
      writer.writeCompactString(request.rackId ?? "")
    } else {
      writer.writeString(request.rackId ?? "")
    }
  }

  // Tagged fields (v12+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Encode a single topic in the request.
 */
function encodeTopicRequest(
  writer: BinaryWriter,
  topic: FetchTopicRequest,
  apiVersion: number,
  isFlexible: boolean
): void {
  // v13+: topic_id (UUID) instead of topic name
  if (apiVersion >= 13) {
    writer.writeUuid(topic.topicId ?? NULL_UUID)
  } else {
    if (isFlexible) {
      writer.writeCompactString(topic.name ?? "")
    } else {
      writer.writeString(topic.name ?? "")
    }
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

  if (isFlexible) {
    writer.writeTaggedFields(topic.taggedFields ?? [])
  }
}

/**
 * Encode a single partition in the request.
 */
function encodePartitionRequest(
  writer: BinaryWriter,
  partition: FetchPartitionRequest,
  apiVersion: number,
  isFlexible: boolean
): void {
  // partition_index (INT32)
  writer.writeInt32(partition.partitionIndex)

  // current_leader_epoch (INT32, v9+)
  if (apiVersion >= 9) {
    writer.writeInt32(partition.currentLeaderEpoch ?? -1)
  }

  // fetch_offset (INT64)
  writer.writeInt64(partition.fetchOffset)

  // last_fetched_epoch (INT32, v12+)
  if (apiVersion >= 12) {
    writer.writeInt32(partition.lastFetchedEpoch ?? -1)
  }

  // log_start_offset (INT64, v5+)
  if (apiVersion >= 5) {
    writer.writeInt64(partition.logStartOffset ?? -1n)
  }

  // partition_max_bytes (INT32)
  writer.writeInt32(partition.partitionMaxBytes)

  if (isFlexible) {
    writer.writeTaggedFields(partition.taggedFields ?? [])
  }
}

/**
 * Encode a forgotten topic entry (v7+).
 */
function encodeForgottenTopic(
  writer: BinaryWriter,
  ft: ForgottenTopic,
  apiVersion: number,
  isFlexible: boolean
): void {
  // v13+: topic_id (UUID) instead of topic name
  if (apiVersion >= 13) {
    writer.writeUuid(ft.topicId ?? NULL_UUID)
  } else {
    if (isFlexible) {
      writer.writeCompactString(ft.name ?? "")
    } else {
      writer.writeString(ft.name ?? "")
    }
  }

  // partitions array (INT32 array of partition indices)
  if (isFlexible) {
    writer.writeUnsignedVarInt(ft.partitions.length + 1)
  } else {
    writer.writeInt32(ft.partitions.length)
  }
  for (const p of ft.partitions) {
    writer.writeInt32(p)
  }

  if (isFlexible) {
    writer.writeTaggedFields(ft.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed Fetch request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–13).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildFetchRequest(
  correlationId: number,
  apiVersion: number,
  request: FetchRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.Fetch,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeFetchRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a Fetch response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–13).
 * @returns The decoded response or a failure.
 */
export function decodeFetchResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<FetchResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 12

  // throttle_time_ms (INT32, v1+)
  let throttleTimeMs = 0
  if (apiVersion >= 1) {
    const result = reader.readInt32()
    if (!result.ok) {
      return result
    }
    throttleTimeMs = result.value
  }

  // error_code (INT16, v7+)
  let errorCode = 0
  if (apiVersion >= 7) {
    const result = reader.readInt16()
    if (!result.ok) {
      return result
    }
    errorCode = result.value
  }

  // session_id (INT32, v7+)
  let sessionId = 0
  if (apiVersion >= 7) {
    const result = reader.readInt32()
    if (!result.ok) {
      return result
    }
    sessionId = result.value
  }

  // topics array
  const topicsResult = decodeTopicResponses(reader, apiVersion, isFlexible)
  if (!topicsResult.ok) {
    return topicsResult
  }

  // Tagged fields (v12+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const result = reader.readTaggedFields()
    if (!result.ok) {
      return result
    }
    taggedFields = result.value
  }

  return decodeSuccess(
    {
      throttleTimeMs,
      errorCode,
      sessionId,
      topics: topicsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

// ---------------------------------------------------------------------------
// Response decoding helpers
// ---------------------------------------------------------------------------

/**
 * Decode the topics array from a Fetch response.
 */
function decodeTopicResponses(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<FetchTopicResponse[]> {
  const startOffset = reader.offset

  let topicCount: number
  if (isFlexible) {
    const result = reader.readUnsignedVarInt()
    if (!result.ok) {
      return result
    }
    topicCount = result.value - 1
  } else {
    const result = reader.readInt32()
    if (!result.ok) {
      return result
    }
    topicCount = result.value
  }

  const topics: FetchTopicResponse[] = []
  for (let i = 0; i < topicCount; i++) {
    const result = decodeTopicResponse(reader, apiVersion, isFlexible)
    if (!result.ok) {
      return result
    }
    topics.push(result.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

/**
 * Decode a single topic response.
 */
function decodeTopicResponse(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<FetchTopicResponse> {
  const startOffset = reader.offset

  // v13+: topic_id (UUID); earlier: topic name
  let name = ""
  let topicId: Uint8Array = NULL_UUID
  if (apiVersion >= 13) {
    const result = reader.readUuid()
    if (!result.ok) {
      return result
    }
    topicId = new Uint8Array(result.value)
  } else {
    if (isFlexible) {
      const result = reader.readCompactString()
      if (!result.ok) {
        return result
      }
      name = result.value ?? ""
    } else {
      const result = reader.readString()
      if (!result.ok) {
        return result
      }
      name = result.value ?? ""
    }
  }

  // partitions array
  const partitionsResult = decodePartitionResponses(reader, apiVersion, isFlexible)
  if (!partitionsResult.ok) {
    return partitionsResult
  }

  // Tagged fields (v12+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const result = reader.readTaggedFields()
    if (!result.ok) {
      return result
    }
    taggedFields = result.value
  }

  return decodeSuccess(
    {
      name,
      topicId,
      partitions: partitionsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

/**
 * Decode the partitions array from a topic response.
 */
function decodePartitionResponses(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<FetchPartitionResponse[]> {
  const startOffset = reader.offset

  let partitionCount: number
  if (isFlexible) {
    const result = reader.readUnsignedVarInt()
    if (!result.ok) {
      return result
    }
    partitionCount = result.value - 1
  } else {
    const result = reader.readInt32()
    if (!result.ok) {
      return result
    }
    partitionCount = result.value
  }

  const partitions: FetchPartitionResponse[] = []
  for (let i = 0; i < partitionCount; i++) {
    const result = decodePartitionResponse(reader, apiVersion, isFlexible)
    if (!result.ok) {
      return result
    }
    partitions.push(result.value)
  }

  return decodeSuccess(partitions, reader.offset - startOffset)
}

/**
 * Decode a single partition response.
 */
function decodePartitionResponse(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<FetchPartitionResponse> {
  const startOffset = reader.offset

  // partition_index (INT32)
  const indexResult = reader.readInt32()
  if (!indexResult.ok) {
    return indexResult
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // high_watermark (INT64)
  const hwResult = reader.readInt64()
  if (!hwResult.ok) {
    return hwResult
  }

  // last_stable_offset (INT64, v4+)
  let lastStableOffset = -1n
  if (apiVersion >= 4) {
    const result = reader.readInt64()
    if (!result.ok) {
      return result
    }
    lastStableOffset = result.value
  }

  // log_start_offset (INT64, v5+)
  let logStartOffset = -1n
  if (apiVersion >= 5) {
    const result = reader.readInt64()
    if (!result.ok) {
      return result
    }
    logStartOffset = result.value
  }

  // aborted_transactions (v4+)
  let abortedTransactions: FetchAbortedTransaction[] = []
  if (apiVersion >= 4) {
    const result = decodeAbortedTransactions(reader, isFlexible)
    if (!result.ok) {
      return result
    }
    abortedTransactions = result.value
  }

  // preferred_read_replica (INT32, v11+)
  let preferredReadReplica = -1
  if (apiVersion >= 11) {
    const result = reader.readInt32()
    if (!result.ok) {
      return result
    }
    preferredReadReplica = result.value
  }

  // records (nullable bytes)
  const recordsResult = isFlexible ? reader.readCompactBytes() : reader.readBytes()
  if (!recordsResult.ok) {
    return recordsResult
  }
  const records = recordsResult.value

  // Tagged fields (v12+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const result = reader.readTaggedFields()
    if (!result.ok) {
      return result
    }
    taggedFields = result.value
  }

  return decodeSuccess(
    {
      partitionIndex: indexResult.value,
      errorCode: errorCodeResult.value,
      highWatermark: hwResult.value,
      lastStableOffset,
      logStartOffset,
      abortedTransactions,
      preferredReadReplica,
      records,
      taggedFields
    },
    reader.offset - startOffset
  )
}

/**
 * Decode the aborted transactions array (v4+).
 */
function decodeAbortedTransactions(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<FetchAbortedTransaction[]> {
  const startOffset = reader.offset

  let count: number
  if (isFlexible) {
    const result = reader.readUnsignedVarInt()
    if (!result.ok) {
      return result
    }
    count = result.value - 1
  } else {
    const result = reader.readInt32()
    if (!result.ok) {
      return result
    }
    count = result.value
  }

  // Nullable array: -1 means null (treated as empty)
  if (count < 0) {
    return decodeSuccess([], reader.offset - startOffset)
  }

  const transactions: FetchAbortedTransaction[] = []
  for (let i = 0; i < count; i++) {
    // producer_id (INT64)
    const pidResult = reader.readInt64()
    if (!pidResult.ok) {
      return pidResult
    }

    // first_offset (INT64)
    const offsetResult = reader.readInt64()
    if (!offsetResult.ok) {
      return offsetResult
    }

    // Tagged fields (v12+)
    let taggedFields: readonly TaggedField[] = []
    if (isFlexible) {
      const result = reader.readTaggedFields()
      if (!result.ok) {
        return result
      }
      taggedFields = result.value
    }

    transactions.push({
      producerId: pidResult.value,
      firstOffset: offsetResult.value,
      taggedFields
    })
  }

  return decodeSuccess(transactions, reader.offset - startOffset)
}
