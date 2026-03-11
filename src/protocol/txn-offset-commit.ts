/**
 * TxnOffsetCommit request/response encoding and decoding.
 *
 * The TxnOffsetCommit API (key 28) commits consumer offsets as part of a
 * transaction. This allows exactly-once semantics when consuming from one
 * topic and producing to another within the same transaction.
 *
 * **Request versions:**
 * - v0–v2: transactional_id, group_id, producer_id, producer_epoch, topics[]
 * - v2+: adds generation_id, member_id
 * - v3+: flexible encoding (KIP-482), adds group_instance_id
 *
 * **Response versions:**
 * - v0: throttle_time_ms, topics[partitions[error_code]]
 * - v3+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_TxnOffsetCommit
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
 * Per-partition offset to commit in a transaction.
 */
export type TxnOffsetCommitPartitionRequest = {
  /** Partition index. */
  readonly partitionIndex: number
  /** The committed offset. */
  readonly committedOffset: bigint
  /** Leader epoch for fencing (v2+). -1 if unknown. */
  readonly committedLeaderEpoch?: number
  /** Metadata associated with the offset (nullable). */
  readonly committedMetadata: string | null
}

/**
 * Per-topic offsets to commit in a transaction.
 */
export type TxnOffsetCommitTopicRequest = {
  /** Topic name. */
  readonly name: string
  /** Per-partition offset data. */
  readonly partitions: readonly TxnOffsetCommitPartitionRequest[]
}

/**
 * TxnOffsetCommit request payload.
 */
export type TxnOffsetCommitRequest = {
  /** The transactional ID. */
  readonly transactionalId: string
  /** The consumer group ID. */
  readonly groupId: string
  /** The current producer ID. */
  readonly producerId: bigint
  /** The current producer epoch. */
  readonly producerEpoch: number
  /** The generation ID for the consumer group (v3+). -1 if not a member. */
  readonly generationId?: number
  /** The member ID within the consumer group (v3+). */
  readonly memberId?: string
  /** The group instance ID for static membership (v3+). */
  readonly groupInstanceId?: string | null
  /** Topics with partition offsets to commit. */
  readonly topics: readonly TxnOffsetCommitTopicRequest[]
  /** Tagged fields (v3+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-partition result in TxnOffsetCommit response.
 */
export type TxnOffsetCommitPartitionResponse = {
  /** Partition index. */
  readonly partitionIndex: number
  /** Error code (0 = success). */
  readonly errorCode: number
}

/**
 * Per-topic result in TxnOffsetCommit response.
 */
export type TxnOffsetCommitTopicResponse = {
  /** Topic name. */
  readonly name: string
  /** Per-partition results. */
  readonly partitions: readonly TxnOffsetCommitPartitionResponse[]
}

/**
 * TxnOffsetCommit response payload.
 */
export type TxnOffsetCommitResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-topic results. */
  readonly topics: readonly TxnOffsetCommitTopicResponse[]
  /** Tagged fields from flexible versions (v3+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding — helpers
// ---------------------------------------------------------------------------

function encodePartition(
  writer: BinaryWriter,
  partition: TxnOffsetCommitPartitionRequest,
  apiVersion: number,
  isFlexible: boolean
): void {
  // partition_index (INT32)
  writer.writeInt32(partition.partitionIndex)

  // committed_offset (INT64)
  writer.writeInt64(partition.committedOffset)

  // committed_leader_epoch (INT32, v2+)
  if (apiVersion >= 2) {
    writer.writeInt32(partition.committedLeaderEpoch ?? -1)
  }

  // committed_metadata (nullable string)
  if (isFlexible) {
    writer.writeCompactString(partition.committedMetadata)
  } else {
    writer.writeString(partition.committedMetadata)
  }

  if (isFlexible) {
    writer.writeTaggedFields([])
  }
}

function encodeTopic(
  writer: BinaryWriter,
  topic: TxnOffsetCommitTopicRequest,
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
    encodePartition(writer, partition, apiVersion, isFlexible)
  }

  if (isFlexible) {
    writer.writeTaggedFields([])
  }
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a TxnOffsetCommit request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–3).
 */
export function encodeTxnOffsetCommitRequest(
  writer: BinaryWriter,
  request: TxnOffsetCommitRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 3

  // transactional_id
  if (isFlexible) {
    writer.writeCompactString(request.transactionalId)
  } else {
    writer.writeString(request.transactionalId)
  }

  // group_id
  if (isFlexible) {
    writer.writeCompactString(request.groupId)
  } else {
    writer.writeString(request.groupId)
  }

  // producer_id (INT64)
  writer.writeInt64(request.producerId)

  // producer_epoch (INT16)
  writer.writeInt16(request.producerEpoch)

  // generation_id (INT32, v3+)
  if (apiVersion >= 3) {
    writer.writeInt32(request.generationId ?? -1)
  }

  // member_id (v3+)
  if (apiVersion >= 3) {
    if (isFlexible) {
      writer.writeCompactString(request.memberId ?? "")
    } else {
      writer.writeString(request.memberId ?? "")
    }
  }

  // group_instance_id (nullable, v3+)
  if (apiVersion >= 3) {
    if (isFlexible) {
      writer.writeCompactString(request.groupInstanceId ?? null)
    } else {
      writer.writeString(request.groupInstanceId ?? null)
    }
  }

  // topics array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.topics.length + 1)
  } else {
    writer.writeInt32(request.topics.length)
  }

  for (const topic of request.topics) {
    encodeTopic(writer, topic, apiVersion, isFlexible)
  }

  // Tagged fields (v3+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed TxnOffsetCommit request ready to send to a broker.
 */
export function buildTxnOffsetCommitRequest(
  correlationId: number,
  apiVersion: number,
  request: TxnOffsetCommitRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.TxnOffsetCommit,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeTxnOffsetCommitRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding — helpers
// ---------------------------------------------------------------------------

function decodePartitionResponse(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<TxnOffsetCommitPartitionResponse> {
  const startOffset = reader.offset

  const partitionIndexResult = reader.readInt32()
  if (!partitionIndexResult.ok) {
    return partitionIndexResult
  }

  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
  }

  return decodeSuccess(
    {
      partitionIndex: partitionIndexResult.value,
      errorCode: errorCodeResult.value
    },
    reader.offset - startOffset
  )
}

function decodeTopicResponse(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<TxnOffsetCommitTopicResponse> {
  const startOffset = reader.offset

  // name
  const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!nameResult.ok) {
    return nameResult
  }

  // partitions array
  let partitionCount: number
  if (isFlexible) {
    const countResult = reader.readUnsignedVarInt()
    if (!countResult.ok) {
      return countResult
    }
    partitionCount = countResult.value - 1
  } else {
    const countResult = reader.readInt32()
    if (!countResult.ok) {
      return countResult
    }
    partitionCount = countResult.value
  }

  const partitions: TxnOffsetCommitPartitionResponse[] = []
  for (let i = 0; i < partitionCount; i++) {
    const partResult = decodePartitionResponse(reader, isFlexible)
    if (!partResult.ok) {
      return partResult
    }
    partitions.push(partResult.value)
  }

  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
  }

  return decodeSuccess({ name: nameResult.value ?? "", partitions }, reader.offset - startOffset)
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a TxnOffsetCommit response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–3).
 * @returns The decoded response or a failure.
 */
export function decodeTxnOffsetCommitResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<TxnOffsetCommitResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 3

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // topics array
  let topicCount: number
  if (isFlexible) {
    const countResult = reader.readUnsignedVarInt()
    if (!countResult.ok) {
      return countResult
    }
    topicCount = countResult.value - 1
  } else {
    const countResult = reader.readInt32()
    if (!countResult.ok) {
      return countResult
    }
    topicCount = countResult.value
  }

  const topics: TxnOffsetCommitTopicResponse[] = []
  for (let i = 0; i < topicCount; i++) {
    const topicResult = decodeTopicResponse(reader, isFlexible)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  // Tagged fields (v3+)
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
      throttleTimeMs: throttleResult.value,
      topics,
      taggedFields
    },
    reader.offset - startOffset
  )
}
