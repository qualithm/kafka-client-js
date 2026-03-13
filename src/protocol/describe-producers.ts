/**
 * DescribeProducers request/response encoding and decoding.
 *
 * The DescribeProducers API (key 61) describes active producers on specified
 * topic partitions, useful for transaction introspection.
 *
 * **Request versions:**
 * - v0: topics array with partition_indexes
 * - All versions use flexible encoding
 *
 * **Response versions:**
 * - v0: throttle_time_ms, topics array with per-partition producer info
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeProducers
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
 * A topic to describe producers for.
 */
export type DescribeProducersTopicRequest = {
  /** The topic name. */
  readonly name: string
  /** The partition indexes to describe. */
  readonly partitionIndexes: readonly number[]
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * DescribeProducers request payload.
 */
export type DescribeProducersRequest = {
  /** Topics and partitions to describe. */
  readonly topics: readonly DescribeProducersTopicRequest[]
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * An active producer on a partition.
 */
export type ProducerState = {
  /** The producer ID. */
  readonly producerId: bigint
  /** The producer epoch. */
  readonly producerEpoch: number
  /** The last sequence produced. */
  readonly lastSequence: number
  /** The last timestamp produced. */
  readonly lastTimestamp: bigint
  /** The coordinator epoch, or -1 if not known. */
  readonly coordinatorEpoch: number
  /** The current transaction start offset, or -1 if not in a transaction. */
  readonly currentTxnStartOffset: bigint
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-partition producer description.
 */
export type DescribeProducersPartitionResponse = {
  /** The partition index. */
  readonly partitionIndex: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Error message, or null. */
  readonly errorMessage: string | null
  /** Active producers on this partition. */
  readonly activeProducers: readonly ProducerState[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-topic producer description.
 */
export type DescribeProducersTopicResponse = {
  /** The topic name. */
  readonly name: string
  /** Per-partition results. */
  readonly partitions: readonly DescribeProducersPartitionResponse[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeProducers response payload.
 */
export type DescribeProducersResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-topic results. */
  readonly topics: readonly DescribeProducersTopicResponse[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeProducers request body into the given writer.
 */
export function encodeDescribeProducersRequest(
  writer: BinaryWriter,
  request: DescribeProducersRequest,
  _apiVersion: number
): void {
  // topics array (compact)
  writer.writeUnsignedVarInt(request.topics.length + 1)
  for (const topic of request.topics) {
    // name (COMPACT_STRING)
    writer.writeCompactString(topic.name)

    // partition_indexes (compact array of INT32)
    writer.writeUnsignedVarInt(topic.partitionIndexes.length + 1)
    for (const idx of topic.partitionIndexes) {
      writer.writeInt32(idx)
    }

    // tagged fields
    writer.writeTaggedFields(topic.taggedFields ?? [])
  }

  // tagged fields
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed DescribeProducers request ready to send to a broker.
 */
export function buildDescribeProducersRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeProducersRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeProducers,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeProducersRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeProducers response body.
 */
export function decodeDescribeProducersResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<DescribeProducersResponse> {
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

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      throttleTimeMs: throttleResult.value,
      topics: topicsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeTopicsArray(reader: BinaryReader): DecodeResult<DescribeProducersTopicResponse[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const topics: DescribeProducersTopicResponse[] = []
  for (let i = 0; i < count; i++) {
    const topicResult = decodeTopicEntry(reader)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

function decodeTopicEntry(reader: BinaryReader): DecodeResult<DescribeProducersTopicResponse> {
  const startOffset = reader.offset

  // name (COMPACT_STRING)
  const nameResult = reader.readCompactString()
  if (!nameResult.ok) {
    return nameResult
  }

  // partitions array
  const partitionsResult = decodePartitionsArray(reader)
  if (!partitionsResult.ok) {
    return partitionsResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      name: nameResult.value ?? "",
      partitions: partitionsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodePartitionsArray(
  reader: BinaryReader
): DecodeResult<DescribeProducersPartitionResponse[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const partitions: DescribeProducersPartitionResponse[] = []
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
): DecodeResult<DescribeProducersPartitionResponse> {
  const startOffset = reader.offset

  // partition_index (INT32)
  const partIdxResult = reader.readInt32()
  if (!partIdxResult.ok) {
    return partIdxResult
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // error_message (COMPACT_NULLABLE_STRING)
  const errorMsgResult = reader.readCompactString()
  if (!errorMsgResult.ok) {
    return errorMsgResult
  }

  // active_producers array
  const producersResult = decodeActiveProducers(reader)
  if (!producersResult.ok) {
    return producersResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      partitionIndex: partIdxResult.value,
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      activeProducers: producersResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeActiveProducers(reader: BinaryReader): DecodeResult<ProducerState[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const producers: ProducerState[] = []
  for (let i = 0; i < count; i++) {
    const prodResult = decodeProducerState(reader)
    if (!prodResult.ok) {
      return prodResult
    }
    producers.push(prodResult.value)
  }

  return decodeSuccess(producers, reader.offset - startOffset)
}

function decodeProducerState(reader: BinaryReader): DecodeResult<ProducerState> {
  const startOffset = reader.offset

  // producer_id (INT64)
  const producerIdResult = reader.readInt64()
  if (!producerIdResult.ok) {
    return producerIdResult
  }

  // producer_epoch (INT32)
  const epochResult = reader.readInt32()
  if (!epochResult.ok) {
    return epochResult
  }

  // last_sequence (INT32)
  const lastSeqResult = reader.readInt32()
  if (!lastSeqResult.ok) {
    return lastSeqResult
  }

  // last_timestamp (INT64)
  const lastTsResult = reader.readInt64()
  if (!lastTsResult.ok) {
    return lastTsResult
  }

  // coordinator_epoch (INT32)
  const coordEpochResult = reader.readInt32()
  if (!coordEpochResult.ok) {
    return coordEpochResult
  }

  // current_txn_start_offset (INT64)
  const txnOffsetResult = reader.readInt64()
  if (!txnOffsetResult.ok) {
    return txnOffsetResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      producerId: producerIdResult.value,
      producerEpoch: epochResult.value,
      lastSequence: lastSeqResult.value,
      lastTimestamp: lastTsResult.value,
      coordinatorEpoch: coordEpochResult.value,
      currentTxnStartOffset: txnOffsetResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
