/**
 * AddPartitionsToTxn request/response encoding and decoding.
 *
 * The AddPartitionsToTxn API (key 24) adds topic-partitions to an ongoing
 * transaction. The broker verifies that the producer is the current owner
 * of the transaction and that the partitions are not already part of the
 * transaction.
 *
 * **Request versions:**
 * - v0–v2: transactional_id, producer_id, producer_epoch, topics[]
 * - v3+: flexible encoding (KIP-482)
 * - v4+: batched transactions (multiple transactional IDs)
 *
 * **Response versions:**
 * - v0: throttle_time_ms, results by topic/partition
 * - v3+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_AddPartitionsToTxn
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
 * A topic with partitions to add to a transaction.
 */
export type AddPartitionsToTxnTopic = {
  /** Topic name. */
  readonly name: string
  /** Partition indexes to add. */
  readonly partitions: readonly number[]
}

/**
 * AddPartitionsToTxn request payload (v0–v3, single transaction).
 */
export type AddPartitionsToTxnRequest = {
  /** The transactional ID. */
  readonly transactionalId: string
  /** The current producer ID. */
  readonly producerId: bigint
  /** The current producer epoch. */
  readonly producerEpoch: number
  /** Topics with partitions to add. */
  readonly topics: readonly AddPartitionsToTxnTopic[]
  /** Tagged fields (v3+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-partition result in AddPartitionsToTxn response.
 */
export type AddPartitionsToTxnPartitionResult = {
  /** Partition index. */
  readonly partitionIndex: number
  /** Error code (0 = success). */
  readonly partitionErrorCode: number
}

/**
 * Per-topic result in AddPartitionsToTxn response.
 */
export type AddPartitionsToTxnTopicResult = {
  /** Topic name. */
  readonly name: string
  /** Per-partition results. */
  readonly partitions: readonly AddPartitionsToTxnPartitionResult[]
}

/**
 * AddPartitionsToTxn response payload.
 */
export type AddPartitionsToTxnResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-topic results. */
  readonly topics: readonly AddPartitionsToTxnTopicResult[]
  /** Tagged fields from flexible versions (v3+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

function encodeAddPartitionsToTxnTopics(
  writer: BinaryWriter,
  topics: readonly AddPartitionsToTxnTopic[],
  isFlexible: boolean
): void {
  if (isFlexible) {
    writer.writeUnsignedVarInt(topics.length + 1)
  } else {
    writer.writeInt32(topics.length)
  }

  for (const topic of topics) {
    if (isFlexible) {
      writer.writeCompactString(topic.name)
    } else {
      writer.writeString(topic.name)
    }

    if (isFlexible) {
      writer.writeUnsignedVarInt(topic.partitions.length + 1)
    } else {
      writer.writeInt32(topic.partitions.length)
    }

    for (const partition of topic.partitions) {
      writer.writeInt32(partition)
    }

    if (isFlexible) {
      writer.writeTaggedFields([])
    }
  }
}

/**
 * Encode an AddPartitionsToTxn request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–3).
 */
export function encodeAddPartitionsToTxnRequest(
  writer: BinaryWriter,
  request: AddPartitionsToTxnRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 3

  // transactional_id
  if (isFlexible) {
    writer.writeCompactString(request.transactionalId)
  } else {
    writer.writeString(request.transactionalId)
  }

  // producer_id (INT64)
  writer.writeInt64(request.producerId)

  // producer_epoch (INT16)
  writer.writeInt16(request.producerEpoch)

  // topics
  encodeAddPartitionsToTxnTopics(writer, request.topics, isFlexible)

  // Tagged fields (v3+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed AddPartitionsToTxn request ready to send to a broker.
 */
export function buildAddPartitionsToTxnRequest(
  correlationId: number,
  apiVersion: number,
  request: AddPartitionsToTxnRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.AddPartitionsToTxn,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeAddPartitionsToTxnRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

function decodePartitionResult(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<AddPartitionsToTxnPartitionResult> {
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
      partitionErrorCode: errorCodeResult.value
    },
    reader.offset - startOffset
  )
}

function decodeTopicResult(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<AddPartitionsToTxnTopicResult> {
  const startOffset = reader.offset

  // topic name
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

  const partitions: AddPartitionsToTxnPartitionResult[] = []
  for (let i = 0; i < partitionCount; i++) {
    const partResult = decodePartitionResult(reader, isFlexible)
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

/**
 * Decode an AddPartitionsToTxn response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–3).
 * @returns The decoded response or a failure.
 */
export function decodeAddPartitionsToTxnResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<AddPartitionsToTxnResponse> {
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

  const topics: AddPartitionsToTxnTopicResult[] = []
  for (let i = 0; i < topicCount; i++) {
    const topicResult = decodeTopicResult(reader, isFlexible)
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
