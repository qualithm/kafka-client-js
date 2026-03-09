/**
 * Produce request/response encoding and decoding.
 *
 * The Produce API (key 0) publishes record batches to topic partitions.
 * It is the primary API used by producers to send messages to Kafka.
 *
 * **Request versions:**
 * - v0: acks, timeout, topic_data with partition record sets
 * - v3+: transactional_id field (null for non-transactional producers)
 * - v9+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: per-partition base_offset and error_code
 * - v1+: adds throttle_time_ms
 * - v2+: adds timestamp (log append time)
 * - v5+: adds log_start_offset
 * - v8+: adds record-level errors and error_message per partition
 * - v9+: flexible encoding
 *
 * The record data for each partition is a raw byte payload containing one or
 * more {@link RecordBatch} encoded via {@link encodeRecordBatch}. The client
 * must pre-encode record batches before building the produce request.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_Produce
 *
 * @packageDocumentation
 */

import { ApiKey } from "./api-keys.js"
import type { BinaryReader, TaggedField } from "./binary-reader.js"
import type { BinaryWriter } from "./binary-writer.js"
import { frameRequest, type RequestHeader } from "./protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "./result.js"

// ---------------------------------------------------------------------------
// Acks
// ---------------------------------------------------------------------------

/**
 * Acknowledgement modes for produce requests.
 *
 * - `All` (-1): Wait for all in-sync replicas to acknowledge.
 * - `None` (0): Fire and forget — no acknowledgement.
 * - `Leader` (1): Wait for the leader to acknowledge.
 */
export const Acks = {
  /** Wait for all in-sync replicas. Strongest durability guarantee. */
  All: -1,
  /** No acknowledgement. Lowest latency but no delivery guarantee. */
  None: 0,
  /** Wait for the partition leader only. */
  Leader: 1
} as const

export type Acks = (typeof Acks)[keyof typeof Acks]

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * Partition data in a Produce request.
 */
export type ProducePartitionData = {
  /** The partition index. */
  readonly partitionIndex: number
  /**
   * Pre-encoded record batch data for this partition.
   * Must contain one or more {@link RecordBatch} encoded with {@link encodeRecordBatch}.
   */
  readonly records: Uint8Array | null
  /** Tagged fields (v9+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Topic data in a Produce request.
 */
export type ProduceTopicData = {
  /** The topic name. */
  readonly name: string
  /** Partition data for this topic. */
  readonly partitions: readonly ProducePartitionData[]
  /** Tagged fields (v9+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Produce request payload.
 */
export type ProduceRequest = {
  /**
   * The transactional ID (v3+). Null for non-transactional producers.
   */
  readonly transactionalId?: string | null
  /**
   * The number of acknowledgements required.
   * - -1 (All): All in-sync replicas
   * - 0 (None): No acknowledgement
   * - 1 (Leader): Leader only
   */
  readonly acks: Acks
  /**
   * Timeout in milliseconds to wait for the required acknowledgements.
   */
  readonly timeoutMs: number
  /** Topic data to produce. */
  readonly topics: readonly ProduceTopicData[]
  /** Tagged fields (v9+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-record error in a Produce response (v8+).
 */
export type ProduceRecordError = {
  /** The index of the record that caused the error (within the batch). */
  readonly batchIndex: number
  /** Human-readable error message. */
  readonly message: string | null
}

/**
 * Partition response in a Produce response.
 */
export type ProducePartitionResponse = {
  /** The partition index. */
  readonly partitionIndex: number
  /** The error code (0 = no error). */
  readonly errorCode: number
  /** The base offset assigned to the first message in the batch. */
  readonly baseOffset: bigint
  /**
   * The log append timestamp (v2+).
   * -1 if the broker does not use log append time.
   */
  readonly logAppendTimeMs: bigint
  /**
   * The log start offset of the partition (v5+).
   * Useful for log compaction tracking.
   */
  readonly logStartOffset: bigint
  /**
   * Per-record errors (v8+).
   * Only populated when specific records fail within a batch.
   */
  readonly recordErrors: readonly ProduceRecordError[]
  /**
   * Human-readable error message for the partition (v8+).
   */
  readonly errorMessage: string | null
  /** Tagged fields (v9+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Topic response in a Produce response.
 */
export type ProduceTopicResponse = {
  /** The topic name. */
  readonly name: string
  /** Partition responses. */
  readonly partitions: readonly ProducePartitionResponse[]
  /** Tagged fields (v9+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Produce response payload.
 */
export type ProduceResponse = {
  /** Topic responses. */
  readonly topics: readonly ProduceTopicResponse[]
  /** Time the request was throttled in milliseconds (v1+). */
  readonly throttleTimeMs: number
  /** Tagged fields (v9+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a Produce request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–9).
 */
export function encodeProduceRequest(
  writer: BinaryWriter,
  request: ProduceRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 9

  // transactional_id (v3+) — nullable string
  if (apiVersion >= 3) {
    if (isFlexible) {
      writer.writeCompactString(request.transactionalId ?? null)
    } else {
      writer.writeString(request.transactionalId ?? null)
    }
  }

  // acks (INT16)
  writer.writeInt16(request.acks)

  // timeout_ms (INT32)
  writer.writeInt32(request.timeoutMs)

  // topic_data array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.topics.length + 1)
  } else {
    writer.writeInt32(request.topics.length)
  }

  for (const topic of request.topics) {
    encodeTopicData(writer, topic, isFlexible)
  }

  // Tagged fields (v9+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Encode a single topic's data in the request.
 */
function encodeTopicData(writer: BinaryWriter, topic: ProduceTopicData, isFlexible: boolean): void {
  // topic name
  if (isFlexible) {
    writer.writeCompactString(topic.name)
  } else {
    writer.writeString(topic.name)
  }

  // partition_data array
  if (isFlexible) {
    writer.writeUnsignedVarInt(topic.partitions.length + 1)
  } else {
    writer.writeInt32(topic.partitions.length)
  }

  for (const partition of topic.partitions) {
    encodePartitionData(writer, partition, isFlexible)
  }

  // Tagged fields per topic (v9+)
  if (isFlexible) {
    writer.writeTaggedFields(topic.taggedFields ?? [])
  }
}

/**
 * Encode a single partition's data in the request.
 */
function encodePartitionData(
  writer: BinaryWriter,
  partition: ProducePartitionData,
  isFlexible: boolean
): void {
  // partition_index (INT32)
  writer.writeInt32(partition.partitionIndex)

  // records — length-prefixed raw bytes (nullable)
  if (partition.records === null) {
    if (isFlexible) {
      // Compact nullable bytes: 0 means null
      writer.writeUnsignedVarInt(0)
    } else {
      writer.writeInt32(-1)
    }
  } else {
    if (isFlexible) {
      writer.writeUnsignedVarInt(partition.records.byteLength + 1)
    } else {
      writer.writeInt32(partition.records.byteLength)
    }
    writer.writeRawBytes(partition.records)
  }

  // Tagged fields per partition (v9+)
  if (isFlexible) {
    writer.writeTaggedFields(partition.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed Produce request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–9).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildProduceRequest(
  correlationId: number,
  apiVersion: number,
  request: ProduceRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.Produce,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeProduceRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a Produce response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–9).
 * @returns The decoded response or a failure.
 */
export function decodeProduceResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<ProduceResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 9

  // topics array
  const topicsResult = decodeTopicResponses(reader, apiVersion, isFlexible)
  if (!topicsResult.ok) {
    return topicsResult
  }

  // throttle_time_ms (v1+)
  let throttleTimeMs = 0
  if (apiVersion >= 1) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  // Tagged fields (v9+)
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
      topics: topicsResult.value,
      throttleTimeMs,
      taggedFields
    },
    reader.offset - startOffset
  )
}

// ---------------------------------------------------------------------------
// Response decoding helpers
// ---------------------------------------------------------------------------

/**
 * Decode the topics array from a Produce response.
 */
function decodeTopicResponses(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<ProduceTopicResponse[]> {
  const startOffset = reader.offset

  // Array length
  let topicCount: number
  if (isFlexible) {
    const lenResult = reader.readUnsignedVarInt()
    if (!lenResult.ok) {
      return lenResult
    }
    topicCount = lenResult.value - 1
  } else {
    const lenResult = reader.readInt32()
    if (!lenResult.ok) {
      return lenResult
    }
    topicCount = lenResult.value
  }

  const topics: ProduceTopicResponse[] = []
  for (let i = 0; i < topicCount; i++) {
    const topicResult = decodeTopicResponse(reader, apiVersion, isFlexible)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
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
): DecodeResult<ProduceTopicResponse> {
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
  const partitionsResult = decodePartitionResponses(reader, apiVersion, isFlexible)
  if (!partitionsResult.ok) {
    return partitionsResult
  }

  // Tagged fields per topic (v9+)
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
function decodePartitionResponses(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<ProducePartitionResponse[]> {
  const startOffset = reader.offset

  let partitionCount: number
  if (isFlexible) {
    const lenResult = reader.readUnsignedVarInt()
    if (!lenResult.ok) {
      return lenResult
    }
    partitionCount = lenResult.value - 1
  } else {
    const lenResult = reader.readInt32()
    if (!lenResult.ok) {
      return lenResult
    }
    partitionCount = lenResult.value
  }

  const partitions: ProducePartitionResponse[] = []
  for (let i = 0; i < partitionCount; i++) {
    const partitionResult = decodePartitionResponse(reader, apiVersion, isFlexible)
    if (!partitionResult.ok) {
      return partitionResult
    }
    partitions.push(partitionResult.value)
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
): DecodeResult<ProducePartitionResponse> {
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

  // base_offset (INT64)
  const offsetResult = reader.readInt64()
  if (!offsetResult.ok) {
    return offsetResult
  }

  // log_append_time_ms (INT64, v2+)
  let logAppendTimeMs = -1n
  if (apiVersion >= 2) {
    const timeResult = reader.readInt64()
    if (!timeResult.ok) {
      return timeResult
    }
    logAppendTimeMs = timeResult.value
  }

  // log_start_offset (INT64, v5+)
  let logStartOffset = -1n
  if (apiVersion >= 5) {
    const lsoResult = reader.readInt64()
    if (!lsoResult.ok) {
      return lsoResult
    }
    logStartOffset = lsoResult.value
  }

  // record_errors (v8+)
  let recordErrors: ProduceRecordError[] = []
  if (apiVersion >= 8) {
    const errorsResult = decodeRecordErrors(reader, isFlexible)
    if (!errorsResult.ok) {
      return errorsResult
    }
    recordErrors = errorsResult.value
  }

  // error_message (v8+) — nullable string
  let errorMessage: string | null = null
  if (apiVersion >= 8) {
    if (isFlexible) {
      const msgResult = reader.readCompactString()
      if (!msgResult.ok) {
        return msgResult
      }
      errorMessage = msgResult.value
    } else {
      const msgResult = reader.readString()
      if (!msgResult.ok) {
        return msgResult
      }
      errorMessage = msgResult.value
    }
  }

  // Tagged fields per partition (v9+)
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
      partitionIndex: indexResult.value,
      errorCode: errorCodeResult.value,
      baseOffset: offsetResult.value,
      logAppendTimeMs,
      logStartOffset,
      recordErrors,
      errorMessage,
      taggedFields
    },
    reader.offset - startOffset
  )
}

/**
 * Decode per-record errors (v8+).
 */
function decodeRecordErrors(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<ProduceRecordError[]> {
  const startOffset = reader.offset

  let count: number
  if (isFlexible) {
    const lenResult = reader.readUnsignedVarInt()
    if (!lenResult.ok) {
      return lenResult
    }
    count = lenResult.value - 1
  } else {
    const lenResult = reader.readInt32()
    if (!lenResult.ok) {
      return lenResult
    }
    count = lenResult.value
  }

  const errors: ProduceRecordError[] = []
  for (let i = 0; i < count; i++) {
    // batch_index (INT32)
    const indexResult = reader.readInt32()
    if (!indexResult.ok) {
      return indexResult
    }

    // batch_index_error_message (nullable string)
    let message: string | null
    if (isFlexible) {
      const msgResult = reader.readCompactString()
      if (!msgResult.ok) {
        return msgResult
      }
      message = msgResult.value
    } else {
      const msgResult = reader.readString()
      if (!msgResult.ok) {
        return msgResult
      }
      message = msgResult.value
    }

    // Tagged fields per record error (v9+)
    if (isFlexible) {
      const tagResult = reader.readTaggedFields()
      if (!tagResult.ok) {
        return tagResult
      }
    }

    errors.push({ batchIndex: indexResult.value, message })
  }

  return decodeSuccess(errors, reader.offset - startOffset)
}
