/**
 * DeleteRecords request/response encoding and decoding.
 *
 * The DeleteRecords API (key 21) deletes records from topic partitions
 * by setting the log start offset to a specified value, effectively
 * truncating older records.
 *
 * **Request versions:**
 * - v0–v1: non-flexible, topics array with partitions + offset
 * - v2: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: throttle_time_ms, topics array with per-partition results
 * - v1+: same structure
 * - v2: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DeleteRecords
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
 * A partition within a DeleteRecords request topic.
 */
export type DeleteRecordsPartitionRequest = {
  /** Partition index. */
  readonly partitionIndex: number
  /** The deletion offset — records before this offset will be deleted. Use -1 for high watermark. */
  readonly offset: bigint
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A topic within a DeleteRecords request.
 */
export type DeleteRecordsTopicRequest = {
  /** Topic name. */
  readonly name: string
  /** Per-partition deletion requests. */
  readonly partitions: readonly DeleteRecordsPartitionRequest[]
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * DeleteRecords request payload.
 */
export type DeleteRecordsRequest = {
  /** Topics with partitions to delete records from. */
  readonly topics: readonly DeleteRecordsTopicRequest[]
  /** Timeout in milliseconds. */
  readonly timeoutMs: number
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-partition result from a DeleteRecords response.
 */
export type DeleteRecordsPartitionResponse = {
  /** Partition index. */
  readonly partitionIndex: number
  /** The new low watermark for this partition. */
  readonly lowWatermark: bigint
  /** Error code (0 = success). */
  readonly errorCode: number
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-topic result from a DeleteRecords response.
 */
export type DeleteRecordsTopicResponse = {
  /** Topic name. */
  readonly name: string
  /** Per-partition results. */
  readonly partitions: readonly DeleteRecordsPartitionResponse[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DeleteRecords response payload.
 */
export type DeleteRecordsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-topic results. */
  readonly topics: readonly DeleteRecordsTopicResponse[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DeleteRecords request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–2).
 */
export function encodeDeleteRecordsRequest(
  writer: BinaryWriter,
  request: DeleteRecordsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // topics array
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

    // partitions array
    if (isFlexible) {
      writer.writeUnsignedVarInt(topic.partitions.length + 1)
    } else {
      writer.writeInt32(topic.partitions.length)
    }

    for (const partition of topic.partitions) {
      // partition_index (INT32)
      writer.writeInt32(partition.partitionIndex)
      // offset (INT64)
      writer.writeInt64(partition.offset)

      // tagged fields (v2+)
      if (isFlexible) {
        writer.writeTaggedFields(partition.taggedFields ?? [])
      }
    }

    // tagged fields (v2+)
    if (isFlexible) {
      writer.writeTaggedFields(topic.taggedFields ?? [])
    }
  }

  // timeout_ms (INT32)
  writer.writeInt32(request.timeoutMs)

  // tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed DeleteRecords request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–2).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildDeleteRecordsRequest(
  correlationId: number,
  apiVersion: number,
  request: DeleteRecordsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DeleteRecords,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDeleteRecordsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DeleteRecords response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–2).
 * @returns The decoded response or a failure.
 */
export function decodeDeleteRecordsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DeleteRecordsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // topics array
  const topicsResult = decodeDeleteRecordsTopics(reader, isFlexible)
  if (!topicsResult.ok) {
    return topicsResult
  }

  // tagged fields (v2+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess(
    { throttleTimeMs: throttleResult.value, topics: topicsResult.value, taggedFields },
    reader.offset - startOffset
  )
}

function decodeDeleteRecordsTopics(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<DeleteRecordsTopicResponse[]> {
  const startOffset = reader.offset
  let topicArrayLength: number
  if (isFlexible) {
    const lengthResult = reader.readUnsignedVarInt()
    if (!lengthResult.ok) {
      return lengthResult
    }
    topicArrayLength = lengthResult.value - 1
  } else {
    const lengthResult = reader.readInt32()
    if (!lengthResult.ok) {
      return lengthResult
    }
    topicArrayLength = lengthResult.value
  }

  const topics: DeleteRecordsTopicResponse[] = []
  for (let i = 0; i < topicArrayLength; i++) {
    const topicResult = decodeDeleteRecordsTopicResponse(reader, isFlexible)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

function decodeDeleteRecordsTopicResponse(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<DeleteRecordsTopicResponse> {
  const startOffset = reader.offset

  // name
  const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!nameResult.ok) {
    return nameResult
  }

  // partitions array
  let partitionArrayLength: number
  if (isFlexible) {
    const pLenResult = reader.readUnsignedVarInt()
    if (!pLenResult.ok) {
      return pLenResult
    }
    partitionArrayLength = pLenResult.value - 1
  } else {
    const pLenResult = reader.readInt32()
    if (!pLenResult.ok) {
      return pLenResult
    }
    partitionArrayLength = pLenResult.value
  }

  const partitions: DeleteRecordsPartitionResponse[] = []
  for (let j = 0; j < partitionArrayLength; j++) {
    const partResult = decodeDeleteRecordsPartitionResponse(reader, isFlexible)
    if (!partResult.ok) {
      return partResult
    }
    partitions.push(partResult.value)
  }

  // tagged fields (v2+)
  let topicTaggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    topicTaggedFields = tagResult.value
  }

  return decodeSuccess(
    { name: nameResult.value ?? "", partitions, taggedFields: topicTaggedFields },
    reader.offset - startOffset
  )
}

function decodeDeleteRecordsPartitionResponse(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<DeleteRecordsPartitionResponse> {
  const startOffset = reader.offset

  // partition_index (INT32)
  const partIndexResult = reader.readInt32()
  if (!partIndexResult.ok) {
    return partIndexResult
  }

  // low_watermark (INT64)
  const lwResult = reader.readInt64()
  if (!lwResult.ok) {
    return lwResult
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // tagged fields (v2+)
  let partTaggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    partTaggedFields = tagResult.value
  }

  return decodeSuccess(
    {
      partitionIndex: partIndexResult.value,
      lowWatermark: lwResult.value,
      errorCode: errorCodeResult.value,
      taggedFields: partTaggedFields
    },
    reader.offset - startOffset
  )
}
