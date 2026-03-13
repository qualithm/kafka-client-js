/**
 * AlterPartitionReassignments request/response encoding and decoding.
 *
 * The AlterPartitionReassignments API (key 45) initiates, cancels, or
 * modifies partition replica reassignments. Introduced in KIP-455.
 *
 * **Request versions:**
 * - v0: flexible encoding only (KIP-482)
 *
 * **Response versions:**
 * - v0: throttle_time_ms, error_code, error_message, per-topic/partition results
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_AlterPartitionReassignments
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
 * A partition reassignment within an AlterPartitionReassignments request.
 */
export type ReassignablePartition = {
  /** Partition index. */
  readonly partitionIndex: number
  /**
   * The target replica list. `null` to cancel a pending reassignment.
   */
  readonly replicas: readonly number[] | null
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A topic within an AlterPartitionReassignments request.
 */
export type ReassignableTopic = {
  /** Topic name. */
  readonly name: string
  /** Partitions to reassign. */
  readonly partitions: readonly ReassignablePartition[]
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * AlterPartitionReassignments request payload.
 */
export type AlterPartitionReassignmentsRequest = {
  /** Timeout in milliseconds. */
  readonly timeoutMs: number
  /** Topics containing partitions to reassign. */
  readonly topics: readonly ReassignableTopic[]
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-partition result from an AlterPartitionReassignments response.
 */
export type ReassignablePartitionResponse = {
  /** Partition index. */
  readonly partitionIndex: number
  /** Error code (0 = success). */
  readonly errorCode: number
  /** Error message, if any. */
  readonly errorMessage: string | null
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-topic result from an AlterPartitionReassignments response.
 */
export type ReassignableTopicResponse = {
  /** Topic name. */
  readonly name: string
  /** Per-partition results. */
  readonly partitions: readonly ReassignablePartitionResponse[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * AlterPartitionReassignments response payload.
 */
export type AlterPartitionReassignmentsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Top-level error code (0 = success). */
  readonly errorCode: number
  /** Top-level error message, if any. */
  readonly errorMessage: string | null
  /** Per-topic reassignment results. */
  readonly responses: readonly ReassignableTopicResponse[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an AlterPartitionReassignments request body into the given writer.
 *
 * All versions use flexible encoding (v0 is the only version, introduced post KIP-482).
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param _apiVersion - The API version to encode for (0).
 */
export function encodeAlterPartitionReassignmentsRequest(
  writer: BinaryWriter,
  request: AlterPartitionReassignmentsRequest,
  _apiVersion: number
): void {
  // timeout_ms (INT32)
  writer.writeInt32(request.timeoutMs)

  // topics — compact array
  writer.writeUnsignedVarInt(request.topics.length + 1)

  for (const topic of request.topics) {
    // topic name — compact string
    writer.writeCompactString(topic.name)

    // partitions — compact array
    writer.writeUnsignedVarInt(topic.partitions.length + 1)

    for (const partition of topic.partitions) {
      // partition_index (INT32)
      writer.writeInt32(partition.partitionIndex)

      // replicas — compact nullable array of INT32
      if (partition.replicas === null) {
        writer.writeUnsignedVarInt(0) // compact null
      } else {
        writer.writeUnsignedVarInt(partition.replicas.length + 1)
        for (const replica of partition.replicas) {
          writer.writeInt32(replica)
        }
      }

      // tagged fields
      writer.writeTaggedFields(partition.taggedFields ?? [])
    }

    // tagged fields
    writer.writeTaggedFields(topic.taggedFields ?? [])
  }

  // tagged fields
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed AlterPartitionReassignments request ready to send.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildAlterPartitionReassignmentsRequest(
  correlationId: number,
  apiVersion: number,
  request: AlterPartitionReassignmentsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.AlterPartitionReassignments,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeAlterPartitionReassignmentsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an AlterPartitionReassignments response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param _apiVersion - The API version of the response (0).
 * @returns The decoded response or a failure.
 */
export function decodeAlterPartitionReassignmentsResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<AlterPartitionReassignmentsResponse> {
  const startOffset = reader.offset

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
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

  // responses — compact array of topics
  const topicsResult = decodeReassignableTopicResponses(reader)
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
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      responses: topicsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeReassignableTopicResponses(
  reader: BinaryReader
): DecodeResult<ReassignableTopicResponse[]> {
  const startOffset = reader.offset

  const lenResult = reader.readUnsignedVarInt()
  if (!lenResult.ok) {
    return lenResult
  }
  const arrayLength = lenResult.value - 1

  const topics: ReassignableTopicResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    const topicResult = decodeReassignableTopicResponse(reader)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

function decodeReassignableTopicResponse(
  reader: BinaryReader
): DecodeResult<ReassignableTopicResponse> {
  const startOffset = reader.offset

  // name — compact string
  const nameResult = reader.readCompactString()
  if (!nameResult.ok) {
    return nameResult
  }

  // partitions — compact array
  const partitionsResult = decodeReassignablePartitionResponses(reader)
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

function decodeReassignablePartitionResponses(
  reader: BinaryReader
): DecodeResult<ReassignablePartitionResponse[]> {
  const startOffset = reader.offset

  const lenResult = reader.readUnsignedVarInt()
  if (!lenResult.ok) {
    return lenResult
  }
  const arrayLength = lenResult.value - 1

  const partitions: ReassignablePartitionResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    const partResult = decodeReassignablePartitionResponse(reader)
    if (!partResult.ok) {
      return partResult
    }
    partitions.push(partResult.value)
  }

  return decodeSuccess(partitions, reader.offset - startOffset)
}

function decodeReassignablePartitionResponse(
  reader: BinaryReader
): DecodeResult<ReassignablePartitionResponse> {
  const startOffset = reader.offset

  // partition_index (INT32)
  const partIdResult = reader.readInt32()
  if (!partIdResult.ok) {
    return partIdResult
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

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      partitionIndex: partIdResult.value,
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
