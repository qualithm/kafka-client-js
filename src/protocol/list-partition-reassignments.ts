/**
 * ListPartitionReassignments request/response encoding and decoding.
 *
 * The ListPartitionReassignments API (key 46) lists ongoing partition
 * replica reassignments. Introduced in KIP-455.
 *
 * **Request versions:**
 * - v0: flexible encoding only (KIP-482)
 *
 * **Response versions:**
 * - v0: throttle_time_ms, error_code, error_message, per-topic/partition reassignment state
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_ListPartitionReassignments
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
 * A topic filter within a ListPartitionReassignments request.
 */
export type ListReassignmentsTopic = {
  /** Topic name. */
  readonly name: string
  /** Partition indices to list. */
  readonly partitionIndexes: readonly number[]
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * ListPartitionReassignments request payload.
 */
export type ListPartitionReassignmentsRequest = {
  /** Timeout in milliseconds. */
  readonly timeoutMs: number
  /**
   * Topics and partitions to list reassignments for.
   * `null` to list all ongoing reassignments.
   */
  readonly topics: readonly ListReassignmentsTopic[] | null
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-partition reassignment state from a ListPartitionReassignments response.
 */
export type OngoingPartitionReassignment = {
  /** Partition index. */
  readonly partitionIndex: number
  /** Current replica set. */
  readonly replicas: readonly number[]
  /** Replicas being added. */
  readonly addingReplicas: readonly number[]
  /** Replicas being removed. */
  readonly removingReplicas: readonly number[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-topic reassignment state from a ListPartitionReassignments response.
 */
export type OngoingTopicReassignment = {
  /** Topic name. */
  readonly name: string
  /** Per-partition reassignment state. */
  readonly partitions: readonly OngoingPartitionReassignment[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * ListPartitionReassignments response payload.
 */
export type ListPartitionReassignmentsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Top-level error code (0 = success). */
  readonly errorCode: number
  /** Top-level error message, if any. */
  readonly errorMessage: string | null
  /** Per-topic ongoing reassignments. */
  readonly topics: readonly OngoingTopicReassignment[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a ListPartitionReassignments request body into the given writer.
 *
 * All versions use flexible encoding (v0 is the only version, introduced post KIP-482).
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param _apiVersion - The API version to encode for (0).
 */
export function encodeListPartitionReassignmentsRequest(
  writer: BinaryWriter,
  request: ListPartitionReassignmentsRequest,
  _apiVersion: number
): void {
  // timeout_ms (INT32)
  writer.writeInt32(request.timeoutMs)

  // topics — compact nullable array
  if (request.topics === null) {
    writer.writeUnsignedVarInt(0) // compact null
  } else {
    writer.writeUnsignedVarInt(request.topics.length + 1)

    for (const topic of request.topics) {
      // name — compact string
      writer.writeCompactString(topic.name)

      // partition_indexes — compact array of INT32
      writer.writeUnsignedVarInt(topic.partitionIndexes.length + 1)
      for (const idx of topic.partitionIndexes) {
        writer.writeInt32(idx)
      }

      // tagged fields
      writer.writeTaggedFields(topic.taggedFields ?? [])
    }
  }

  // tagged fields
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed ListPartitionReassignments request ready to send.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildListPartitionReassignmentsRequest(
  correlationId: number,
  apiVersion: number,
  request: ListPartitionReassignmentsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.ListPartitionReassignments,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeListPartitionReassignmentsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a ListPartitionReassignments response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param _apiVersion - The API version of the response (0).
 * @returns The decoded response or a failure.
 */
export function decodeListPartitionReassignmentsResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<ListPartitionReassignmentsResponse> {
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

  // topics — compact array
  const topicsResult = decodeOngoingTopicReassignments(reader)
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
      topics: topicsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeOngoingTopicReassignments(
  reader: BinaryReader
): DecodeResult<OngoingTopicReassignment[]> {
  const startOffset = reader.offset

  const lenResult = reader.readUnsignedVarInt()
  if (!lenResult.ok) {
    return lenResult
  }
  const arrayLength = lenResult.value - 1

  const topics: OngoingTopicReassignment[] = []
  for (let i = 0; i < arrayLength; i++) {
    const topicResult = decodeOngoingTopicReassignment(reader)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

function decodeOngoingTopicReassignment(
  reader: BinaryReader
): DecodeResult<OngoingTopicReassignment> {
  const startOffset = reader.offset

  // name — compact string
  const nameResult = reader.readCompactString()
  if (!nameResult.ok) {
    return nameResult
  }

  // partitions — compact array
  const partitionsResult = decodeOngoingPartitionReassignments(reader)
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

function decodeOngoingPartitionReassignments(
  reader: BinaryReader
): DecodeResult<OngoingPartitionReassignment[]> {
  const startOffset = reader.offset

  const lenResult = reader.readUnsignedVarInt()
  if (!lenResult.ok) {
    return lenResult
  }
  const arrayLength = lenResult.value - 1

  const partitions: OngoingPartitionReassignment[] = []
  for (let i = 0; i < arrayLength; i++) {
    const partResult = decodeOngoingPartitionReassignment(reader)
    if (!partResult.ok) {
      return partResult
    }
    partitions.push(partResult.value)
  }

  return decodeSuccess(partitions, reader.offset - startOffset)
}

function decodeOngoingPartitionReassignment(
  reader: BinaryReader
): DecodeResult<OngoingPartitionReassignment> {
  const startOffset = reader.offset

  // partition_index (INT32)
  const partIdResult = reader.readInt32()
  if (!partIdResult.ok) {
    return partIdResult
  }

  // replicas — compact array of INT32
  const replicasResult = decodeCompactInt32Array(reader)
  if (!replicasResult.ok) {
    return replicasResult
  }

  // adding_replicas — compact array of INT32
  const addingResult = decodeCompactInt32Array(reader)
  if (!addingResult.ok) {
    return addingResult
  }

  // removing_replicas — compact array of INT32
  const removingResult = decodeCompactInt32Array(reader)
  if (!removingResult.ok) {
    return removingResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      partitionIndex: partIdResult.value,
      replicas: replicasResult.value,
      addingReplicas: addingResult.value,
      removingReplicas: removingResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeCompactInt32Array(reader: BinaryReader): DecodeResult<number[]> {
  const startOffset = reader.offset

  const lenResult = reader.readUnsignedVarInt()
  if (!lenResult.ok) {
    return lenResult
  }
  const arrayLength = lenResult.value - 1

  const values: number[] = []
  for (let i = 0; i < arrayLength; i++) {
    const valResult = reader.readInt32()
    if (!valResult.ok) {
      return valResult
    }
    values.push(valResult.value)
  }

  return decodeSuccess(values, reader.offset - startOffset)
}
