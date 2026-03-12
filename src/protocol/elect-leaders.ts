/**
 * ElectLeaders request/response encoding and decoding.
 *
 * The ElectLeaders API (key 43) triggers leader election for specified
 * topic partitions. Supports preferred and unclean leader election types.
 *
 * **Request versions:**
 * - v0: election_type (INT8), topic_partitions (nullable array), timeout_ms
 * - v1: same structure as v0
 * - v2: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: throttle_time_ms, error_code (top-level), replica_election_results
 * - v1+: same structure
 * - v2: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_ElectLeaders
 *
 * @packageDocumentation
 */

import { ApiKey } from "../codec/api-keys.js"
import type { BinaryReader, TaggedField } from "../codec/binary-reader.js"
import type { BinaryWriter } from "../codec/binary-writer.js"
import { frameRequest, type RequestHeader } from "../codec/protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "../result.js"

// ---------------------------------------------------------------------------
// Election type
// ---------------------------------------------------------------------------

/**
 * Type of leader election to perform.
 *
 * - `0` (PREFERRED): Elect the preferred replica as leader.
 * - `1` (UNCLEAN): Elect an unclean replica as leader (may cause data loss).
 */
export type ElectionType = 0 | 1

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * A partition within an ElectLeaders request topic.
 */
export type ElectLeadersPartitionRequest = {
  /** Partition index. */
  readonly partitionId: number
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A topic within an ElectLeaders request.
 */
export type ElectLeadersTopicRequest = {
  /** Topic name. */
  readonly topic: string
  /** Partitions to trigger election for. */
  readonly partitions: readonly ElectLeadersPartitionRequest[]
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * ElectLeaders request payload.
 */
export type ElectLeadersRequest = {
  /** Type of election to perform. */
  readonly electionType: ElectionType
  /** Topics and partitions to elect leaders for. `null` means all partitions. */
  readonly topicPartitions: readonly ElectLeadersTopicRequest[] | null
  /** Timeout in milliseconds. */
  readonly timeoutMs: number
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-partition result from an ElectLeaders response.
 */
export type ElectLeadersPartitionResponse = {
  /** Partition index. */
  readonly partitionId: number
  /** Error code (0 = success). */
  readonly errorCode: number
  /** Error message, if any. */
  readonly errorMessage: string | null
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-topic result from an ElectLeaders response.
 */
export type ElectLeadersTopicResponse = {
  /** Topic name. */
  readonly topic: string
  /** Per-partition results. */
  readonly partitions: readonly ElectLeadersPartitionResponse[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * ElectLeaders response payload.
 */
export type ElectLeadersResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Top-level error code (0 = success). */
  readonly errorCode: number
  /** Per-topic election results. */
  readonly replicaElectionResults: readonly ElectLeadersTopicResponse[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an ElectLeaders request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–2).
 */
export function encodeElectLeadersRequest(
  writer: BinaryWriter,
  request: ElectLeadersRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // election_type (INT8)
  writer.writeInt8(request.electionType)

  // topic_partitions — nullable array
  if (request.topicPartitions === null) {
    if (isFlexible) {
      // compact nullable array: 0 means null
      writer.writeUnsignedVarInt(0)
    } else {
      writer.writeInt32(-1)
    }
  } else {
    if (isFlexible) {
      writer.writeUnsignedVarInt(request.topicPartitions.length + 1)
    } else {
      writer.writeInt32(request.topicPartitions.length)
    }

    for (const topic of request.topicPartitions) {
      // topic name
      if (isFlexible) {
        writer.writeCompactString(topic.topic)
      } else {
        writer.writeString(topic.topic)
      }

      // partitions array
      if (isFlexible) {
        writer.writeUnsignedVarInt(topic.partitions.length + 1)
      } else {
        writer.writeInt32(topic.partitions.length)
      }

      for (const partition of topic.partitions) {
        // partition_id (INT32)
        writer.writeInt32(partition.partitionId)

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
  }

  // timeout_ms (INT32)
  writer.writeInt32(request.timeoutMs)

  // tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed ElectLeaders request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–2).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildElectLeadersRequest(
  correlationId: number,
  apiVersion: number,
  request: ElectLeadersRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.ElectLeaders,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeElectLeadersRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an ElectLeaders response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–2).
 * @returns The decoded response or a failure.
 */
export function decodeElectLeadersResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<ElectLeadersResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // error_code (INT16) — top-level, present in v1+
  let errorCode = 0
  if (apiVersion >= 1) {
    const errorResult = reader.readInt16()
    if (!errorResult.ok) {
      return errorResult
    }
    errorCode = errorResult.value
  }

  // replica_election_results array
  const resultsResult = decodeElectLeadersTopics(reader, isFlexible)
  if (!resultsResult.ok) {
    return resultsResult
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
    {
      throttleTimeMs: throttleResult.value,
      errorCode,
      replicaElectionResults: resultsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeElectLeadersTopics(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<ElectLeadersTopicResponse[]> {
  const startOffset = reader.offset
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

  const topics: ElectLeadersTopicResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    const topicResult = decodeElectLeadersTopicResponse(reader, isFlexible)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

function decodeElectLeadersTopicResponse(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<ElectLeadersTopicResponse> {
  const startOffset = reader.offset

  // topic name
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

  const partitions: ElectLeadersPartitionResponse[] = []
  for (let j = 0; j < partitionArrayLength; j++) {
    const partResult = decodeElectLeadersPartitionResponse(reader, isFlexible)
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
    { topic: nameResult.value ?? "", partitions, taggedFields: topicTaggedFields },
    reader.offset - startOffset
  )
}

function decodeElectLeadersPartitionResponse(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<ElectLeadersPartitionResponse> {
  const startOffset = reader.offset

  // partition_id (INT32)
  const partIdResult = reader.readInt32()
  if (!partIdResult.ok) {
    return partIdResult
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // error_message (NULLABLE_STRING / COMPACT_NULLABLE_STRING)
  const errorMsgResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!errorMsgResult.ok) {
    return errorMsgResult
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
      partitionId: partIdResult.value,
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      taggedFields: partTaggedFields
    },
    reader.offset - startOffset
  )
}
