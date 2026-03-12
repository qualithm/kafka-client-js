/**
 * OffsetForLeaderEpoch request/response encoding and decoding.
 *
 * The OffsetForLeaderEpoch API (key 23) finds the earliest offset for each
 * partition whose leader epoch is at least the requested epoch. This is used
 * for log truncation detection and follower replication.
 *
 * **Request versions:**
 * - v0–v2: non-flexible, per-topic/partition leader epoch
 * - v3: adds currentLeaderEpoch per partition
 * - v4: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: per-topic/partition error code + end offset
 * - v1+: adds leader epoch in response
 * - v2+: adds topics-level structure
 * - v4: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_OffsetForLeaderEpoch
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
 * A partition within an OffsetForLeaderEpoch request topic.
 */
export type OffsetForLeaderEpochPartitionRequest = {
  /** Partition index. */
  readonly partitionIndex: number
  /**
   * The current leader epoch of the requester (v3+).
   * Used by the broker to determine if the requester's view is stale.
   * Set to -1 if not known.
   */
  readonly currentLeaderEpoch?: number
  /** The epoch to look up. */
  readonly leaderEpoch: number
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A topic within an OffsetForLeaderEpoch request.
 */
export type OffsetForLeaderEpochTopicRequest = {
  /** Topic name. */
  readonly name: string
  /** Per-partition requests. */
  readonly partitions: readonly OffsetForLeaderEpochPartitionRequest[]
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * OffsetForLeaderEpoch request payload.
 */
export type OffsetForLeaderEpochRequest = {
  /**
   * Broker ID of the follower (v3+).
   * Set to -1 for consumer clients, or the follower's broker ID for replication.
   */
  readonly replicaId?: number
  /** Topics with partitions to query. */
  readonly topics: readonly OffsetForLeaderEpochTopicRequest[]
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-partition result from an OffsetForLeaderEpoch response.
 */
export type OffsetForLeaderEpochPartitionResponse = {
  /** Error code (0 = success). */
  readonly errorCode: number
  /** Partition index. */
  readonly partitionIndex: number
  /** The leader epoch of the returned offset (v1+). -1 if not available. */
  readonly leaderEpoch: number
  /** The end offset. -1 if the requested epoch is not known. */
  readonly endOffset: bigint
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-topic result from an OffsetForLeaderEpoch response.
 */
export type OffsetForLeaderEpochTopicResponse = {
  /** Topic name. */
  readonly name: string
  /** Per-partition results. */
  readonly partitions: readonly OffsetForLeaderEpochPartitionResponse[]
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * OffsetForLeaderEpoch response payload.
 */
export type OffsetForLeaderEpochResponse = {
  /** Time the request was throttled in milliseconds (v2+). */
  readonly throttleTimeMs: number
  /** Per-topic results. */
  readonly topics: readonly OffsetForLeaderEpochTopicResponse[]
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an OffsetForLeaderEpoch request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–4).
 */
export function encodeOffsetForLeaderEpochRequest(
  writer: BinaryWriter,
  request: OffsetForLeaderEpochRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 4

  // replica_id (INT32, v3+)
  if (apiVersion >= 3) {
    writer.writeInt32(request.replicaId ?? -1)
  }

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

      // current_leader_epoch (INT32, v3+)
      if (apiVersion >= 3) {
        writer.writeInt32(partition.currentLeaderEpoch ?? -1)
      }

      // leader_epoch (INT32)
      writer.writeInt32(partition.leaderEpoch)

      // tagged fields (v4+)
      if (isFlexible) {
        writer.writeTaggedFields(partition.taggedFields ?? [])
      }
    }

    // tagged fields (v4+)
    if (isFlexible) {
      writer.writeTaggedFields(topic.taggedFields ?? [])
    }
  }

  // tagged fields (v4+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed OffsetForLeaderEpoch request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–4).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildOffsetForLeaderEpochRequest(
  correlationId: number,
  apiVersion: number,
  request: OffsetForLeaderEpochRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.OffsetForLeaderEpoch,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeOffsetForLeaderEpochRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an OffsetForLeaderEpoch response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–4).
 * @returns The decoded response or a failure.
 */
export function decodeOffsetForLeaderEpochResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<OffsetForLeaderEpochResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 4

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
  const topicsResult = decodeOffsetForLeaderEpochTopics(reader, apiVersion, isFlexible)
  if (!topicsResult.ok) {
    return topicsResult
  }

  // tagged fields (v4+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess(
    { throttleTimeMs, topics: topicsResult.value, taggedFields },
    reader.offset - startOffset
  )
}

function decodeOffsetForLeaderEpochTopics(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<OffsetForLeaderEpochTopicResponse[]> {
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

  const topics: OffsetForLeaderEpochTopicResponse[] = []
  for (let i = 0; i < topicArrayLength; i++) {
    const topicResult = decodeOffsetForLeaderEpochTopicResponse(reader, apiVersion, isFlexible)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

function decodeOffsetForLeaderEpochTopicResponse(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<OffsetForLeaderEpochTopicResponse> {
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

  const partitions: OffsetForLeaderEpochPartitionResponse[] = []
  for (let j = 0; j < partitionArrayLength; j++) {
    const partResult = decodeOffsetForLeaderEpochPartitionResponse(reader, apiVersion, isFlexible)
    if (!partResult.ok) {
      return partResult
    }
    partitions.push(partResult.value)
  }

  // tagged fields (v4+)
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

function decodeOffsetForLeaderEpochPartitionResponse(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<OffsetForLeaderEpochPartitionResponse> {
  const startOffset = reader.offset

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // partition_index (INT32)
  const partIndexResult = reader.readInt32()
  if (!partIndexResult.ok) {
    return partIndexResult
  }

  // leader_epoch (INT32, v1+)
  let leaderEpoch = -1
  if (apiVersion >= 1) {
    const epochResult = reader.readInt32()
    if (!epochResult.ok) {
      return epochResult
    }
    leaderEpoch = epochResult.value
  }

  // end_offset (INT64)
  const endOffsetResult = reader.readInt64()
  if (!endOffsetResult.ok) {
    return endOffsetResult
  }

  // tagged fields (v4+)
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
      errorCode: errorCodeResult.value,
      partitionIndex: partIndexResult.value,
      leaderEpoch,
      endOffset: endOffsetResult.value,
      taggedFields: partTaggedFields
    },
    reader.offset - startOffset
  )
}
