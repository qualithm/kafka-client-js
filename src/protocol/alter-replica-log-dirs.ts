/**
 * AlterReplicaLogDirs request/response encoding and decoding.
 *
 * The AlterReplicaLogDirs API (key 34) moves topic-partition replicas
 * between log directories on a broker.
 *
 * **Request versions:**
 * - v0–v1: non-flexible encoding
 * - v2+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0+: per-topic-partition error codes
 * - v2+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_AlterReplicaLogDirs
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
 * A topic whose partitions should be moved to the specified log directory.
 */
export type AlterReplicaLogDirsTopic = {
  /** The topic name. */
  readonly topic: string
  /** The partition indices to move. */
  readonly partitions: readonly number[]
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A log directory assignment: move the given topic-partitions to this path.
 */
export type AlterReplicaLogDirsEntry = {
  /** The absolute log directory path. */
  readonly logDir: string
  /** Topics and partitions to move to this directory. */
  readonly topics: readonly AlterReplicaLogDirsTopic[]
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * AlterReplicaLogDirs request payload.
 */
export type AlterReplicaLogDirsRequest = {
  /** Log directory assignments. */
  readonly dirs: readonly AlterReplicaLogDirsEntry[]
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-partition result from AlterReplicaLogDirs.
 */
export type AlterReplicaLogDirsPartitionResponse = {
  /** The partition index. */
  readonly partitionIndex: number
  /** Error code for this partition (0 = success). */
  readonly errorCode: number
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-topic result from AlterReplicaLogDirs.
 */
export type AlterReplicaLogDirsTopicResponse = {
  /** The topic name. */
  readonly topicName: string
  /** Per-partition results. */
  readonly partitions: readonly AlterReplicaLogDirsPartitionResponse[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * AlterReplicaLogDirs response payload.
 */
export type AlterReplicaLogDirsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-topic results. */
  readonly results: readonly AlterReplicaLogDirsTopicResponse[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an AlterReplicaLogDirs request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–2).
 */
export function encodeAlterReplicaLogDirsRequest(
  writer: BinaryWriter,
  request: AlterReplicaLogDirsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // dirs array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.dirs.length + 1)
  } else {
    writer.writeInt32(request.dirs.length)
  }

  for (const dir of request.dirs) {
    // log_dir
    if (isFlexible) {
      writer.writeCompactString(dir.logDir)
    } else {
      writer.writeString(dir.logDir)
    }

    // topics array
    if (isFlexible) {
      writer.writeUnsignedVarInt(dir.topics.length + 1)
    } else {
      writer.writeInt32(dir.topics.length)
    }

    for (const topic of dir.topics) {
      // topic name
      if (isFlexible) {
        writer.writeCompactString(topic.topic)
      } else {
        writer.writeString(topic.topic)
      }

      // partitions array (INT32[])
      if (isFlexible) {
        writer.writeUnsignedVarInt(topic.partitions.length + 1)
      } else {
        writer.writeInt32(topic.partitions.length)
      }
      for (const partition of topic.partitions) {
        writer.writeInt32(partition)
      }

      if (isFlexible) {
        writer.writeTaggedFields(topic.taggedFields ?? [])
      }
    }

    if (isFlexible) {
      writer.writeTaggedFields(dir.taggedFields ?? [])
    }
  }

  // Tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed AlterReplicaLogDirs request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–2).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildAlterReplicaLogDirsRequest(
  correlationId: number,
  apiVersion: number,
  request: AlterReplicaLogDirsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.AlterReplicaLogDirs,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeAlterReplicaLogDirsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an AlterReplicaLogDirs response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–2).
 * @returns The decoded response or a failure.
 */
export function decodeAlterReplicaLogDirsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<AlterReplicaLogDirsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }
  const throttleTimeMs = throttleResult.value

  // results array
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

  const results: AlterReplicaLogDirsTopicResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    const topicResult = decodeAlterReplicaLogDirsTopicResponse(reader, isFlexible)
    if (!topicResult.ok) {
      return topicResult
    }
    results.push(topicResult.value)
  }

  // Tagged fields (v2+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess({ throttleTimeMs, results, taggedFields }, reader.offset - startOffset)
}

function decodeAlterReplicaLogDirsTopicResponse(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<AlterReplicaLogDirsTopicResponse> {
  const startOffset = reader.offset

  // topic_name (STRING)
  const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!nameResult.ok) {
    return nameResult
  }

  // partitions array
  let partitionsLength: number
  if (isFlexible) {
    const lengthResult = reader.readUnsignedVarInt()
    if (!lengthResult.ok) {
      return lengthResult
    }
    partitionsLength = lengthResult.value - 1
  } else {
    const lengthResult = reader.readInt32()
    if (!lengthResult.ok) {
      return lengthResult
    }
    partitionsLength = lengthResult.value
  }

  const partitions: AlterReplicaLogDirsPartitionResponse[] = []
  for (let j = 0; j < partitionsLength; j++) {
    // partition_index (INT32)
    const partResult = reader.readInt32()
    if (!partResult.ok) {
      return partResult
    }

    // error_code (INT16)
    const errorResult = reader.readInt16()
    if (!errorResult.ok) {
      return errorResult
    }

    // Tagged fields (v2+)
    let partTaggedFields: readonly TaggedField[] = []
    if (isFlexible) {
      const tagResult = reader.readTaggedFields()
      if (!tagResult.ok) {
        return tagResult
      }
      partTaggedFields = tagResult.value
    }

    partitions.push({
      partitionIndex: partResult.value,
      errorCode: errorResult.value,
      taggedFields: partTaggedFields
    })
  }

  // Tagged fields (v2+)
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
      topicName: nameResult.value ?? "",
      partitions,
      taggedFields
    },
    reader.offset - startOffset
  )
}
