/**
 * DescribeLogDirs request/response encoding and decoding.
 *
 * The DescribeLogDirs API (key 35) returns information about log directories
 * on brokers, including size and offset lag for each topic-partition replica.
 *
 * **Request versions:**
 * - v0–v1: non-flexible encoding
 * - v2+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: log dirs with topic-partition size info
 * - v1+: adds is_future_key
 * - v2+: flexible encoding
 * - v3+: adds total_bytes and usable_bytes per log dir
 * - v4+: adds total_bytes and usable_bytes per topic-partition
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeLogDirs
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
 * A topic whose partitions to describe log dirs for.
 */
export type DescribeLogDirsTopic = {
  /** The topic name. */
  readonly topic: string
  /** The partition indices to describe. */
  readonly partitions: readonly number[]
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * DescribeLogDirs request payload.
 */
export type DescribeLogDirsRequest = {
  /** Topics to describe. Null to describe all topics on the broker. */
  readonly topics: readonly DescribeLogDirsTopic[] | null
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-partition log dir information.
 */
export type DescribeLogDirsPartition = {
  /** The partition index. */
  readonly partitionIndex: number
  /** The size of the partition in bytes. */
  readonly partitionSize: bigint
  /** The lag of the log's LEO vs the partition's HW. */
  readonly offsetLag: bigint
  /** Whether this replica is the future replica (v1+). */
  readonly isFutureKey: boolean
  /** Total bytes of the partition (v4+). -1 if unknown. */
  readonly totalBytes: bigint
  /** Usable bytes of the partition (v4+). -1 if unknown. */
  readonly usableBytes: bigint
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-topic log dir information.
 */
export type DescribeLogDirsTopicResponse = {
  /** The topic name. */
  readonly name: string
  /** Per-partition log dir info. */
  readonly partitions: readonly DescribeLogDirsPartition[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-log-directory result.
 */
export type DescribeLogDirsResult = {
  /** Error code for this log dir (0 = success). */
  readonly errorCode: number
  /** The absolute log directory path. */
  readonly logDir: string
  /** Per-topic results within this log dir. */
  readonly topics: readonly DescribeLogDirsTopicResponse[]
  /** Total bytes of the volume (v3+). -1 if unknown. */
  readonly totalBytes: bigint
  /** Usable bytes of the volume (v3+). -1 if unknown. */
  readonly usableBytes: bigint
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeLogDirs response payload.
 */
export type DescribeLogDirsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-log-directory results. */
  readonly results: readonly DescribeLogDirsResult[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeLogDirs request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–4).
 */
export function encodeDescribeLogDirsRequest(
  writer: BinaryWriter,
  request: DescribeLogDirsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // topics (nullable array)
  if (request.topics === null) {
    if (isFlexible) {
      writer.writeUnsignedVarInt(0) // null compact array
    } else {
      writer.writeInt32(-1)
    }
  } else {
    if (isFlexible) {
      writer.writeUnsignedVarInt(request.topics.length + 1)
    } else {
      writer.writeInt32(request.topics.length)
    }

    for (const topic of request.topics) {
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
  }

  // Tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed DescribeLogDirs request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–4).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildDescribeLogDirsRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeLogDirsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeLogDirs,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeLogDirsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeLogDirs response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–4).
 * @returns The decoded response or a failure.
 */
export function decodeDescribeLogDirsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeLogDirsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }
  const throttleTimeMs = throttleResult.value

  // error_code (INT16) — v3+
  // Note: top-level error_code is NOT in DescribeLogDirs, it's per-logdir

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

  const results: DescribeLogDirsResult[] = []
  for (let i = 0; i < arrayLength; i++) {
    const resultItem = decodeDescribeLogDirsResult(reader, apiVersion, isFlexible)
    if (!resultItem.ok) {
      return resultItem
    }
    results.push(resultItem.value)
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

function decodeDescribeLogDirsResult(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribeLogDirsResult> {
  const startOffset = reader.offset

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // log_dir (STRING)
  const logDirResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!logDirResult.ok) {
    return logDirResult
  }

  // topics array
  let topicsLength: number
  if (isFlexible) {
    const lengthResult = reader.readUnsignedVarInt()
    if (!lengthResult.ok) {
      return lengthResult
    }
    topicsLength = lengthResult.value - 1
  } else {
    const lengthResult = reader.readInt32()
    if (!lengthResult.ok) {
      return lengthResult
    }
    topicsLength = lengthResult.value
  }

  const topics: DescribeLogDirsTopicResponse[] = []
  for (let j = 0; j < topicsLength; j++) {
    const topicResult = decodeDescribeLogDirsTopicResponse(reader, apiVersion, isFlexible)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  // total_bytes (INT64) — v3+
  let totalBytes = BigInt(-1)
  if (apiVersion >= 3) {
    const totalResult = reader.readInt64()
    if (!totalResult.ok) {
      return totalResult
    }
    totalBytes = totalResult.value
  }

  // usable_bytes (INT64) — v3+
  let usableBytes = BigInt(-1)
  if (apiVersion >= 3) {
    const usableResult = reader.readInt64()
    if (!usableResult.ok) {
      return usableResult
    }
    usableBytes = usableResult.value
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
      errorCode: errorCodeResult.value,
      logDir: logDirResult.value ?? "",
      topics,
      totalBytes,
      usableBytes,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeDescribeLogDirsTopicResponse(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribeLogDirsTopicResponse> {
  const startOffset = reader.offset

  // name (STRING)
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

  const partitions: DescribeLogDirsPartition[] = []
  for (let k = 0; k < partitionsLength; k++) {
    const partResult = decodeDescribeLogDirsPartition(reader, apiVersion, isFlexible)
    if (!partResult.ok) {
      return partResult
    }
    partitions.push(partResult.value)
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
    { name: nameResult.value ?? "", partitions, taggedFields },
    reader.offset - startOffset
  )
}

function decodeDescribeLogDirsPartition(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribeLogDirsPartition> {
  const startOffset = reader.offset

  // partition_index (INT32)
  const partitionResult = reader.readInt32()
  if (!partitionResult.ok) {
    return partitionResult
  }

  // partition_size (INT64)
  const sizeResult = reader.readInt64()
  if (!sizeResult.ok) {
    return sizeResult
  }

  // offset_lag (INT64)
  const lagResult = reader.readInt64()
  if (!lagResult.ok) {
    return lagResult
  }

  // is_future_key (BOOLEAN) — v1+
  let isFutureKey = false
  if (apiVersion >= 1) {
    const futureResult = reader.readBoolean()
    if (!futureResult.ok) {
      return futureResult
    }
    isFutureKey = futureResult.value
  }

  // total_bytes (INT64) — v4+ (per-partition)
  let totalBytes = BigInt(-1)
  if (apiVersion >= 4) {
    const totalResult = reader.readInt64()
    if (!totalResult.ok) {
      return totalResult
    }
    totalBytes = totalResult.value
  }

  // usable_bytes (INT64) — v4+ (per-partition)
  let usableBytes = BigInt(-1)
  if (apiVersion >= 4) {
    const usableResult = reader.readInt64()
    if (!usableResult.ok) {
      return usableResult
    }
    usableBytes = usableResult.value
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
      partitionIndex: partitionResult.value,
      partitionSize: sizeResult.value,
      offsetLag: lagResult.value,
      isFutureKey,
      totalBytes,
      usableBytes,
      taggedFields
    },
    reader.offset - startOffset
  )
}
