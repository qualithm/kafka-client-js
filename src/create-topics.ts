/**
 * CreateTopics request/response encoding and decoding.
 *
 * The CreateTopics API (key 19) creates one or more topics in the cluster.
 *
 * **Request versions:**
 * - v0–v4: non-flexible encoding
 * - v5+: flexible encoding (KIP-482)
 * - v1+: adds validate_only
 * - v0: timeout_ms
 *
 * **Response versions:**
 * - v0: topic-level error codes
 * - v1+: adds error_message per topic
 * - v2+: adds throttle_time_ms
 * - v5+: flexible encoding, adds num_partitions, replication_factor, configs to response
 * - v7+: adds topic_id
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_CreateTopics
 *
 * @packageDocumentation
 */

import { ApiKey } from "./api-keys.js"
import type { BinaryReader, TaggedField } from "./binary-reader.js"
import type { BinaryWriter } from "./binary-writer.js"
import { frameRequest, type RequestHeader } from "./protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "./result.js"

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * Replica assignment for a partition in a CreateTopics request.
 */
export type CreateTopicsReplicaAssignment = {
  /** The partition index. */
  readonly partitionIndex: number
  /** The broker IDs for the replicas. */
  readonly brokerIds: readonly number[]
  /** Tagged fields (v5+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Configuration entry for a topic in a CreateTopics request.
 */
export type CreateTopicsConfigEntry = {
  /** The configuration key. */
  readonly name: string
  /** The configuration value. Null to use the default. */
  readonly value: string | null
  /** Tagged fields (v5+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A single topic to create.
 */
export type CreateTopicRequest = {
  /** The topic name. */
  readonly name: string
  /** Number of partitions (-1 to use broker default). */
  readonly numPartitions: number
  /** Replication factor (-1 to use broker default). */
  readonly replicationFactor: number
  /** Manual replica assignments (overrides numPartitions/replicationFactor). */
  readonly assignments?: readonly CreateTopicsReplicaAssignment[]
  /** Topic configuration overrides. */
  readonly configs?: readonly CreateTopicsConfigEntry[]
  /** Tagged fields (v5+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * CreateTopics request payload.
 */
export type CreateTopicsRequest = {
  /** Topics to create. */
  readonly topics: readonly CreateTopicRequest[]
  /** Timeout in milliseconds. */
  readonly timeoutMs: number
  /** If true, validate the request without creating topics (v1+). */
  readonly validateOnly?: boolean
  /** Tagged fields (v5+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Configuration entry in a CreateTopics response (v5+).
 */
export type CreateTopicsResponseConfigEntry = {
  /** The configuration key. */
  readonly name: string
  /** The configuration value. */
  readonly value: string | null
  /** Whether this is a read-only config. */
  readonly readOnly: boolean
  /**
   * The config source.
   * 1=DYNAMIC_TOPIC, 2=DYNAMIC_BROKER, 4=STATIC_BROKER, 5=DEFAULT, 6=DYNAMIC_DEFAULT_BROKER.
   */
  readonly configSource: number
  /** Whether this is a sensitive config (value hidden). */
  readonly isSensitive: boolean
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-topic result from a CreateTopics response.
 */
export type CreateTopicsTopicResponse = {
  /** The topic name. */
  readonly name: string
  /** Topic ID (v7+, UUID as 16 bytes). */
  readonly topicId?: Uint8Array
  /** Error code (0 = success). */
  readonly errorCode: number
  /** Error message (v1+). */
  readonly errorMessage: string | null
  /** Number of partitions (v5+). -1 if not available. */
  readonly numPartitions: number
  /** Replication factor (v5+). -1 if not available. */
  readonly replicationFactor: number
  /** Topic configuration entries (v5+). */
  readonly configs: readonly CreateTopicsResponseConfigEntry[]
  /** Tagged fields (v5+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * CreateTopics response payload.
 */
export type CreateTopicsResponse = {
  /** Time the request was throttled in milliseconds (v2+). */
  readonly throttleTimeMs: number
  /** Per-topic results. */
  readonly topics: readonly CreateTopicsTopicResponse[]
  /** Tagged fields (v5+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a CreateTopics request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–7).
 */
export function encodeCreateTopicsRequest(
  writer: BinaryWriter,
  request: CreateTopicsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 5

  // Topics array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.topics.length + 1)
  } else {
    writer.writeInt32(request.topics.length)
  }

  for (const topic of request.topics) {
    encodeCreateTopicEntry(writer, topic, isFlexible)
  }

  // timeout_ms (INT32)
  writer.writeInt32(request.timeoutMs)

  // validate_only (v1+) (BOOLEAN)
  if (apiVersion >= 1) {
    writer.writeBoolean(request.validateOnly ?? false)
  }

  // Tagged fields (v5+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

function encodeCreateTopicEntry(
  writer: BinaryWriter,
  topic: CreateTopicRequest,
  isFlexible: boolean
): void {
  // Topic name
  if (isFlexible) {
    writer.writeCompactString(topic.name)
  } else {
    writer.writeString(topic.name)
  }

  // num_partitions (INT32)
  writer.writeInt32(topic.numPartitions)

  // replication_factor (INT16)
  writer.writeInt16(topic.replicationFactor)

  // Replica assignments array
  const assignments = topic.assignments ?? []
  if (isFlexible) {
    writer.writeUnsignedVarInt(assignments.length + 1)
  } else {
    writer.writeInt32(assignments.length)
  }
  for (const assignment of assignments) {
    writer.writeInt32(assignment.partitionIndex)
    if (isFlexible) {
      writer.writeUnsignedVarInt(assignment.brokerIds.length + 1)
    } else {
      writer.writeInt32(assignment.brokerIds.length)
    }
    for (const brokerId of assignment.brokerIds) {
      writer.writeInt32(brokerId)
    }
    if (isFlexible) {
      writer.writeTaggedFields(assignment.taggedFields ?? [])
    }
  }

  // Configs array
  const configs = topic.configs ?? []
  if (isFlexible) {
    writer.writeUnsignedVarInt(configs.length + 1)
  } else {
    writer.writeInt32(configs.length)
  }
  for (const config of configs) {
    if (isFlexible) {
      writer.writeCompactString(config.name)
      writer.writeCompactString(config.value)
    } else {
      writer.writeString(config.name)
      writer.writeString(config.value)
    }
    if (isFlexible) {
      writer.writeTaggedFields(config.taggedFields ?? [])
    }
  }

  if (isFlexible) {
    writer.writeTaggedFields(topic.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed CreateTopics request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–7).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildCreateTopicsRequest(
  correlationId: number,
  apiVersion: number,
  request: CreateTopicsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.CreateTopics,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeCreateTopicsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a CreateTopics response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–7).
 * @returns The decoded response or a failure.
 */
export function decodeCreateTopicsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<CreateTopicsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 5

  // Throttle time (v2+)
  let throttleTimeMs = 0
  if (apiVersion >= 2) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  // Topics array
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

  const topics: CreateTopicsTopicResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    const topicResult = decodeCreateTopicsTopicResponse(reader, apiVersion, isFlexible)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  // Tagged fields (v5+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess({ throttleTimeMs, topics, taggedFields }, reader.offset - startOffset)
}

function decodeCreateTopicsTopicResponse(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<CreateTopicsTopicResponse> {
  const startOffset = reader.offset

  // Topic name
  const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!nameResult.ok) {
    return nameResult
  }
  const name = nameResult.value ?? ""

  // Topic ID (v7+, UUID 16 bytes)
  let topicId: Uint8Array | undefined
  if (apiVersion >= 7) {
    const uuidResult = reader.readRawBytes(16)
    if (!uuidResult.ok) {
      return uuidResult
    }
    topicId = uuidResult.value
  }

  // Error code
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // Error message (v1+)
  let errorMessage: string | null = null
  if (apiVersion >= 1) {
    const errMsgResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!errMsgResult.ok) {
      return errMsgResult
    }
    errorMessage = errMsgResult.value
  }

  // num_partitions (v5+)
  let numPartitions = -1
  if (apiVersion >= 5) {
    const npResult = reader.readInt32()
    if (!npResult.ok) {
      return npResult
    }
    numPartitions = npResult.value
  }

  // replication_factor (v5+)
  let replicationFactor = -1
  if (apiVersion >= 5) {
    const rfResult = reader.readInt16()
    if (!rfResult.ok) {
      return rfResult
    }
    replicationFactor = rfResult.value
  }

  // configs array (v5+)
  let configs: CreateTopicsResponseConfigEntry[] = []
  if (apiVersion >= 5) {
    const configsLenResult = reader.readUnsignedVarInt()
    if (!configsLenResult.ok) {
      return configsLenResult
    }
    const configsLen = configsLenResult.value - 1

    configs = []
    for (let j = 0; j < configsLen; j++) {
      const configResult = decodeCreateTopicsConfigEntry(reader)
      if (!configResult.ok) {
        return configResult
      }
      configs.push(configResult.value)
    }
  }

  // Tagged fields (v5+)
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
      topicId,
      errorCode: errorCodeResult.value,
      errorMessage,
      numPartitions,
      replicationFactor,
      configs,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeCreateTopicsConfigEntry(
  reader: BinaryReader
): DecodeResult<CreateTopicsResponseConfigEntry> {
  const startOffset = reader.offset

  // name
  const nameResult = reader.readCompactString()
  if (!nameResult.ok) {
    return nameResult
  }

  // value
  const valueResult = reader.readCompactString()
  if (!valueResult.ok) {
    return valueResult
  }

  // read_only
  const readOnlyResult = reader.readBoolean()
  if (!readOnlyResult.ok) {
    return readOnlyResult
  }

  // config_source
  const sourceResult = reader.readInt8()
  if (!sourceResult.ok) {
    return sourceResult
  }

  // is_sensitive
  const sensitiveResult = reader.readBoolean()
  if (!sensitiveResult.ok) {
    return sensitiveResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      name: nameResult.value ?? "",
      value: valueResult.value,
      readOnly: readOnlyResult.value,
      configSource: sourceResult.value,
      isSensitive: sensitiveResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
