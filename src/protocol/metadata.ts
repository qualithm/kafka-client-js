/**
 * Metadata request/response encoding and decoding.
 *
 * The Metadata API (key 3) returns broker and topic information for the cluster.
 * It is used to discover brokers, topic partitions, and their leaders.
 *
 * **Request versions:**
 * - v0–v3: topics array (null = all topics in v1+)
 * - v4+: adds allow_auto_topic_creation
 * - v8–v10: adds include_cluster_authorized_operations
 * - v8+: adds include_topic_authorized_operations
 * - v9+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: brokers + topics
 * - v1+: adds controller_id, is_internal flag
 * - v2+: adds cluster_id
 * - v3+: adds throttle_time_ms
 * - v5+: adds offline_replicas
 * - v7+: adds leader_epoch
 * - v8+: adds authorized_operations
 * - v9+: flexible encoding
 * - v10+: removes cluster_authorized_operations
 * - v12+: topic ID (UUID)
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_Metadata
 *
 * @packageDocumentation
 */

import { ApiKey } from "../codec/api-keys.js"
import type { BinaryReader, TaggedField } from "../codec/binary-reader.js"
import type { BinaryWriter } from "../codec/binary-writer.js"
import { frameRequest, type RequestHeader } from "../codec/protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "../result.js"

// NULL UUID (16 zero bytes) used when topic ID is not provided
const NULL_UUID = new Uint8Array(16)

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * Topic to request metadata for.
 */
export type MetadataRequestTopic = {
  /** Topic ID (v10+, UUID as 16 bytes). Currently unused in requests. */
  readonly topicId?: Uint8Array | null
  /** Topic name. Null if fetching by topic ID only (v12+). */
  readonly name: string | null
  /** Tagged fields (v9+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Metadata request payload.
 */
export type MetadataRequest = {
  /**
   * Topics to fetch metadata for.
   * - Empty array = no topics (useful for broker discovery)
   * - Null (v1+) = all topics
   * - Array of topic names = specific topics
   */
  readonly topics: readonly MetadataRequestTopic[] | null
  /**
   * Whether to allow auto-creation of topics (v4+).
   * @default true
   */
  readonly allowAutoTopicCreation?: boolean
  /**
   * Include cluster-level authorised operations (v8–v10).
   * Removed in v11+.
   */
  readonly includeClusterAuthorisedOperations?: boolean
  /**
   * Include topic-level authorised operations (v8+).
   */
  readonly includeTopicAuthorisedOperations?: boolean
  /** Tagged fields (v9+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Broker information from the metadata response.
 */
export type MetadataBroker = {
  /** The broker's node ID. */
  readonly nodeId: number
  /** The broker's hostname. */
  readonly host: string
  /** The broker's port. */
  readonly port: number
  /** The broker's rack (v1+). Null if not configured. */
  readonly rack: string | null
  /** Tagged fields (v9+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Partition information from the metadata response.
 */
export type MetadataPartition = {
  /** Error code for this partition (0 = no error). */
  readonly errorCode: number
  /** The partition index. */
  readonly partitionIndex: number
  /** The ID of the leader broker. -1 if no leader. */
  readonly leaderId: number
  /** The leader epoch (v7+). -1 if unknown. */
  readonly leaderEpoch: number
  /** The replica broker IDs. */
  readonly replicaNodes: readonly number[]
  /** The in-sync replica broker IDs. */
  readonly isrNodes: readonly number[]
  /** The offline replica broker IDs (v5+). */
  readonly offlineReplicas: readonly number[]
  /** Tagged fields (v9+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Topic information from the metadata response.
 */
export type MetadataTopic = {
  /** Error code for this topic (0 = no error). */
  readonly errorCode: number
  /** The topic name. */
  readonly name: string | null
  /** Topic ID (v10+, UUID as 16 bytes). */
  readonly topicId?: Uint8Array
  /** Whether this is an internal topic (v1+). */
  readonly isInternal: boolean
  /** Partition metadata. */
  readonly partitions: readonly MetadataPartition[]
  /** Topic-level authorised operations bitmask (v8+). */
  readonly topicAuthorisedOperations: number
  /** Tagged fields (v9+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Metadata response payload.
 */
export type MetadataResponse = {
  /** Time the request was throttled in milliseconds (v3+). */
  readonly throttleTimeMs: number
  /** Broker information. */
  readonly brokers: readonly MetadataBroker[]
  /** Cluster ID (v2+). Null if not available. */
  readonly clusterId: string | null
  /** Controller broker ID (v1+). -1 if unknown. */
  readonly controllerId: number
  /** Topic metadata. */
  readonly topics: readonly MetadataTopic[]
  /** Cluster-level authorised operations bitmask (v8–v10). */
  readonly clusterAuthorisedOperations: number
  /** Tagged fields (v9+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a Metadata request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–12).
 */
export function encodeMetadataRequest(
  writer: BinaryWriter,
  request: MetadataRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 9

  // Topics array
  if (request.topics === null) {
    // Null array = all topics (v1+)
    if (isFlexible) {
      // Compact nullable: -1 => 0
      writer.writeUnsignedVarInt(0)
    } else {
      writer.writeInt32(-1)
    }
  } else {
    // Encode topic array
    if (isFlexible) {
      writer.writeUnsignedVarInt(request.topics.length + 1)
    } else {
      writer.writeInt32(request.topics.length)
    }

    for (const topic of request.topics) {
      // Topic ID (v10+) - 16 zero bytes for now (not sending by UUID)
      if (apiVersion >= 10) {
        const uuid = topic.topicId ?? NULL_UUID
        writer.writeRawBytes(uuid)
      }

      // Topic name
      if (isFlexible) {
        writer.writeCompactString(topic.name)
      } else {
        writer.writeString(topic.name)
      }

      // Tagged fields per topic (v9+)
      if (isFlexible) {
        writer.writeTaggedFields(topic.taggedFields ?? [])
      }
    }
  }

  // allow_auto_topic_creation (v4+)
  if (apiVersion >= 4) {
    writer.writeBoolean(request.allowAutoTopicCreation ?? true)
  }

  // include_cluster_authorized_operations (v8–v10)
  if (apiVersion >= 8 && apiVersion <= 10) {
    writer.writeBoolean(request.includeClusterAuthorisedOperations ?? false)
  }

  // include_topic_authorized_operations (v8+)
  if (apiVersion >= 8) {
    writer.writeBoolean(request.includeTopicAuthorisedOperations ?? false)
  }

  // Tagged fields (v9+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed Metadata request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–12).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildMetadataRequest(
  correlationId: number,
  apiVersion: number,
  request: MetadataRequest = { topics: null },
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.Metadata,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeMetadataRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a Metadata response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–12).
 * @returns The decoded response or a failure.
 */
export function decodeMetadataResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<MetadataResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 9

  // Throttle time (v3+)
  let throttleTimeMs = 0
  if (apiVersion >= 3) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  // Brokers array
  const brokersResult = decodeBrokers(reader, apiVersion, isFlexible)
  if (!brokersResult.ok) {
    return brokersResult
  }
  const brokers = brokersResult.value

  // Cluster ID (v2+)
  let clusterId: string | null = null
  if (apiVersion >= 2) {
    const clusterIdResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!clusterIdResult.ok) {
      return clusterIdResult
    }
    clusterId = clusterIdResult.value
  }

  // Controller ID (v1+)
  let controllerId = -1
  if (apiVersion >= 1) {
    const controllerResult = reader.readInt32()
    if (!controllerResult.ok) {
      return controllerResult
    }
    controllerId = controllerResult.value
  }

  // Topics array
  const topicsResult = decodeTopics(reader, apiVersion, isFlexible)
  if (!topicsResult.ok) {
    return topicsResult
  }
  const topics = topicsResult.value

  // Cluster authorized operations (v8–v10)
  let clusterAuthorisedOperations = -2147483648 // INT32_MIN = not requested
  if (apiVersion >= 8 && apiVersion <= 10) {
    const opsResult = reader.readInt32()
    if (!opsResult.ok) {
      return opsResult
    }
    clusterAuthorisedOperations = opsResult.value
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
      throttleTimeMs,
      brokers,
      clusterId,
      controllerId,
      topics,
      clusterAuthorisedOperations,
      taggedFields
    },
    reader.offset - startOffset
  )
}

// ---------------------------------------------------------------------------
// Helper decoders
// ---------------------------------------------------------------------------

function decodeBrokers(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<MetadataBroker[]> {
  // Array length
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

  if (arrayLength < 0) {
    arrayLength = 0
  }

  const brokers: MetadataBroker[] = []

  for (let i = 0; i < arrayLength; i++) {
    // Node ID
    const nodeIdResult = reader.readInt32()
    if (!nodeIdResult.ok) {
      return nodeIdResult
    }

    // Host
    const hostResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!hostResult.ok) {
      return hostResult
    }
    // Host should never be null in practice
    const host = hostResult.value ?? ""

    // Port
    const portResult = reader.readInt32()
    if (!portResult.ok) {
      return portResult
    }

    // Rack (v1+)
    let rack: string | null = null
    if (apiVersion >= 1) {
      const rackResult = isFlexible ? reader.readCompactString() : reader.readString()
      if (!rackResult.ok) {
        return rackResult
      }
      rack = rackResult.value
    }

    // Tagged fields (v9+)
    let entryTaggedFields: readonly TaggedField[] = []
    if (isFlexible) {
      const tagResult = reader.readTaggedFields()
      if (!tagResult.ok) {
        return tagResult
      }
      entryTaggedFields = tagResult.value
    }

    brokers.push({
      nodeId: nodeIdResult.value,
      host,
      port: portResult.value,
      rack,
      taggedFields: entryTaggedFields
    })
  }

  return decodeSuccess(brokers, 0)
}

function decodeTopics(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<MetadataTopic[]> {
  // Array length
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

  if (arrayLength < 0) {
    arrayLength = 0
  }

  const topics: MetadataTopic[] = []

  for (let i = 0; i < arrayLength; i++) {
    // Error code
    const errorCodeResult = reader.readInt16()
    if (!errorCodeResult.ok) {
      return errorCodeResult
    }

    // Topic name
    const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!nameResult.ok) {
      return nameResult
    }

    // Topic ID (v10+) - UUID as 16 bytes
    let topicId: Uint8Array | undefined
    if (apiVersion >= 10) {
      const uuidResult = reader.readRawBytes(16)
      if (!uuidResult.ok) {
        return uuidResult
      }
      topicId = uuidResult.value
    }

    // Is internal (v1+)
    let isInternal = false
    if (apiVersion >= 1) {
      const internalResult = reader.readBoolean()
      if (!internalResult.ok) {
        return internalResult
      }
      isInternal = internalResult.value
    }

    // Partitions
    const partitionsResult = decodePartitions(reader, apiVersion, isFlexible)
    if (!partitionsResult.ok) {
      return partitionsResult
    }

    // Topic authorized operations (v8+)
    let topicAuthorisedOperations = -2147483648
    if (apiVersion >= 8) {
      const opsResult = reader.readInt32()
      if (!opsResult.ok) {
        return opsResult
      }
      topicAuthorisedOperations = opsResult.value
    }

    // Tagged fields (v9+)
    let entryTaggedFields: readonly TaggedField[] = []
    if (isFlexible) {
      const tagResult = reader.readTaggedFields()
      if (!tagResult.ok) {
        return tagResult
      }
      entryTaggedFields = tagResult.value
    }

    topics.push({
      errorCode: errorCodeResult.value,
      name: nameResult.value,
      topicId,
      isInternal,
      partitions: partitionsResult.value,
      topicAuthorisedOperations,
      taggedFields: entryTaggedFields
    })
  }

  return decodeSuccess(topics, 0)
}

function decodePartitions(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<MetadataPartition[]> {
  // Array length
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

  if (arrayLength < 0) {
    arrayLength = 0
  }

  const partitions: MetadataPartition[] = []

  for (let i = 0; i < arrayLength; i++) {
    // Error code
    const errorCodeResult = reader.readInt16()
    if (!errorCodeResult.ok) {
      return errorCodeResult
    }

    // Partition index
    const partitionIndexResult = reader.readInt32()
    if (!partitionIndexResult.ok) {
      return partitionIndexResult
    }

    // Leader ID
    const leaderIdResult = reader.readInt32()
    if (!leaderIdResult.ok) {
      return leaderIdResult
    }

    // Leader epoch (v7+)
    let leaderEpoch = -1
    if (apiVersion >= 7) {
      const epochResult = reader.readInt32()
      if (!epochResult.ok) {
        return epochResult
      }
      leaderEpoch = epochResult.value
    }

    // Replica nodes
    const replicasResult = decodeInt32Array(reader, isFlexible)
    if (!replicasResult.ok) {
      return replicasResult
    }

    // ISR nodes
    const isrResult = decodeInt32Array(reader, isFlexible)
    if (!isrResult.ok) {
      return isrResult
    }

    // Offline replicas (v5+)
    let offlineReplicas: readonly number[] = []
    if (apiVersion >= 5) {
      const offlineResult = decodeInt32Array(reader, isFlexible)
      if (!offlineResult.ok) {
        return offlineResult
      }
      offlineReplicas = offlineResult.value
    }

    // Tagged fields (v9+)
    let entryTaggedFields: readonly TaggedField[] = []
    if (isFlexible) {
      const tagResult = reader.readTaggedFields()
      if (!tagResult.ok) {
        return tagResult
      }
      entryTaggedFields = tagResult.value
    }

    partitions.push({
      errorCode: errorCodeResult.value,
      partitionIndex: partitionIndexResult.value,
      leaderId: leaderIdResult.value,
      leaderEpoch,
      replicaNodes: replicasResult.value,
      isrNodes: isrResult.value,
      offlineReplicas,
      taggedFields: entryTaggedFields
    })
  }

  return decodeSuccess(partitions, 0)
}

function decodeInt32Array(reader: BinaryReader, isFlexible: boolean): DecodeResult<number[]> {
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

  if (arrayLength < 0) {
    arrayLength = 0
  }

  const result: number[] = []
  for (let i = 0; i < arrayLength; i++) {
    const valueResult = reader.readInt32()
    if (!valueResult.ok) {
      return valueResult
    }
    result.push(valueResult.value)
  }

  return decodeSuccess(result, 0)
}
