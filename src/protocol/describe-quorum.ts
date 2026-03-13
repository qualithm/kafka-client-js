/**
 * DescribeQuorum request/response encoding and decoding.
 *
 * The DescribeQuorum API (key 55) describes the current quorum state of
 * KRaft controller partitions.
 *
 * **Request versions:**
 * - v0: topics array with partition_indexes
 * - v1: same structure, adds extra response fields
 * - All versions use flexible encoding
 *
 * **Response versions:**
 * - v0: error_code, error_message, topics array with quorum info
 * - v1: adds nodes array, per-replica last_caught_up_timestamp and last_fetch_timestamp
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeQuorum
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
 * A topic partition to describe quorum for.
 */
export type DescribeQuorumTopicRequest = {
  /** The topic name. */
  readonly topicName: string
  /** The partition indexes. */
  readonly partitions: readonly DescribeQuorumPartitionRequest[]
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A partition in the quorum request.
 */
export type DescribeQuorumPartitionRequest = {
  /** The partition index. */
  readonly partitionIndex: number
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * DescribeQuorum request payload.
 */
export type DescribeQuorumRequest = {
  /** Topics and partitions to describe. */
  readonly topics: readonly DescribeQuorumTopicRequest[]
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Replica state information for a voter or observer.
 */
export type ReplicaState = {
  /** The replica ID. */
  readonly replicaId: number
  /** The end offset of the log for this replica. */
  readonly logEndOffset: bigint
  /** The last caught-up timestamp in milliseconds (v1+, -1 if unknown). */
  readonly lastCaughtUpTimestamp: bigint
  /** The last fetch timestamp in milliseconds (v1+, -1 if unknown). */
  readonly lastFetchTimestamp: bigint
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-partition quorum information.
 */
export type DescribeQuorumPartitionResponse = {
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** The partition index. */
  readonly partitionIndex: number
  /** The current leader ID. */
  readonly leaderId: number
  /** The current leader epoch. */
  readonly leaderEpoch: number
  /** The high watermark. */
  readonly highWatermark: bigint
  /** Current voters in the quorum. */
  readonly currentVoters: readonly ReplicaState[]
  /** Current observers of the quorum. */
  readonly observers: readonly ReplicaState[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-topic quorum information.
 */
export type DescribeQuorumTopicResponse = {
  /** The topic name. */
  readonly topicName: string
  /** Per-partition quorum information. */
  readonly partitions: readonly DescribeQuorumPartitionResponse[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * A node in the quorum (v1+).
 */
export type QuorumNode = {
  /** The node ID. */
  readonly nodeId: number
  /** Listeners for this node. */
  readonly listeners: readonly QuorumNodeListener[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * A listener endpoint for a quorum node (v1+).
 */
export type QuorumNodeListener = {
  /** The listener name. */
  readonly name: string
  /** The hostname. */
  readonly host: string
  /** The port. */
  readonly port: number
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeQuorum response payload.
 */
export type DescribeQuorumResponse = {
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Error message, or null. */
  readonly errorMessage: string | null
  /** Per-topic quorum information. */
  readonly topics: readonly DescribeQuorumTopicResponse[]
  /** Quorum nodes (v1+). */
  readonly nodes: readonly QuorumNode[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeQuorum request body into the given writer.
 */
export function encodeDescribeQuorumRequest(
  writer: BinaryWriter,
  request: DescribeQuorumRequest,
  _apiVersion: number
): void {
  // topics (compact array)
  writer.writeUnsignedVarInt(request.topics.length + 1)
  for (const topic of request.topics) {
    // topic_name (COMPACT_STRING)
    writer.writeCompactString(topic.topicName)

    // partitions (compact array)
    writer.writeUnsignedVarInt(topic.partitions.length + 1)
    for (const partition of topic.partitions) {
      // partition_index (INT32)
      writer.writeInt32(partition.partitionIndex)
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
 * Build a complete, framed DescribeQuorum request ready to send to a broker.
 */
export function buildDescribeQuorumRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeQuorumRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeQuorum,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeQuorumRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeQuorum response body.
 */
export function decodeDescribeQuorumResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeQuorumResponse> {
  const startOffset = reader.offset

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // error_message (COMPACT_NULLABLE_STRING — present in all flexible versions)
  const errorMsgResult = reader.readCompactString()
  if (!errorMsgResult.ok) {
    return errorMsgResult
  }

  // topics array
  const topicsResult = decodeQuorumTopicsArray(reader, apiVersion)
  if (!topicsResult.ok) {
    return topicsResult
  }

  // nodes (v1+)
  let nodes: QuorumNode[] = []
  if (apiVersion >= 1) {
    const nodesResult = decodeNodesArray(reader)
    if (!nodesResult.ok) {
      return nodesResult
    }
    nodes = nodesResult.value
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      topics: topicsResult.value,
      nodes,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeQuorumTopicsArray(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeQuorumTopicResponse[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const topics: DescribeQuorumTopicResponse[] = []
  for (let i = 0; i < count; i++) {
    const topicResult = decodeQuorumTopicEntry(reader, apiVersion)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

function decodeQuorumTopicEntry(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeQuorumTopicResponse> {
  const startOffset = reader.offset

  // topic_name (COMPACT_STRING)
  const nameResult = reader.readCompactString()
  if (!nameResult.ok) {
    return nameResult
  }

  // partitions array
  const partitionsResult = decodeQuorumPartitionsArray(reader, apiVersion)
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
      topicName: nameResult.value ?? "",
      partitions: partitionsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeQuorumPartitionsArray(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeQuorumPartitionResponse[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const partitions: DescribeQuorumPartitionResponse[] = []
  for (let i = 0; i < count; i++) {
    const partResult = decodeQuorumPartitionEntry(reader, apiVersion)
    if (!partResult.ok) {
      return partResult
    }
    partitions.push(partResult.value)
  }

  return decodeSuccess(partitions, reader.offset - startOffset)
}

function decodeQuorumPartitionEntry(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeQuorumPartitionResponse> {
  const startOffset = reader.offset

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // partition_index (INT32)
  const partIdxResult = reader.readInt32()
  if (!partIdxResult.ok) {
    return partIdxResult
  }

  // leader_id (INT32)
  const leaderResult = reader.readInt32()
  if (!leaderResult.ok) {
    return leaderResult
  }

  // leader_epoch (INT32)
  const epochResult = reader.readInt32()
  if (!epochResult.ok) {
    return epochResult
  }

  // high_watermark (INT64)
  const hwResult = reader.readInt64()
  if (!hwResult.ok) {
    return hwResult
  }

  // current_voters array
  const votersResult = decodeReplicaStatesArray(reader, apiVersion)
  if (!votersResult.ok) {
    return votersResult
  }

  // observers array
  const observersResult = decodeReplicaStatesArray(reader, apiVersion)
  if (!observersResult.ok) {
    return observersResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      errorCode: errorCodeResult.value,
      partitionIndex: partIdxResult.value,
      leaderId: leaderResult.value,
      leaderEpoch: epochResult.value,
      highWatermark: hwResult.value,
      currentVoters: votersResult.value,
      observers: observersResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeReplicaStatesArray(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<ReplicaState[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const replicas: ReplicaState[] = []
  for (let i = 0; i < count; i++) {
    const replicaResult = decodeReplicaState(reader, apiVersion)
    if (!replicaResult.ok) {
      return replicaResult
    }
    replicas.push(replicaResult.value)
  }

  return decodeSuccess(replicas, reader.offset - startOffset)
}

function decodeReplicaState(reader: BinaryReader, apiVersion: number): DecodeResult<ReplicaState> {
  const startOffset = reader.offset

  // replica_id (INT32)
  const replicaIdResult = reader.readInt32()
  if (!replicaIdResult.ok) {
    return replicaIdResult
  }

  // log_end_offset (INT64)
  const logEndResult = reader.readInt64()
  if (!logEndResult.ok) {
    return logEndResult
  }

  // last_caught_up_timestamp (INT64, v1+)
  let lastCaughtUpTimestamp = -1n
  if (apiVersion >= 1) {
    const caughtUpResult = reader.readInt64()
    if (!caughtUpResult.ok) {
      return caughtUpResult
    }
    lastCaughtUpTimestamp = caughtUpResult.value
  }

  // last_fetch_timestamp (INT64, v1+)
  let lastFetchTimestamp = -1n
  if (apiVersion >= 1) {
    const fetchTsResult = reader.readInt64()
    if (!fetchTsResult.ok) {
      return fetchTsResult
    }
    lastFetchTimestamp = fetchTsResult.value
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      replicaId: replicaIdResult.value,
      logEndOffset: logEndResult.value,
      lastCaughtUpTimestamp,
      lastFetchTimestamp,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeNodesArray(reader: BinaryReader): DecodeResult<QuorumNode[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const nodes: QuorumNode[] = []
  for (let i = 0; i < count; i++) {
    const nodeResult = decodeNodeEntry(reader)
    if (!nodeResult.ok) {
      return nodeResult
    }
    nodes.push(nodeResult.value)
  }

  return decodeSuccess(nodes, reader.offset - startOffset)
}

function decodeNodeEntry(reader: BinaryReader): DecodeResult<QuorumNode> {
  const startOffset = reader.offset

  // node_id (INT32)
  const nodeIdResult = reader.readInt32()
  if (!nodeIdResult.ok) {
    return nodeIdResult
  }

  // listeners array
  const listenersResult = decodeListenersArray(reader)
  if (!listenersResult.ok) {
    return listenersResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      nodeId: nodeIdResult.value,
      listeners: listenersResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeListenersArray(reader: BinaryReader): DecodeResult<QuorumNodeListener[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const listeners: QuorumNodeListener[] = []
  for (let i = 0; i < count; i++) {
    const listenerResult = decodeListenerEntry(reader)
    if (!listenerResult.ok) {
      return listenerResult
    }
    listeners.push(listenerResult.value)
  }

  return decodeSuccess(listeners, reader.offset - startOffset)
}

function decodeListenerEntry(reader: BinaryReader): DecodeResult<QuorumNodeListener> {
  const startOffset = reader.offset

  // name (COMPACT_STRING)
  const nameResult = reader.readCompactString()
  if (!nameResult.ok) {
    return nameResult
  }

  // host (COMPACT_STRING)
  const hostResult = reader.readCompactString()
  if (!hostResult.ok) {
    return hostResult
  }

  // port (UINT16 — read as INT32 for consistency with Metadata broker pattern)
  const portResult = reader.readInt16()
  if (!portResult.ok) {
    return portResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      name: nameResult.value ?? "",
      host: hostResult.value ?? "",
      port: portResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
