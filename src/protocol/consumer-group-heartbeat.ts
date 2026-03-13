/**
 * ConsumerGroupHeartbeat request/response encoding and decoding.
 *
 * The ConsumerGroupHeartbeat API (key 68) is the single RPC used by
 * KIP-848 "new consumer group protocol". It replaces the classic
 * JoinGroup/SyncGroup/Heartbeat/LeaveGroup flow with a single
 * declarative heartbeat that carries the consumer's subscription and
 * assignment state.
 *
 * **Request versions:**
 * - v0: group_id, member_id, member_epoch, instance_id, rack_id,
 *        rebalance_timeout_ms, subscribed_topic_names, server_assignor,
 *        topic_partitions
 * - All versions use flexible encoding
 *
 * **Response versions:**
 * - v0: throttle_time_ms, error_code, error_message, member_id,
 *        member_epoch, heartbeat_interval_ms, assignment
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848
 * @see https://kafka.apache.org/protocol.html#The_Messages_ConsumerGroupHeartbeat
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
 * A topic-partition pair in the consumer's current assignment.
 */
export type ConsumerGroupHeartbeatRequestTopicPartition = {
  /** The topic ID (UUID). */
  readonly topicId: Uint8Array
  /** The partition indexes assigned to this consumer. */
  readonly partitions: readonly number[]
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * ConsumerGroupHeartbeat request payload (v0).
 */
export type ConsumerGroupHeartbeatRequest = {
  /** The group ID. */
  readonly groupId: string
  /** The member ID (empty string on first heartbeat). */
  readonly memberId: string
  /**
   * The member epoch.
   *   0 to join the group.
   *  -1 to leave the group.
   *  -2 to signal a completely new member that wants to join.
   */
  readonly memberEpoch: number
  /** The instance ID for static membership, or null. */
  readonly instanceId: string | null
  /** The rack ID, or null. */
  readonly rackId: string | null
  /** The maximum time in ms for the group to rebalance. */
  readonly rebalanceTimeoutMs: number
  /** The list of subscribed topic names, or null (unchanged). */
  readonly subscribedTopicNames: readonly string[] | null
  /** The server-side assignor name, or null. */
  readonly serverAssignor: string | null
  /** The consumer's current topic-partition assignment, or null (unchanged). */
  readonly topicPartitions: readonly ConsumerGroupHeartbeatRequestTopicPartition[] | null
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A topic-partition pair in the assignment returned by the group coordinator.
 */
export type ConsumerGroupHeartbeatResponseTopicPartition = {
  /** The topic ID (UUID). */
  readonly topicId: Uint8Array
  /** The assigned partition indexes. */
  readonly partitions: readonly number[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * The assignment returned in the ConsumerGroupHeartbeat response.
 */
export type ConsumerGroupHeartbeatAssignment = {
  /** The topic-partitions assigned to the consumer. */
  readonly topicPartitions: readonly ConsumerGroupHeartbeatResponseTopicPartition[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * ConsumerGroupHeartbeat response payload (v0).
 */
export type ConsumerGroupHeartbeatResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Error message, or null. */
  readonly errorMessage: string | null
  /** The member ID assigned by the coordinator, or null. */
  readonly memberId: string | null
  /** The member epoch. */
  readonly memberEpoch: number
  /** The heartbeat interval in milliseconds. */
  readonly heartbeatIntervalMs: number
  /** The assignment, or null if unchanged. */
  readonly assignment: ConsumerGroupHeartbeatAssignment | null
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a ConsumerGroupHeartbeat request body into the given writer.
 */
export function encodeConsumerGroupHeartbeatRequest(
  writer: BinaryWriter,
  request: ConsumerGroupHeartbeatRequest,
  _apiVersion: number
): void {
  // group_id (COMPACT_STRING)
  writer.writeCompactString(request.groupId)

  // member_id (COMPACT_STRING)
  writer.writeCompactString(request.memberId)

  // member_epoch (INT32)
  writer.writeInt32(request.memberEpoch)

  // instance_id (COMPACT_NULLABLE_STRING)
  writer.writeCompactString(request.instanceId ?? null)

  // rack_id (COMPACT_NULLABLE_STRING)
  writer.writeCompactString(request.rackId ?? null)

  // rebalance_timeout_ms (INT32)
  writer.writeInt32(request.rebalanceTimeoutMs)

  // subscribed_topic_names (COMPACT_NULLABLE_ARRAY of COMPACT_STRING)
  if (request.subscribedTopicNames === null) {
    writer.writeUnsignedVarInt(0) // null marker for compact array
  } else {
    writer.writeUnsignedVarInt(request.subscribedTopicNames.length + 1)
    for (const topic of request.subscribedTopicNames) {
      writer.writeCompactString(topic)
    }
  }

  // server_assignor (COMPACT_NULLABLE_STRING)
  writer.writeCompactString(request.serverAssignor ?? null)

  // topic_partitions (COMPACT_NULLABLE_ARRAY of TopicPartition)
  if (request.topicPartitions === null) {
    writer.writeUnsignedVarInt(0) // null marker
  } else {
    writer.writeUnsignedVarInt(request.topicPartitions.length + 1)
    for (const tp of request.topicPartitions) {
      // topic_id (UUID)
      writer.writeUuid(tp.topicId)
      // partitions (COMPACT_ARRAY of INT32)
      writer.writeUnsignedVarInt(tp.partitions.length + 1)
      for (const p of tp.partitions) {
        writer.writeInt32(p)
      }
      // tagged fields
      writer.writeTaggedFields(tp.taggedFields ?? [])
    }
  }

  // tagged fields
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed ConsumerGroupHeartbeat request ready to send.
 */
export function buildConsumerGroupHeartbeatRequest(
  correlationId: number,
  apiVersion: number,
  request: ConsumerGroupHeartbeatRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.ConsumerGroupHeartbeat,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeConsumerGroupHeartbeatRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a ConsumerGroupHeartbeat response body.
 */
export function decodeConsumerGroupHeartbeatResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<ConsumerGroupHeartbeatResponse> {
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

  // member_id (COMPACT_NULLABLE_STRING)
  const memberIdResult = reader.readCompactString()
  if (!memberIdResult.ok) {
    return memberIdResult
  }

  // member_epoch (INT32)
  const memberEpochResult = reader.readInt32()
  if (!memberEpochResult.ok) {
    return memberEpochResult
  }

  // heartbeat_interval_ms (INT32)
  const heartbeatIntervalResult = reader.readInt32()
  if (!heartbeatIntervalResult.ok) {
    return heartbeatIntervalResult
  }

  // assignment (nullable struct)
  const assignmentResult = decodeNullableAssignment(reader)
  if (!assignmentResult.ok) {
    return assignmentResult
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
      memberId: memberIdResult.value,
      memberEpoch: memberEpochResult.value,
      heartbeatIntervalMs: heartbeatIntervalResult.value,
      assignment: assignmentResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function decodeNullableAssignment(
  reader: BinaryReader
): DecodeResult<ConsumerGroupHeartbeatAssignment | null> {
  const startOffset = reader.offset

  // The assignment is a tagged field struct with an INT8 presence marker.
  // In the protocol, the assignment is encoded as a compact nullable array
  // wrapping topic-partition entries. null array -> no assignment change.

  // topic_partitions (COMPACT_NULLABLE_ARRAY)
  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }

  if (countResult.value === 0) {
    // null array -> assignment is null
    // Still need to read tagged fields for the assignment struct
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    return decodeSuccess(null, reader.offset - startOffset)
  }

  const count = countResult.value - 1
  const topicPartitions: ConsumerGroupHeartbeatResponseTopicPartition[] = []

  for (let i = 0; i < count; i++) {
    const tpResult = decodeResponseTopicPartition(reader)
    if (!tpResult.ok) {
      return tpResult
    }
    topicPartitions.push(tpResult.value)
  }

  // tagged fields for the assignment struct
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      topicPartitions,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeResponseTopicPartition(
  reader: BinaryReader
): DecodeResult<ConsumerGroupHeartbeatResponseTopicPartition> {
  const startOffset = reader.offset

  // topic_id (UUID)
  const topicIdResult = reader.readUuid()
  if (!topicIdResult.ok) {
    return topicIdResult
  }

  // partitions (COMPACT_ARRAY of INT32)
  const partCountResult = reader.readUnsignedVarInt()
  if (!partCountResult.ok) {
    return partCountResult
  }
  const partCount = partCountResult.value - 1
  const partitions: number[] = []
  for (let i = 0; i < partCount; i++) {
    const pResult = reader.readInt32()
    if (!pResult.ok) {
      return pResult
    }
    partitions.push(pResult.value)
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      topicId: topicIdResult.value,
      partitions,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
