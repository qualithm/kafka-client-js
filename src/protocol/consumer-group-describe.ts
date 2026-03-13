/**
 * ConsumerGroupDescribe request/response encoding and decoding.
 *
 * The ConsumerGroupDescribe API (key 69) returns detailed information
 * about one or more consumer groups using the KIP-848 "new consumer
 * group protocol". It provides member-level details including current
 * and target assignments.
 *
 * **Request versions:**
 * - v0: group_ids, include_authorized_operations
 * - All versions use flexible encoding
 *
 * **Response versions:**
 * - v0: throttle_time_ms, groups[] (error_code, error_message, group_id,
 *        group_state, group_epoch, assignment_epoch, assignor_name,
 *        members[], authorized_operations)
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848
 * @see https://kafka.apache.org/protocol.html#The_Messages_ConsumerGroupDescribe
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
 * ConsumerGroupDescribe request payload (v0).
 */
export type ConsumerGroupDescribeRequest = {
  /** The IDs of the groups to describe. */
  readonly groupIds: readonly string[]
  /** Whether to include authorised operations. */
  readonly includeAuthorizedOperations: boolean
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A topic-partition entry within an assignment.
 */
export type ConsumerGroupDescribeTopicPartition = {
  /** The topic ID (UUID). */
  readonly topicId: Uint8Array
  /** The topic name. */
  readonly topicName: string
  /** The partition indexes. */
  readonly partitions: readonly number[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * An assignment (current or target) for a member.
 */
export type ConsumerGroupDescribeAssignment = {
  /** The assigned topic-partitions. */
  readonly topicPartitions: readonly ConsumerGroupDescribeTopicPartition[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * A member within a described consumer group.
 */
export type ConsumerGroupDescribeMember = {
  /** The member ID. */
  readonly memberId: string
  /** The member instance ID, or null. */
  readonly instanceId: string | null
  /** The member rack ID, or null. */
  readonly rackId: string | null
  /** The current member epoch. */
  readonly memberEpoch: number
  /** The client ID. */
  readonly clientId: string
  /** The client host. */
  readonly clientHost: string
  /** The subscribed topic names. */
  readonly subscribedTopicNames: readonly string[]
  /** The subscribed topic regex, or null. */
  readonly subscribedTopicRegex: string | null
  /** The current assignment. */
  readonly assignment: ConsumerGroupDescribeAssignment
  /** The target assignment. */
  readonly targetAssignment: ConsumerGroupDescribeAssignment
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * A described consumer group in the response.
 */
export type ConsumerGroupDescribeGroup = {
  /** The describe error code, or 0 if no error. */
  readonly errorCode: number
  /** The error message, or null. */
  readonly errorMessage: string | null
  /** The group ID. */
  readonly groupId: string
  /** The group state string, or empty string. */
  readonly groupState: string
  /** The group epoch. */
  readonly groupEpoch: number
  /** The assignment epoch. */
  readonly assignmentEpoch: number
  /** The selected assignor. */
  readonly assignorName: string
  /** The group members. */
  readonly members: readonly ConsumerGroupDescribeMember[]
  /** 32-bit bitfield of authorised operations, or -2147483648 if not requested. */
  readonly authorizedOperations: number
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * ConsumerGroupDescribe response payload (v0).
 */
export type ConsumerGroupDescribeResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Each described group. */
  readonly groups: readonly ConsumerGroupDescribeGroup[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a ConsumerGroupDescribe request body into the given writer.
 */
export function encodeConsumerGroupDescribeRequest(
  writer: BinaryWriter,
  request: ConsumerGroupDescribeRequest,
  _apiVersion: number
): void {
  // group_ids (COMPACT_ARRAY of COMPACT_STRING)
  writer.writeUnsignedVarInt(request.groupIds.length + 1)
  for (const groupId of request.groupIds) {
    writer.writeCompactString(groupId)
  }

  // include_authorized_operations (BOOL)
  writer.writeInt8(request.includeAuthorizedOperations ? 1 : 0)

  // tagged fields
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed ConsumerGroupDescribe request ready to send.
 */
export function buildConsumerGroupDescribeRequest(
  correlationId: number,
  apiVersion: number,
  request: ConsumerGroupDescribeRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.ConsumerGroupDescribe,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeConsumerGroupDescribeRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a ConsumerGroupDescribe response body.
 */
export function decodeConsumerGroupDescribeResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<ConsumerGroupDescribeResponse> {
  const startOffset = reader.offset

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // groups (COMPACT_ARRAY of DescribedGroup)
  const groupCountResult = reader.readUnsignedVarInt()
  if (!groupCountResult.ok) {
    return groupCountResult
  }
  const groupCount = groupCountResult.value - 1
  const groups: ConsumerGroupDescribeGroup[] = []

  for (let i = 0; i < groupCount; i++) {
    const groupResult = decodeDescribedGroup(reader)
    if (!groupResult.ok) {
      return groupResult
    }
    groups.push(groupResult.value)
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      throttleTimeMs: throttleResult.value,
      groups,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function decodeDescribedGroup(reader: BinaryReader): DecodeResult<ConsumerGroupDescribeGroup> {
  const startOffset = reader.offset

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

  // group_id (COMPACT_STRING)
  const groupIdResult = reader.readCompactString()
  if (!groupIdResult.ok) {
    return groupIdResult
  }

  // group_state (COMPACT_STRING)
  const groupStateResult = reader.readCompactString()
  if (!groupStateResult.ok) {
    return groupStateResult
  }

  // group_epoch (INT32)
  const groupEpochResult = reader.readInt32()
  if (!groupEpochResult.ok) {
    return groupEpochResult
  }

  // assignment_epoch (INT32)
  const assignmentEpochResult = reader.readInt32()
  if (!assignmentEpochResult.ok) {
    return assignmentEpochResult
  }

  // assignor_name (COMPACT_STRING)
  const assignorNameResult = reader.readCompactString()
  if (!assignorNameResult.ok) {
    return assignorNameResult
  }

  // members (COMPACT_ARRAY of Member)
  const memberCountResult = reader.readUnsignedVarInt()
  if (!memberCountResult.ok) {
    return memberCountResult
  }
  const memberCount = memberCountResult.value - 1
  const members: ConsumerGroupDescribeMember[] = []

  for (let i = 0; i < memberCount; i++) {
    const memberResult = decodeMember(reader)
    if (!memberResult.ok) {
      return memberResult
    }
    members.push(memberResult.value)
  }

  // authorized_operations (INT32)
  const authorizedOpsResult = reader.readInt32()
  if (!authorizedOpsResult.ok) {
    return authorizedOpsResult
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
      groupId: groupIdResult.value ?? "",
      groupState: groupStateResult.value ?? "",
      groupEpoch: groupEpochResult.value,
      assignmentEpoch: assignmentEpochResult.value,
      assignorName: assignorNameResult.value ?? "",
      members,
      authorizedOperations: authorizedOpsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeMember(reader: BinaryReader): DecodeResult<ConsumerGroupDescribeMember> {
  const startOffset = reader.offset

  // member_id (COMPACT_STRING)
  const memberIdResult = reader.readCompactString()
  if (!memberIdResult.ok) {
    return memberIdResult
  }

  // instance_id (COMPACT_NULLABLE_STRING)
  const instanceIdResult = reader.readCompactString()
  if (!instanceIdResult.ok) {
    return instanceIdResult
  }

  // rack_id (COMPACT_NULLABLE_STRING)
  const rackIdResult = reader.readCompactString()
  if (!rackIdResult.ok) {
    return rackIdResult
  }

  // member_epoch (INT32)
  const memberEpochResult = reader.readInt32()
  if (!memberEpochResult.ok) {
    return memberEpochResult
  }

  // client_id (COMPACT_STRING)
  const clientIdResult = reader.readCompactString()
  if (!clientIdResult.ok) {
    return clientIdResult
  }

  // client_host (COMPACT_STRING)
  const clientHostResult = reader.readCompactString()
  if (!clientHostResult.ok) {
    return clientHostResult
  }

  // subscribed_topic_names (COMPACT_ARRAY of COMPACT_STRING)
  const topicCountResult = reader.readUnsignedVarInt()
  if (!topicCountResult.ok) {
    return topicCountResult
  }
  const topicCount = topicCountResult.value - 1
  const subscribedTopicNames: string[] = []
  for (let i = 0; i < topicCount; i++) {
    const topicResult = reader.readCompactString()
    if (!topicResult.ok) {
      return topicResult
    }
    subscribedTopicNames.push(topicResult.value ?? "")
  }

  // subscribed_topic_regex (COMPACT_NULLABLE_STRING)
  const regexResult = reader.readCompactString()
  if (!regexResult.ok) {
    return regexResult
  }

  // assignment (Assignment struct)
  const assignmentResult = decodeAssignment(reader)
  if (!assignmentResult.ok) {
    return assignmentResult
  }

  // target_assignment (Assignment struct)
  const targetAssignmentResult = decodeAssignment(reader)
  if (!targetAssignmentResult.ok) {
    return targetAssignmentResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      memberId: memberIdResult.value ?? "",
      instanceId: instanceIdResult.value,
      rackId: rackIdResult.value,
      memberEpoch: memberEpochResult.value,
      clientId: clientIdResult.value ?? "",
      clientHost: clientHostResult.value ?? "",
      subscribedTopicNames,
      subscribedTopicRegex: regexResult.value,
      assignment: assignmentResult.value,
      targetAssignment: targetAssignmentResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeAssignment(reader: BinaryReader): DecodeResult<ConsumerGroupDescribeAssignment> {
  const startOffset = reader.offset

  // topic_partitions (COMPACT_ARRAY of TopicPartitions)
  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1
  const topicPartitions: ConsumerGroupDescribeTopicPartition[] = []

  for (let i = 0; i < count; i++) {
    const tpResult = decodeTopicPartition(reader)
    if (!tpResult.ok) {
      return tpResult
    }
    topicPartitions.push(tpResult.value)
  }

  // tagged fields
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

function decodeTopicPartition(
  reader: BinaryReader
): DecodeResult<ConsumerGroupDescribeTopicPartition> {
  const startOffset = reader.offset

  // topic_id (UUID)
  const topicIdResult = reader.readUuid()
  if (!topicIdResult.ok) {
    return topicIdResult
  }

  // topic_name (COMPACT_STRING)
  const topicNameResult = reader.readCompactString()
  if (!topicNameResult.ok) {
    return topicNameResult
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
      topicName: topicNameResult.value ?? "",
      partitions,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
