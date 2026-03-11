/**
 * JoinGroup request/response encoding and decoding.
 *
 * The JoinGroup API (key 11) is the first step of consumer group coordination.
 * Members send a JoinGroup request to the group coordinator. The coordinator
 * elects a leader and returns protocol metadata to all members.
 *
 * **Request versions:**
 * - v0: group_id, session_timeout_ms, member_id, protocol_type, protocols[]
 * - v1: adds rebalance_timeout_ms
 * - v4: if member_id required, broker rejects with MEMBER_ID_REQUIRED, client retries with assigned ID
 * - v5: adds group_instance_id (KIP-345 static membership)
 * - v6+: flexible encoding (KIP-482)
 * - v8: adds reason
 * - v9: adds reason (same structure)
 *
 * **Response versions:**
 * - v0: throttle_time_ms (v2+), error_code, generation_id, protocol_type (v7+), protocol_name, leader, member_id, members[]
 * - v5: adds group_instance_id per member
 * - v6+: flexible encoding
 * - v7: adds protocol_type to response
 * - v9: adds skip_assignment (KIP-814)
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_JoinGroup
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
 * A protocol that a consumer group member supports.
 */
export type JoinGroupProtocol = {
  /** Protocol name (e.g. "range", "roundrobin"). */
  readonly name: string
  /** Protocol metadata (opaque bytes, typically serialised assignment strategy metadata). */
  readonly metadata: Uint8Array
  /** Tagged fields (v6+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * JoinGroup request payload.
 */
export type JoinGroupRequest = {
  /** The group ID to join. */
  readonly groupId: string
  /** The session timeout in milliseconds. */
  readonly sessionTimeoutMs: number
  /** The rebalance timeout in milliseconds (v1+). Defaults to sessionTimeoutMs if not set. */
  readonly rebalanceTimeoutMs?: number
  /** The member ID. Empty string for first join; assigned ID for subsequent joins. */
  readonly memberId: string
  /** The group instance ID for static membership (v5+). Null if not static. */
  readonly groupInstanceId?: string | null
  /** Protocol type (e.g. "consumer"). */
  readonly protocolType: string
  /** Supported protocols with metadata. */
  readonly protocols: readonly JoinGroupProtocol[]
  /** Reason for joining (v8+). */
  readonly reason?: string | null
  /** Tagged fields (v6+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A member in the JoinGroup response.
 */
export type JoinGroupMember = {
  /** The member ID. */
  readonly memberId: string
  /** The group instance ID (v5+). Null if not static. */
  readonly groupInstanceId: string | null
  /** The member's protocol metadata. */
  readonly metadata: Uint8Array
  /** Tagged fields (v6+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * JoinGroup response payload.
 */
export type JoinGroupResponse = {
  /** Time the request was throttled in milliseconds (v2+). */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** The generation ID for this group. */
  readonly generationId: number
  /** The group protocol type (v7+). Null if not available. */
  readonly protocolType: string | null
  /** The group protocol name chosen by the coordinator. */
  readonly protocolName: string | null
  /** The leader member ID. */
  readonly leader: string
  /** Whether the broker skipped assignment (v9+, KIP-814). */
  readonly skipAssignment: boolean
  /** This member's assigned member ID. */
  readonly memberId: string
  /** Members in the group (only populated for the leader). */
  readonly members: readonly JoinGroupMember[]
  /** Tagged fields (v6+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a JoinGroup request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–9).
 */
export function encodeJoinGroupRequest(
  writer: BinaryWriter,
  request: JoinGroupRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 6

  // group_id
  if (isFlexible) {
    writer.writeCompactString(request.groupId)
  } else {
    writer.writeString(request.groupId)
  }

  // session_timeout_ms (INT32)
  writer.writeInt32(request.sessionTimeoutMs)

  // rebalance_timeout_ms (INT32, v1+)
  if (apiVersion >= 1) {
    writer.writeInt32(request.rebalanceTimeoutMs ?? request.sessionTimeoutMs)
  }

  // member_id
  if (isFlexible) {
    writer.writeCompactString(request.memberId)
  } else {
    writer.writeString(request.memberId)
  }

  // group_instance_id (v5+, nullable)
  if (apiVersion >= 5) {
    if (isFlexible) {
      writer.writeCompactString(request.groupInstanceId ?? null)
    } else {
      writer.writeString(request.groupInstanceId ?? null)
    }
  }

  // protocol_type
  if (isFlexible) {
    writer.writeCompactString(request.protocolType)
  } else {
    writer.writeString(request.protocolType)
  }

  // protocols array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.protocols.length + 1)
  } else {
    writer.writeInt32(request.protocols.length)
  }

  for (const protocol of request.protocols) {
    encodeJoinGroupProtocol(writer, protocol, isFlexible)
  }

  // reason (v8+, nullable)
  if (apiVersion >= 8) {
    writer.writeCompactString(request.reason ?? null)
  }

  // Tagged fields (v6+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

function encodeJoinGroupProtocol(
  writer: BinaryWriter,
  protocol: JoinGroupProtocol,
  isFlexible: boolean
): void {
  // name
  if (isFlexible) {
    writer.writeCompactString(protocol.name)
  } else {
    writer.writeString(protocol.name)
  }

  // metadata (bytes)
  if (isFlexible) {
    writer.writeCompactBytes(protocol.metadata)
  } else {
    writer.writeBytes(protocol.metadata)
  }

  // Tagged fields
  if (isFlexible) {
    writer.writeTaggedFields(protocol.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed JoinGroup request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–9).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildJoinGroupRequest(
  correlationId: number,
  apiVersion: number,
  request: JoinGroupRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.JoinGroup,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeJoinGroupRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a JoinGroup response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–9).
 * @returns The decoded response or a failure.
 */
export function decodeJoinGroupResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<JoinGroupResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 6

  // throttle_time_ms (v2+)
  let throttleTimeMs = 0
  if (apiVersion >= 2) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // generation_id (INT32)
  const generationResult = reader.readInt32()
  if (!generationResult.ok) {
    return generationResult
  }

  // protocol_type (v7+, nullable compact string)
  let protocolType: string | null = null
  if (apiVersion >= 7) {
    const ptResult = reader.readCompactString()
    if (!ptResult.ok) {
      return ptResult
    }
    protocolType = ptResult.value
  }

  // protocol_name (nullable string)
  const protocolNameResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!protocolNameResult.ok) {
    return protocolNameResult
  }

  // leader
  const leaderResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!leaderResult.ok) {
    return leaderResult
  }

  // skip_assignment (v9+, KIP-814)
  let skipAssignment = false
  if (apiVersion >= 9) {
    const skipResult = reader.readBoolean()
    if (!skipResult.ok) {
      return skipResult
    }
    skipAssignment = skipResult.value
  }

  // member_id
  const memberIdResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!memberIdResult.ok) {
    return memberIdResult
  }

  // members array
  const membersResult = decodeJoinGroupMembers(reader, apiVersion, isFlexible)
  if (!membersResult.ok) {
    return membersResult
  }

  // Tagged fields (v6+)
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
      errorCode: errorCodeResult.value,
      generationId: generationResult.value,
      protocolType,
      protocolName: protocolNameResult.value,
      leader: leaderResult.value ?? "",
      skipAssignment,
      memberId: memberIdResult.value ?? "",
      members: membersResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeJoinGroupMembers(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<JoinGroupMember[]> {
  const startOffset = reader.offset
  let count: number

  if (isFlexible) {
    const countResult = reader.readUnsignedVarInt()
    if (!countResult.ok) {
      return countResult
    }
    count = countResult.value - 1
  } else {
    const countResult = reader.readInt32()
    if (!countResult.ok) {
      return countResult
    }
    count = countResult.value
  }

  const members: JoinGroupMember[] = []
  for (let i = 0; i < count; i++) {
    const memberResult = decodeJoinGroupMember(reader, apiVersion, isFlexible)
    if (!memberResult.ok) {
      return memberResult
    }
    members.push(memberResult.value)
  }

  return decodeSuccess(members, reader.offset - startOffset)
}

function decodeJoinGroupMember(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<JoinGroupMember> {
  const startOffset = reader.offset

  // member_id
  const memberIdResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!memberIdResult.ok) {
    return memberIdResult
  }

  // group_instance_id (v5+, nullable)
  let groupInstanceId: string | null = null
  if (apiVersion >= 5) {
    const instanceResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!instanceResult.ok) {
      return instanceResult
    }
    groupInstanceId = instanceResult.value
  }

  // metadata (bytes)
  const metadataResult = isFlexible ? reader.readCompactBytes() : reader.readBytes()
  if (!metadataResult.ok) {
    return metadataResult
  }

  // Tagged fields
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
      memberId: memberIdResult.value ?? "",
      groupInstanceId,
      metadata: metadataResult.value ?? new Uint8Array(0),
      taggedFields
    },
    reader.offset - startOffset
  )
}
