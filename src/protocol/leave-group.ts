/**
 * LeaveGroup request/response encoding and decoding.
 *
 * The LeaveGroup API (key 13) is used by consumer group members to leave
 * the group gracefully. This triggers a rebalance so remaining members
 * can take over the leaving member's partitions.
 *
 * **Request versions:**
 * - v0–v2: group_id, member_id (single member leave)
 * - v3+: members[] (batched leave for multiple members)
 * - v4+: flexible encoding (KIP-482)
 * - v5: adds reason per member
 *
 * **Response versions:**
 * - v0: error_code
 * - v1+: throttle_time_ms
 * - v3+: members[] with per-member error
 * - v4+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_LeaveGroup
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
 * A member leaving the group (v3+).
 */
export type LeaveGroupMemberRequest = {
  /** The member ID. */
  readonly memberId: string
  /** The group instance ID for static membership. Null if not static. */
  readonly groupInstanceId?: string | null
  /** Reason for leaving (v5+). */
  readonly reason?: string | null
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * LeaveGroup request payload.
 */
export type LeaveGroupRequest = {
  /** The group ID. */
  readonly groupId: string
  /** The member ID (v0–v2 single-member leave). */
  readonly memberId?: string
  /** Members leaving the group (v3+ batched leave). */
  readonly members?: readonly LeaveGroupMemberRequest[]
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-member leave result (v3+).
 */
export type LeaveGroupMemberResponse = {
  /** The member ID. */
  readonly memberId: string
  /** The group instance ID. */
  readonly groupInstanceId: string | null
  /** Error code for this member (0 = no error). */
  readonly errorCode: number
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * LeaveGroup response payload.
 */
export type LeaveGroupResponse = {
  /** Time the request was throttled in milliseconds (v1+). */
  readonly throttleTimeMs: number
  /** Top-level error code (0 = no error). */
  readonly errorCode: number
  /** Per-member leave results (v3+). */
  readonly members: readonly LeaveGroupMemberResponse[]
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a LeaveGroup request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–5).
 */
export function encodeLeaveGroupRequest(
  writer: BinaryWriter,
  request: LeaveGroupRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 4

  // group_id
  if (isFlexible) {
    writer.writeCompactString(request.groupId)
  } else {
    writer.writeString(request.groupId)
  }

  if (apiVersion < 3) {
    // v0–v2: single member_id
    if (isFlexible) {
      writer.writeCompactString(request.memberId ?? "")
    } else {
      writer.writeString(request.memberId ?? "")
    }
  } else {
    // v3+: members array
    const members = request.members ?? []
    if (isFlexible) {
      writer.writeUnsignedVarInt(members.length + 1)
    } else {
      writer.writeInt32(members.length)
    }

    for (const member of members) {
      encodeLeaveGroupMember(writer, member, apiVersion, isFlexible)
    }
  }

  // Tagged fields (v4+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

function encodeLeaveGroupMember(
  writer: BinaryWriter,
  member: LeaveGroupMemberRequest,
  apiVersion: number,
  isFlexible: boolean
): void {
  // member_id
  if (isFlexible) {
    writer.writeCompactString(member.memberId)
  } else {
    writer.writeString(member.memberId)
  }

  // group_instance_id (nullable)
  if (isFlexible) {
    writer.writeCompactString(member.groupInstanceId ?? null)
  } else {
    writer.writeString(member.groupInstanceId ?? null)
  }

  // reason (v5+, nullable)
  if (apiVersion >= 5) {
    writer.writeCompactString(member.reason ?? null)
  }

  // Tagged fields
  if (isFlexible) {
    writer.writeTaggedFields(member.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed LeaveGroup request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–5).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildLeaveGroupRequest(
  correlationId: number,
  apiVersion: number,
  request: LeaveGroupRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.LeaveGroup,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeLeaveGroupRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a LeaveGroup response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–5).
 * @returns The decoded response or a failure.
 */
export function decodeLeaveGroupResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<LeaveGroupResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 4

  // throttle_time_ms (v1+)
  let throttleTimeMs = 0
  if (apiVersion >= 1) {
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

  // members array (v3+)
  let members: LeaveGroupMemberResponse[] = []
  if (apiVersion >= 3) {
    const membersResult = decodeLeaveGroupMemberResponses(reader, isFlexible)
    if (!membersResult.ok) {
      return membersResult
    }
    members = membersResult.value
  }

  // Tagged fields (v4+)
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
      members,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeLeaveGroupMemberResponses(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<LeaveGroupMemberResponse[]> {
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

  const members: LeaveGroupMemberResponse[] = []
  for (let i = 0; i < count; i++) {
    const memberResult = decodeLeaveGroupMemberResponse(reader, isFlexible)
    if (!memberResult.ok) {
      return memberResult
    }
    members.push(memberResult.value)
  }

  return decodeSuccess(members, reader.offset - startOffset)
}

function decodeLeaveGroupMemberResponse(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<LeaveGroupMemberResponse> {
  const startOffset = reader.offset

  // member_id
  const memberIdResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!memberIdResult.ok) {
    return memberIdResult
  }

  // group_instance_id (nullable)
  const instanceResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!instanceResult.ok) {
    return instanceResult
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
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
      groupInstanceId: instanceResult.value,
      errorCode: errorCodeResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}
