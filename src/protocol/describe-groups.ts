/**
 * DescribeGroups request/response encoding and decoding.
 *
 * The DescribeGroups API (key 15) returns detailed metadata about one or
 * more consumer groups, including state, protocol, members, and their
 * current assignments.
 *
 * **Request versions:**
 * - v0: groups (array of group IDs)
 * - v3: adds include_authorized_operations
 * - v5+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: per-group metadata with members
 * - v1+: throttle_time_ms
 * - v3+: authorized_operations per group
 * - v4+: group_instance_id per member
 * - v5+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeGroups
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
 * DescribeGroups request payload.
 */
export type DescribeGroupsRequest = {
  /** The group IDs to describe. */
  readonly groups: readonly string[]
  /** Whether to include authorised operations (v3+). */
  readonly includeAuthorizedOperations?: boolean
  /** Tagged fields (v5+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A member within a described group.
 */
export type DescribeGroupsMember = {
  /** The member ID. */
  readonly memberId: string
  /** The group instance ID for static membership (v4+). */
  readonly groupInstanceId: string | null
  /** The client ID of the member. */
  readonly clientId: string
  /** The client host of the member. */
  readonly clientHost: string
  /** The member metadata (subscription). */
  readonly memberMetadata: Uint8Array
  /** The member assignment (partition assignment). */
  readonly memberAssignment: Uint8Array
  /** Tagged fields (v5+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * A described group.
 */
export type DescribeGroupsGroup = {
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** The group ID. */
  readonly groupId: string
  /** The group state (e.g. "Stable", "Empty", "Dead"). */
  readonly groupState: string
  /** The protocol type (e.g. "consumer"). */
  readonly protocolType: string
  /** The active protocol (e.g. "range", "roundrobin"). */
  readonly protocolData: string
  /** Group members with their metadata and assignments. */
  readonly members: readonly DescribeGroupsMember[]
  /** Authorised operations bitmask (v3+). -2147483648 if not requested. */
  readonly authorizedOperations: number
  /** Tagged fields (v5+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeGroups response payload.
 */
export type DescribeGroupsResponse = {
  /** Time the request was throttled in milliseconds (v1+). */
  readonly throttleTimeMs: number
  /** Described groups. */
  readonly groups: readonly DescribeGroupsGroup[]
  /** Tagged fields (v5+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeGroups request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–5).
 */
export function encodeDescribeGroupsRequest(
  writer: BinaryWriter,
  request: DescribeGroupsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 5

  // groups array
  const { groups } = request
  if (isFlexible) {
    writer.writeUnsignedVarInt(groups.length + 1)
  } else {
    writer.writeInt32(groups.length)
  }
  for (const groupId of groups) {
    if (isFlexible) {
      writer.writeCompactString(groupId)
    } else {
      writer.writeString(groupId)
    }
  }

  // include_authorized_operations (v3+)
  if (apiVersion >= 3) {
    writer.writeInt8(request.includeAuthorizedOperations === true ? 1 : 0)
  }

  // Tagged fields (v5+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed DescribeGroups request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–5).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildDescribeGroupsRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeGroupsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeGroups,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeGroupsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeGroups response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–5).
 * @returns The decoded response or a failure.
 */
export function decodeDescribeGroupsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeGroupsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 5

  // throttle_time_ms (v1+)
  let throttleTimeMs = 0
  if (apiVersion >= 1) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  // groups array
  const groupsResult = decodeDescribeGroupsGroupArray(reader, apiVersion, isFlexible)
  if (!groupsResult.ok) {
    return groupsResult
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
      throttleTimeMs,
      groups: groupsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeDescribeGroupsGroupArray(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribeGroupsGroup[]> {
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

  const groups: DescribeGroupsGroup[] = []
  for (let i = 0; i < count; i++) {
    const groupResult = decodeDescribeGroupsGroup(reader, apiVersion, isFlexible)
    if (!groupResult.ok) {
      return groupResult
    }
    groups.push(groupResult.value)
  }

  return decodeSuccess(groups, reader.offset - startOffset)
}

function decodeDescribeGroupsGroup(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribeGroupsGroup> {
  const startOffset = reader.offset

  // error_code
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // group_id
  const groupIdResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!groupIdResult.ok) {
    return groupIdResult
  }

  // group_state
  const groupStateResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!groupStateResult.ok) {
    return groupStateResult
  }

  // protocol_type
  const protocolTypeResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!protocolTypeResult.ok) {
    return protocolTypeResult
  }

  // protocol_data
  const protocolDataResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!protocolDataResult.ok) {
    return protocolDataResult
  }

  // members array
  const membersResult = decodeDescribeGroupsMemberArray(reader, apiVersion, isFlexible)
  if (!membersResult.ok) {
    return membersResult
  }

  // authorized_operations (v3+)
  let authorizedOperations = -2_147_483_648 // INT32_MIN — "not requested"
  if (apiVersion >= 3) {
    const authOpsResult = reader.readInt32()
    if (!authOpsResult.ok) {
      return authOpsResult
    }
    authorizedOperations = authOpsResult.value
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
      errorCode: errorCodeResult.value,
      groupId: groupIdResult.value ?? "",
      groupState: groupStateResult.value ?? "",
      protocolType: protocolTypeResult.value ?? "",
      protocolData: protocolDataResult.value ?? "",
      members: membersResult.value,
      authorizedOperations,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeDescribeGroupsMemberArray(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribeGroupsMember[]> {
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

  const members: DescribeGroupsMember[] = []
  for (let i = 0; i < count; i++) {
    const memberResult = decodeDescribeGroupsMember(reader, apiVersion, isFlexible)
    if (!memberResult.ok) {
      return memberResult
    }
    members.push(memberResult.value)
  }

  return decodeSuccess(members, reader.offset - startOffset)
}

function decodeDescribeGroupsMember(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribeGroupsMember> {
  const startOffset = reader.offset
  const readStr = isFlexible ? () => reader.readCompactString() : () => reader.readString()
  const readBuf = isFlexible ? () => reader.readCompactBytes() : () => reader.readBytes()

  // member_id
  const memberIdResult = readStr()
  if (!memberIdResult.ok) {
    return memberIdResult
  }

  // group_instance_id (v4+, nullable)
  let groupInstanceId: string | null = null
  if (apiVersion >= 4) {
    const instanceResult = readStr()
    if (!instanceResult.ok) {
      return instanceResult
    }
    groupInstanceId = instanceResult.value
  }

  // client_id
  const clientIdResult = readStr()
  if (!clientIdResult.ok) {
    return clientIdResult
  }

  // client_host
  const clientHostResult = readStr()
  if (!clientHostResult.ok) {
    return clientHostResult
  }

  // member_metadata (BYTES)
  const metadataResult = readBuf()
  if (!metadataResult.ok) {
    return metadataResult
  }

  // member_assignment (BYTES)
  const assignmentResult = readBuf()
  if (!assignmentResult.ok) {
    return assignmentResult
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
      memberId: memberIdResult.value ?? "",
      groupInstanceId,
      clientId: clientIdResult.value ?? "",
      clientHost: clientHostResult.value ?? "",
      memberMetadata: metadataResult.value ?? new Uint8Array(0),
      memberAssignment: assignmentResult.value ?? new Uint8Array(0),
      taggedFields
    },
    reader.offset - startOffset
  )
}
