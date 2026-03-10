/**
 * SyncGroup request/response encoding and decoding.
 *
 * The SyncGroup API (key 14) distributes partition assignments to consumer
 * group members. After JoinGroup, the leader computes assignments and sends
 * them to the coordinator via SyncGroup. All members (including the leader)
 * receive their assignment from the SyncGroup response.
 *
 * **Request versions:**
 * - v0: group_id, generation_id, member_id, assignments[]
 * - v3: adds group_instance_id (KIP-345)
 * - v4+: flexible encoding (KIP-482)
 * - v5: adds protocol_type, protocol_name
 *
 * **Response versions:**
 * - v0: error_code, assignment (bytes)
 * - v1+: throttle_time_ms
 * - v4+: flexible encoding
 * - v5: adds protocol_type, protocol_name
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_SyncGroup
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
 * A partition assignment for a group member (sent by the leader).
 */
export type SyncGroupAssignment = {
  /** The member ID to assign to. */
  readonly memberId: string
  /** The assignment data (opaque bytes, typically serialised partition assignment). */
  readonly assignment: Uint8Array
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * SyncGroup request payload.
 */
export type SyncGroupRequest = {
  /** The group ID. */
  readonly groupId: string
  /** The generation ID from the JoinGroup response. */
  readonly generationId: number
  /** This member's member ID. */
  readonly memberId: string
  /** The group instance ID for static membership (v3+). Null if not static. */
  readonly groupInstanceId?: string | null
  /** Protocol type (v5+). */
  readonly protocolType?: string | null
  /** Protocol name (v5+). */
  readonly protocolName?: string | null
  /** Partition assignments (only provided by the leader). */
  readonly assignments: readonly SyncGroupAssignment[]
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * SyncGroup response payload.
 */
export type SyncGroupResponse = {
  /** Time the request was throttled in milliseconds (v1+). */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Protocol type (v5+). Null if not available. */
  readonly protocolType: string | null
  /** Protocol name (v5+). Null if not available. */
  readonly protocolName: string | null
  /** This member's partition assignment (opaque bytes). */
  readonly assignment: Uint8Array
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a SyncGroup request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–5).
 */
export function encodeSyncGroupRequest(
  writer: BinaryWriter,
  request: SyncGroupRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 4

  // group_id
  if (isFlexible) {
    writer.writeCompactString(request.groupId)
  } else {
    writer.writeString(request.groupId)
  }

  // generation_id (INT32)
  writer.writeInt32(request.generationId)

  // member_id
  if (isFlexible) {
    writer.writeCompactString(request.memberId)
  } else {
    writer.writeString(request.memberId)
  }

  // group_instance_id (v3+, nullable)
  if (apiVersion >= 3) {
    if (isFlexible) {
      writer.writeCompactString(request.groupInstanceId ?? null)
    } else {
      writer.writeString(request.groupInstanceId ?? null)
    }
  }

  // protocol_type (v5+, nullable)
  if (apiVersion >= 5) {
    writer.writeCompactString(request.protocolType ?? null)
  }

  // protocol_name (v5+, nullable)
  if (apiVersion >= 5) {
    writer.writeCompactString(request.protocolName ?? null)
  }

  // assignments array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.assignments.length + 1)
  } else {
    writer.writeInt32(request.assignments.length)
  }

  for (const assignment of request.assignments) {
    encodeSyncGroupAssignment(writer, assignment, isFlexible)
  }

  // Tagged fields (v4+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

function encodeSyncGroupAssignment(
  writer: BinaryWriter,
  assignment: SyncGroupAssignment,
  isFlexible: boolean
): void {
  // member_id
  if (isFlexible) {
    writer.writeCompactString(assignment.memberId)
  } else {
    writer.writeString(assignment.memberId)
  }

  // assignment (bytes)
  if (isFlexible) {
    writer.writeCompactBytes(assignment.assignment)
  } else {
    writer.writeBytes(assignment.assignment)
  }

  // Tagged fields
  if (isFlexible) {
    writer.writeTaggedFields(assignment.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed SyncGroup request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–5).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildSyncGroupRequest(
  correlationId: number,
  apiVersion: number,
  request: SyncGroupRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.SyncGroup,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeSyncGroupRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a SyncGroup response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–5).
 * @returns The decoded response or a failure.
 */
export function decodeSyncGroupResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<SyncGroupResponse> {
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

  // protocol_type (v5+, nullable compact string)
  let protocolType: string | null = null
  if (apiVersion >= 5) {
    const ptResult = reader.readCompactString()
    if (!ptResult.ok) {
      return ptResult
    }
    protocolType = ptResult.value
  }

  // protocol_name (v5+, nullable compact string)
  let protocolName: string | null = null
  if (apiVersion >= 5) {
    const pnResult = reader.readCompactString()
    if (!pnResult.ok) {
      return pnResult
    }
    protocolName = pnResult.value
  }

  // assignment (bytes)
  const assignmentResult = isFlexible ? reader.readCompactBytes() : reader.readBytes()
  if (!assignmentResult.ok) {
    return assignmentResult
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
      protocolType,
      protocolName,
      assignment: assignmentResult.value ?? new Uint8Array(0),
      taggedFields
    },
    reader.offset - startOffset
  )
}
