/**
 * Heartbeat request/response encoding and decoding.
 *
 * The Heartbeat API (key 12) is used by consumer group members to keep
 * their session alive. Members send periodic heartbeats to the group
 * coordinator, which responds with the current group state. If the
 * coordinator returns REBALANCE_IN_PROGRESS, the member must rejoin.
 *
 * **Request versions:**
 * - v0: group_id, generation_id, member_id
 * - v3: adds group_instance_id (KIP-345)
 * - v4+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: error_code
 * - v1+: throttle_time_ms
 * - v4+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_Heartbeat
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
 * Heartbeat request payload.
 */
export type HeartbeatRequest = {
  /** The group ID. */
  readonly groupId: string
  /** The generation ID from the most recent JoinGroup response. */
  readonly generationId: number
  /** This member's member ID. */
  readonly memberId: string
  /** The group instance ID for static membership (v3+). Null if not static. */
  readonly groupInstanceId?: string | null
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Heartbeat response payload.
 */
export type HeartbeatResponse = {
  /** Time the request was throttled in milliseconds (v1+). */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a Heartbeat request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–4).
 */
export function encodeHeartbeatRequest(
  writer: BinaryWriter,
  request: HeartbeatRequest,
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

  // Tagged fields (v4+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed Heartbeat request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–4).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildHeartbeatRequest(
  correlationId: number,
  apiVersion: number,
  request: HeartbeatRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.Heartbeat,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeHeartbeatRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a Heartbeat response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–4).
 * @returns The decoded response or a failure.
 */
export function decodeHeartbeatResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<HeartbeatResponse> {
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
      taggedFields
    },
    reader.offset - startOffset
  )
}
