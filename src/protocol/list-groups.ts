/**
 * ListGroups request/response encoding and decoding.
 *
 * The ListGroups API (key 16) lists all consumer groups known to the broker.
 * Optionally filters by group state (v4+).
 *
 * **Request versions:**
 * - v0–v2: empty request body
 * - v3+: flexible encoding (KIP-482)
 * - v4: adds states_filter
 *
 * **Response versions:**
 * - v0: error_code, groups[]
 * - v1+: throttle_time_ms
 * - v3+: flexible encoding
 * - v4+: group_state per group
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_ListGroups
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
 * ListGroups request payload.
 */
export type ListGroupsRequest = {
  /** Filter by group states (v4+). Empty means no filter. */
  readonly statesFilter?: readonly string[]
  /** Tagged fields (v3+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A listed group.
 */
export type ListGroupsGroup = {
  /** The group ID. */
  readonly groupId: string
  /** The protocol type (e.g. "consumer"). */
  readonly protocolType: string
  /** The group state (v4+, e.g. "Stable", "Empty"). */
  readonly groupState: string
  /** Tagged fields (v3+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * ListGroups response payload.
 */
export type ListGroupsResponse = {
  /** Time the request was throttled in milliseconds (v1+). */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Listed groups. */
  readonly groups: readonly ListGroupsGroup[]
  /** Tagged fields (v3+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a ListGroups request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–4).
 */
export function encodeListGroupsRequest(
  writer: BinaryWriter,
  request: ListGroupsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 3

  // states_filter (v4+)
  if (apiVersion >= 4) {
    const states = request.statesFilter ?? []
    if (isFlexible) {
      writer.writeUnsignedVarInt(states.length + 1)
    } else {
      writer.writeInt32(states.length)
    }
    for (const state of states) {
      if (isFlexible) {
        writer.writeCompactString(state)
      } else {
        writer.writeString(state)
      }
    }
  }

  // Tagged fields (v3+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed ListGroups request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–4).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildListGroupsRequest(
  correlationId: number,
  apiVersion: number,
  request: ListGroupsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.ListGroups,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeListGroupsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a ListGroups response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–4).
 * @returns The decoded response or a failure.
 */
export function decodeListGroupsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<ListGroupsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 3

  // throttle_time_ms (v1+)
  let throttleTimeMs = 0
  if (apiVersion >= 1) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  // error_code
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // groups array
  const groupsResult = decodeListGroupsGroupArray(reader, apiVersion, isFlexible)
  if (!groupsResult.ok) {
    return groupsResult
  }

  // Tagged fields (v3+)
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
      groups: groupsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeListGroupsGroupArray(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<ListGroupsGroup[]> {
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

  const groups: ListGroupsGroup[] = []
  for (let i = 0; i < count; i++) {
    const groupResult = decodeListGroupsGroupEntry(reader, apiVersion, isFlexible)
    if (!groupResult.ok) {
      return groupResult
    }
    groups.push(groupResult.value)
  }

  return decodeSuccess(groups, reader.offset - startOffset)
}

function decodeListGroupsGroupEntry(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<ListGroupsGroup> {
  const startOffset = reader.offset

  // group_id
  const groupIdResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!groupIdResult.ok) {
    return groupIdResult
  }

  // protocol_type
  const protocolTypeResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!protocolTypeResult.ok) {
    return protocolTypeResult
  }

  // group_state (v4+)
  let groupState = ""
  if (apiVersion >= 4) {
    const stateResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!stateResult.ok) {
      return stateResult
    }
    groupState = stateResult.value ?? ""
  }

  // Tagged fields (v3+)
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
      groupId: groupIdResult.value ?? "",
      protocolType: protocolTypeResult.value ?? "",
      groupState,
      taggedFields
    },
    reader.offset - startOffset
  )
}
