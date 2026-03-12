/**
 * DeleteGroups request/response encoding and decoding.
 *
 * The DeleteGroups API (key 42) deletes one or more consumer groups.
 * Groups must be empty (no active members) to be deleted.
 *
 * **Request versions:**
 * - v0: groups_names (array of group IDs)
 * - v2+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: per-group results with error codes
 * - v1+: throttle_time_ms
 * - v2+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DeleteGroups
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
 * DeleteGroups request payload.
 */
export type DeleteGroupsRequest = {
  /** The group IDs to delete. */
  readonly groupsNames: readonly string[]
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-group deletion result.
 */
export type DeleteGroupsResult = {
  /** The group ID. */
  readonly groupId: string
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DeleteGroups response payload.
 */
export type DeleteGroupsResponse = {
  /** Time the request was throttled in milliseconds (v1+). */
  readonly throttleTimeMs: number
  /** Per-group deletion results. */
  readonly results: readonly DeleteGroupsResult[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DeleteGroups request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–2).
 */
export function encodeDeleteGroupsRequest(
  writer: BinaryWriter,
  request: DeleteGroupsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // groups_names array
  const groups = request.groupsNames
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

  // Tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed DeleteGroups request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–2).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildDeleteGroupsRequest(
  correlationId: number,
  apiVersion: number,
  request: DeleteGroupsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DeleteGroups,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDeleteGroupsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DeleteGroups response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–2).
 * @returns The decoded response or a failure.
 */
export function decodeDeleteGroupsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DeleteGroupsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // throttle_time_ms (v1+)
  let throttleTimeMs = 0
  if (apiVersion >= 1) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  // results array
  const resultsResult = decodeDeleteGroupsResultArray(reader, isFlexible)
  if (!resultsResult.ok) {
    return resultsResult
  }

  // Tagged fields (v2+)
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
      results: resultsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeDeleteGroupsResultArray(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<DeleteGroupsResult[]> {
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

  const results: DeleteGroupsResult[] = []
  for (let i = 0; i < count; i++) {
    const resultEntry = decodeDeleteGroupsResultEntry(reader, isFlexible)
    if (!resultEntry.ok) {
      return resultEntry
    }
    results.push(resultEntry.value)
  }

  return decodeSuccess(results, reader.offset - startOffset)
}

function decodeDeleteGroupsResultEntry(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<DeleteGroupsResult> {
  const startOffset = reader.offset

  // group_id
  const groupIdResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!groupIdResult.ok) {
    return groupIdResult
  }

  // error_code
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // Tagged fields (v2+)
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
      errorCode: errorCodeResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}
