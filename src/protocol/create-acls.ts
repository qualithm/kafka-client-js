/**
 * CreateAcls request/response encoding and decoding.
 *
 * The CreateAcls API (key 30) creates one or more ACL bindings.
 *
 * **Request versions:**
 * - v0: array of ACL creations (resource type, name, principal, host, operation, permission)
 * - v1+: adds resource pattern type
 * - v2+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: per-creation results with error codes
 * - v2+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_CreateAcls
 *
 * @packageDocumentation
 */

import { ApiKey } from "../codec/api-keys.js"
import type { BinaryReader, TaggedField } from "../codec/binary-reader.js"
import type { BinaryWriter } from "../codec/binary-writer.js"
import { frameRequest, type RequestHeader } from "../codec/protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "../result.js"
import { AclResourcePatternType } from "./describe-acls.js"

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * A single ACL creation entry.
 */
export type AclCreation = {
  /** The resource type. */
  readonly resourceType: number
  /** The resource name. */
  readonly resourceName: string
  /** The resource pattern type (v1+, default LITERAL=3). */
  readonly resourcePatternType?: number
  /** The principal. */
  readonly principal: string
  /** The host. */
  readonly host: string
  /** The operation. */
  readonly operation: number
  /** The permission type. */
  readonly permissionType: number
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * CreateAcls request payload.
 */
export type CreateAclsRequest = {
  /** The ACL creations. */
  readonly creations: readonly AclCreation[]
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-creation result.
 */
export type AclCreationResult = {
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Error message. Null if no error. */
  readonly errorMessage: string | null
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * CreateAcls response payload.
 */
export type CreateAclsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-creation results. */
  readonly results: readonly AclCreationResult[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a CreateAcls request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–3).
 */
export function encodeCreateAclsRequest(
  writer: BinaryWriter,
  request: CreateAclsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // creations array
  const { creations } = request
  if (isFlexible) {
    writer.writeUnsignedVarInt(creations.length + 1)
  } else {
    writer.writeInt32(creations.length)
  }

  for (const creation of creations) {
    // resource_type (INT8)
    writer.writeInt8(creation.resourceType)

    // resource_name
    if (isFlexible) {
      writer.writeCompactString(creation.resourceName)
    } else {
      writer.writeString(creation.resourceName)
    }

    // resource_pattern_type (INT8, v1+)
    if (apiVersion >= 1) {
      writer.writeInt8(creation.resourcePatternType ?? AclResourcePatternType.Literal)
    }

    // principal
    if (isFlexible) {
      writer.writeCompactString(creation.principal)
    } else {
      writer.writeString(creation.principal)
    }

    // host
    if (isFlexible) {
      writer.writeCompactString(creation.host)
    } else {
      writer.writeString(creation.host)
    }

    // operation (INT8)
    writer.writeInt8(creation.operation)

    // permission_type (INT8)
    writer.writeInt8(creation.permissionType)

    // Tagged fields (v2+)
    if (isFlexible) {
      writer.writeTaggedFields(creation.taggedFields ?? [])
    }
  }

  // Tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed CreateAcls request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–3).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildCreateAclsRequest(
  correlationId: number,
  apiVersion: number,
  request: CreateAclsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.CreateAcls,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeCreateAclsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a CreateAcls response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–3).
 * @returns The decoded response or a failure.
 */
export function decodeCreateAclsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<CreateAclsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // results array
  const resultsResult = decodeAclCreationResultArray(reader, isFlexible)
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
      throttleTimeMs: throttleResult.value,
      results: resultsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeAclCreationResultArray(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<AclCreationResult[]> {
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

  const results: AclCreationResult[] = []
  for (let i = 0; i < count; i++) {
    const resultEntry = decodeAclCreationResultEntry(reader, isFlexible)
    if (!resultEntry.ok) {
      return resultEntry
    }
    results.push(resultEntry.value)
  }

  return decodeSuccess(results, reader.offset - startOffset)
}

function decodeAclCreationResultEntry(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<AclCreationResult> {
  const startOffset = reader.offset

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // error_message (nullable string)
  const errorMsgResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!errorMsgResult.ok) {
    return errorMsgResult
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
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}
