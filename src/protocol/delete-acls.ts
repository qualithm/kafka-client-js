/**
 * DeleteAcls request/response encoding and decoding.
 *
 * The DeleteAcls API (key 31) deletes ACL bindings matching the
 * specified filters.
 *
 * **Request versions:**
 * - v0: array of ACL filters (resource type, name, principal, host, operation, permission)
 * - v1+: adds resource pattern type filter
 * - v2+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: per-filter results with matching ACLs
 * - v1+: resource pattern type in matched ACLs
 * - v2+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DeleteAcls
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
 * A single ACL filter for deletion.
 */
export type DeleteAclsFilter = {
  /** The resource type filter. */
  readonly resourceTypeFilter: number
  /** The resource name filter. Null matches any. */
  readonly resourceNameFilter: string | null
  /** The resource pattern type filter (v1+). */
  readonly patternTypeFilter?: number
  /** The principal filter. Null matches any. */
  readonly principalFilter: string | null
  /** The host filter. Null matches any. */
  readonly hostFilter: string | null
  /** The operation filter. */
  readonly operation: number
  /** The permission type filter. */
  readonly permissionType: number
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * DeleteAcls request payload.
 */
export type DeleteAclsRequest = {
  /** The ACL filters. */
  readonly filters: readonly DeleteAclsFilter[]
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A single ACL binding that was deleted.
 */
export type DeleteAclsMatchingAcl = {
  /** Error code for this individual match (0 = no error). */
  readonly errorCode: number
  /** Error message for this individual match. */
  readonly errorMessage: string | null
  /** The resource type. */
  readonly resourceType: number
  /** The resource name. */
  readonly resourceName: string
  /** The resource pattern type (v1+, default LITERAL=3). */
  readonly patternType: number
  /** The principal. */
  readonly principal: string
  /** The host. */
  readonly host: string
  /** The operation. */
  readonly operation: number
  /** The permission type. */
  readonly permissionType: number
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-filter deletion result.
 */
export type DeleteAclsFilterResult = {
  /** Error code for this filter (0 = no error). */
  readonly errorCode: number
  /** Error message for this filter. */
  readonly errorMessage: string | null
  /** ACL bindings that matched this filter and were deleted. */
  readonly matchingAcls: readonly DeleteAclsMatchingAcl[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DeleteAcls response payload.
 */
export type DeleteAclsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-filter results. */
  readonly filterResults: readonly DeleteAclsFilterResult[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DeleteAcls request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–3).
 */
export function encodeDeleteAclsRequest(
  writer: BinaryWriter,
  request: DeleteAclsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // filters array
  const { filters } = request
  if (isFlexible) {
    writer.writeUnsignedVarInt(filters.length + 1)
  } else {
    writer.writeInt32(filters.length)
  }

  for (const filter of filters) {
    // resource_type_filter (INT8)
    writer.writeInt8(filter.resourceTypeFilter)

    // resource_name_filter (nullable string)
    if (isFlexible) {
      writer.writeCompactString(filter.resourceNameFilter)
    } else {
      writer.writeString(filter.resourceNameFilter)
    }

    // pattern_type_filter (INT8, v1+)
    if (apiVersion >= 1) {
      writer.writeInt8(filter.patternTypeFilter ?? AclResourcePatternType.Literal)
    }

    // principal_filter (nullable string)
    if (isFlexible) {
      writer.writeCompactString(filter.principalFilter)
    } else {
      writer.writeString(filter.principalFilter)
    }

    // host_filter (nullable string)
    if (isFlexible) {
      writer.writeCompactString(filter.hostFilter)
    } else {
      writer.writeString(filter.hostFilter)
    }

    // operation (INT8)
    writer.writeInt8(filter.operation)

    // permission_type (INT8)
    writer.writeInt8(filter.permissionType)

    // Tagged fields (v2+)
    if (isFlexible) {
      writer.writeTaggedFields(filter.taggedFields ?? [])
    }
  }

  // Tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed DeleteAcls request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–3).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildDeleteAclsRequest(
  correlationId: number,
  apiVersion: number,
  request: DeleteAclsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DeleteAcls,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDeleteAclsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DeleteAcls response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–3).
 * @returns The decoded response or a failure.
 */
export function decodeDeleteAclsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DeleteAclsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // filter_results array
  const filterResultsResult = decodeDeleteAclsFilterResultArray(reader, apiVersion, isFlexible)
  if (!filterResultsResult.ok) {
    return filterResultsResult
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
      filterResults: filterResultsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeDeleteAclsFilterResultArray(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DeleteAclsFilterResult[]> {
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

  const results: DeleteAclsFilterResult[] = []
  for (let i = 0; i < count; i++) {
    const result = decodeDeleteAclsFilterResultEntry(reader, apiVersion, isFlexible)
    if (!result.ok) {
      return result
    }
    results.push(result.value)
  }

  return decodeSuccess(results, reader.offset - startOffset)
}

function decodeDeleteAclsFilterResultEntry(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DeleteAclsFilterResult> {
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

  // matching_acls array
  const matchingAclsResult = decodeMatchingAclArray(reader, apiVersion, isFlexible)
  if (!matchingAclsResult.ok) {
    return matchingAclsResult
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
      matchingAcls: matchingAclsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeMatchingAclArray(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DeleteAclsMatchingAcl[]> {
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

  const acls: DeleteAclsMatchingAcl[] = []
  for (let i = 0; i < count; i++) {
    const aclResult = decodeMatchingAcl(reader, apiVersion, isFlexible)
    if (!aclResult.ok) {
      return aclResult
    }
    acls.push(aclResult.value)
  }

  return decodeSuccess(acls, reader.offset - startOffset)
}

function decodeMatchingAcl(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DeleteAclsMatchingAcl> {
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

  // resource_type (INT8)
  const resourceTypeResult = reader.readInt8()
  if (!resourceTypeResult.ok) {
    return resourceTypeResult
  }

  // resource_name
  const resourceNameResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!resourceNameResult.ok) {
    return resourceNameResult
  }

  // pattern_type (INT8, v1+)
  let patternType: number = AclResourcePatternType.Literal
  if (apiVersion >= 1) {
    const patternResult = reader.readInt8()
    if (!patternResult.ok) {
      return patternResult
    }
    patternType = patternResult.value
  }

  // principal
  const principalResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!principalResult.ok) {
    return principalResult
  }

  // host
  const hostResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!hostResult.ok) {
    return hostResult
  }

  // operation (INT8)
  const operationResult = reader.readInt8()
  if (!operationResult.ok) {
    return operationResult
  }

  // permission_type (INT8)
  const permissionTypeResult = reader.readInt8()
  if (!permissionTypeResult.ok) {
    return permissionTypeResult
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
      resourceType: resourceTypeResult.value,
      resourceName: resourceNameResult.value ?? "",
      patternType,
      principal: principalResult.value ?? "",
      host: hostResult.value ?? "",
      operation: operationResult.value,
      permissionType: permissionTypeResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}
