/**
 * DescribeAcls request/response encoding and decoding.
 *
 * The DescribeAcls API (key 29) returns ACL bindings matching the
 * specified filter criteria.
 *
 * **Request versions:**
 * - v0: resource type, name, pattern type, principal, host, operation, permission type
 * - v1+: adds resource pattern type filter (MATCH, LITERAL, PREFIXED, ANY)
 * - v2+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: per-resource ACL bindings
 * - v1+: resource pattern type in results
 * - v2+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeAcls
 *
 * @packageDocumentation
 */

import { ApiKey } from "../codec/api-keys.js"
import type { BinaryReader, TaggedField } from "../codec/binary-reader.js"
import type { BinaryWriter } from "../codec/binary-writer.js"
import { frameRequest, type RequestHeader } from "../codec/protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "../result.js"

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/**
 * ACL resource types.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeAcls
 */
export const AclResourceType = {
  /** Unknown resource type. */
  Unknown: 0,
  /** Matches any resource type (filter only). */
  Any: 1,
  /** Topic resource. */
  Topic: 2,
  /** Consumer group resource. */
  Group: 3,
  /** Cluster resource. */
  Cluster: 4,
  /** Transactional ID resource. */
  TransactionalId: 5,
  /** Delegation token resource. */
  DelegationToken: 6
} as const

export type AclResourceType = (typeof AclResourceType)[keyof typeof AclResourceType]

/**
 * ACL resource pattern types.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeAcls
 */
export const AclResourcePatternType = {
  /** Unknown pattern type. */
  Unknown: 0,
  /** Matches any pattern type (filter only). */
  Any: 1,
  /** Matches resources with an exact name match. */
  Match: 2,
  /** Literal name match. */
  Literal: 3,
  /** Prefix name match. */
  Prefixed: 4
} as const

export type AclResourcePatternType =
  (typeof AclResourcePatternType)[keyof typeof AclResourcePatternType]

/**
 * ACL operations.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeAcls
 */
export const AclOperation = {
  /** Unknown operation. */
  Unknown: 0,
  /** Matches any operation (filter only). */
  Any: 1,
  /** All operations. */
  All: 2,
  /** Read operation. */
  Read: 3,
  /** Write operation. */
  Write: 4,
  /** Create operation. */
  Create: 5,
  /** Delete operation. */
  Delete: 6,
  /** Alter operation. */
  Alter: 7,
  /** Describe operation. */
  Describe: 8,
  /** ClusterAction operation. */
  ClusterAction: 9,
  /** DescribeConfigs operation. */
  DescribeConfigs: 10,
  /** AlterConfigs operation. */
  AlterConfigs: 11,
  /** IdempotentWrite operation. */
  IdempotentWrite: 12,
  /** CreateTokens operation. */
  CreateTokens: 13,
  /** DescribeTokens operation. */
  DescribeTokens: 14
} as const

export type AclOperation = (typeof AclOperation)[keyof typeof AclOperation]

/**
 * ACL permission types.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeAcls
 */
export const AclPermissionType = {
  /** Unknown permission type. */
  Unknown: 0,
  /** Matches any permission type (filter only). */
  Any: 1,
  /** Deny permission. */
  Deny: 2,
  /** Allow permission. */
  Allow: 3
} as const

export type AclPermissionType = (typeof AclPermissionType)[keyof typeof AclPermissionType]

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * DescribeAcls request payload — an ACL filter.
 */
export type DescribeAclsRequest = {
  /** The resource type filter. */
  readonly resourceTypeFilter: AclResourceType
  /** The resource name filter. Null matches any. */
  readonly resourceNameFilter: string | null
  /** The resource pattern type filter (v1+). */
  readonly patternTypeFilter?: AclResourcePatternType
  /** The principal filter. Null matches any. */
  readonly principalFilter: string | null
  /** The host filter. Null matches any. */
  readonly hostFilter: string | null
  /** The operation filter. */
  readonly operation: AclOperation
  /** The permission type filter. */
  readonly permissionType: AclPermissionType
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A single ACL entry within a resource.
 */
export type AclDescription = {
  /** The ACL principal. */
  readonly principal: string
  /** The ACL host. */
  readonly host: string
  /** The ACL operation. */
  readonly operation: number
  /** The ACL permission type. */
  readonly permissionType: number
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * A resource with its ACL entries.
 */
export type DescribeAclsResource = {
  /** The resource type. */
  readonly resourceType: number
  /** The resource name. */
  readonly resourceName: string
  /** The resource pattern type (v1+, default LITERAL=3). */
  readonly patternType: number
  /** The ACL entries for this resource. */
  readonly acls: readonly AclDescription[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeAcls response payload.
 */
export type DescribeAclsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Top-level error code (0 = no error). */
  readonly errorCode: number
  /** Error message. Null if no error. */
  readonly errorMessage: string | null
  /** Matching ACL resources. */
  readonly resources: readonly DescribeAclsResource[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeAcls request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–3).
 */
export function encodeDescribeAclsRequest(
  writer: BinaryWriter,
  request: DescribeAclsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // resource_type_filter (INT8)
  writer.writeInt8(request.resourceTypeFilter)

  // resource_name_filter (nullable string)
  if (isFlexible) {
    writer.writeCompactString(request.resourceNameFilter)
  } else {
    writer.writeString(request.resourceNameFilter)
  }

  // pattern_type_filter (INT8, v1+)
  if (apiVersion >= 1) {
    writer.writeInt8(request.patternTypeFilter ?? AclResourcePatternType.Literal)
  }

  // principal_filter (nullable string)
  if (isFlexible) {
    writer.writeCompactString(request.principalFilter)
  } else {
    writer.writeString(request.principalFilter)
  }

  // host_filter (nullable string)
  if (isFlexible) {
    writer.writeCompactString(request.hostFilter)
  } else {
    writer.writeString(request.hostFilter)
  }

  // operation (INT8)
  writer.writeInt8(request.operation)

  // permission_type (INT8)
  writer.writeInt8(request.permissionType)

  // Tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed DescribeAcls request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–3).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildDescribeAclsRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeAclsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeAcls,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeAclsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeAcls response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–3).
 * @returns The decoded response or a failure.
 */
export function decodeDescribeAclsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeAclsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

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

  // resources array
  const resourcesResult = decodeDescribeAclsResourceArray(reader, apiVersion, isFlexible)
  if (!resourcesResult.ok) {
    return resourcesResult
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
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      resources: resourcesResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeDescribeAclsResourceArray(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribeAclsResource[]> {
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

  const resources: DescribeAclsResource[] = []
  for (let i = 0; i < count; i++) {
    const resourceResult = decodeDescribeAclsResourceEntry(reader, apiVersion, isFlexible)
    if (!resourceResult.ok) {
      return resourceResult
    }
    resources.push(resourceResult.value)
  }

  return decodeSuccess(resources, reader.offset - startOffset)
}

function decodeDescribeAclsResourceEntry(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribeAclsResource> {
  const startOffset = reader.offset

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

  // acls array
  const aclsResult = decodeAclDescriptionArray(reader, isFlexible)
  if (!aclsResult.ok) {
    return aclsResult
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
      resourceType: resourceTypeResult.value,
      resourceName: resourceNameResult.value ?? "",
      patternType,
      acls: aclsResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeAclDescriptionArray(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<AclDescription[]> {
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

  const acls: AclDescription[] = []
  for (let i = 0; i < count; i++) {
    const aclResult = decodeAclDescription(reader, isFlexible)
    if (!aclResult.ok) {
      return aclResult
    }
    acls.push(aclResult.value)
  }

  return decodeSuccess(acls, reader.offset - startOffset)
}

function decodeAclDescription(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<AclDescription> {
  const startOffset = reader.offset

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
      principal: principalResult.value ?? "",
      host: hostResult.value ?? "",
      operation: operationResult.value,
      permissionType: permissionTypeResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}
