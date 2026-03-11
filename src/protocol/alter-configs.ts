/**
 * AlterConfigs request/response encoding and decoding.
 *
 * The AlterConfigs API (key 33) sets configuration entries for specified
 * resources (topics, brokers). This is a non-incremental set — all config
 * keys must be provided; omitted keys revert to their defaults.
 *
 * **Request versions:**
 * - v0–v1: non-flexible encoding
 * - v2+: flexible encoding (KIP-482)
 * - v1+: adds validate_only
 *
 * **Response versions:**
 * - v0: per-resource error code + error message
 * - v1+: adds throttle_time_ms
 * - v2+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_AlterConfigs
 *
 * @packageDocumentation
 */

import { ApiKey } from "../codec/api-keys.js"
import type { BinaryReader, TaggedField } from "../codec/binary-reader.js"
import type { BinaryWriter } from "../codec/binary-writer.js"
import { frameRequest, type RequestHeader } from "../codec/protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "../result.js"

// Re-export ConfigResourceType from describe-configs for convenience
export type { ConfigResourceType as ConfigResourceTypeValue } from "./describe-configs.js"
export { ConfigResourceType } from "./describe-configs.js"

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * A configuration entry to set.
 */
export type AlterConfigsEntry = {
  /** The configuration key. */
  readonly name: string
  /** The configuration value. Null to reset to default. */
  readonly value: string | null
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A resource whose configuration to alter.
 */
export type AlterConfigsResource = {
  /** The resource type. */
  readonly resourceType: number
  /** The resource name (topic name or broker ID string). */
  readonly resourceName: string
  /** Configuration entries to set. */
  readonly configs: readonly AlterConfigsEntry[]
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * AlterConfigs request payload.
 */
export type AlterConfigsRequest = {
  /** Resources to alter. */
  readonly resources: readonly AlterConfigsResource[]
  /** If true, validate the request without applying changes (v1+). */
  readonly validateOnly?: boolean
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-resource result from an AlterConfigs response.
 */
export type AlterConfigsResourceResponse = {
  /** Error code (0 = success). */
  readonly errorCode: number
  /** Error message. Null if no error. */
  readonly errorMessage: string | null
  /** The resource type. */
  readonly resourceType: number
  /** The resource name. */
  readonly resourceName: string
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * AlterConfigs response payload.
 */
export type AlterConfigsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-resource results. */
  readonly resources: readonly AlterConfigsResourceResponse[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an AlterConfigs request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–2).
 */
export function encodeAlterConfigsRequest(
  writer: BinaryWriter,
  request: AlterConfigsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // Resources array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.resources.length + 1)
  } else {
    writer.writeInt32(request.resources.length)
  }

  for (const resource of request.resources) {
    // resource_type (INT8)
    writer.writeInt8(resource.resourceType)

    // resource_name
    if (isFlexible) {
      writer.writeCompactString(resource.resourceName)
    } else {
      writer.writeString(resource.resourceName)
    }

    // configs array
    if (isFlexible) {
      writer.writeUnsignedVarInt(resource.configs.length + 1)
    } else {
      writer.writeInt32(resource.configs.length)
    }

    for (const config of resource.configs) {
      // name
      if (isFlexible) {
        writer.writeCompactString(config.name)
      } else {
        writer.writeString(config.name)
      }

      // value (nullable)
      if (isFlexible) {
        writer.writeCompactString(config.value)
      } else {
        writer.writeString(config.value)
      }

      if (isFlexible) {
        writer.writeTaggedFields(config.taggedFields ?? [])
      }
    }

    if (isFlexible) {
      writer.writeTaggedFields(resource.taggedFields ?? [])
    }
  }

  // validate_only (BOOLEAN)
  writer.writeBoolean(request.validateOnly ?? false)

  // Tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed AlterConfigs request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–2).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildAlterConfigsRequest(
  correlationId: number,
  apiVersion: number,
  request: AlterConfigsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.AlterConfigs,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeAlterConfigsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an AlterConfigs response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–2).
 * @returns The decoded response or a failure.
 */
export function decodeAlterConfigsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<AlterConfigsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }
  const throttleTimeMs = throttleResult.value

  // Resources array
  let arrayLength: number
  if (isFlexible) {
    const lengthResult = reader.readUnsignedVarInt()
    if (!lengthResult.ok) {
      return lengthResult
    }
    arrayLength = lengthResult.value - 1
  } else {
    const lengthResult = reader.readInt32()
    if (!lengthResult.ok) {
      return lengthResult
    }
    arrayLength = lengthResult.value
  }

  const resources: AlterConfigsResourceResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    // error_code
    const errorCodeResult = reader.readInt16()
    if (!errorCodeResult.ok) {
      return errorCodeResult
    }

    // error_message
    const errMsgResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!errMsgResult.ok) {
      return errMsgResult
    }

    // resource_type
    const resourceTypeResult = reader.readInt8()
    if (!resourceTypeResult.ok) {
      return resourceTypeResult
    }

    // resource_name
    const resourceNameResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!resourceNameResult.ok) {
      return resourceNameResult
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

    resources.push({
      errorCode: errorCodeResult.value,
      errorMessage: errMsgResult.value,
      resourceType: resourceTypeResult.value,
      resourceName: resourceNameResult.value ?? "",
      taggedFields
    })
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

  return decodeSuccess({ throttleTimeMs, resources, taggedFields }, reader.offset - startOffset)
}
