/**
 * IncrementalAlterConfigs request/response encoding and decoding.
 *
 * The IncrementalAlterConfigs API (key 44) incrementally updates configuration
 * entries for specified resources (topics, brokers). Unlike AlterConfigs, only
 * the specified keys are modified — omitted keys retain their current values.
 *
 * **Request versions:**
 * - v0: non-flexible encoding
 * - v1: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: throttle_time_ms, per-resource error code + error message
 * - v1: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_IncrementalAlterConfigs
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
// Config operation
// ---------------------------------------------------------------------------

/**
 * The type of operation to perform on a configuration entry.
 *
 * - `0` (SET): Set the value of the configuration entry.
 * - `1` (DELETE): Revert the configuration entry to the default value.
 * - `2` (APPEND): Append to the value of the configuration entry (list types).
 * - `3` (SUBTRACT): Remove from the value of the configuration entry (list types).
 */
export const AlterConfigOp = {
  Set: 0,
  Delete: 1,
  Append: 2,
  Subtract: 3
} as const

export type AlterConfigOp = (typeof AlterConfigOp)[keyof typeof AlterConfigOp]

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * A configuration entry to alter incrementally.
 */
export type IncrementalAlterConfigsEntry = {
  /** The configuration key. */
  readonly name: string
  /** The type of operation to perform. */
  readonly configOperation: AlterConfigOp
  /** The configuration value. Null to use the default or delete. */
  readonly value: string | null
  /** Tagged fields (v1+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A resource whose configuration to alter.
 */
export type IncrementalAlterConfigsResource = {
  /** The resource type. */
  readonly resourceType: number
  /** The resource name (topic name or broker ID string). */
  readonly resourceName: string
  /** Configuration entries to alter. */
  readonly configs: readonly IncrementalAlterConfigsEntry[]
  /** Tagged fields (v1+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * IncrementalAlterConfigs request payload.
 */
export type IncrementalAlterConfigsRequest = {
  /** Resources to alter. */
  readonly resources: readonly IncrementalAlterConfigsResource[]
  /** If true, validate the request without applying changes. */
  readonly validateOnly?: boolean
  /** Tagged fields (v1+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-resource result from an IncrementalAlterConfigs response.
 */
export type IncrementalAlterConfigsResourceResponse = {
  /** Error code (0 = success). */
  readonly errorCode: number
  /** Error message. Null if no error. */
  readonly errorMessage: string | null
  /** The resource type. */
  readonly resourceType: number
  /** The resource name. */
  readonly resourceName: string
  /** Tagged fields (v1+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * IncrementalAlterConfigs response payload.
 */
export type IncrementalAlterConfigsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-resource results. */
  readonly responses: readonly IncrementalAlterConfigsResourceResponse[]
  /** Tagged fields (v1+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an IncrementalAlterConfigs request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–1).
 */
export function encodeIncrementalAlterConfigsRequest(
  writer: BinaryWriter,
  request: IncrementalAlterConfigsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 1

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

      // config_operation (INT8)
      writer.writeInt8(config.configOperation)

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

  // Tagged fields (v1+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed IncrementalAlterConfigs request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–1).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildIncrementalAlterConfigsRequest(
  correlationId: number,
  apiVersion: number,
  request: IncrementalAlterConfigsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.IncrementalAlterConfigs,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeIncrementalAlterConfigsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an IncrementalAlterConfigs response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–1).
 * @returns The decoded response or a failure.
 */
export function decodeIncrementalAlterConfigsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<IncrementalAlterConfigsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 1

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }
  const throttleTimeMs = throttleResult.value

  // Responses array
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

  const responses: IncrementalAlterConfigsResourceResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    // error_code (INT16)
    const errorCodeResult = reader.readInt16()
    if (!errorCodeResult.ok) {
      return errorCodeResult
    }

    // error_message (NULLABLE_STRING / COMPACT_NULLABLE_STRING)
    const errMsgResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!errMsgResult.ok) {
      return errMsgResult
    }

    // resource_type (INT8)
    const resourceTypeResult = reader.readInt8()
    if (!resourceTypeResult.ok) {
      return resourceTypeResult
    }

    // resource_name (STRING / COMPACT_STRING)
    const resourceNameResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!resourceNameResult.ok) {
      return resourceNameResult
    }

    // Tagged fields (v1+)
    let taggedFields: readonly TaggedField[] = []
    if (isFlexible) {
      const tagResult = reader.readTaggedFields()
      if (!tagResult.ok) {
        return tagResult
      }
      taggedFields = tagResult.value
    }

    responses.push({
      errorCode: errorCodeResult.value,
      errorMessage: errMsgResult.value,
      resourceType: resourceTypeResult.value,
      resourceName: resourceNameResult.value ?? "",
      taggedFields
    })
  }

  // Tagged fields (v1+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess({ throttleTimeMs, responses, taggedFields }, reader.offset - startOffset)
}
