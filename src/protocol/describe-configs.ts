/**
 * DescribeConfigs request/response encoding and decoding.
 *
 * The DescribeConfigs API (key 32) returns configuration entries for
 * specified resources (topics, brokers, etc.).
 *
 * **Request versions:**
 * - v0–v3: non-flexible encoding
 * - v4+: flexible encoding (KIP-482)
 * - v1+: adds include_synonyms
 * - v3+: adds include_documentation
 *
 * **Response versions:**
 * - v0: resource configs with read_only and is_sensitive
 * - v1+: adds config_source, synonyms
 * - v3+: adds config_documentation
 * - v4+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeConfigs
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
 * Resource types for DescribeConfigs/AlterConfigs.
 */
export const ConfigResourceType = {
  /** Unknown / invalid. */
  Unknown: 0,
  /** Topic resource. */
  Topic: 2,
  /** Broker resource. */
  Broker: 4,
  /** Broker logger resource. */
  BrokerLogger: 8
} as const

export type ConfigResourceType = (typeof ConfigResourceType)[keyof typeof ConfigResourceType]

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * A resource to describe configuration for.
 */
export type DescribeConfigsResource = {
  /** The resource type. */
  readonly resourceType: ConfigResourceType
  /** The resource name (topic name or broker ID string). */
  readonly resourceName: string
  /** Specific config keys to describe. Null to describe all. */
  readonly configNames: readonly string[] | null
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * DescribeConfigs request payload.
 */
export type DescribeConfigsRequest = {
  /** Resources to describe. */
  readonly resources: readonly DescribeConfigsResource[]
  /** Whether to include config synonyms (v1+). */
  readonly includeSynonyms?: boolean
  /** Whether to include config documentation (v3+). */
  readonly includeDocumentation?: boolean
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A config synonym entry (v1+).
 */
export type ConfigSynonym = {
  /** The synonym name. */
  readonly name: string
  /** The synonym value. */
  readonly value: string | null
  /**
   * The config source.
   * 1=DYNAMIC_TOPIC, 2=DYNAMIC_BROKER, 4=STATIC_BROKER, 5=DEFAULT, 6=DYNAMIC_DEFAULT_BROKER.
   */
  readonly source: number
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * A single configuration entry in a DescribeConfigs response.
 */
export type DescribeConfigsEntry = {
  /** The configuration key. */
  readonly name: string
  /** The configuration value. Null if sensitive and hidden. */
  readonly value: string | null
  /** Whether this is a read-only config. */
  readonly readOnly: boolean
  /** Whether this is the default value (v0 only — use configSource in v1+). */
  readonly isDefault: boolean
  /** Whether this is a sensitive config (value hidden). */
  readonly isSensitive: boolean
  /**
   * The config source (v1+).
   * -1 if not available.
   */
  readonly configSource: number
  /** Config synonyms (v1+). */
  readonly synonyms: readonly ConfigSynonym[]
  /** Config data type (v3+). 1=BOOLEAN, 2=STRING, 3=INT, 4=SHORT, 5=LONG, 6=DOUBLE, 7=LIST, 8=CLASS, 9=PASSWORD. */
  readonly configType: number
  /** Config documentation (v3+). */
  readonly documentation: string | null
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-resource result from a DescribeConfigs response.
 */
export type DescribeConfigsResourceResponse = {
  /** Error code (0 = success). */
  readonly errorCode: number
  /** Error message. Null if no error. */
  readonly errorMessage: string | null
  /** The resource type. */
  readonly resourceType: number
  /** The resource name. */
  readonly resourceName: string
  /** Configuration entries for this resource. */
  readonly configs: readonly DescribeConfigsEntry[]
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeConfigs response payload.
 */
export type DescribeConfigsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-resource results. */
  readonly resources: readonly DescribeConfigsResourceResponse[]
  /** Tagged fields (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeConfigs request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–4).
 */
export function encodeDescribeConfigsRequest(
  writer: BinaryWriter,
  request: DescribeConfigsRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 4

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

    // config_names (nullable array)
    if (resource.configNames === null) {
      if (isFlexible) {
        writer.writeUnsignedVarInt(0) // null compact array
      } else {
        writer.writeInt32(-1)
      }
    } else {
      if (isFlexible) {
        writer.writeUnsignedVarInt(resource.configNames.length + 1)
      } else {
        writer.writeInt32(resource.configNames.length)
      }
      for (const name of resource.configNames) {
        if (isFlexible) {
          writer.writeCompactString(name)
        } else {
          writer.writeString(name)
        }
      }
    }

    if (isFlexible) {
      writer.writeTaggedFields(resource.taggedFields ?? [])
    }
  }

  // include_synonyms (v1+)
  if (apiVersion >= 1) {
    writer.writeBoolean(request.includeSynonyms ?? false)
  }

  // include_documentation (v3+)
  if (apiVersion >= 3) {
    writer.writeBoolean(request.includeDocumentation ?? false)
  }

  // Tagged fields (v4+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed DescribeConfigs request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–4).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildDescribeConfigsRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeConfigsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeConfigs,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeConfigsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeConfigs response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–4).
 * @returns The decoded response or a failure.
 */
export function decodeDescribeConfigsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeConfigsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 4

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

  const resources: DescribeConfigsResourceResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    const resourceResult = decodeDescribeConfigsResource(reader, apiVersion, isFlexible)
    if (!resourceResult.ok) {
      return resourceResult
    }
    resources.push(resourceResult.value)
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

  return decodeSuccess({ throttleTimeMs, resources, taggedFields }, reader.offset - startOffset)
}

function decodeDescribeConfigsResource(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribeConfigsResourceResponse> {
  const startOffset = reader.offset

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

  // configs array
  let configsLength: number
  if (isFlexible) {
    const lengthResult = reader.readUnsignedVarInt()
    if (!lengthResult.ok) {
      return lengthResult
    }
    configsLength = lengthResult.value - 1
  } else {
    const lengthResult = reader.readInt32()
    if (!lengthResult.ok) {
      return lengthResult
    }
    configsLength = lengthResult.value
  }

  const configs: DescribeConfigsEntry[] = []
  for (let j = 0; j < configsLength; j++) {
    const configResult = decodeDescribeConfigsEntry(reader, apiVersion, isFlexible)
    if (!configResult.ok) {
      return configResult
    }
    configs.push(configResult.value)
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
      errorCode: errorCodeResult.value,
      errorMessage: errMsgResult.value,
      resourceType: resourceTypeResult.value,
      resourceName: resourceNameResult.value ?? "",
      configs,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeDescribeConfigsEntry(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribeConfigsEntry> {
  const startOffset = reader.offset

  // name
  const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!nameResult.ok) {
    return nameResult
  }

  // value
  const valueResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!valueResult.ok) {
    return valueResult
  }

  // read_only
  const readOnlyResult = reader.readBoolean()
  if (!readOnlyResult.ok) {
    return readOnlyResult
  }

  // is_default (v0 only — in v1+ the broker sends config_source instead)
  let isDefault = false
  if (apiVersion === 0) {
    const isDefaultResult = reader.readBoolean()
    if (!isDefaultResult.ok) {
      return isDefaultResult
    }
    isDefault = isDefaultResult.value
  }

  // config_source (v1+)
  let configSource = -1
  if (apiVersion >= 1) {
    const sourceResult = reader.readInt8()
    if (!sourceResult.ok) {
      return sourceResult
    }
    configSource = sourceResult.value
  }

  // is_sensitive
  const sensitiveResult = reader.readBoolean()
  if (!sensitiveResult.ok) {
    return sensitiveResult
  }

  // synonyms (v1+)
  const synonymsResult = decodeSynonymsArray(reader, apiVersion, isFlexible)
  if (!synonymsResult.ok) {
    return synonymsResult
  }
  const synonyms = synonymsResult.value

  // config_type (v3+)
  let configType = 0
  if (apiVersion >= 3) {
    const typeResult = reader.readInt8()
    if (!typeResult.ok) {
      return typeResult
    }
    configType = typeResult.value
  }

  // config_documentation (v3+)
  let documentation: string | null = null
  if (apiVersion >= 3) {
    const docResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!docResult.ok) {
      return docResult
    }
    documentation = docResult.value
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
      name: nameResult.value ?? "",
      value: valueResult.value,
      readOnly: readOnlyResult.value,
      isDefault,
      isSensitive: sensitiveResult.value,
      configSource,
      synonyms,
      configType,
      documentation,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeSynonymsArray(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<ConfigSynonym[]> {
  const startOffset = reader.offset

  if (apiVersion < 1) {
    return decodeSuccess([], 0)
  }

  let synonymsLength: number
  if (isFlexible) {
    const lengthResult = reader.readUnsignedVarInt()
    if (!lengthResult.ok) {
      return lengthResult
    }
    synonymsLength = lengthResult.value - 1
  } else {
    const lengthResult = reader.readInt32()
    if (!lengthResult.ok) {
      return lengthResult
    }
    synonymsLength = lengthResult.value
  }

  const synonyms: ConfigSynonym[] = []
  for (let k = 0; k < synonymsLength; k++) {
    const synResult = decodeConfigSynonym(reader, isFlexible)
    if (!synResult.ok) {
      return synResult
    }
    synonyms.push(synResult.value)
  }

  return decodeSuccess(synonyms, reader.offset - startOffset)
}

function decodeConfigSynonym(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<ConfigSynonym> {
  const startOffset = reader.offset

  // name
  const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!nameResult.ok) {
    return nameResult
  }

  // value
  const valueResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!valueResult.ok) {
    return valueResult
  }

  // source (INT8)
  const sourceResult = reader.readInt8()
  if (!sourceResult.ok) {
    return sourceResult
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
      name: nameResult.value ?? "",
      value: valueResult.value,
      source: sourceResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}
