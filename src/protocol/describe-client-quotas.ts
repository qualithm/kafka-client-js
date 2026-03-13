/**
 * DescribeClientQuotas request/response encoding and decoding.
 *
 * The DescribeClientQuotas API (key 48) returns quota configurations
 * matching the specified entity filters.
 *
 * **Request versions:**
 * - v0: non-flexible encoding
 * - v1: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: throttle, top-level error, nullable entries with entity/values
 * - v1: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeClientQuotas
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
 * Match type for quota entity component filters.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeClientQuotas
 */
export const QuotaMatchType = {
  /** Match the entity exactly by name. */
  Exact: 0,
  /** Match the default entity (name must be null). */
  Default: 1,
  /** Match any entity of this type (name must be null). */
  Any: 2
} as const

export type QuotaMatchType = (typeof QuotaMatchType)[keyof typeof QuotaMatchType]

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * A component filter for describing client quotas.
 */
export type DescribeClientQuotasComponent = {
  /** The entity type to match (e.g. "user", "client-id", "ip"). */
  readonly entityType: string
  /** The match type (EXACT, DEFAULT, or ANY). */
  readonly matchType: QuotaMatchType
  /** The entity name to match (null for DEFAULT or ANY match types). */
  readonly match: string | null
  /** Tagged fields (v1+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * DescribeClientQuotas request payload.
 */
export type DescribeClientQuotasRequest = {
  /** Component filters that must all match. */
  readonly components: readonly DescribeClientQuotasComponent[]
  /** If true, exact match only — do not include entries with extra entity types. */
  readonly strict: boolean
  /** Tagged fields (v1+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A quota entity component in a response entry.
 */
export type QuotaEntity = {
  /** The entity type (e.g. "user", "client-id", "ip"). */
  readonly entityType: string
  /** The entity name, or null for the default entity. */
  readonly entityName: string | null
  /** Tagged fields (v1+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * A quota value in a response entry.
 */
export type QuotaValue = {
  /** The quota configuration key. */
  readonly key: string
  /** The quota configuration value. */
  readonly value: number
  /** Tagged fields (v1+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * A single quota entry in the response.
 */
export type DescribeClientQuotasEntry = {
  /** The entity components for this quota entry. */
  readonly entity: readonly QuotaEntity[]
  /** The quota values for this entity. */
  readonly values: readonly QuotaValue[]
  /** Tagged fields (v1+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeClientQuotas response payload.
 */
export type DescribeClientQuotasResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Top-level error code (0 = no error). */
  readonly errorCode: number
  /** Top-level error message. Null if no error. */
  readonly errorMessage: string | null
  /** Matching quota entries, or null if there was an error. */
  readonly entries: readonly DescribeClientQuotasEntry[] | null
  /** Tagged fields (v1+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeClientQuotas request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–1).
 */
export function encodeDescribeClientQuotasRequest(
  writer: BinaryWriter,
  request: DescribeClientQuotasRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 1

  // components array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.components.length + 1)
  } else {
    writer.writeInt32(request.components.length)
  }

  for (const component of request.components) {
    // entity_type
    if (isFlexible) {
      writer.writeCompactString(component.entityType)
    } else {
      writer.writeString(component.entityType)
    }

    // match_type (INT8)
    writer.writeInt8(component.matchType)

    // match (nullable string)
    if (isFlexible) {
      writer.writeCompactString(component.match)
    } else {
      writer.writeString(component.match)
    }

    if (isFlexible) {
      writer.writeTaggedFields(component.taggedFields ?? [])
    }
  }

  // strict (BOOLEAN)
  writer.writeBoolean(request.strict)

  // Tagged fields (v1+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed DescribeClientQuotas request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–1).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildDescribeClientQuotasRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeClientQuotasRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeClientQuotas,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeClientQuotasRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeClientQuotas response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–1).
 * @returns The decoded response or a failure.
 */
export function decodeDescribeClientQuotasResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeClientQuotasResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 1

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

  // entries (nullable array)
  const entriesResult = decodeEntriesArray(reader, isFlexible)
  if (!entriesResult.ok) {
    return entriesResult
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

  return decodeSuccess(
    {
      throttleTimeMs: throttleResult.value,
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      entries: entriesResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

// ---------------------------------------------------------------------------
// Internal decoders
// ---------------------------------------------------------------------------

function decodeEntriesArray(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<DescribeClientQuotasEntry[] | null> {
  const startOffset = reader.offset

  let arrayLength: number
  if (isFlexible) {
    const lengthResult = reader.readUnsignedVarInt()
    if (!lengthResult.ok) {
      return lengthResult
    }
    // compact nullable array: 0 = null, N+1 = N elements
    if (lengthResult.value === 0) {
      return decodeSuccess(null, reader.offset - startOffset)
    }
    arrayLength = lengthResult.value - 1
  } else {
    const lengthResult = reader.readInt32()
    if (!lengthResult.ok) {
      return lengthResult
    }
    if (lengthResult.value < 0) {
      return decodeSuccess(null, reader.offset - startOffset)
    }
    arrayLength = lengthResult.value
  }

  const entries: DescribeClientQuotasEntry[] = []
  for (let i = 0; i < arrayLength; i++) {
    const entryResult = decodeEntry(reader, isFlexible)
    if (!entryResult.ok) {
      return entryResult
    }
    entries.push(entryResult.value)
  }

  return decodeSuccess(entries, reader.offset - startOffset)
}

function decodeEntry(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<DescribeClientQuotasEntry> {
  const startOffset = reader.offset

  // entity (array)
  const entityResult = decodeEntityArray(reader, isFlexible)
  if (!entityResult.ok) {
    return entityResult
  }

  // values (array)
  const valuesResult = decodeValuesArray(reader, isFlexible)
  if (!valuesResult.ok) {
    return valuesResult
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

  return decodeSuccess(
    {
      entity: entityResult.value,
      values: valuesResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeEntityArray(reader: BinaryReader, isFlexible: boolean): DecodeResult<QuotaEntity[]> {
  const startOffset = reader.offset

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

  const entities: QuotaEntity[] = []
  for (let i = 0; i < arrayLength; i++) {
    // entity_type
    const typeResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!typeResult.ok) {
      return typeResult
    }

    // entity_name (nullable string)
    const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!nameResult.ok) {
      return nameResult
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

    entities.push({
      entityType: typeResult.value ?? "",
      entityName: nameResult.value,
      taggedFields
    })
  }

  return decodeSuccess(entities, reader.offset - startOffset)
}

function decodeValuesArray(reader: BinaryReader, isFlexible: boolean): DecodeResult<QuotaValue[]> {
  const startOffset = reader.offset

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

  const values: QuotaValue[] = []
  for (let i = 0; i < arrayLength; i++) {
    // key
    const keyResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!keyResult.ok) {
      return keyResult
    }

    // value (FLOAT64)
    const valueResult = reader.readFloat64()
    if (!valueResult.ok) {
      return valueResult
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

    values.push({
      key: keyResult.value ?? "",
      value: valueResult.value,
      taggedFields
    })
  }

  return decodeSuccess(values, reader.offset - startOffset)
}
