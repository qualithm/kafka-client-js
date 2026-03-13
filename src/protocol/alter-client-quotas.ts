/**
 * AlterClientQuotas request/response encoding and decoding.
 *
 * The AlterClientQuotas API (key 49) sets or removes client quota
 * configuration entries for specified entities.
 *
 * **Request versions:**
 * - v0: non-flexible encoding
 * - v1: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: throttle, per-entity error codes
 * - v1: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_AlterClientQuotas
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
 * An entity component identifying the quota target.
 */
export type AlterClientQuotasEntity = {
  /** The entity type (e.g. "user", "client-id", "ip"). */
  readonly entityType: string
  /** The entity name, or null for the default entity. */
  readonly entityName: string | null
  /** Tagged fields (v1+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * An operation to perform on a quota configuration key.
 */
export type AlterClientQuotasOp = {
  /** The quota configuration key. */
  readonly key: string
  /** The value to set. Ignored when remove is true. */
  readonly value: number
  /** If true, remove this quota key; otherwise set/update it. */
  readonly remove: boolean
  /** Tagged fields (v1+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A single entry in the AlterClientQuotas request.
 */
export type AlterClientQuotasEntry = {
  /** The entity components identifying the quota target. */
  readonly entity: readonly AlterClientQuotasEntity[]
  /** The quota operations to perform. */
  readonly ops: readonly AlterClientQuotasOp[]
  /** Tagged fields (v1+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * AlterClientQuotas request payload.
 */
export type AlterClientQuotasRequest = {
  /** Quota alteration entries. */
  readonly entries: readonly AlterClientQuotasEntry[]
  /** If true, validate the request without applying changes. */
  readonly validateOnly: boolean
  /** Tagged fields (v1+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * An entity component in a response entry.
 */
export type AlterClientQuotasResponseEntity = {
  /** The entity type. */
  readonly entityType: string
  /** The entity name, or null for the default entity. */
  readonly entityName: string | null
  /** Tagged fields (v1+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Per-entity result from an AlterClientQuotas response.
 */
export type AlterClientQuotasEntryResponse = {
  /** Error code for this entity (0 = no error). */
  readonly errorCode: number
  /** Error message. Null if no error. */
  readonly errorMessage: string | null
  /** The entity components this result applies to. */
  readonly entity: readonly AlterClientQuotasResponseEntity[]
  /** Tagged fields (v1+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * AlterClientQuotas response payload.
 */
export type AlterClientQuotasResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-entity alteration results. */
  readonly entries: readonly AlterClientQuotasEntryResponse[]
  /** Tagged fields (v1+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an AlterClientQuotas request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–1).
 */
export function encodeAlterClientQuotasRequest(
  writer: BinaryWriter,
  request: AlterClientQuotasRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 1

  // entries array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.entries.length + 1)
  } else {
    writer.writeInt32(request.entries.length)
  }

  for (const entry of request.entries) {
    // entity array
    if (isFlexible) {
      writer.writeUnsignedVarInt(entry.entity.length + 1)
    } else {
      writer.writeInt32(entry.entity.length)
    }

    for (const entityComponent of entry.entity) {
      // entity_type
      if (isFlexible) {
        writer.writeCompactString(entityComponent.entityType)
      } else {
        writer.writeString(entityComponent.entityType)
      }

      // entity_name (nullable)
      if (isFlexible) {
        writer.writeCompactString(entityComponent.entityName)
      } else {
        writer.writeString(entityComponent.entityName)
      }

      if (isFlexible) {
        writer.writeTaggedFields(entityComponent.taggedFields ?? [])
      }
    }

    // ops array
    if (isFlexible) {
      writer.writeUnsignedVarInt(entry.ops.length + 1)
    } else {
      writer.writeInt32(entry.ops.length)
    }

    for (const op of entry.ops) {
      // key
      if (isFlexible) {
        writer.writeCompactString(op.key)
      } else {
        writer.writeString(op.key)
      }

      // value (FLOAT64)
      writer.writeFloat64(op.value)

      // remove (BOOLEAN)
      writer.writeBoolean(op.remove)

      if (isFlexible) {
        writer.writeTaggedFields(op.taggedFields ?? [])
      }
    }

    if (isFlexible) {
      writer.writeTaggedFields(entry.taggedFields ?? [])
    }
  }

  // validate_only (BOOLEAN)
  writer.writeBoolean(request.validateOnly)

  // Tagged fields (v1+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed AlterClientQuotas request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–1).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildAlterClientQuotasRequest(
  correlationId: number,
  apiVersion: number,
  request: AlterClientQuotasRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.AlterClientQuotas,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeAlterClientQuotasRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an AlterClientQuotas response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–1).
 * @returns The decoded response or a failure.
 */
export function decodeAlterClientQuotasResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<AlterClientQuotasResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 1

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // entries array
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

  const entries: AlterClientQuotasEntryResponse[] = []
  for (let i = 0; i < arrayLength; i++) {
    const entryResult = decodeResponseEntry(reader, isFlexible)
    if (!entryResult.ok) {
      return entryResult
    }
    entries.push(entryResult.value)
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
      entries,
      taggedFields
    },
    reader.offset - startOffset
  )
}

// ---------------------------------------------------------------------------
// Internal decoders
// ---------------------------------------------------------------------------

function decodeResponseEntry(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<AlterClientQuotasEntryResponse> {
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

  // entity array
  const entityResult = decodeResponseEntityArray(reader, isFlexible)
  if (!entityResult.ok) {
    return entityResult
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
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      entity: entityResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeResponseEntityArray(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<AlterClientQuotasResponseEntity[]> {
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

  const entities: AlterClientQuotasResponseEntity[] = []
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
