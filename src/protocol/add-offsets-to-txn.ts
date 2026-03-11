/**
 * AddOffsetsToTxn request/response encoding and decoding.
 *
 * The AddOffsetsToTxn API (key 25) adds a consumer group's offsets to an
 * ongoing transaction. This allows transactional consumers to commit offsets
 * atomically with produced records.
 *
 * **Request versions:**
 * - v0–v2: transactional_id, producer_id, producer_epoch, group_id
 * - v3+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: throttle_time_ms, error_code
 * - v3+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_AddOffsetsToTxn
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
 * AddOffsetsToTxn request payload.
 */
export type AddOffsetsToTxnRequest = {
  /** The transactional ID. */
  readonly transactionalId: string
  /** The current producer ID. */
  readonly producerId: bigint
  /** The current producer epoch. */
  readonly producerEpoch: number
  /** The consumer group ID whose offsets should be included in the transaction. */
  readonly groupId: string
  /** Tagged fields (v3+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * AddOffsetsToTxn response payload.
 */
export type AddOffsetsToTxnResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Tagged fields from flexible versions (v3+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an AddOffsetsToTxn request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–3).
 */
export function encodeAddOffsetsToTxnRequest(
  writer: BinaryWriter,
  request: AddOffsetsToTxnRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 3

  // transactional_id
  if (isFlexible) {
    writer.writeCompactString(request.transactionalId)
  } else {
    writer.writeString(request.transactionalId)
  }

  // producer_id (INT64)
  writer.writeInt64(request.producerId)

  // producer_epoch (INT16)
  writer.writeInt16(request.producerEpoch)

  // group_id
  if (isFlexible) {
    writer.writeCompactString(request.groupId)
  } else {
    writer.writeString(request.groupId)
  }

  // Tagged fields (v3+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed AddOffsetsToTxn request ready to send to a broker.
 */
export function buildAddOffsetsToTxnRequest(
  correlationId: number,
  apiVersion: number,
  request: AddOffsetsToTxnRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.AddOffsetsToTxn,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeAddOffsetsToTxnRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an AddOffsetsToTxn response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–3).
 * @returns The decoded response or a failure.
 */
export function decodeAddOffsetsToTxnResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<AddOffsetsToTxnResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 3

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
      throttleTimeMs: throttleResult.value,
      errorCode: errorCodeResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}
