/**
 * EndTxn request/response encoding and decoding.
 *
 * The EndTxn API (key 26) commits or aborts an ongoing transaction. The broker
 * writes the COMMIT or ABORT marker to the transaction log and releases all
 * partition locks held by the transaction.
 *
 * **Request versions:**
 * - v0–v2: transactional_id, producer_id, producer_epoch, committed
 * - v3+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: throttle_time_ms, error_code
 * - v3+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_EndTxn
 *
 * @packageDocumentation
 */

import { ApiKey } from "./api-keys.js"
import type { BinaryReader, TaggedField } from "./binary-reader.js"
import type { BinaryWriter } from "./binary-writer.js"
import { frameRequest, type RequestHeader } from "./protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "./result.js"

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * EndTxn request payload.
 */
export type EndTxnRequest = {
  /** The transactional ID. */
  readonly transactionalId: string
  /** The current producer ID. */
  readonly producerId: bigint
  /** The current producer epoch. */
  readonly producerEpoch: number
  /** Whether the transaction should be committed (true) or aborted (false). */
  readonly committed: boolean
  /** Tagged fields (v3+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * EndTxn response payload.
 */
export type EndTxnResponse = {
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
 * Encode an EndTxn request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–3).
 */
export function encodeEndTxnRequest(
  writer: BinaryWriter,
  request: EndTxnRequest,
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

  // committed (BOOLEAN)
  writer.writeBoolean(request.committed)

  // Tagged fields (v3+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed EndTxn request ready to send to a broker.
 */
export function buildEndTxnRequest(
  correlationId: number,
  apiVersion: number,
  request: EndTxnRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.EndTxn,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeEndTxnRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an EndTxn response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–3).
 * @returns The decoded response or a failure.
 */
export function decodeEndTxnResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<EndTxnResponse> {
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
