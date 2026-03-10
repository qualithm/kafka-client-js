/**
 * InitProducerId request/response encoding and decoding.
 *
 * The InitProducerId API (key 22) allocates a producer ID and epoch for
 * idempotent and transactional producers. The broker assigns a unique
 * producer ID and an initial epoch of 0 for new producers. For existing
 * transactional producers, it bumps the epoch and fences zombie instances.
 *
 * **Request versions:**
 * - v0–v1: transactional_id, transaction_timeout_ms
 * - v2+: flexible encoding (KIP-482)
 * - v3+: adds producer_id and producer_epoch for existing sessions
 * - v4+: identical to v3 with flexible encoding
 *
 * **Response versions:**
 * - v0: throttle_time_ms, error_code, producer_id, producer_epoch
 * - v1+: identical structure with newer error codes
 * - v2+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_InitProducerId
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
 * InitProducerId request payload.
 */
export type InitProducerIdRequest = {
  /**
   * The transactional ID, or null for idempotent-only producers.
   */
  readonly transactionalId: string | null
  /**
   * The time in milliseconds to wait before aborting idle transactions.
   * Only relevant for transactional producers; set to -1 for idempotent-only.
   */
  readonly transactionTimeoutMs: number
  /**
   * The current producer ID (v3+). Use -1 for new producers.
   */
  readonly producerId?: bigint
  /**
   * The current producer epoch (v3+). Use -1 for new producers.
   */
  readonly producerEpoch?: number
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * InitProducerId response payload.
 */
export type InitProducerIdResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** The producer ID assigned by the broker. -1 on error. */
  readonly producerId: bigint
  /** The producer epoch assigned by the broker. -1 on error. */
  readonly producerEpoch: number
  /** Tagged fields from flexible versions (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an InitProducerId request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–4).
 */
export function encodeInitProducerIdRequest(
  writer: BinaryWriter,
  request: InitProducerIdRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // transactional_id — nullable string
  if (isFlexible) {
    writer.writeCompactString(request.transactionalId)
  } else {
    writer.writeString(request.transactionalId)
  }

  // transaction_timeout_ms (INT32)
  writer.writeInt32(request.transactionTimeoutMs)

  // producer_id (INT64, v3+)
  if (apiVersion >= 3) {
    writer.writeInt64(request.producerId ?? -1n)
  }

  // producer_epoch (INT16, v3+)
  if (apiVersion >= 3) {
    writer.writeInt16(request.producerEpoch ?? -1)
  }

  // Tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed InitProducerId request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–4).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildInitProducerIdRequest(
  correlationId: number,
  apiVersion: number,
  request: InitProducerIdRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.InitProducerId,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeInitProducerIdRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an InitProducerId response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–4).
 * @returns The decoded response or a failure.
 */
export function decodeInitProducerIdResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<InitProducerIdResponse> {
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

  // producer_id (INT64)
  const producerIdResult = reader.readInt64()
  if (!producerIdResult.ok) {
    return producerIdResult
  }

  // producer_epoch (INT16)
  const producerEpochResult = reader.readInt16()
  if (!producerEpochResult.ok) {
    return producerEpochResult
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
      producerId: producerIdResult.value,
      producerEpoch: producerEpochResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}
