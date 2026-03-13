/**
 * ExpireDelegationToken request/response encoding and decoding.
 *
 * The ExpireDelegationToken API (key 40) changes the expiry time of an
 * existing delegation token. Can be used to expire a token immediately
 * by setting `expiryTimePeriodMs` to a negative value.
 *
 * **Request versions:**
 * - v0: hmac (BYTES), expiry_time_period_ms (INT64)
 * - v1: same as v0
 * - v2: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: error_code, expiry_timestamp_ms, throttle_time_ms
 * - v1: same as v0
 * - v2: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_ExpireDelegationToken
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
 * ExpireDelegationToken request payload.
 */
export type ExpireDelegationTokenRequest = {
  /** The HMAC of the delegation token. */
  readonly hmac: Uint8Array
  /** Expiry time period in milliseconds. Negative value expires immediately. */
  readonly expiryTimePeriodMs: bigint
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * ExpireDelegationToken response payload.
 */
export type ExpireDelegationTokenResponse = {
  /** Error code (0 = success). */
  readonly errorCode: number
  /** New expiry timestamp (epoch ms). */
  readonly expiryTimestampMs: bigint
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an ExpireDelegationToken request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–2).
 */
export function encodeExpireDelegationTokenRequest(
  writer: BinaryWriter,
  request: ExpireDelegationTokenRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // hmac (BYTES / COMPACT_BYTES)
  if (isFlexible) {
    writer.writeCompactBytes(request.hmac)
  } else {
    writer.writeBytes(request.hmac)
  }

  // expiry_time_period_ms (INT64)
  writer.writeInt64(request.expiryTimePeriodMs)

  // tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed ExpireDelegationToken request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–2).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildExpireDelegationTokenRequest(
  correlationId: number,
  apiVersion: number,
  request: ExpireDelegationTokenRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.ExpireDelegationToken,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeExpireDelegationTokenRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an ExpireDelegationToken response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–2).
 * @returns The decoded response or a failure.
 */
export function decodeExpireDelegationTokenResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<ExpireDelegationTokenResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // expiry_timestamp_ms (INT64)
  const expiryResult = reader.readInt64()
  if (!expiryResult.ok) {
    return expiryResult
  }

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // tagged fields (v2+)
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
      expiryTimestampMs: expiryResult.value,
      throttleTimeMs: throttleResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}
