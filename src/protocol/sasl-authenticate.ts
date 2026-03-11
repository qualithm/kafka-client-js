/**
 * SaslAuthenticate request/response encoding and decoding.
 *
 * The SaslAuthenticate API (key 36) carries SASL authentication tokens
 * after the mechanism has been negotiated via SaslHandshake. The client
 * and broker exchange one or more authenticate requests/responses until
 * authentication succeeds or fails.
 *
 * **Request versions:**
 * - v0: auth_bytes
 * - v2+: flexible encoding (KIP-482)
 *
 * **Response versions:**
 * - v0: error_code, error_message, auth_bytes
 * - v1+: session_lifetime_ms
 * - v2+: flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate
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
 * SaslAuthenticate request payload.
 */
export type SaslAuthenticateRequest = {
  /** The SASL authentication bytes (mechanism-specific). */
  readonly authBytes: Uint8Array
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * SaslAuthenticate response payload.
 */
export type SaslAuthenticateResponse = {
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Error message from the broker, or null if no error. */
  readonly errorMessage: string | null
  /** SASL authentication bytes from the broker (mechanism-specific). */
  readonly authBytes: Uint8Array | null
  /** Session lifetime in milliseconds (v1+). 0 means no lifetime. */
  readonly sessionLifetimeMs: bigint
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a SaslAuthenticate request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–2).
 */
export function encodeSaslAuthenticateRequest(
  writer: BinaryWriter,
  request: SaslAuthenticateRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // auth_bytes (BYTES / COMPACT_BYTES)
  if (isFlexible) {
    writer.writeCompactBytes(request.authBytes)
  } else {
    writer.writeBytes(request.authBytes)
  }

  // Tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed SaslAuthenticate request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–2).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildSaslAuthenticateRequest(
  correlationId: number,
  apiVersion: number,
  request: SaslAuthenticateRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.SaslAuthenticate,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeSaslAuthenticateRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a SaslAuthenticate response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–2).
 * @returns The decoded response or a failure.
 */
export function decodeSaslAuthenticateResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<SaslAuthenticateResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // error_message (NULLABLE_STRING / COMPACT_NULLABLE_STRING)
  const errorMessageResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!errorMessageResult.ok) {
    return errorMessageResult
  }

  // auth_bytes (BYTES / COMPACT_BYTES)
  const authBytesResult = isFlexible ? reader.readCompactBytes() : reader.readBytes()
  if (!authBytesResult.ok) {
    return authBytesResult
  }

  // session_lifetime_ms (INT64, v1+)
  let sessionLifetimeMs = 0n
  if (apiVersion >= 1) {
    const sessionResult = reader.readInt64()
    if (!sessionResult.ok) {
      return sessionResult
    }
    sessionLifetimeMs = sessionResult.value
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
      errorCode: errorCodeResult.value,
      errorMessage: errorMessageResult.value,
      authBytes: authBytesResult.value,
      sessionLifetimeMs,
      taggedFields
    },
    reader.offset - startOffset
  )
}
