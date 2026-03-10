/**
 * SaslHandshake request/response encoding and decoding.
 *
 * The SaslHandshake API (key 17) negotiates the SASL mechanism to use
 * for authentication. The client sends its desired mechanism, and the
 * broker responds with the list of mechanisms it supports plus an error
 * code indicating whether the requested mechanism is available.
 *
 * **Request versions:**
 * - v0: mechanism
 * - v1: mechanism (same wire format, but v1 uses SaslAuthenticate for auth
 *   instead of raw SASL tokens over the socket)
 *
 * **Response versions:**
 * - v0–v1: error_code, mechanisms
 *
 * SaslHandshake never uses flexible headers within the versions we support.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_SaslHandshake
 *
 * @packageDocumentation
 */

import { ApiKey } from "./api-keys.js"
import type { BinaryReader } from "./binary-reader.js"
import type { BinaryWriter } from "./binary-writer.js"
import { frameRequest, type RequestHeader } from "./protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "./result.js"

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * SaslHandshake request payload.
 */
export type SaslHandshakeRequest = {
  /** The SASL mechanism the client wants to use (e.g. "PLAIN", "SCRAM-SHA-256"). */
  readonly mechanism: string
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * SaslHandshake response payload.
 */
export type SaslHandshakeResponse = {
  /** Error code (0 = no error, 33 = UNSUPPORTED_SASL_MECHANISM). */
  readonly errorCode: number
  /** List of SASL mechanisms the broker supports. */
  readonly mechanisms: readonly string[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a SaslHandshake request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param _apiVersion - The API version to encode for (0–1). Both versions have the same wire format.
 */
export function encodeSaslHandshakeRequest(
  writer: BinaryWriter,
  request: SaslHandshakeRequest,
  _apiVersion: number
): void {
  // mechanism (STRING) — same format for v0 and v1, never flexible
  writer.writeString(request.mechanism)
}

/**
 * Build a complete, framed SaslHandshake request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–1).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildSaslHandshakeRequest(
  correlationId: number,
  apiVersion: number,
  request: SaslHandshakeRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.SaslHandshake,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeSaslHandshakeRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a SaslHandshake response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param _apiVersion - The API version of the response (0–1).
 * @returns The decoded response or a failure.
 */
export function decodeSaslHandshakeResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<SaslHandshakeResponse> {
  const startOffset = reader.offset

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // mechanisms (ARRAY of STRING) — non-flexible, so standard array/string
  const mechanismsResult = reader.readArray((r) => r.readString())
  if (!mechanismsResult.ok) {
    return mechanismsResult
  }

  const mechanisms: string[] = []
  if (mechanismsResult.value !== null) {
    for (const m of mechanismsResult.value) {
      if (m !== null) {
        mechanisms.push(m)
      }
    }
  }

  return decodeSuccess(
    {
      errorCode: errorCodeResult.value,
      mechanisms
    },
    reader.offset - startOffset
  )
}
