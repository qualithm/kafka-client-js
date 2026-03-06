/**
 * Protocol framing for Kafka request and response headers.
 *
 * Kafka uses size-prefixed messages where each request/response starts with
 * an INT32 length prefix followed by the header and body.
 *
 * Request headers come in three versions:
 * - **v0**: API key, API version, correlation ID (no client ID)
 * - **v1**: v0 + nullable client ID string
 * - **v2**: flexible version — API key, API version, correlation ID,
 *   compact nullable client ID, tagged fields (KIP-482)
 *
 * Response headers come in two versions:
 * - **v0**: correlation ID only
 * - **v1**: flexible version — correlation ID + tagged fields
 *
 * Header version selection is determined by whether the API key + version
 * uses flexible encoding per {@link isFlexibleVersion}.
 *
 * @see https://kafka.apache.org/protocol.html#protocol_messages
 *
 * @packageDocumentation
 */

import { type ApiKey, isFlexibleVersion } from "./api-keys.js"
import { BinaryReader, type TaggedField } from "./binary-reader.js"
import { BinaryWriter } from "./binary-writer.js"
import { decodeFailure, type DecodeResult, decodeSuccess } from "./result.js"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Fields for a Kafka request header.
 *
 * Which fields are actually encoded depends on the header version:
 * - v0: `apiKey`, `apiVersion`, `correlationId`
 * - v1: v0 + `clientId`
 * - v2: v1 + `taggedFields` (compact encoding)
 */
export type RequestHeader = {
  /** API key identifying the request type. */
  readonly apiKey: ApiKey
  /** API version for this request. */
  readonly apiVersion: number
  /** Correlation ID to match responses to requests. */
  readonly correlationId: number
  /** Client identifier. Used in v1+ headers. */
  readonly clientId?: string | null
  /** Tagged fields for flexible versions (v2). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * Decoded Kafka response header.
 */
export type ResponseHeader = {
  /** Correlation ID matching the originating request. */
  readonly correlationId: number
  /** Tagged fields from flexible version responses (v1). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * Request header version.
 * - 0: minimal header (API key, version, correlation ID)
 * - 1: adds client ID (non-flexible)
 * - 2: flexible encoding with compact strings and tagged fields
 */
export type RequestHeaderVersion = 0 | 1 | 2

/**
 * Response header version.
 * - 0: correlation ID only (non-flexible)
 * - 1: correlation ID + tagged fields (flexible)
 */
export type ResponseHeaderVersion = 0 | 1

// ---------------------------------------------------------------------------
// Header version selection
// ---------------------------------------------------------------------------

/**
 * Determine the request header version for a given API key and version.
 *
 * - Flexible APIs (per KIP-482) use request header v2
 * - Non-flexible APIs use request header v1 (with client ID)
 *
 * Note: Request header v0 (no client ID) is not selected automatically.
 * It exists for protocol completeness but modern clients always send a
 * client ID (v1+).
 */
export function requestHeaderVersion(apiKey: ApiKey, apiVersion: number): RequestHeaderVersion {
  return isFlexibleVersion(apiKey, apiVersion) ? 2 : 1
}

/**
 * Determine the response header version for a given API key and version.
 *
 * - Flexible APIs use response header v1 (with tagged fields)
 * - Non-flexible APIs use response header v0
 */
export function responseHeaderVersion(apiKey: ApiKey, apiVersion: number): ResponseHeaderVersion {
  return isFlexibleVersion(apiKey, apiVersion) ? 1 : 0
}

// ---------------------------------------------------------------------------
// Request header encoding
// ---------------------------------------------------------------------------

/**
 * Encode a request header into the given writer.
 *
 * The header version is determined automatically from the API key and version,
 * or can be overridden via `headerVersion`.
 *
 * @param writer - The writer to encode into.
 * @param header - Request header fields.
 * @param headerVersion - Override the header version (defaults to auto-detection).
 */
export function encodeRequestHeader(
  writer: BinaryWriter,
  header: RequestHeader,
  headerVersion?: RequestHeaderVersion
): void {
  const version = headerVersion ?? requestHeaderVersion(header.apiKey, header.apiVersion)

  // All versions: API key (int16), API version (int16), correlation ID (int32)
  writer.writeInt16(header.apiKey)
  writer.writeInt16(header.apiVersion)
  writer.writeInt32(header.correlationId)

  if (version === 0) {
    return
  }

  if (version === 1) {
    // v1: nullable string client ID (INT16 length prefix)
    writer.writeString(header.clientId ?? null)
    return
  }

  // v2 (flexible): compact nullable string client ID + tagged fields
  writer.writeCompactString(header.clientId ?? null)
  writer.writeTaggedFields(header.taggedFields ?? [])
}

// ---------------------------------------------------------------------------
// Response header decoding
// ---------------------------------------------------------------------------

/**
 * Decode a response header from the given reader.
 *
 * The header version is determined automatically from the API key and version,
 * or can be overridden via `headerVersion`.
 *
 * @param reader - The reader to decode from.
 * @param apiKey - API key of the originating request.
 * @param apiVersion - API version of the originating request.
 * @param headerVersion - Override the header version (defaults to auto-detection).
 */
export function decodeResponseHeader(
  reader: BinaryReader,
  apiKey: ApiKey,
  apiVersion: number,
  headerVersion?: ResponseHeaderVersion
): DecodeResult<ResponseHeader> {
  const version = headerVersion ?? responseHeaderVersion(apiKey, apiVersion)
  const startPos = reader.offset

  const correlationResult = reader.readInt32()
  if (!correlationResult.ok) {
    return correlationResult as DecodeResult<ResponseHeader>
  }

  if (version === 0) {
    return decodeSuccess(
      { correlationId: correlationResult.value, taggedFields: [] },
      reader.offset - startPos
    )
  }

  // v1 (flexible): read tagged fields
  const taggedResult = reader.readTaggedFields()
  if (!taggedResult.ok) {
    return taggedResult as DecodeResult<ResponseHeader>
  }

  return decodeSuccess(
    { correlationId: correlationResult.value, taggedFields: taggedResult.value },
    reader.offset - startPos
  )
}

// ---------------------------------------------------------------------------
// Size-prefixed message framing
// ---------------------------------------------------------------------------

/**
 * Frame a request by prepending a size prefix.
 *
 * Encodes the request header and body, then prepends an INT32 size prefix
 * containing the total byte length of header + body (excluding the prefix itself).
 *
 * @param header - Request header fields.
 * @param encodeBody - Callback to encode the request body into the writer.
 * @param headerVersion - Override the header version (defaults to auto-detection).
 * @returns The complete framed request as a {@link Uint8Array}.
 */
export function frameRequest(
  header: RequestHeader,
  encodeBody: (writer: BinaryWriter) => void,
  headerVersion?: RequestHeaderVersion
): Uint8Array {
  // Encode header + body into a temporary writer
  const inner = new BinaryWriter()
  encodeRequestHeader(inner, header, headerVersion)
  encodeBody(inner)

  const payload = inner.finish()

  // Prepend INT32 size prefix
  const framed = new BinaryWriter(payload.byteLength + 4)
  framed.writeInt32(payload.byteLength)
  framed.writeRawBytes(payload)
  return framed.finish()
}

/**
 * Read a size-prefixed response frame from the reader.
 *
 * Reads the INT32 size prefix, then returns a new {@link BinaryReader}
 * scoped to exactly that many bytes for decoding the response header and body.
 *
 * @param reader - Reader positioned at the start of a framed response.
 * @returns A reader scoped to the response payload, or a decode failure.
 */
export function readResponseFrame(reader: BinaryReader): DecodeResult<BinaryReader> {
  const startPos = reader.offset

  const sizeResult = reader.readInt32()
  if (!sizeResult.ok) {
    return sizeResult as DecodeResult<BinaryReader>
  }

  const size = sizeResult.value
  if (size < 0) {
    return decodeFailure("INVALID_DATA", `invalid response frame size: ${String(size)}`, startPos)
  }

  const payloadResult = reader.readRawBytes(size)
  if (!payloadResult.ok) {
    return payloadResult as DecodeResult<BinaryReader>
  }

  return decodeSuccess(new BinaryReader(payloadResult.value), reader.offset - startPos)
}
