/**
 * ApiVersions request/response encoding and decoding.
 *
 * The ApiVersions API (key 18) is typically the first request sent to a broker.
 * It returns the version ranges supported by the broker for each API, enabling
 * clients to negotiate compatible versions.
 *
 * **Request versions:**
 * - v0–v2: Empty body (non-flexible)
 * - v3: Flexible version with client software name/version (KIP-511)
 *
 * **Response versions:**
 * - v0: error code + api versions array
 * - v1–v2: adds throttle time
 * - v3: flexible encoding with compact arrays and tagged fields
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
 *
 * @packageDocumentation
 */

import { ApiKey, type ApiVersionRange } from "./api-keys.js"
import type { BinaryReader, TaggedField } from "./binary-reader.js"
import type { BinaryWriter } from "./binary-writer.js"
import { frameRequest, type RequestHeader } from "./protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "./result.js"

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * ApiVersions request payload.
 *
 * For v0–v2, the body is empty. For v3+, the client software name and
 * version are included for telemetry (KIP-511).
 */
export type ApiVersionsRequest = {
  /**
   * Client software name (v3+ only).
   * Example: "kafka-client-js"
   */
  readonly clientSoftwareName?: string | null
  /**
   * Client software version (v3+ only).
   * Example: "1.0.0"
   */
  readonly clientSoftwareVersion?: string | null
  /** Tagged fields for flexible versions (v3+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A single API version range from the broker response.
 */
export type ApiVersionEntry = {
  /** The API key. */
  readonly apiKey: number
  /** Minimum supported version. */
  readonly minVersion: number
  /** Maximum supported version. */
  readonly maxVersion: number
  /** Tagged fields (flexible versions only). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * ApiVersions response payload.
 */
export type ApiVersionsResponse = {
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Supported API version ranges from the broker. */
  readonly apiVersions: readonly ApiVersionEntry[]
  /** Time the request was throttled in milliseconds (v1+). */
  readonly throttleTimeMs: number
  /** Tagged fields from flexible versions (v3+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an ApiVersions request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–3).
 */
export function encodeApiVersionsRequest(
  writer: BinaryWriter,
  request: ApiVersionsRequest,
  apiVersion: number
): void {
  if (apiVersion < 3) {
    // v0–v2: empty body
    return
  }

  // v3+: flexible encoding with client software info
  writer.writeCompactString(request.clientSoftwareName ?? null)
  writer.writeCompactString(request.clientSoftwareVersion ?? null)
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed ApiVersions request ready to send to a broker.
 *
 * This encodes both the request header and body, wrapped in a size prefix.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–3).
 * @param request - Request payload (empty for v0–v2).
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildApiVersionsRequest(
  correlationId: number,
  apiVersion: number,
  request: ApiVersionsRequest = {},
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.ApiVersions,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeApiVersionsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an ApiVersions response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–3).
 * @returns The decoded response or a failure.
 */
export function decodeApiVersionsResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<ApiVersionsResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 3

  // Error code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }
  const errorCode = errorCodeResult.value

  // Api versions array
  let arrayLength: number

  if (isFlexible) {
    // Compact array: length is unsigned varint, actual length = N - 1
    const lengthResult = reader.readUnsignedVarInt()
    if (!lengthResult.ok) {
      return lengthResult
    }
    arrayLength = lengthResult.value - 1
  } else {
    // Normal array: INT32 length
    const lengthResult = reader.readInt32()
    if (!lengthResult.ok) {
      return lengthResult
    }
    arrayLength = lengthResult.value
  }

  if (arrayLength < 0) {
    // Null array - treat as empty
    arrayLength = 0
  }

  const apiVersions: ApiVersionEntry[] = []

  for (let i = 0; i < arrayLength; i++) {
    // Api key (INT16)
    const apiKeyResult = reader.readInt16()
    if (!apiKeyResult.ok) {
      return apiKeyResult
    }

    // Min version (INT16)
    const minVersionResult = reader.readInt16()
    if (!minVersionResult.ok) {
      return minVersionResult
    }

    // Max version (INT16)
    const maxVersionResult = reader.readInt16()
    if (!maxVersionResult.ok) {
      return maxVersionResult
    }

    let entryTaggedFields: readonly TaggedField[] = []
    if (isFlexible) {
      const tagResult = reader.readTaggedFields()
      if (!tagResult.ok) {
        return tagResult
      }
      entryTaggedFields = tagResult.value
    }

    apiVersions.push({
      apiKey: apiKeyResult.value,
      minVersion: minVersionResult.value,
      maxVersion: maxVersionResult.value,
      taggedFields: entryTaggedFields
    })
  }

  // Throttle time (INT32) - present in v1+
  let throttleTimeMs = 0
  if (apiVersion >= 1) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  // Tagged fields for flexible versions
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
      errorCode,
      apiVersions,
      throttleTimeMs,
      taggedFields
    },
    reader.offset - startOffset
  )
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

/**
 * Convert an ApiVersionsResponse to a map for easy lookup.
 *
 * @param response - The decoded ApiVersions response.
 * @returns A map from API key to version range.
 */
export function apiVersionsToMap(response: ApiVersionsResponse): Map<number, ApiVersionRange> {
  const map = new Map<number, ApiVersionRange>()
  for (const entry of response.apiVersions) {
    map.set(entry.apiKey, {
      minVersion: entry.minVersion,
      maxVersion: entry.maxVersion
    })
  }
  return map
}
