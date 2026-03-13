/**
 * UpdateFeatures request/response encoding and decoding.
 *
 * The UpdateFeatures API (key 57) updates finalized feature flags in the cluster.
 *
 * **Request versions:**
 * - v0: timeout_ms, feature_updates array
 * - v1: adds validate_only (BOOLEAN)
 * - All versions use flexible encoding
 *
 * **Response versions:**
 * - v0: throttle_time_ms, error_code, error_message, results array
 * - v1: same structure
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_UpdateFeatures
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
 * A feature update entry.
 */
export type FeatureUpdateKey = {
  /** The feature name. */
  readonly feature: string
  /** The new maximum version level for the feature, or 0 to delete. */
  readonly maxVersionLevel: number
  /** The upgrade type: 1 = upgrade, 2 = safe_downgrade, 3 = unsafe_downgrade (v1+, default 1). */
  readonly upgradeType?: number
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * UpdateFeatures request payload.
 */
export type UpdateFeaturesRequest = {
  /** Timeout in milliseconds. */
  readonly timeoutMs: number
  /** The feature updates. */
  readonly featureUpdates: readonly FeatureUpdateKey[]
  /** Whether to validate only without applying (v1+). */
  readonly validateOnly?: boolean
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-feature update result.
 */
export type UpdatableFeatureResult = {
  /** The feature name. */
  readonly feature: string
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Error message, or null. */
  readonly errorMessage: string | null
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * UpdateFeatures response payload.
 */
export type UpdateFeaturesResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Top-level error code (0 = no error). */
  readonly errorCode: number
  /** Top-level error message, or null. */
  readonly errorMessage: string | null
  /** Per-feature results. */
  readonly results: readonly UpdatableFeatureResult[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an UpdateFeatures request body into the given writer.
 */
export function encodeUpdateFeaturesRequest(
  writer: BinaryWriter,
  request: UpdateFeaturesRequest,
  apiVersion: number
): void {
  // timeout_ms (INT32)
  writer.writeInt32(request.timeoutMs)

  // feature_updates (compact array)
  writer.writeUnsignedVarInt(request.featureUpdates.length + 1)
  for (const update of request.featureUpdates) {
    // feature (COMPACT_STRING)
    writer.writeCompactString(update.feature)

    // max_version_level (INT16)
    writer.writeInt16(update.maxVersionLevel)

    // upgrade_type (INT8, v1+) — v0 uses allow_downgrade (BOOLEAN) but was replaced in v1
    if (apiVersion >= 1) {
      writer.writeInt8(update.upgradeType ?? 1)
    } else {
      // v0: allow_downgrade (BOOLEAN) — default false
      writer.writeBoolean(update.upgradeType === 3)
    }

    // tagged fields
    writer.writeTaggedFields(update.taggedFields ?? [])
  }

  // validate_only (BOOLEAN, v1+)
  if (apiVersion >= 1) {
    writer.writeBoolean(request.validateOnly ?? false)
  }

  // tagged fields
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed UpdateFeatures request ready to send to a broker.
 */
export function buildUpdateFeaturesRequest(
  correlationId: number,
  apiVersion: number,
  request: UpdateFeaturesRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.UpdateFeatures,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeUpdateFeaturesRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an UpdateFeatures response body.
 */
export function decodeUpdateFeaturesResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<UpdateFeaturesResponse> {
  const startOffset = reader.offset

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

  // error_message (COMPACT_NULLABLE_STRING)
  const errorMsgResult = reader.readCompactString()
  if (!errorMsgResult.ok) {
    return errorMsgResult
  }

  // results array
  const resultsResult = decodeResultsArray(reader)
  if (!resultsResult.ok) {
    return resultsResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      throttleTimeMs: throttleResult.value,
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      results: resultsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeResultsArray(reader: BinaryReader): DecodeResult<UpdatableFeatureResult[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const results: UpdatableFeatureResult[] = []
  for (let i = 0; i < count; i++) {
    const resultEntry = decodeFeatureResult(reader)
    if (!resultEntry.ok) {
      return resultEntry
    }
    results.push(resultEntry.value)
  }

  return decodeSuccess(results, reader.offset - startOffset)
}

function decodeFeatureResult(reader: BinaryReader): DecodeResult<UpdatableFeatureResult> {
  const startOffset = reader.offset

  // feature (COMPACT_STRING)
  const featureResult = reader.readCompactString()
  if (!featureResult.ok) {
    return featureResult
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // error_message (COMPACT_NULLABLE_STRING)
  const errorMsgResult = reader.readCompactString()
  if (!errorMsgResult.ok) {
    return errorMsgResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      feature: featureResult.value ?? "",
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
