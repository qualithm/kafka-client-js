/**
 * GetTelemetrySubscriptions request/response encoding and decoding.
 *
 * The GetTelemetrySubscriptions API (key 71) allows clients to discover
 * what telemetry metrics the broker wants and how to push them (KIP-714).
 *
 * **Request versions:**
 * - v0: client_instance_id (UUID)
 *
 * **Response versions:**
 * - v0: throttle_time_ms, error_code, client_instance_id, subscription_id,
 *        accepted_compression_types[], push_interval_ms, telemetry_max_bytes,
 *        delta_temporality, requested_metrics[]
 *
 * All versions use flexible encoding.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability
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
 * GetTelemetrySubscriptions request payload.
 */
export type GetTelemetrySubscriptionsRequest = {
  /** Client instance ID (UUID, 16 bytes). Zero UUID for initial request. */
  readonly clientInstanceId: Uint8Array
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * GetTelemetrySubscriptions response payload.
 */
export type GetTelemetrySubscriptionsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Assigned client instance ID (UUID, 16 bytes). */
  readonly clientInstanceId: Uint8Array
  /** Subscription ID used to correlate with PushTelemetry. */
  readonly subscriptionId: number
  /** Compression types accepted by the broker for pushed metrics. */
  readonly acceptedCompressionTypes: readonly number[]
  /** Interval at which the client should push telemetry, in milliseconds. */
  readonly pushIntervalMs: number
  /** Maximum size of serialised metrics in bytes. */
  readonly telemetryMaxBytes: number
  /** Whether the broker requests delta temporality for metrics. */
  readonly deltaTemporality: boolean
  /** Metric name prefixes the broker wants reported (empty = all metrics). */
  readonly requestedMetrics: readonly string[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a GetTelemetrySubscriptions request body into the given writer.
 */
export function encodeGetTelemetrySubscriptionsRequest(
  writer: BinaryWriter,
  request: GetTelemetrySubscriptionsRequest,
  _apiVersion: number
): void {
  // client_instance_id (UUID)
  writer.writeUuid(request.clientInstanceId)

  // tagged fields (all versions are flexible)
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed GetTelemetrySubscriptions request ready to send to a broker.
 */
export function buildGetTelemetrySubscriptionsRequest(
  correlationId: number,
  apiVersion: number,
  request: GetTelemetrySubscriptionsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.GetTelemetrySubscriptions,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeGetTelemetrySubscriptionsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a GetTelemetrySubscriptions response body.
 */
export function decodeGetTelemetrySubscriptionsResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<GetTelemetrySubscriptionsResponse> {
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

  // client_instance_id (UUID)
  const clientIdResult = reader.readUuid()
  if (!clientIdResult.ok) {
    return clientIdResult
  }

  // subscription_id (INT32)
  const subscriptionIdResult = reader.readInt32()
  if (!subscriptionIdResult.ok) {
    return subscriptionIdResult
  }

  // accepted_compression_types (COMPACT_ARRAY of INT8)
  const compressionResult = decodeInt8Array(reader)
  if (!compressionResult.ok) {
    return compressionResult
  }

  // push_interval_ms (INT32)
  const pushIntervalResult = reader.readInt32()
  if (!pushIntervalResult.ok) {
    return pushIntervalResult
  }

  // telemetry_max_bytes (INT32)
  const maxBytesResult = reader.readInt32()
  if (!maxBytesResult.ok) {
    return maxBytesResult
  }

  // delta_temporality (BOOLEAN)
  const deltaResult = reader.readBoolean()
  if (!deltaResult.ok) {
    return deltaResult
  }

  // requested_metrics (COMPACT_ARRAY of COMPACT_STRING)
  const metricsResult = decodeStringArray(reader)
  if (!metricsResult.ok) {
    return metricsResult
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
      clientInstanceId: clientIdResult.value,
      subscriptionId: subscriptionIdResult.value,
      acceptedCompressionTypes: compressionResult.value,
      pushIntervalMs: pushIntervalResult.value,
      telemetryMaxBytes: maxBytesResult.value,
      deltaTemporality: deltaResult.value,
      requestedMetrics: metricsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeInt8Array(reader: BinaryReader): DecodeResult<number[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const items: number[] = []
  for (let i = 0; i < count; i++) {
    const itemResult = reader.readInt8()
    if (!itemResult.ok) {
      return itemResult
    }
    items.push(itemResult.value)
  }

  return decodeSuccess(items, reader.offset - startOffset)
}

function decodeStringArray(reader: BinaryReader): DecodeResult<string[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const items: string[] = []
  for (let i = 0; i < count; i++) {
    const itemResult = reader.readCompactString()
    if (!itemResult.ok) {
      return itemResult
    }
    items.push(itemResult.value ?? "")
  }

  return decodeSuccess(items, reader.offset - startOffset)
}
