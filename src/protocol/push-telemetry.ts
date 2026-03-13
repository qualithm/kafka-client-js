/**
 * PushTelemetry request/response encoding and decoding.
 *
 * The PushTelemetry API (key 72) allows clients to push collected
 * telemetry metrics to the broker (KIP-714).
 *
 * **Request versions:**
 * - v0: client_instance_id (UUID), subscription_id (INT32), terminating (BOOLEAN),
 *        compression_type (INT8), metrics (COMPACT_BYTES)
 *
 * **Response versions:**
 * - v0: throttle_time_ms (INT32), error_code (INT16)
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
 * PushTelemetry request payload.
 */
export type PushTelemetryRequest = {
  /** Client instance ID (UUID, 16 bytes). */
  readonly clientInstanceId: Uint8Array
  /** Subscription ID from GetTelemetrySubscriptions. */
  readonly subscriptionId: number
  /** Whether this is the final push before the client shuts down. */
  readonly terminating: boolean
  /** Compression type used for the metrics payload (0 = none). */
  readonly compressionType: number
  /** Serialised metrics payload (OTLP format). */
  readonly metrics: Uint8Array
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * PushTelemetry response payload.
 */
export type PushTelemetryResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a PushTelemetry request body into the given writer.
 */
export function encodePushTelemetryRequest(
  writer: BinaryWriter,
  request: PushTelemetryRequest,
  _apiVersion: number
): void {
  // client_instance_id (UUID)
  writer.writeUuid(request.clientInstanceId)

  // subscription_id (INT32)
  writer.writeInt32(request.subscriptionId)

  // terminating (BOOLEAN)
  writer.writeBoolean(request.terminating)

  // compression_type (INT8)
  writer.writeInt8(request.compressionType)

  // metrics (COMPACT_BYTES)
  writer.writeCompactBytes(request.metrics)

  // tagged fields (all versions are flexible)
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed PushTelemetry request ready to send to a broker.
 */
export function buildPushTelemetryRequest(
  correlationId: number,
  apiVersion: number,
  request: PushTelemetryRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.PushTelemetry,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodePushTelemetryRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a PushTelemetry response body.
 */
export function decodePushTelemetryResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<PushTelemetryResponse> {
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

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      throttleTimeMs: throttleResult.value,
      errorCode: errorCodeResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
