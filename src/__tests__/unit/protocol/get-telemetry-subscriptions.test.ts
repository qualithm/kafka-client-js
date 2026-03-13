import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildGetTelemetrySubscriptionsRequest,
  decodeGetTelemetrySubscriptionsResponse,
  encodeGetTelemetrySubscriptionsRequest,
  type GetTelemetrySubscriptionsRequest
} from "../../../protocol/get-telemetry-subscriptions"

const ZERO_UUID = new Uint8Array(16)

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeGetTelemetrySubscriptionsRequest", () => {
  it("encodes v0 request with zero UUID", () => {
    const writer = new BinaryWriter()
    const request: GetTelemetrySubscriptionsRequest = {
      clientInstanceId: ZERO_UUID
    }
    encodeGetTelemetrySubscriptionsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // UUID (16 bytes)
    const uuidResult = reader.readUuid()
    expect(uuidResult.ok).toBe(true)
    if (uuidResult.ok) {
      expect(uuidResult.value).toEqual(ZERO_UUID)
    }

    // tagged fields
    const tagResult = reader.readTaggedFields()
    expect(tagResult.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 request with non-zero UUID", () => {
    const uuid = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    const writer = new BinaryWriter()
    const request: GetTelemetrySubscriptionsRequest = {
      clientInstanceId: uuid
    }
    encodeGetTelemetrySubscriptionsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    const uuidResult = reader.readUuid()
    expect(uuidResult.ok).toBe(true)
    if (uuidResult.ok) {
      expect(uuidResult.value).toEqual(uuid)
    }

    const tagResult = reader.readTaggedFields()
    expect(tagResult.ok).toBe(true)
    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// buildGetTelemetrySubscriptionsRequest (framed)
// ---------------------------------------------------------------------------

describe("buildGetTelemetrySubscriptionsRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildGetTelemetrySubscriptionsRequest(
      42,
      0,
      { clientInstanceId: ZERO_UUID },
      "test-client"
    )
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    if (apiKeyResult.ok) {
      expect(apiKeyResult.value).toBe(ApiKey.GetTelemetrySubscriptions)
    }
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeGetTelemetrySubscriptionsResponse", () => {
  it("decodes v0 response with metrics and compression types", () => {
    const clientUuid = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeUuid(clientUuid) // client_instance_id
    w.writeInt32(1) // subscription_id
    // accepted_compression_types (compact array: count + 1)
    w.writeUnsignedVarInt(3) // 2 items + 1
    w.writeInt8(0) // none
    w.writeInt8(1) // gzip
    // push_interval_ms
    w.writeInt32(5000)
    // telemetry_max_bytes
    w.writeInt32(1048576)
    // delta_temporality
    w.writeBoolean(true)
    // requested_metrics (compact array: count + 1)
    w.writeUnsignedVarInt(3) // 2 items + 1
    w.writeCompactString("org.apache.kafka.producer.")
    w.writeCompactString("org.apache.kafka.consumer.")
    // tagged fields
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeGetTelemetrySubscriptionsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.errorCode).toBe(0)
    expect(result.value.clientInstanceId).toEqual(clientUuid)
    expect(result.value.subscriptionId).toBe(1)
    expect(result.value.acceptedCompressionTypes).toEqual([0, 1])
    expect(result.value.pushIntervalMs).toBe(5000)
    expect(result.value.telemetryMaxBytes).toBe(1048576)
    expect(result.value.deltaTemporality).toBe(true)
    expect(result.value.requestedMetrics).toEqual([
      "org.apache.kafka.producer.",
      "org.apache.kafka.consumer."
    ])
  })

  it("decodes v0 response with empty metrics (all metrics requested)", () => {
    const w = new BinaryWriter()
    w.writeInt32(10) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeUuid(ZERO_UUID) // client_instance_id
    w.writeInt32(42) // subscription_id
    w.writeUnsignedVarInt(1) // 0 compression types + 1
    w.writeInt32(60000) // push_interval_ms
    w.writeInt32(524288) // telemetry_max_bytes
    w.writeBoolean(false) // delta_temporality
    w.writeUnsignedVarInt(1) // 0 metrics + 1
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeGetTelemetrySubscriptionsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(10)
    expect(result.value.subscriptionId).toBe(42)
    expect(result.value.acceptedCompressionTypes).toEqual([])
    expect(result.value.pushIntervalMs).toBe(60000)
    expect(result.value.telemetryMaxBytes).toBe(524288)
    expect(result.value.deltaTemporality).toBe(false)
    expect(result.value.requestedMetrics).toEqual([])
  })

  it("decodes error response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(87) // TELEMETRY_TOO_LARGE or similar
    w.writeUuid(ZERO_UUID) // client_instance_id
    w.writeInt32(0) // subscription_id
    w.writeUnsignedVarInt(1) // 0 compression types + 1
    w.writeInt32(0) // push_interval_ms
    w.writeInt32(0) // telemetry_max_bytes
    w.writeBoolean(false) // delta_temporality
    w.writeUnsignedVarInt(1) // 0 metrics + 1
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeGetTelemetrySubscriptionsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.errorCode).toBe(87)
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeGetTelemetrySubscriptionsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated compression array", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeUuid(ZERO_UUID) // client_instance_id
      w.writeInt32(1) // subscription_id
      w.writeUnsignedVarInt(100) // claims 99 items but buffer ends

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeGetTelemetrySubscriptionsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })
  })
})
