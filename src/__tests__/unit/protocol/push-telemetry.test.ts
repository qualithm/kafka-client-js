import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildPushTelemetryRequest,
  decodePushTelemetryResponse,
  encodePushTelemetryRequest,
  type PushTelemetryRequest
} from "../../../protocol/push-telemetry"

const SAMPLE_UUID = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodePushTelemetryRequest", () => {
  it("encodes v0 request with uncompressed metrics", () => {
    const metrics = new Uint8Array([0x0a, 0x0b, 0x0c])
    const writer = new BinaryWriter()
    const request: PushTelemetryRequest = {
      clientInstanceId: SAMPLE_UUID,
      subscriptionId: 7,
      terminating: false,
      compressionType: 0,
      metrics
    }
    encodePushTelemetryRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // UUID
    const uuidResult = reader.readUuid()
    expect(uuidResult.ok).toBe(true)
    if (uuidResult.ok) {
      expect(uuidResult.value).toEqual(SAMPLE_UUID)
    }

    // subscription_id (INT32)
    const subResult = reader.readInt32()
    expect(subResult.ok).toBe(true)
    if (subResult.ok) {
      expect(subResult.value).toBe(7)
    }

    // terminating (BOOLEAN)
    const termResult = reader.readBoolean()
    expect(termResult.ok).toBe(true)
    if (termResult.ok) {
      expect(termResult.value).toBe(false)
    }

    // compression_type (INT8)
    const compResult = reader.readInt8()
    expect(compResult.ok).toBe(true)
    if (compResult.ok) {
      expect(compResult.value).toBe(0)
    }

    // metrics (COMPACT_BYTES)
    const metricsResult = reader.readCompactBytes()
    expect(metricsResult.ok).toBe(true)
    if (metricsResult.ok) {
      expect(metricsResult.value).toEqual(metrics)
    }

    // tagged fields
    const tagResult = reader.readTaggedFields()
    expect(tagResult.ok).toBe(true)
    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 request with terminating flag", () => {
    const writer = new BinaryWriter()
    const request: PushTelemetryRequest = {
      clientInstanceId: SAMPLE_UUID,
      subscriptionId: 1,
      terminating: true,
      compressionType: 1,
      metrics: new Uint8Array(0)
    }
    encodePushTelemetryRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    reader.readUuid() // skip UUID
    reader.readInt32() // skip subscription_id

    const termResult = reader.readBoolean()
    expect(termResult.ok).toBe(true)
    if (termResult.ok) {
      expect(termResult.value).toBe(true)
    }

    const compResult = reader.readInt8()
    expect(compResult.ok).toBe(true)
    if (compResult.ok) {
      expect(compResult.value).toBe(1)
    }
  })
})

// ---------------------------------------------------------------------------
// buildPushTelemetryRequest (framed)
// ---------------------------------------------------------------------------

describe("buildPushTelemetryRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildPushTelemetryRequest(
      99,
      0,
      {
        clientInstanceId: SAMPLE_UUID,
        subscriptionId: 1,
        terminating: false,
        compressionType: 0,
        metrics: new Uint8Array([0x01])
      },
      "test-client"
    )
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    if (apiKeyResult.ok) {
      expect(apiKeyResult.value).toBe(ApiKey.PushTelemetry)
    }
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodePushTelemetryResponse", () => {
  it("decodes v0 success response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodePushTelemetryResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.errorCode).toBe(0)
  })

  it("decodes v0 response with throttle", () => {
    const w = new BinaryWriter()
    w.writeInt32(500) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodePushTelemetryResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(500)
    expect(result.value.errorCode).toBe(0)
  })

  it("decodes error response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(86) // UNKNOWN_SUBSCRIPTION_ID or similar
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodePushTelemetryResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.errorCode).toBe(86)
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodePushTelemetryResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on empty input", () => {
      const reader = new BinaryReader(new Uint8Array(0))
      const result = decodePushTelemetryResponse(reader, 0)
      expect(result.ok).toBe(false)
    })
  })
})
