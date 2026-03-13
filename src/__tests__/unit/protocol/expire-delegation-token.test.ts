import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildExpireDelegationTokenRequest,
  decodeExpireDelegationTokenResponse,
  encodeExpireDelegationTokenRequest,
  type ExpireDelegationTokenRequest
} from "../../../protocol/expire-delegation-token"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeExpireDelegationTokenRequest", () => {
  describe("v0–v1 — non-flexible encoding", () => {
    it("encodes hmac and expiry time period", () => {
      const writer = new BinaryWriter()
      const request: ExpireDelegationTokenRequest = {
        hmac: new Uint8Array([0x11, 0x22, 0x33]),
        expiryTimePeriodMs: 7200000n
      }
      encodeExpireDelegationTokenRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // hmac (BYTES)
      const hmac = reader.readBytes()
      expect(hmac.ok).toBe(true)
      if (hmac.ok) {
        expect(hmac.value).toEqual(new Uint8Array([0x11, 0x22, 0x33]))
      }

      // expiry_time_period_ms (INT64)
      const expiryPeriod = reader.readInt64()
      expect(expiryPeriod.ok && expiryPeriod.value).toBe(7200000n)

      expect(reader.remaining).toBe(0)
    })

    it("encodes negative expiry to expire immediately", () => {
      const writer = new BinaryWriter()
      const request: ExpireDelegationTokenRequest = {
        hmac: new Uint8Array([0xff]),
        expiryTimePeriodMs: -1n
      }
      encodeExpireDelegationTokenRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const hmac = reader.readBytes()
      expect(hmac.ok).toBe(true)

      const expiryPeriod = reader.readInt64()
      expect(expiryPeriod.ok && expiryPeriod.value).toBe(-1n)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("encodes with compact bytes and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: ExpireDelegationTokenRequest = {
        hmac: new Uint8Array([0xaa, 0xbb]),
        expiryTimePeriodMs: 3600000n
      }
      encodeExpireDelegationTokenRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // hmac (COMPACT_BYTES)
      const hmac = reader.readCompactBytes()
      expect(hmac.ok).toBe(true)
      if (hmac.ok) {
        expect(hmac.value).toEqual(new Uint8Array([0xaa, 0xbb]))
      }

      // expiry_time_period_ms (INT64)
      const expiryPeriod = reader.readInt64()
      expect(expiryPeriod.ok && expiryPeriod.value).toBe(3600000n)

      // tagged fields
      const tag = reader.readTaggedFields()
      expect(tag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

describe("buildExpireDelegationTokenRequest", () => {
  it("produces a framed request with correct header", () => {
    const request: ExpireDelegationTokenRequest = {
      hmac: new Uint8Array([0xaa]),
      expiryTimePeriodMs: -1n
    }
    const frame = buildExpireDelegationTokenRequest(55, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.ExpireDelegationToken)

    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(0)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(55)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeExpireDelegationTokenResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes a successful response", () => {
      const writer = new BinaryWriter()

      writer.writeInt16(0) // error_code
      writer.writeInt64(1700086400000n) // expiry_timestamp_ms
      writer.writeInt32(30) // throttle_time_ms

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeExpireDelegationTokenResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.expiryTimestampMs).toBe(1700086400000n)
      expect(result.value.throttleTimeMs).toBe(30)
    })

    it("decodes an error response", () => {
      const writer = new BinaryWriter()

      writer.writeInt16(65) // DELEGATION_TOKEN_NOT_FOUND
      writer.writeInt64(0n)
      writer.writeInt32(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeExpireDelegationTokenResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(65)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("decodes with tagged fields", () => {
      const writer = new BinaryWriter()

      writer.writeInt16(0)
      writer.writeInt64(1700172800000n)
      writer.writeInt32(15)
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeExpireDelegationTokenResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.expiryTimestampMs).toBe(1700172800000n)
      expect(result.value.throttleTimeMs).toBe(15)
      expect(result.value.taggedFields).toEqual([])
    })
  })

  it("returns failure on truncated input", () => {
    const reader = new BinaryReader(new Uint8Array([0, 0]))
    const result = decodeExpireDelegationTokenResponse(reader, 0)
    expect(result.ok).toBe(false)
  })
})
