import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildRenewDelegationTokenRequest,
  decodeRenewDelegationTokenResponse,
  encodeRenewDelegationTokenRequest,
  type RenewDelegationTokenRequest
} from "../../../protocol/renew-delegation-token"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeRenewDelegationTokenRequest", () => {
  describe("v0–v1 — non-flexible encoding", () => {
    it("encodes hmac and renew period", () => {
      const writer = new BinaryWriter()
      const request: RenewDelegationTokenRequest = {
        hmac: new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd]),
        renewPeriodMs: 3600000n
      }
      encodeRenewDelegationTokenRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // hmac (BYTES)
      const hmac = reader.readBytes()
      expect(hmac.ok).toBe(true)
      if (hmac.ok) {
        expect(hmac.value).toEqual(new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd]))
      }

      // renew_period_ms (INT64)
      const renewPeriod = reader.readInt64()
      expect(renewPeriod.ok && renewPeriod.value).toBe(3600000n)

      expect(reader.remaining).toBe(0)
    })

    it("encodes empty hmac", () => {
      const writer = new BinaryWriter()
      const request: RenewDelegationTokenRequest = {
        hmac: new Uint8Array(0),
        renewPeriodMs: 0n
      }
      encodeRenewDelegationTokenRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const hmac = reader.readBytes()
      expect(hmac.ok).toBe(true)
      if (hmac.ok) {
        expect(hmac.value).toEqual(new Uint8Array(0))
      }

      const renewPeriod = reader.readInt64()
      expect(renewPeriod.ok && renewPeriod.value).toBe(0n)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("encodes with compact bytes and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: RenewDelegationTokenRequest = {
        hmac: new Uint8Array([0x01, 0x02, 0x03]),
        renewPeriodMs: 7200000n
      }
      encodeRenewDelegationTokenRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // hmac (COMPACT_BYTES)
      const hmac = reader.readCompactBytes()
      expect(hmac.ok).toBe(true)
      if (hmac.ok) {
        expect(hmac.value).toEqual(new Uint8Array([0x01, 0x02, 0x03]))
      }

      // renew_period_ms (INT64)
      const renewPeriod = reader.readInt64()
      expect(renewPeriod.ok && renewPeriod.value).toBe(7200000n)

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

describe("buildRenewDelegationTokenRequest", () => {
  it("produces a framed request with correct header", () => {
    const request: RenewDelegationTokenRequest = {
      hmac: new Uint8Array([0xaa, 0xbb]),
      renewPeriodMs: 3600000n
    }
    const frame = buildRenewDelegationTokenRequest(10, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.RenewDelegationToken)

    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(0)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(10)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeRenewDelegationTokenResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes a successful response", () => {
      const writer = new BinaryWriter()

      writer.writeInt16(0) // error_code
      writer.writeInt64(1700086400000n) // expiry_timestamp_ms
      writer.writeInt32(25) // throttle_time_ms

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeRenewDelegationTokenResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.expiryTimestampMs).toBe(1700086400000n)
      expect(result.value.throttleTimeMs).toBe(25)
    })

    it("decodes an error response", () => {
      const writer = new BinaryWriter()

      writer.writeInt16(65) // DELEGATION_TOKEN_NOT_FOUND
      writer.writeInt64(0n)
      writer.writeInt32(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeRenewDelegationTokenResponse(reader, 1)

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
      writer.writeInt32(10)
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeRenewDelegationTokenResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.expiryTimestampMs).toBe(1700172800000n)
      expect(result.value.throttleTimeMs).toBe(10)
      expect(result.value.taggedFields).toEqual([])
    })
  })

  it("returns failure on truncated input", () => {
    const reader = new BinaryReader(new Uint8Array([0, 0]))
    const result = decodeRenewDelegationTokenResponse(reader, 0)
    expect(result.ok).toBe(false)
  })
})
