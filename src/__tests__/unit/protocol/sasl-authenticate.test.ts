import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildSaslAuthenticateRequest,
  decodeSaslAuthenticateResponse,
  encodeSaslAuthenticateRequest,
  type SaslAuthenticateRequest
} from "../../../protocol/sasl-authenticate"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeSaslAuthenticateRequest", () => {
  describe("v0 — non-flexible", () => {
    it("encodes auth bytes", () => {
      const writer = new BinaryWriter()
      const authBytes = new Uint8Array([0, 117, 115, 101, 114, 0, 112, 97, 115, 115]) // \0user\0pass
      const request: SaslAuthenticateRequest = { authBytes }
      encodeSaslAuthenticateRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const bytesResult = reader.readBytes()
      expect(bytesResult.ok).toBe(true)
      if (!bytesResult.ok) {
        return
      }
      expect(bytesResult.value).toEqual(authBytes)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v1 — same as v0 (non-flexible)", () => {
    it("encodes auth bytes", () => {
      const writer = new BinaryWriter()
      const authBytes = new Uint8Array([1, 2, 3, 4])
      const request: SaslAuthenticateRequest = { authBytes }
      encodeSaslAuthenticateRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const bytesResult = reader.readBytes()
      expect(bytesResult.ok).toBe(true)
      if (!bytesResult.ok) {
        return
      }
      expect(bytesResult.value).toEqual(authBytes)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("encodes with compact bytes and tagged fields", () => {
      const writer = new BinaryWriter()
      const authBytes = new Uint8Array([10, 20, 30])
      const request: SaslAuthenticateRequest = { authBytes }
      encodeSaslAuthenticateRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // compact bytes
      const bytesResult = reader.readCompactBytes()
      expect(bytesResult.ok).toBe(true)
      if (!bytesResult.ok) {
        return
      }
      expect(bytesResult.value).toEqual(authBytes)

      // tagged fields
      const tagResult = reader.readTaggedFields()
      expect(tagResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// buildSaslAuthenticateRequest (framed)
// ---------------------------------------------------------------------------

describe("buildSaslAuthenticateRequest", () => {
  it("builds a framed v0 request", () => {
    const request: SaslAuthenticateRequest = {
      authBytes: new Uint8Array([0, 117, 0, 112])
    }
    const framed = buildSaslAuthenticateRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.SaslAuthenticate)
  })

  it("builds a framed v1 request", () => {
    const request: SaslAuthenticateRequest = {
      authBytes: new Uint8Array([1, 2, 3])
    }
    const framed = buildSaslAuthenticateRequest(42, 1, request, "my-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.SaslAuthenticate)

    const versionResult = reader.readInt16()
    expect(versionResult.ok).toBe(true)
    expect(versionResult.ok && versionResult.value).toBe(1)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeSaslAuthenticateResponse", () => {
  function buildResponseV0(
    errorCode: number,
    errorMessage: string | null,
    authBytes: Uint8Array
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt16(errorCode)
    w.writeString(errorMessage)
    w.writeBytes(authBytes)
    return w.finish()
  }

  function buildResponseV1(
    errorCode: number,
    errorMessage: string | null,
    authBytes: Uint8Array,
    sessionLifetimeMs: bigint
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt16(errorCode)
    w.writeString(errorMessage)
    w.writeBytes(authBytes)
    w.writeInt64(sessionLifetimeMs)
    return w.finish()
  }

  describe("v0 — non-flexible", () => {
    it("decodes a successful response", () => {
      const authBytes = new Uint8Array([10, 20, 30])
      const body = buildResponseV0(0, null, authBytes)
      const reader = new BinaryReader(body)
      const result = decodeSaslAuthenticateResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.errorMessage).toBe(null)
      expect(result.value.authBytes).toEqual(authBytes)
      expect(result.value.sessionLifetimeMs).toBe(0n)
    })

    it("decodes an error response", () => {
      const body = buildResponseV0(58, "authentication failed", new Uint8Array(0))
      const reader = new BinaryReader(body)
      const result = decodeSaslAuthenticateResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(58)
      expect(result.value.errorMessage).toBe("authentication failed")
    })
  })

  describe("v1 — with session lifetime", () => {
    it("decodes session_lifetime_ms", () => {
      const authBytes = new Uint8Array([1, 2])
      const body = buildResponseV1(0, null, authBytes, 3600000n)
      const reader = new BinaryReader(body)
      const result = decodeSaslAuthenticateResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.sessionLifetimeMs).toBe(3600000n)
      expect(result.value.authBytes).toEqual(authBytes)
    })
  })

  describe("v2 — flexible encoding with tagged fields", () => {
    it("decodes a v2 response", () => {
      const w = new BinaryWriter()
      w.writeInt16(0) // error_code
      w.writeCompactString(null) // error_message (compact nullable)
      w.writeCompactBytes(new Uint8Array([5, 6])) // auth_bytes (compact)
      w.writeInt64(7200000n) // session_lifetime_ms
      w.writeUnsignedVarInt(0) // empty tagged fields
      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeSaslAuthenticateResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.errorMessage).toBe(null)
      expect(result.value.authBytes).toEqual(new Uint8Array([5, 6]))
      expect(result.value.sessionLifetimeMs).toBe(7200000n)
      expect(result.value.taggedFields).toHaveLength(0)
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeSaslAuthenticateResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v1 input", () => {
      const w = new BinaryWriter()
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message
      w.writeBytes(new Uint8Array(0)) // auth_bytes
      // missing session_lifetime_ms
      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeSaslAuthenticateResponse(reader, 1)
      expect(result.ok).toBe(false)
    })
  })
})
