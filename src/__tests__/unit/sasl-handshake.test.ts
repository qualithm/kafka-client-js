import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  buildSaslHandshakeRequest,
  decodeSaslHandshakeResponse,
  encodeSaslHandshakeRequest,
  type SaslHandshakeRequest
} from "../../sasl-handshake"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeSaslHandshakeRequest", () => {
  describe("v0 — mechanism string", () => {
    it("encodes the mechanism", () => {
      const writer = new BinaryWriter()
      const request: SaslHandshakeRequest = {
        mechanism: "PLAIN"
      }
      encodeSaslHandshakeRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const mechanismResult = reader.readString()
      expect(mechanismResult.ok).toBe(true)
      expect(mechanismResult.ok && mechanismResult.value).toBe("PLAIN")

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v1 — same wire format as v0", () => {
    it("encodes SCRAM-SHA-256 mechanism", () => {
      const writer = new BinaryWriter()
      const request: SaslHandshakeRequest = {
        mechanism: "SCRAM-SHA-256"
      }
      encodeSaslHandshakeRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const mechanismResult = reader.readString()
      expect(mechanismResult.ok).toBe(true)
      expect(mechanismResult.ok && mechanismResult.value).toBe("SCRAM-SHA-256")

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// buildSaslHandshakeRequest (framed)
// ---------------------------------------------------------------------------

describe("buildSaslHandshakeRequest", () => {
  it("builds a framed v0 request", () => {
    const request: SaslHandshakeRequest = {
      mechanism: "PLAIN"
    }
    const framed = buildSaslHandshakeRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.SaslHandshake)
  })

  it("builds a framed v1 request", () => {
    const request: SaslHandshakeRequest = {
      mechanism: "SCRAM-SHA-512"
    }
    const framed = buildSaslHandshakeRequest(42, 1, request, "my-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.SaslHandshake)

    const versionResult = reader.readInt16()
    expect(versionResult.ok).toBe(true)
    expect(versionResult.ok && versionResult.value).toBe(1)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeSaslHandshakeResponse", () => {
  function buildResponse(errorCode: number, mechanisms: string[]): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt16(errorCode)
    w.writeInt32(mechanisms.length)
    for (const m of mechanisms) {
      w.writeString(m)
    }
    return w.finish()
  }

  describe("v0 — successful handshake", () => {
    it("decodes a successful response with mechanisms", () => {
      const body = buildResponse(0, ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
      const reader = new BinaryReader(body)
      const result = decodeSaslHandshakeResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.mechanisms).toEqual(["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
    })

    it("decodes a response with a single mechanism", () => {
      const body = buildResponse(0, ["PLAIN"])
      const reader = new BinaryReader(body)
      const result = decodeSaslHandshakeResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.mechanisms).toEqual(["PLAIN"])
    })
  })

  describe("v0 — unsupported mechanism error", () => {
    it("decodes an error response with error code 33", () => {
      const body = buildResponse(33, ["PLAIN"])
      const reader = new BinaryReader(body)
      const result = decodeSaslHandshakeResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(33)
      expect(result.value.mechanisms).toEqual(["PLAIN"])
    })
  })

  describe("v1 — same wire format as v0", () => {
    it("decodes a v1 response", () => {
      const body = buildResponse(0, ["SCRAM-SHA-256"])
      const reader = new BinaryReader(body)
      const result = decodeSaslHandshakeResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.mechanisms).toEqual(["SCRAM-SHA-256"])
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeSaslHandshakeResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure when mechanisms array is truncated", () => {
      const w = new BinaryWriter()
      w.writeInt16(0) // error_code
      w.writeInt32(2) // says 2 mechanisms but provides none
      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeSaslHandshakeResponse(reader, 0)
      expect(result.ok).toBe(false)
    })
  })
})
