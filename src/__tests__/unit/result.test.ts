import { describe, expect, it } from "vitest"

import { decodeFailure, type DecodeResult, decodeSuccess } from "../../result"

describe("DecodeResult", () => {
  describe("decodeSuccess", () => {
    it("creates a success result with value and bytesRead", () => {
      const result = decodeSuccess(42, 4)

      expect(result.ok).toBe(true)
      expect(result.value).toBe(42)
      expect(result.bytesRead).toBe(4)
    })

    it("works with complex value types", () => {
      const value = { topic: "test", partition: 0 }
      const result = decodeSuccess(value, 16)

      expect(result.ok).toBe(true)
      expect(result.value).toEqual({ topic: "test", partition: 0 })
      expect(result.bytesRead).toBe(16)
    })

    it("works with null value", () => {
      const result = decodeSuccess(null, 1)

      expect(result.ok).toBe(true)
      expect(result.value).toBeNull()
      expect(result.bytesRead).toBe(1)
    })
  })

  describe("decodeFailure", () => {
    it("creates a failure result with code and message", () => {
      const result = decodeFailure("BUFFER_UNDERFLOW", "need 4 bytes, got 2")

      expect(result.ok).toBe(false)
      expect(result.error.code).toBe("BUFFER_UNDERFLOW")
      expect(result.error.message).toBe("need 4 bytes, got 2")
      expect(result.error.offset).toBeUndefined()
    })

    it("includes byte offset when provided", () => {
      const result = decodeFailure("INVALID_DATA", "unexpected magic byte", 8)

      expect(result.ok).toBe(false)
      expect(result.error.code).toBe("INVALID_DATA")
      expect(result.error.offset).toBe(8)
    })

    it("supports all error codes", () => {
      const codes = [
        "BUFFER_UNDERFLOW",
        "INVALID_DATA",
        "UNSUPPORTED_VERSION",
        "CRC_MISMATCH",
        "INVALID_VARINT"
      ] as const

      for (const code of codes) {
        const result = decodeFailure(code, `error: ${code}`)
        expect(result.error.code).toBe(code)
      }
    })
  })

  describe("type narrowing", () => {
    function asResult<T>(result: DecodeResult<T>): DecodeResult<T> {
      return result
    }

    it("narrows to success when ok is true", () => {
      const result = asResult<number>(decodeSuccess(42, 4))

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value).toBe(42)
        expect(result.bytesRead).toBe(4)
      }
    })

    it("narrows to failure when ok is false", () => {
      const result = asResult<number>(decodeFailure("BUFFER_UNDERFLOW", "truncated"))

      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("BUFFER_UNDERFLOW")
      }
    })
  })
})
