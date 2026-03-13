import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildUpdateFeaturesRequest,
  decodeUpdateFeaturesResponse,
  encodeUpdateFeaturesRequest,
  type UpdateFeaturesRequest
} from "../../../protocol/update-features"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeUpdateFeaturesRequest", () => {
  it("encodes v0 request with feature updates", () => {
    const writer = new BinaryWriter()
    const request: UpdateFeaturesRequest = {
      timeoutMs: 30000,
      featureUpdates: [{ feature: "metadata.version", maxVersionLevel: 17 }]
    }
    encodeUpdateFeaturesRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // timeout_ms
    const timeout = reader.readInt32()
    expect(timeout.ok && timeout.value).toBe(30000)

    // feature_updates (compact: 1 + 1)
    const count = reader.readUnsignedVarInt()
    expect(count.ok && count.value).toBe(2)

    // feature name
    const name = reader.readCompactString()
    expect(name.ok && name.value).toBe("metadata.version")

    // max_version_level (INT16)
    const level = reader.readInt16()
    expect(level.ok && level.value).toBe(17)

    // allow_downgrade (BOOLEAN, v0)
    const downgrade = reader.readBoolean()
    expect(downgrade.ok && downgrade.value).toBe(false)

    // feature tagged fields
    reader.readTaggedFields()

    // request tagged fields
    const tag = reader.readTaggedFields()
    expect(tag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 request with allow_downgrade true", () => {
    const writer = new BinaryWriter()
    const request: UpdateFeaturesRequest = {
      timeoutMs: 5000,
      featureUpdates: [{ feature: "f1", maxVersionLevel: 3, upgradeType: 3 }]
    }
    encodeUpdateFeaturesRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    reader.readInt32() // timeout_ms
    reader.readUnsignedVarInt() // feature_updates count
    reader.readCompactString() // feature name
    reader.readInt16() // max_version_level

    // allow_downgrade (BOOLEAN, v0) — upgradeType 3 means true
    const downgrade = reader.readBoolean()
    expect(downgrade.ok && downgrade.value).toBe(true)
  })

  it("encodes v1 request with validate_only", () => {
    const writer = new BinaryWriter()
    const request: UpdateFeaturesRequest = {
      timeoutMs: 10000,
      featureUpdates: [{ feature: "kraft.version", maxVersionLevel: 1, upgradeType: 2 }],
      validateOnly: true
    }
    encodeUpdateFeaturesRequest(writer, request, 1)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // timeout_ms
    reader.readInt32()

    // feature_updates
    reader.readUnsignedVarInt()
    reader.readCompactString()
    reader.readInt16()

    // upgrade_type (INT8, v1)
    const upgradeType = reader.readInt8()
    expect(upgradeType.ok && upgradeType.value).toBe(2)

    // feature tagged fields
    reader.readTaggedFields()

    // validate_only (BOOLEAN, v1)
    const validateOnly = reader.readBoolean()
    expect(validateOnly.ok && validateOnly.value).toBe(true)

    // request tagged fields
    reader.readTaggedFields()

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// buildUpdateFeaturesRequest (framed)
// ---------------------------------------------------------------------------

describe("buildUpdateFeaturesRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildUpdateFeaturesRequest(
      1,
      0,
      { timeoutMs: 30000, featureUpdates: [] },
      "test-client"
    )
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.UpdateFeatures)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeUpdateFeaturesResponse", () => {
  it("decodes v0 response with results", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    // results (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // result entry
    w.writeCompactString("metadata.version") // feature
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeTaggedFields([]) // result tagged
    // end results
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeUpdateFeaturesResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.errorCode).toBe(0)
    expect(result.value.errorMessage).toBeNull()
    expect(result.value.results).toHaveLength(1)
    expect(result.value.results[0].feature).toBe("metadata.version")
    expect(result.value.results[0].errorCode).toBe(0)
    expect(result.value.results[0].errorMessage).toBeNull()
  })

  it("decodes error response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(1) // NONE (top-level error)
    w.writeCompactString("feature update failed") // error_message
    // results (compact: 0 + 1)
    w.writeUnsignedVarInt(1)
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeUpdateFeaturesResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.errorCode).toBe(1)
    expect(result.value.errorMessage).toBe("feature update failed")
    expect(result.value.results).toHaveLength(0)
  })

  it("decodes response with multiple results including errors", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    // results (compact: 2 + 1)
    w.writeUnsignedVarInt(3)
    // result 1: success
    w.writeCompactString("metadata.version")
    w.writeInt16(0)
    w.writeCompactString(null)
    w.writeTaggedFields([])
    // result 2: error
    w.writeCompactString("kraft.version")
    w.writeInt16(40) // FEATURE_UPDATE_FAILED
    w.writeCompactString("downgrade not allowed")
    w.writeTaggedFields([])
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeUpdateFeaturesResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.results).toHaveLength(2)
    expect(result.value.results[0].feature).toBe("metadata.version")
    expect(result.value.results[0].errorCode).toBe(0)
    expect(result.value.results[1].feature).toBe("kraft.version")
    expect(result.value.results[1].errorCode).toBe(40)
    expect(result.value.results[1].errorMessage).toBe("downgrade not allowed")
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeUpdateFeaturesResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated feature result", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeCompactString(null) // error_message
      w.writeUnsignedVarInt(2) // 1 result + 1
      w.writeCompactString("metadata.version") // feature name
      // Missing error_code and error_message

      const reader = new BinaryReader(w.finish())
      const result = decodeUpdateFeaturesResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated error_message", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      // Missing error_message

      const reader = new BinaryReader(w.finish())
      const result = decodeUpdateFeaturesResponse(reader, 0)
      expect(result.ok).toBe(false)
    })
  })
})
