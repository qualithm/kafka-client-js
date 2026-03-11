/**
 * ApiVersions (API key 18) request/response tests.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
 */

import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  type ApiVersionsRequest,
  type ApiVersionsResponse,
  apiVersionsToMap,
  buildApiVersionsRequest,
  decodeApiVersionsResponse,
  encodeApiVersionsRequest
} from "../../../protocol/api-versions"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeApiVersionsRequest", () => {
  describe("v0–v2 — empty body", () => {
    it("encodes empty body for v0", () => {
      const writer = new BinaryWriter()
      encodeApiVersionsRequest(writer, {}, 0)
      expect(writer.offset).toBe(0)
    })

    it("encodes empty body for v2", () => {
      const writer = new BinaryWriter()
      encodeApiVersionsRequest(writer, {}, 2)
      expect(writer.offset).toBe(0)
    })
  })

  describe("v3 — flexible with client software info", () => {
    it("encodes client software name and version", () => {
      const writer = new BinaryWriter()
      const request: ApiVersionsRequest = {
        clientSoftwareName: "kafka-client-js",
        clientSoftwareVersion: "1.0.0"
      }

      encodeApiVersionsRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Client software name (compact string)
      const nameResult = reader.readCompactString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("kafka-client-js")

      // Client software version (compact string)
      const versionResult = reader.readCompactString()
      expect(versionResult.ok).toBe(true)
      expect(versionResult.ok && versionResult.value).toBe("1.0.0")

      // Tagged fields (empty)
      const tagResult = reader.readTaggedFields()
      expect(tagResult.ok).toBe(true)
      expect(tagResult.ok && tagResult.value).toEqual([])

      expect(reader.remaining).toBe(0)
    })

    it("encodes null client software fields", () => {
      const writer = new BinaryWriter()
      encodeApiVersionsRequest(writer, {}, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Null compact strings are encoded as length 0
      const nameResult = reader.readCompactString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBeNull()

      const versionResult = reader.readCompactString()
      expect(versionResult.ok).toBe(true)
      expect(versionResult.ok && versionResult.value).toBeNull()

      // Empty tagged fields
      const tagResult = reader.readTaggedFields()
      expect(tagResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

describe("buildApiVersionsRequest", () => {
  it("builds a framed v0 request", () => {
    const frame = buildApiVersionsRequest(1, 0)

    // Read size prefix
    const reader = new BinaryReader(frame)
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)
    expect(sizeResult.ok && sizeResult.value).toBe(frame.length - 4)

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.ApiVersions)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok).toBe(true)
    expect(versionResult.ok && versionResult.value).toBe(0)

    // Correlation ID
    const corrIdResult = reader.readInt32()
    expect(corrIdResult.ok).toBe(true)
    expect(corrIdResult.ok && corrIdResult.value).toBe(1)

    // Client ID (nullable string, v1 header)
    const clientIdResult = reader.readString()
    expect(clientIdResult.ok).toBe(true)
    expect(clientIdResult.ok && clientIdResult.value).toBeNull()

    // Empty body for v0
    expect(reader.remaining).toBe(0)
  })

  it("builds a framed v3 request with client info", () => {
    const frame = buildApiVersionsRequest(
      42,
      3,
      {
        clientSoftwareName: "test-client",
        clientSoftwareVersion: "0.1.0"
      },
      "my-client"
    )

    const reader = new BinaryReader(frame)

    // Size prefix
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.ApiVersions)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok).toBe(true)
    expect(versionResult.ok && versionResult.value).toBe(3)

    // Correlation ID
    const corrIdResult = reader.readInt32()
    expect(corrIdResult.ok).toBe(true)
    expect(corrIdResult.ok && corrIdResult.value).toBe(42)

    // Client ID (nullable string, v2 header per KIP-482)
    const clientIdResult = reader.readString()
    expect(clientIdResult.ok).toBe(true)
    expect(clientIdResult.ok && clientIdResult.value).toBe("my-client")

    // Header tagged fields
    const headerTagsResult = reader.readTaggedFields()
    expect(headerTagsResult.ok).toBe(true)

    // Body: client software name
    const swNameResult = reader.readCompactString()
    expect(swNameResult.ok).toBe(true)
    expect(swNameResult.ok && swNameResult.value).toBe("test-client")

    // Body: client software version
    const swVersionResult = reader.readCompactString()
    expect(swVersionResult.ok).toBe(true)
    expect(swVersionResult.ok && swVersionResult.value).toBe("0.1.0")

    // Body tagged fields
    const bodyTagsResult = reader.readTaggedFields()
    expect(bodyTagsResult.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeApiVersionsResponse", () => {
  describe("v0 — error code and api versions", () => {
    it("decodes a successful response with multiple APIs", () => {
      const writer = new BinaryWriter()
      // Error code: 0 (no error)
      writer.writeInt16(0)
      // Array length: 2
      writer.writeInt32(2)
      // Entry 1: Produce (0), min 0, max 9
      writer.writeInt16(0)
      writer.writeInt16(0)
      writer.writeInt16(9)
      // Entry 2: Fetch (1), min 0, max 13
      writer.writeInt16(1)
      writer.writeInt16(0)
      writer.writeInt16(13)

      const reader = new BinaryReader(writer.finish())
      const result = decodeApiVersionsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.errorCode).toBe(0)
        expect(result.value.apiVersions).toHaveLength(2)
        expect(result.value.apiVersions[0]).toEqual({
          apiKey: 0,
          minVersion: 0,
          maxVersion: 9,
          taggedFields: []
        })
        expect(result.value.apiVersions[1]).toEqual({
          apiKey: 1,
          minVersion: 0,
          maxVersion: 13,
          taggedFields: []
        })
        expect(result.value.throttleTimeMs).toBe(0)
        expect(result.value.taggedFields).toEqual([])
      }
    })

    it("decodes an error response", () => {
      const writer = new BinaryWriter()
      // Error code: 35 (UNSUPPORTED_VERSION)
      writer.writeInt16(35)
      // Empty array
      writer.writeInt32(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeApiVersionsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.errorCode).toBe(35)
        expect(result.value.apiVersions).toHaveLength(0)
      }
    })
  })

  describe("v1–v2 — adds throttle time", () => {
    it("decodes response with throttle time", () => {
      const writer = new BinaryWriter()
      // Error code
      writer.writeInt16(0)
      // Array length: 1
      writer.writeInt32(1)
      // Entry: ApiVersions (18), min 0, max 3
      writer.writeInt16(18)
      writer.writeInt16(0)
      writer.writeInt16(3)
      // Throttle time: 100ms
      writer.writeInt32(100)

      const reader = new BinaryReader(writer.finish())
      const result = decodeApiVersionsResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.errorCode).toBe(0)
        expect(result.value.apiVersions).toHaveLength(1)
        expect(result.value.throttleTimeMs).toBe(100)
      }
    })
  })

  describe("v3 — flexible encoding", () => {
    it("decodes response with compact array and tagged fields", () => {
      const writer = new BinaryWriter()
      // Error code
      writer.writeInt16(0)
      // Compact array: length + 1 = 2 (1 entry)
      writer.writeUnsignedVarInt(2)
      // Entry: Metadata (3), min 0, max 12
      writer.writeInt16(3)
      writer.writeInt16(0)
      writer.writeInt16(12)
      // Entry tagged fields (empty)
      writer.writeTaggedFields([])
      // Throttle time
      writer.writeInt32(50)
      // Response tagged fields (empty)
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeApiVersionsResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.errorCode).toBe(0)
        expect(result.value.apiVersions).toHaveLength(1)
        expect(result.value.apiVersions[0]).toEqual({
          apiKey: 3,
          minVersion: 0,
          maxVersion: 12,
          taggedFields: []
        })
        expect(result.value.throttleTimeMs).toBe(50)
        expect(result.value.taggedFields).toEqual([])
      }
    })

    it("decodes response with null array as empty", () => {
      const writer = new BinaryWriter()
      // Error code
      writer.writeInt16(0)
      // Compact array: 0 = null
      writer.writeUnsignedVarInt(0)
      // Throttle time
      writer.writeInt32(0)
      // Response tagged fields
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeApiVersionsResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.apiVersions).toHaveLength(0)
      }
    })
  })

  describe("error cases", () => {
    it("fails on truncated error code", () => {
      const reader = new BinaryReader(new Uint8Array([0]))
      const result = decodeApiVersionsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("fails on truncated array length", () => {
      const writer = new BinaryWriter()
      writer.writeInt16(0) // error code
      writer.writeInt16(0) // only 2 bytes of array length

      const reader = new BinaryReader(writer.finish())
      const result = decodeApiVersionsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("fails on truncated array entry", () => {
      const writer = new BinaryWriter()
      writer.writeInt16(0) // error code
      writer.writeInt32(1) // 1 entry
      writer.writeInt16(0) // api key only

      const reader = new BinaryReader(writer.finish())
      const result = decodeApiVersionsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })
  })
})

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

describe("apiVersionsToMap", () => {
  it("converts response to lookup map", () => {
    const response: ApiVersionsResponse = {
      errorCode: 0,
      apiVersions: [
        { apiKey: 0, minVersion: 0, maxVersion: 9 },
        { apiKey: 1, minVersion: 0, maxVersion: 13 },
        { apiKey: 18, minVersion: 0, maxVersion: 3 }
      ],
      throttleTimeMs: 0,
      taggedFields: []
    }

    const map = apiVersionsToMap(response)

    expect(map.size).toBe(3)
    expect(map.get(0)).toEqual({ minVersion: 0, maxVersion: 9 })
    expect(map.get(1)).toEqual({ minVersion: 0, maxVersion: 13 })
    expect(map.get(18)).toEqual({ minVersion: 0, maxVersion: 3 })
    expect(map.get(999)).toBeUndefined()
  })

  it("returns empty map for empty response", () => {
    const response: ApiVersionsResponse = {
      errorCode: 0,
      apiVersions: [],
      throttleTimeMs: 0,
      taggedFields: []
    }

    const map = apiVersionsToMap(response)
    expect(map.size).toBe(0)
  })
})
