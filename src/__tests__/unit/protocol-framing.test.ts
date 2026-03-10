/**
 * Protocol framing tests.
 *
 * @see https://kafka.apache.org/protocol.html#protocol_messages — Request/Response headers
 * @see https://kafka.apache.org/protocol.html#protocol_common — Size-prefixed framing
 */

import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  decodeResponseHeader,
  encodeRequestHeader,
  frameRequest,
  readResponseFrame,
  type RequestHeader,
  requestHeaderVersion,
  responseHeaderVersion
} from "../../protocol-framing"

// eslint-disable-next-line @typescript-eslint/no-empty-function
const noop = (): void => {}

// ---------------------------------------------------------------------------
// Header version selection
// @see https://kafka.apache.org/protocol.html#protocol_messages — Header version rules
// ---------------------------------------------------------------------------

describe("requestHeaderVersion", () => {
  it("returns 1 for non-flexible API versions", () => {
    // ApiVersions v0 is non-flexible (threshold is v3)
    expect(requestHeaderVersion(ApiKey.ApiVersions, 0)).toBe(1)
    expect(requestHeaderVersion(ApiKey.ApiVersions, 2)).toBe(1)
  })

  it("returns 2 for flexible API versions", () => {
    // ApiVersions v3 is flexible
    expect(requestHeaderVersion(ApiKey.ApiVersions, 3)).toBe(2)
  })

  it("returns 1 for APIs that are never flexible", () => {
    // SaslHandshake is never flexible
    expect(requestHeaderVersion(ApiKey.SaslHandshake, 0)).toBe(1)
    expect(requestHeaderVersion(ApiKey.SaslHandshake, 1)).toBe(1)
  })

  it("returns correct version for Metadata at the boundary", () => {
    // Metadata threshold is v9
    expect(requestHeaderVersion(ApiKey.Metadata, 8)).toBe(1)
    expect(requestHeaderVersion(ApiKey.Metadata, 9)).toBe(2)
  })
})

describe("responseHeaderVersion", () => {
  it("returns 0 for non-flexible API versions", () => {
    expect(responseHeaderVersion(ApiKey.ApiVersions, 0)).toBe(0)
    expect(responseHeaderVersion(ApiKey.ApiVersions, 2)).toBe(0)
  })

  it("returns 1 for flexible API versions", () => {
    expect(responseHeaderVersion(ApiKey.ApiVersions, 3)).toBe(1)
  })

  it("returns 0 for APIs that are never flexible", () => {
    expect(responseHeaderVersion(ApiKey.SaslHandshake, 0)).toBe(0)
    expect(responseHeaderVersion(ApiKey.SaslHandshake, 1)).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Request header encoding
// ---------------------------------------------------------------------------

describe("encodeRequestHeader", () => {
  describe("v0 — minimal header", () => {
    it("encodes API key, version, and correlation ID", () => {
      const writer = new BinaryWriter()
      const header: RequestHeader = {
        apiKey: ApiKey.ApiVersions,
        apiVersion: 0,
        correlationId: 42
      }

      encodeRequestHeader(writer, header, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // API key (int16)
      const apiKey = reader.readInt16()
      expect(apiKey.ok).toBe(true)
      expect(apiKey.ok && apiKey.value).toBe(ApiKey.ApiVersions)

      // API version (int16)
      const apiVersion = reader.readInt16()
      expect(apiVersion.ok).toBe(true)
      expect(apiVersion.ok && apiVersion.value).toBe(0)

      // Correlation ID (int32)
      const corrId = reader.readInt32()
      expect(corrId.ok).toBe(true)
      expect(corrId.ok && corrId.value).toBe(42)

      // Should be fully consumed
      expect(reader.remaining).toBe(0)
    })

    it("is exactly 8 bytes (2 + 2 + 4)", () => {
      const writer = new BinaryWriter()
      encodeRequestHeader(writer, { apiKey: ApiKey.Produce, apiVersion: 0, correlationId: 1 }, 0)
      expect(writer.offset).toBe(8)
    })
  })

  describe("v1 — non-flexible with client ID", () => {
    it("encodes header with nullable string client ID", () => {
      const writer = new BinaryWriter()
      const header: RequestHeader = {
        apiKey: ApiKey.Metadata,
        apiVersion: 1,
        correlationId: 100,
        clientId: "test-client"
      }

      encodeRequestHeader(writer, header, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const apiKey = reader.readInt16()
      expect(apiKey.ok && apiKey.value).toBe(ApiKey.Metadata)

      const apiVersion = reader.readInt16()
      expect(apiVersion.ok && apiVersion.value).toBe(1)

      const corrId = reader.readInt32()
      expect(corrId.ok && corrId.value).toBe(100)

      const clientId = reader.readString()
      expect(clientId.ok).toBe(true)
      expect(clientId.ok && clientId.value).toBe("test-client")

      expect(reader.remaining).toBe(0)
    })

    it("encodes null client ID when omitted", () => {
      const writer = new BinaryWriter()
      encodeRequestHeader(writer, { apiKey: ApiKey.Metadata, apiVersion: 1, correlationId: 1 }, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt16() // api key
      reader.readInt16() // api version
      reader.readInt32() // correlation id

      const clientId = reader.readString()
      expect(clientId.ok).toBe(true)
      expect(clientId.ok && clientId.value).toBeNull()
    })
  })

  describe("v2 — flexible with compact strings and tagged fields", () => {
    it("encodes header with compact string client ID and empty tagged fields", () => {
      const writer = new BinaryWriter()
      const header: RequestHeader = {
        apiKey: ApiKey.ApiVersions,
        apiVersion: 3,
        correlationId: 200,
        clientId: "flex-client"
      }

      encodeRequestHeader(writer, header, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const apiKey = reader.readInt16()
      expect(apiKey.ok && apiKey.value).toBe(ApiKey.ApiVersions)

      const apiVersion = reader.readInt16()
      expect(apiVersion.ok && apiVersion.value).toBe(3)

      const corrId = reader.readInt32()
      expect(corrId.ok && corrId.value).toBe(200)

      const clientId = reader.readCompactString()
      expect(clientId.ok).toBe(true)
      expect(clientId.ok && clientId.value).toBe("flex-client")

      const tagged = reader.readTaggedFields()
      expect(tagged.ok).toBe(true)
      expect(tagged.ok && tagged.value).toEqual([])

      expect(reader.remaining).toBe(0)
    })

    it("encodes tagged fields when provided", () => {
      const writer = new BinaryWriter()
      const tagData = new Uint8Array([0xca, 0xfe])
      const header: RequestHeader = {
        apiKey: ApiKey.Metadata,
        apiVersion: 12,
        correlationId: 300,
        clientId: null,
        taggedFields: [{ tag: 0, data: tagData }]
      }

      encodeRequestHeader(writer, header, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt16() // api key
      reader.readInt16() // api version
      reader.readInt32() // correlation id
      reader.readCompactString() // client id (null)

      const tagged = reader.readTaggedFields()
      expect(tagged.ok).toBe(true)
      expect(tagged.ok && tagged.value).toHaveLength(1)
      expect(tagged.ok && tagged.value[0].tag).toBe(0)
      expect(tagged.ok && tagged.value[0].data).toEqual(tagData)
    })
  })

  describe("auto header version selection", () => {
    it("uses v1 for non-flexible API versions", () => {
      const writer = new BinaryWriter()
      encodeRequestHeader(writer, {
        apiKey: ApiKey.ApiVersions,
        apiVersion: 0,
        correlationId: 1,
        clientId: "auto-client"
      })
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt16() // api key
      reader.readInt16() // api version
      reader.readInt32() // correlation id

      // v1 uses INT16 length-prefixed string
      const clientId = reader.readString()
      expect(clientId.ok).toBe(true)
      expect(clientId.ok && clientId.value).toBe("auto-client")
      expect(reader.remaining).toBe(0)
    })

    it("uses v2 for flexible API versions", () => {
      const writer = new BinaryWriter()
      encodeRequestHeader(writer, {
        apiKey: ApiKey.ApiVersions,
        apiVersion: 3,
        correlationId: 1,
        clientId: "auto-client"
      })
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt16() // api key
      reader.readInt16() // api version
      reader.readInt32() // correlation id

      // v2 uses compact string
      const clientId = reader.readCompactString()
      expect(clientId.ok).toBe(true)
      expect(clientId.ok && clientId.value).toBe("auto-client")

      // v2 has tagged fields
      const tagged = reader.readTaggedFields()
      expect(tagged.ok).toBe(true)
      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// Response header decoding
// ---------------------------------------------------------------------------

describe("decodeResponseHeader", () => {
  describe("v0 — non-flexible", () => {
    it("decodes correlation ID", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(42)
      const reader = new BinaryReader(writer.finish())

      const result = decodeResponseHeader(reader, ApiKey.ApiVersions, 0)
      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.correlationId).toBe(42)
        expect(result.value.taggedFields).toEqual([])
        expect(result.bytesRead).toBe(4)
      }
    })

    it("fails on truncated buffer", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeResponseHeader(reader, ApiKey.ApiVersions, 0)
      expect(result.ok).toBe(false)
    })
  })

  describe("v1 — flexible", () => {
    it("decodes correlation ID and empty tagged fields", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(99)
      writer.writeTaggedFields([])
      const reader = new BinaryReader(writer.finish())

      const result = decodeResponseHeader(reader, ApiKey.ApiVersions, 3)
      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.correlationId).toBe(99)
        expect(result.value.taggedFields).toEqual([])
        expect(result.bytesRead).toBe(5) // 4 (int32) + 1 (varint 0)
      }
    })

    it("decodes correlation ID and tagged fields", () => {
      const tagData = new Uint8Array([0xde, 0xad])
      const writer = new BinaryWriter()
      writer.writeInt32(150)
      writer.writeTaggedFields([{ tag: 1, data: tagData }])
      const reader = new BinaryReader(writer.finish())

      const result = decodeResponseHeader(reader, ApiKey.ApiVersions, 3)
      expect(result.ok).toBe(true)
      expect(result.ok && result.value.correlationId).toBe(150)
      expect(result.ok && result.value.taggedFields).toHaveLength(1)
      expect(result.ok && result.value.taggedFields[0].tag).toBe(1)
      expect(result.ok && result.value.taggedFields[0].data).toEqual(tagData)
    })

    it("fails when tagged fields are truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(1)
      // Write varint count of 1 but no actual field data
      writer.writeUnsignedVarInt(1)
      const reader = new BinaryReader(writer.finish())

      const result = decodeResponseHeader(reader, ApiKey.ApiVersions, 3)
      expect(result.ok).toBe(false)
    })
  })

  describe("explicit header version override", () => {
    it("uses v0 when explicitly specified even for flexible API", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(55)
      const reader = new BinaryReader(writer.finish())

      // ApiVersions v3 would normally use v1, but we override to v0
      const result = decodeResponseHeader(reader, ApiKey.ApiVersions, 3, 0)
      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.correlationId).toBe(55)
        expect(result.value.taggedFields).toEqual([])
      }
    })

    it("uses v1 when explicitly specified for non-flexible API", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(66)
      writer.writeTaggedFields([])
      const reader = new BinaryReader(writer.finish())

      // ApiVersions v0 would normally use v0, but we override to v1
      const result = decodeResponseHeader(reader, ApiKey.ApiVersions, 0, 1)
      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.correlationId).toBe(66)
        expect(result.value.taggedFields).toEqual([])
      }
    })
  })
})

// ---------------------------------------------------------------------------
// Size-prefixed message framing
// ---------------------------------------------------------------------------

describe("frameRequest", () => {
  it("prepends INT32 size prefix", () => {
    const header: RequestHeader = {
      apiKey: ApiKey.ApiVersions,
      apiVersion: 0,
      correlationId: 1,
      clientId: null
    }

    const framed = frameRequest(header, noop)
    const reader = new BinaryReader(framed)

    // Read size prefix
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)
    expect(sizeResult.ok && sizeResult.value).toBe(framed.byteLength - 4)
  })

  it("contains valid header after size prefix", () => {
    const header: RequestHeader = {
      apiKey: ApiKey.Metadata,
      apiVersion: 1,
      correlationId: 77,
      clientId: "frame-test"
    }

    const framed = frameRequest(header, noop)
    const reader = new BinaryReader(framed)

    // Skip size prefix
    reader.readInt32()

    // Decode header
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.Metadata)

    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(1)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(77)

    const clientId = reader.readString()
    expect(clientId.ok && clientId.value).toBe("frame-test")

    expect(reader.remaining).toBe(0)
  })

  it("includes body bytes in frame", () => {
    const header: RequestHeader = {
      apiKey: ApiKey.ApiVersions,
      apiVersion: 0,
      correlationId: 1,
      clientId: null
    }

    const framed = frameRequest(
      header,
      (w) => {
        w.writeInt32(0xdeadbeef)
      },
      0
    )

    const reader = new BinaryReader(framed)

    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)
    // 8 (v0 header) + 4 (body) = 12
    expect(sizeResult.ok && sizeResult.value).toBe(12)
  })

  it("allows header version override", () => {
    const header: RequestHeader = {
      apiKey: ApiKey.ApiVersions,
      apiVersion: 0,
      correlationId: 1
    }

    // Use v0 (no client ID) — should be 8 bytes for header only
    const framed = frameRequest(header, noop, 0)
    const reader = new BinaryReader(framed)
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok && sizeResult.value).toBe(8)
  })
})

describe("readResponseFrame", () => {
  it("reads a valid response frame", () => {
    // Build a response: size prefix + correlation ID
    const payload = new BinaryWriter()
    payload.writeInt32(42) // correlation ID

    const frame = new BinaryWriter()
    const payloadBytes = payload.finish()
    frame.writeInt32(payloadBytes.byteLength) // size prefix
    frame.writeRawBytes(payloadBytes)

    const reader = new BinaryReader(frame.finish())
    const result = readResponseFrame(reader)

    expect(result.ok).toBe(true)
    if (result.ok) {
      const innerReader = result.value
      const corrId = innerReader.readInt32()
      expect(corrId.ok && corrId.value).toBe(42)
      expect(innerReader.remaining).toBe(0)
    }
  })

  it("fails on truncated size prefix", () => {
    const reader = new BinaryReader(new Uint8Array(2))
    const result = readResponseFrame(reader)
    expect(result.ok).toBe(false)
  })

  it("fails on negative size", () => {
    const writer = new BinaryWriter()
    writer.writeInt32(-1)
    const reader = new BinaryReader(writer.finish())
    const result = readResponseFrame(reader)

    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("INVALID_DATA")
    }
  })

  it("fails when payload is truncated", () => {
    const writer = new BinaryWriter()
    writer.writeInt32(100) // claims 100 bytes
    writer.writeInt32(42) // but only has 4
    const reader = new BinaryReader(writer.finish())

    const result = readResponseFrame(reader)
    expect(result.ok).toBe(false)
  })

  it("scopes reader to exactly the frame payload", () => {
    const payload = new BinaryWriter()
    payload.writeInt32(1)
    payload.writeInt32(2)
    const payloadBytes = payload.finish()

    const frame = new BinaryWriter()
    frame.writeInt32(payloadBytes.byteLength)
    frame.writeRawBytes(payloadBytes)
    // Add extra bytes after the frame
    frame.writeInt32(0xffffffff)

    const reader = new BinaryReader(frame.finish())
    const result = readResponseFrame(reader)

    expect(result.ok).toBe(true)
    if (result.ok) {
      // Inner reader should only see the 8 bytes
      expect(result.value.remaining).toBe(8)
      // Outer reader should have 4 bytes left (the trailing 0xffffffff)
      expect(reader.remaining).toBe(4)
    }
  })
})

// ---------------------------------------------------------------------------
// ApiVersions framing round-trips (acceptance criteria)
// ---------------------------------------------------------------------------

describe("ApiVersions framing", () => {
  it("frames ApiVersions v0 request correctly", () => {
    const header: RequestHeader = {
      apiKey: ApiKey.ApiVersions,
      apiVersion: 0,
      correlationId: 1,
      clientId: "test"
    }

    // v0 ApiVersions uses request header v1 (non-flexible)
    const framed = frameRequest(header, noop)
    const reader = new BinaryReader(framed)

    // Size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    // Header fields
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(18) // ApiVersions = 18

    const version = reader.readInt16()
    expect(version.ok && version.value).toBe(0)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(1)

    // v1 header: nullable string
    const clientId = reader.readString()
    expect(clientId.ok && clientId.value).toBe("test")

    expect(reader.remaining).toBe(0)
  })

  it("frames ApiVersions v3 request with flexible header", () => {
    const header: RequestHeader = {
      apiKey: ApiKey.ApiVersions,
      apiVersion: 3,
      correlationId: 2,
      clientId: "flex"
    }

    // v3 ApiVersions uses request header v2 (flexible)
    const framed = frameRequest(header, noop)
    const reader = new BinaryReader(framed)

    // Size prefix
    reader.readInt32()

    // Header fields
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(18)

    const version = reader.readInt16()
    expect(version.ok && version.value).toBe(3)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(2)

    // v2 header: compact string
    const clientId = reader.readCompactString()
    expect(clientId.ok && clientId.value).toBe("flex")

    // v2 header: tagged fields
    const tagged = reader.readTaggedFields()
    expect(tagged.ok).toBe(true)
    expect(tagged.ok && tagged.value).toEqual([])

    expect(reader.remaining).toBe(0)
  })

  it("decodes ApiVersions v0 response with v0 header", () => {
    const writer = new BinaryWriter()
    writer.writeInt32(7) // correlation ID
    const reader = new BinaryReader(writer.finish())

    const result = decodeResponseHeader(reader, ApiKey.ApiVersions, 0)
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value.correlationId).toBe(7)
    }
  })

  it("decodes ApiVersions v3 response with v1 header", () => {
    const writer = new BinaryWriter()
    writer.writeInt32(8) // correlation ID
    writer.writeTaggedFields([]) // empty tagged fields
    const reader = new BinaryReader(writer.finish())

    const result = decodeResponseHeader(reader, ApiKey.ApiVersions, 3)
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value.correlationId).toBe(8)
      expect(result.value.taggedFields).toEqual([])
    }
  })
})
