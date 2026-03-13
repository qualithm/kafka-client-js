import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDescribeClientQuotasRequest,
  decodeDescribeClientQuotasResponse,
  type DescribeClientQuotasRequest,
  encodeDescribeClientQuotasRequest,
  QuotaMatchType
} from "../../../protocol/describe-client-quotas"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeClientQuotasRequest", () => {
  it("encodes v0 request with component filters", () => {
    const writer = new BinaryWriter()
    const request: DescribeClientQuotasRequest = {
      components: [
        { entityType: "user", matchType: QuotaMatchType.Exact, match: "alice" },
        { entityType: "client-id", matchType: QuotaMatchType.Any, match: null }
      ],
      strict: true
    }
    encodeDescribeClientQuotasRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // components array (non-flexible: INT32 length)
    const len = reader.readInt32()
    expect(len.ok && len.value).toBe(2)

    // component 0
    const type0 = reader.readString()
    expect(type0.ok && type0.value).toBe("user")
    const match0Type = reader.readInt8()
    expect(match0Type.ok && match0Type.value).toBe(QuotaMatchType.Exact)
    const match0 = reader.readString()
    expect(match0.ok && match0.value).toBe("alice")

    // component 1
    const type1 = reader.readString()
    expect(type1.ok && type1.value).toBe("client-id")
    const match1Type = reader.readInt8()
    expect(match1Type.ok && match1Type.value).toBe(QuotaMatchType.Any)
    const match1 = reader.readString()
    expect(match1.ok && match1.value).toBeNull()

    // strict
    const strict = reader.readBoolean()
    expect(strict.ok && strict.value).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v1 flexible request", () => {
    const writer = new BinaryWriter()
    const request: DescribeClientQuotasRequest = {
      components: [{ entityType: "user", matchType: QuotaMatchType.Default, match: null }],
      strict: false
    }
    encodeDescribeClientQuotasRequest(writer, request, 1)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // compact array (length + 1)
    const len = reader.readUnsignedVarInt()
    expect(len.ok && len.value).toBe(2) // 1 + 1

    // component 0
    const type0 = reader.readCompactString()
    expect(type0.ok && type0.value).toBe("user")
    const match0Type = reader.readInt8()
    expect(match0Type.ok && match0Type.value).toBe(QuotaMatchType.Default)
    const match0 = reader.readCompactString()
    expect(match0.ok && match0.value).toBeNull()
    const tag0 = reader.readTaggedFields()
    expect(tag0.ok).toBe(true)

    // strict
    const strict = reader.readBoolean()
    expect(strict.ok && strict.value).toBe(false)

    // request tagged fields
    const reqTag = reader.readTaggedFields()
    expect(reqTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes empty components array", () => {
    const writer = new BinaryWriter()
    const request: DescribeClientQuotasRequest = {
      components: [],
      strict: false
    }
    encodeDescribeClientQuotasRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    const len = reader.readInt32()
    expect(len.ok && len.value).toBe(0)

    const strict = reader.readBoolean()
    expect(strict.ok && strict.value).toBe(false)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

describe("buildDescribeClientQuotasRequest", () => {
  it("produces a framed request with correct header (v0)", () => {
    const request: DescribeClientQuotasRequest = {
      components: [{ entityType: "user", matchType: QuotaMatchType.Exact, match: "alice" }],
      strict: false
    }
    const frame = buildDescribeClientQuotasRequest(42, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    // size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    // API key
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.DescribeClientQuotas)

    // API version
    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(0)

    // correlation id
    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(42)
  })

  it("produces a framed request with correct header (v1)", () => {
    const request: DescribeClientQuotasRequest = {
      components: [],
      strict: false
    }
    const frame = buildDescribeClientQuotasRequest(7, 1, request, "test-client")
    const reader = new BinaryReader(frame)

    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.DescribeClientQuotas)

    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(1)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(7)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeClientQuotasResponse", () => {
  it("decodes v0 response with entries", () => {
    const w = new BinaryWriter()
    // throttle_time_ms
    w.writeInt32(0)
    // error_code
    w.writeInt16(0)
    // error_message (null)
    w.writeString(null)
    // entries (non-nullable array, 1 entry)
    w.writeInt32(1)

    // entry 0
    // entity array (1 element)
    w.writeInt32(1)
    w.writeString("user") // entity_type
    w.writeString("alice") // entity_name

    // values array (1 element)
    w.writeInt32(1)
    w.writeString("producer_byte_rate") // key
    w.writeFloat64(1048576.0) // value

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeClientQuotasResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.errorCode).toBe(0)
    expect(result.value.errorMessage).toBeNull()
    expect(result.value.entries).toHaveLength(1)

    const entry = result.value.entries![0]
    expect(entry.entity).toHaveLength(1)
    expect(entry.entity[0].entityType).toBe("user")
    expect(entry.entity[0].entityName).toBe("alice")

    expect(entry.values).toHaveLength(1)
    expect(entry.values[0].key).toBe("producer_byte_rate")
    expect(entry.values[0].value).toBe(1048576.0)
  })

  it("decodes v1 flexible response with entries", () => {
    const w = new BinaryWriter()
    // throttle_time_ms
    w.writeInt32(50)
    // error_code
    w.writeInt16(0)
    // error_message (null compact string)
    w.writeCompactString(null)
    // entries compact array (1 + 1)
    w.writeUnsignedVarInt(2)

    // entry 0
    // entity compact array (2 + 1)
    w.writeUnsignedVarInt(3)
    w.writeCompactString("user") // entity_type
    w.writeCompactString("alice") // entity_name
    w.writeTaggedFields([]) // entity tagged fields
    w.writeCompactString("client-id") // entity_type
    w.writeCompactString("producer-1") // entity_name
    w.writeTaggedFields([]) // entity tagged fields

    // values compact array (1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeCompactString("consumer_byte_rate") // key
    w.writeFloat64(2097152.0) // value
    w.writeTaggedFields([]) // value tagged fields

    // entry tagged fields
    w.writeTaggedFields([])

    // response tagged fields
    w.writeTaggedFields([])

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeClientQuotasResponse(reader, 1)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(50)
    expect(result.value.errorCode).toBe(0)
    expect(result.value.entries).toHaveLength(1)

    const entry = result.value.entries![0]
    expect(entry.entity).toHaveLength(2)
    expect(entry.entity[0].entityType).toBe("user")
    expect(entry.entity[0].entityName).toBe("alice")
    expect(entry.entity[1].entityType).toBe("client-id")
    expect(entry.entity[1].entityName).toBe("producer-1")

    expect(entry.values).toHaveLength(1)
    expect(entry.values[0].key).toBe("consumer_byte_rate")
    expect(entry.values[0].value).toBe(2097152.0)
  })

  it("decodes v0 response with null entries (error case)", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(58) // error_code (INVALID_REQUEST)
    w.writeString("invalid filter") // error_message
    w.writeInt32(-1) // null entries array

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeClientQuotasResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.errorCode).toBe(58)
    expect(result.value.errorMessage).toBe("invalid filter")
    expect(result.value.entries).toBeNull()
  })

  it("decodes v1 response with null entries", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(58) // error_code
    w.writeCompactString("bad request") // error_message
    w.writeUnsignedVarInt(0) // null compact array
    w.writeTaggedFields([]) // response tagged fields

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeClientQuotasResponse(reader, 1)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.errorCode).toBe(58)
    expect(result.value.errorMessage).toBe("bad request")
    expect(result.value.entries).toBeNull()
  })

  it("decodes v0 response with empty entries", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeString(null) // error_message
    w.writeInt32(0) // empty entries array

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeClientQuotasResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.entries).toEqual([])
  })

  it("handles truncated response gracefully", () => {
    const buf = new Uint8Array([0x00, 0x00]) // too short
    const reader = new BinaryReader(buf)
    const result = decodeDescribeClientQuotasResponse(reader, 0)

    expect(result.ok).toBe(false)
  })

  it("decodes v0 response with multiple entries and values", () => {
    const w = new BinaryWriter()
    w.writeInt32(100) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeString(null) // error_message
    w.writeInt32(2) // 2 entries

    // entry 0: user=alice
    w.writeInt32(1) // entity array
    w.writeString("user")
    w.writeString("alice")
    w.writeInt32(2) // 2 values
    w.writeString("producer_byte_rate")
    w.writeFloat64(1048576.0)
    w.writeString("consumer_byte_rate")
    w.writeFloat64(2097152.0)

    // entry 1: default user
    w.writeInt32(1) // entity array
    w.writeString("user")
    w.writeString(null) // default
    w.writeInt32(1) // 1 value
    w.writeString("request_percentage")
    w.writeFloat64(25.0)

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeClientQuotasResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(100)
    expect(result.value.entries).toHaveLength(2)

    const entry0 = result.value.entries![0]
    expect(entry0.entity[0].entityName).toBe("alice")
    expect(entry0.values).toHaveLength(2)

    const entry1 = result.value.entries![1]
    expect(entry1.entity[0].entityName).toBeNull()
    expect(entry1.values[0].value).toBe(25.0)
  })
})

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

describe("QuotaMatchType", () => {
  it("has correct enum values", () => {
    expect(QuotaMatchType.Exact).toBe(0)
    expect(QuotaMatchType.Default).toBe(1)
    expect(QuotaMatchType.Any).toBe(2)
  })
})
