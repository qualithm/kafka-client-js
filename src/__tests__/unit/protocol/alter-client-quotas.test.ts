import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  type AlterClientQuotasRequest,
  buildAlterClientQuotasRequest,
  decodeAlterClientQuotasResponse,
  encodeAlterClientQuotasRequest
} from "../../../protocol/alter-client-quotas"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeAlterClientQuotasRequest", () => {
  it("encodes v0 request with entries", () => {
    const writer = new BinaryWriter()
    const request: AlterClientQuotasRequest = {
      entries: [
        {
          entity: [
            { entityType: "user", entityName: "alice" },
            { entityType: "client-id", entityName: "producer-1" }
          ],
          ops: [
            { key: "producer_byte_rate", value: 1048576.0, remove: false },
            { key: "consumer_byte_rate", value: 0.0, remove: true }
          ]
        }
      ],
      validateOnly: false
    }
    encodeAlterClientQuotasRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // entries array (INT32)
    const entriesLen = reader.readInt32()
    expect(entriesLen.ok && entriesLen.value).toBe(1)

    // entry 0 entity array
    const entityLen = reader.readInt32()
    expect(entityLen.ok && entityLen.value).toBe(2)

    const eType0 = reader.readString()
    expect(eType0.ok && eType0.value).toBe("user")
    const eName0 = reader.readString()
    expect(eName0.ok && eName0.value).toBe("alice")

    const eType1 = reader.readString()
    expect(eType1.ok && eType1.value).toBe("client-id")
    const eName1 = reader.readString()
    expect(eName1.ok && eName1.value).toBe("producer-1")

    // entry 0 ops array
    const opsLen = reader.readInt32()
    expect(opsLen.ok && opsLen.value).toBe(2)

    const key0 = reader.readString()
    expect(key0.ok && key0.value).toBe("producer_byte_rate")
    const val0 = reader.readFloat64()
    expect(val0.ok && val0.value).toBe(1048576.0)
    const rem0 = reader.readBoolean()
    expect(rem0.ok && rem0.value).toBe(false)

    const key1 = reader.readString()
    expect(key1.ok && key1.value).toBe("consumer_byte_rate")
    const val1 = reader.readFloat64()
    expect(val1.ok && val1.value).toBe(0.0)
    const rem1 = reader.readBoolean()
    expect(rem1.ok && rem1.value).toBe(true)

    // validate_only
    const validateOnly = reader.readBoolean()
    expect(validateOnly.ok && validateOnly.value).toBe(false)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v1 flexible request", () => {
    const writer = new BinaryWriter()
    const request: AlterClientQuotasRequest = {
      entries: [
        {
          entity: [{ entityType: "user", entityName: null }],
          ops: [{ key: "request_percentage", value: 50.0, remove: false }]
        }
      ],
      validateOnly: true
    }
    encodeAlterClientQuotasRequest(writer, request, 1)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // entries compact array (1 + 1)
    const entriesLen = reader.readUnsignedVarInt()
    expect(entriesLen.ok && entriesLen.value).toBe(2)

    // entity compact array (1 + 1)
    const entityLen = reader.readUnsignedVarInt()
    expect(entityLen.ok && entityLen.value).toBe(2)

    const eType = reader.readCompactString()
    expect(eType.ok && eType.value).toBe("user")
    const eName = reader.readCompactString()
    expect(eName.ok && eName.value).toBeNull()
    const entityTag = reader.readTaggedFields()
    expect(entityTag.ok).toBe(true)

    // ops compact array (1 + 1)
    const opsLen = reader.readUnsignedVarInt()
    expect(opsLen.ok && opsLen.value).toBe(2)

    const key = reader.readCompactString()
    expect(key.ok && key.value).toBe("request_percentage")
    const val = reader.readFloat64()
    expect(val.ok && val.value).toBe(50.0)
    const rem = reader.readBoolean()
    expect(rem.ok && rem.value).toBe(false)
    const opTag = reader.readTaggedFields()
    expect(opTag.ok).toBe(true)

    // entry tagged fields
    const entryTag = reader.readTaggedFields()
    expect(entryTag.ok).toBe(true)

    // validate_only
    const validateOnly = reader.readBoolean()
    expect(validateOnly.ok && validateOnly.value).toBe(true)

    // request tagged fields
    const reqTag = reader.readTaggedFields()
    expect(reqTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes empty entries array", () => {
    const writer = new BinaryWriter()
    const request: AlterClientQuotasRequest = {
      entries: [],
      validateOnly: false
    }
    encodeAlterClientQuotasRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    const len = reader.readInt32()
    expect(len.ok && len.value).toBe(0)

    const validateOnly = reader.readBoolean()
    expect(validateOnly.ok && validateOnly.value).toBe(false)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

describe("buildAlterClientQuotasRequest", () => {
  it("produces a framed request with correct header (v0)", () => {
    const request: AlterClientQuotasRequest = {
      entries: [],
      validateOnly: false
    }
    const frame = buildAlterClientQuotasRequest(42, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.AlterClientQuotas)

    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(0)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(42)
  })

  it("produces a framed request with correct header (v1)", () => {
    const request: AlterClientQuotasRequest = {
      entries: [],
      validateOnly: false
    }
    const frame = buildAlterClientQuotasRequest(99, 1, request, "test-client")
    const reader = new BinaryReader(frame)

    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.AlterClientQuotas)

    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(1)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(99)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeAlterClientQuotasResponse", () => {
  it("decodes v0 response with success entries", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt32(1) // entries array

    // entry 0
    w.writeInt16(0) // error_code
    w.writeString(null) // error_message
    w.writeInt32(1) // entity array
    w.writeString("user") // entity_type
    w.writeString("alice") // entity_name

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterClientQuotasResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.entries).toHaveLength(1)

    const entry = result.value.entries[0]
    expect(entry.errorCode).toBe(0)
    expect(entry.errorMessage).toBeNull()
    expect(entry.entity).toHaveLength(1)
    expect(entry.entity[0].entityType).toBe("user")
    expect(entry.entity[0].entityName).toBe("alice")
  })

  it("decodes v1 flexible response", () => {
    const w = new BinaryWriter()
    w.writeInt32(25) // throttle_time_ms
    w.writeUnsignedVarInt(2) // entries compact array (1 + 1)

    // entry 0
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeUnsignedVarInt(3) // entity compact array (2 + 1)
    w.writeCompactString("user") // entity_type
    w.writeCompactString("bob") // entity_name
    w.writeTaggedFields([]) // entity tagged fields
    w.writeCompactString("client-id") // entity_type
    w.writeCompactString("consumer-1") // entity_name
    w.writeTaggedFields([]) // entity tagged fields
    w.writeTaggedFields([]) // entry tagged fields

    // response tagged fields
    w.writeTaggedFields([])

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterClientQuotasResponse(reader, 1)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(25)
    expect(result.value.entries).toHaveLength(1)

    const entry = result.value.entries[0]
    expect(entry.errorCode).toBe(0)
    expect(entry.entity).toHaveLength(2)
    expect(entry.entity[0].entityType).toBe("user")
    expect(entry.entity[0].entityName).toBe("bob")
    expect(entry.entity[1].entityType).toBe("client-id")
    expect(entry.entity[1].entityName).toBe("consumer-1")
  })

  it("decodes v0 response with error", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt32(1) // entries array

    // entry 0 with error
    w.writeInt16(49) // error_code (INVALID_CONFIG)
    w.writeString("unknown quota key") // error_message
    w.writeInt32(1) // entity array
    w.writeString("user") // entity_type
    w.writeString("alice") // entity_name

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterClientQuotasResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.entries[0].errorCode).toBe(49)
    expect(result.value.entries[0].errorMessage).toBe("unknown quota key")
  })

  it("decodes v0 response with empty entries", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt32(0) // empty entries array

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterClientQuotasResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.entries).toEqual([])
  })

  it("decodes response with default entity (null name)", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt32(1) // entries array

    w.writeInt16(0) // error_code
    w.writeString(null) // error_message
    w.writeInt32(1) // entity array
    w.writeString("user") // entity_type
    w.writeString(null) // entity_name (default)

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterClientQuotasResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.entries[0].entity[0].entityName).toBeNull()
  })

  it("handles truncated response gracefully", () => {
    const buf = new Uint8Array([0x00, 0x00]) // too short
    const reader = new BinaryReader(buf)
    const result = decodeAlterClientQuotasResponse(reader, 0)

    expect(result.ok).toBe(false)
  })
})
