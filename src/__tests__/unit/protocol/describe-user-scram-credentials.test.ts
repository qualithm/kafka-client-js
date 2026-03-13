import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDescribeUserScramCredentialsRequest,
  decodeDescribeUserScramCredentialsResponse,
  type DescribeUserScramCredentialsRequest,
  encodeDescribeUserScramCredentialsRequest,
  ScramMechanism
} from "../../../protocol/describe-user-scram-credentials"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeUserScramCredentialsRequest", () => {
  it("encodes request with users filter", () => {
    const writer = new BinaryWriter()
    const request: DescribeUserScramCredentialsRequest = {
      users: [{ name: "alice" }, { name: "bob" }]
    }
    encodeDescribeUserScramCredentialsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // users compact array (length + 1)
    const usersLen = reader.readUnsignedVarInt()
    expect(usersLen.ok && usersLen.value).toBe(3) // 2 + 1

    // user 0
    const name0 = reader.readCompactString()
    expect(name0.ok && name0.value).toBe("alice")
    const tag0 = reader.readTaggedFields()
    expect(tag0.ok).toBe(true)

    // user 1
    const name1 = reader.readCompactString()
    expect(name1.ok && name1.value).toBe("bob")
    const tag1 = reader.readTaggedFields()
    expect(tag1.ok).toBe(true)

    // request tagged fields
    const reqTag = reader.readTaggedFields()
    expect(reqTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes request with null users (describe all)", () => {
    const writer = new BinaryWriter()
    const request: DescribeUserScramCredentialsRequest = {
      users: null
    }
    encodeDescribeUserScramCredentialsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // null compact array: written as 0
    const usersLen = reader.readUnsignedVarInt()
    expect(usersLen.ok && usersLen.value).toBe(0)

    // request tagged fields
    const reqTag = reader.readTaggedFields()
    expect(reqTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes request with empty users array", () => {
    const writer = new BinaryWriter()
    const request: DescribeUserScramCredentialsRequest = {
      users: []
    }
    encodeDescribeUserScramCredentialsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // empty compact array: 0 + 1 = 1
    const usersLen = reader.readUnsignedVarInt()
    expect(usersLen.ok && usersLen.value).toBe(1)

    // request tagged fields
    const reqTag = reader.readTaggedFields()
    expect(reqTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

describe("buildDescribeUserScramCredentialsRequest", () => {
  it("produces a framed request with correct header", () => {
    const request: DescribeUserScramCredentialsRequest = {
      users: [{ name: "alice" }]
    }
    const frame = buildDescribeUserScramCredentialsRequest(42, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    // size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    // API key
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.DescribeUserScramCredentials)

    // API version
    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(0)

    // correlation id
    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(42)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeUserScramCredentialsResponse", () => {
  it("decodes successful response with credentials", () => {
    const w = new BinaryWriter()
    // throttle_time_ms
    w.writeInt32(0)
    // error_code
    w.writeInt16(0)
    // error_message (null compact string)
    w.writeCompactString(null)
    // results compact array (1 + 1)
    w.writeUnsignedVarInt(2)

    // result 0
    w.writeCompactString("alice") // user
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    // credential_infos compact array (2 + 1)
    w.writeUnsignedVarInt(3)
    // credential 0
    w.writeInt8(ScramMechanism.ScramSha256) // mechanism
    w.writeInt32(4096) // iterations
    w.writeTaggedFields([]) // credential tagged fields
    // credential 1
    w.writeInt8(ScramMechanism.ScramSha512) // mechanism
    w.writeInt32(8192) // iterations
    w.writeTaggedFields([]) // credential tagged fields
    w.writeTaggedFields([]) // result tagged fields

    // response tagged fields
    w.writeTaggedFields([])

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeUserScramCredentialsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.errorCode).toBe(0)
    expect(result.value.errorMessage).toBeNull()
    expect(result.value.results).toHaveLength(1)

    const userResult = result.value.results[0]
    expect(userResult.user).toBe("alice")
    expect(userResult.errorCode).toBe(0)
    expect(userResult.credentialInfos).toHaveLength(2)
    expect(userResult.credentialInfos[0].mechanism).toBe(ScramMechanism.ScramSha256)
    expect(userResult.credentialInfos[0].iterations).toBe(4096)
    expect(userResult.credentialInfos[1].mechanism).toBe(ScramMechanism.ScramSha512)
    expect(userResult.credentialInfos[1].iterations).toBe(8192)
  })

  it("decodes response with no results", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeUnsignedVarInt(1) // empty results array (0 + 1)
    w.writeTaggedFields([]) // response tagged fields

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeUserScramCredentialsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.results).toHaveLength(0)
  })

  it("decodes per-user error response", () => {
    const w = new BinaryWriter()
    w.writeInt32(50) // throttle_time_ms
    w.writeInt16(0) // top-level error_code
    w.writeCompactString(null) // error_message
    w.writeUnsignedVarInt(2) // 1 result
    // result with error
    w.writeCompactString("unknown-user") // user
    w.writeInt16(73) // RESOURCE_NOT_FOUND
    w.writeCompactString("user not found") // error_message
    w.writeUnsignedVarInt(1) // empty credential_infos (0 + 1)
    w.writeTaggedFields([]) // result tagged fields
    w.writeTaggedFields([]) // response tagged fields

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeUserScramCredentialsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(50)
    expect(result.value.results).toHaveLength(1)
    expect(result.value.results[0].user).toBe("unknown-user")
    expect(result.value.results[0].errorCode).toBe(73)
    expect(result.value.results[0].errorMessage).toBe("user not found")
    expect(result.value.results[0].credentialInfos).toHaveLength(0)
  })

  it("returns failure on truncated input", () => {
    const reader = new BinaryReader(new Uint8Array([0, 0]))
    const result = decodeDescribeUserScramCredentialsResponse(reader, 0)
    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated result entry", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeUnsignedVarInt(2) // results array (1 entry)
    w.writeCompactString("alice") // user
    w.writeInt16(0) // error_code
    // error_message missing
    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeUserScramCredentialsResponse(reader, 0)
    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated credential info", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeUnsignedVarInt(2) // results array (1 entry)
    w.writeCompactString("alice") // user
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeUnsignedVarInt(2) // credential_infos (1 entry)
    w.writeInt8(1) // mechanism — but iterations missing
    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeUserScramCredentialsResponse(reader, 0)
    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated error_message field", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    // error_message missing
    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeUserScramCredentialsResponse(reader, 0)
    expect(result.ok).toBe(false)
  })
})
