import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  type AlterUserScramCredentialsRequest,
  buildAlterUserScramCredentialsRequest,
  decodeAlterUserScramCredentialsResponse,
  encodeAlterUserScramCredentialsRequest
} from "../../../protocol/alter-user-scram-credentials"
import { ScramMechanism } from "../../../protocol/describe-user-scram-credentials"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeAlterUserScramCredentialsRequest", () => {
  it("encodes deletions and upsertions", () => {
    const writer = new BinaryWriter()
    const request: AlterUserScramCredentialsRequest = {
      deletions: [{ name: "alice", mechanism: ScramMechanism.ScramSha256 }],
      upsertions: [
        {
          name: "bob",
          mechanism: ScramMechanism.ScramSha512,
          iterations: 4096,
          salt: new Uint8Array([0x01, 0x02, 0x03]),
          saltedPassword: new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd])
        }
      ]
    }
    encodeAlterUserScramCredentialsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // deletions compact array (1 + 1)
    const delLen = reader.readUnsignedVarInt()
    expect(delLen.ok && delLen.value).toBe(2)

    // deletion 0
    const delName = reader.readCompactString()
    expect(delName.ok && delName.value).toBe("alice")
    const delMech = reader.readInt8()
    expect(delMech.ok && delMech.value).toBe(ScramMechanism.ScramSha256)
    const delTag = reader.readTaggedFields()
    expect(delTag.ok).toBe(true)

    // upsertions compact array (1 + 1)
    const upsLen = reader.readUnsignedVarInt()
    expect(upsLen.ok && upsLen.value).toBe(2)

    // upsertion 0
    const upsName = reader.readCompactString()
    expect(upsName.ok && upsName.value).toBe("bob")
    const upsMech = reader.readInt8()
    expect(upsMech.ok && upsMech.value).toBe(ScramMechanism.ScramSha512)
    const upsIter = reader.readInt32()
    expect(upsIter.ok && upsIter.value).toBe(4096)
    const upsSalt = reader.readCompactBytes()
    expect(upsSalt.ok).toBe(true)
    if (upsSalt.ok) {
      expect(upsSalt.value).toEqual(new Uint8Array([0x01, 0x02, 0x03]))
    }
    const upsPwd = reader.readCompactBytes()
    expect(upsPwd.ok).toBe(true)
    if (upsPwd.ok) {
      expect(upsPwd.value).toEqual(new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd]))
    }
    const upsTag = reader.readTaggedFields()
    expect(upsTag.ok).toBe(true)

    // request tagged fields
    const reqTag = reader.readTaggedFields()
    expect(reqTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes empty deletions and upsertions", () => {
    const writer = new BinaryWriter()
    const request: AlterUserScramCredentialsRequest = {
      deletions: [],
      upsertions: []
    }
    encodeAlterUserScramCredentialsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // empty deletions (0 + 1)
    const delLen = reader.readUnsignedVarInt()
    expect(delLen.ok && delLen.value).toBe(1)

    // empty upsertions (0 + 1)
    const upsLen = reader.readUnsignedVarInt()
    expect(upsLen.ok && upsLen.value).toBe(1)

    // request tagged fields
    const reqTag = reader.readTaggedFields()
    expect(reqTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes multiple deletions", () => {
    const writer = new BinaryWriter()
    const request: AlterUserScramCredentialsRequest = {
      deletions: [
        { name: "alice", mechanism: ScramMechanism.ScramSha256 },
        { name: "bob", mechanism: ScramMechanism.ScramSha512 }
      ],
      upsertions: []
    }
    encodeAlterUserScramCredentialsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // deletions compact array (2 + 1)
    const delLen = reader.readUnsignedVarInt()
    expect(delLen.ok && delLen.value).toBe(3)

    // deletion 0
    const name0 = reader.readCompactString()
    expect(name0.ok && name0.value).toBe("alice")
    reader.readInt8() // mechanism
    reader.readTaggedFields() // tagged fields

    // deletion 1
    const name1 = reader.readCompactString()
    expect(name1.ok && name1.value).toBe("bob")
    const mech1 = reader.readInt8()
    expect(mech1.ok && mech1.value).toBe(ScramMechanism.ScramSha512)
    reader.readTaggedFields() // tagged fields

    // empty upsertions
    const upsLen = reader.readUnsignedVarInt()
    expect(upsLen.ok && upsLen.value).toBe(1)

    // request tagged fields
    reader.readTaggedFields()

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

describe("buildAlterUserScramCredentialsRequest", () => {
  it("produces a framed request with correct header", () => {
    const request: AlterUserScramCredentialsRequest = {
      deletions: [],
      upsertions: []
    }
    const frame = buildAlterUserScramCredentialsRequest(99, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    // size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    // API key
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.AlterUserScramCredentials)

    // API version
    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(0)

    // correlation id
    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(99)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeAlterUserScramCredentialsResponse", () => {
  it("decodes successful response with results", () => {
    const w = new BinaryWriter()
    // throttle_time_ms
    w.writeInt32(0)
    // results compact array (2 + 1)
    w.writeUnsignedVarInt(3)
    // result 0
    w.writeCompactString("alice") // user
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeTaggedFields([]) // result tagged fields
    // result 1
    w.writeCompactString("bob") // user
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeTaggedFields([]) // result tagged fields
    // response tagged fields
    w.writeTaggedFields([])

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterUserScramCredentialsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.results).toHaveLength(2)

    expect(result.value.results[0].user).toBe("alice")
    expect(result.value.results[0].errorCode).toBe(0)
    expect(result.value.results[0].errorMessage).toBeNull()

    expect(result.value.results[1].user).toBe("bob")
    expect(result.value.results[1].errorCode).toBe(0)
  })

  it("decodes response with per-user errors", () => {
    const w = new BinaryWriter()
    w.writeInt32(100) // throttle_time_ms
    w.writeUnsignedVarInt(2) // 1 result
    // result with error
    w.writeCompactString("charlie") // user
    w.writeInt16(69) // UNKNOWN_SCRAM_MECHANISM
    w.writeCompactString("unsupported mechanism") // error_message
    w.writeTaggedFields([]) // result tagged fields
    w.writeTaggedFields([]) // response tagged fields

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterUserScramCredentialsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(100)
    expect(result.value.results).toHaveLength(1)
    expect(result.value.results[0].user).toBe("charlie")
    expect(result.value.results[0].errorCode).toBe(69)
    expect(result.value.results[0].errorMessage).toBe("unsupported mechanism")
  })

  it("decodes empty results", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeUnsignedVarInt(1) // empty results (0 + 1)
    w.writeTaggedFields([]) // response tagged fields

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterUserScramCredentialsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.results).toHaveLength(0)
  })

  it("returns failure on truncated input", () => {
    const reader = new BinaryReader(new Uint8Array([0, 0]))
    const result = decodeAlterUserScramCredentialsResponse(reader, 0)
    expect(result.ok).toBe(false)
  })
})
