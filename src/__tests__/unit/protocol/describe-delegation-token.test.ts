import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDescribeDelegationTokenRequest,
  decodeDescribeDelegationTokenResponse,
  type DescribeDelegationTokenRequest,
  encodeDescribeDelegationTokenRequest
} from "../../../protocol/describe-delegation-token"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeDelegationTokenRequest", () => {
  describe("v0–v1 — non-flexible encoding", () => {
    it("encodes null owners (describe all)", () => {
      const writer = new BinaryWriter()
      const request: DescribeDelegationTokenRequest = {
        owners: null
      }
      encodeDescribeDelegationTokenRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // owners — nullable array, null encoded as -1
      const arrayLen = reader.readInt32()
      expect(arrayLen.ok && arrayLen.value).toBe(-1)

      expect(reader.remaining).toBe(0)
    })

    it("encodes a list of owners", () => {
      const writer = new BinaryWriter()
      const request: DescribeDelegationTokenRequest = {
        owners: [
          { principalType: "User", principalName: "alice" },
          { principalType: "User", principalName: "bob" }
        ]
      }
      encodeDescribeDelegationTokenRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // array length
      const arrayLen = reader.readInt32()
      expect(arrayLen.ok && arrayLen.value).toBe(2)

      // owner 0
      const type0 = reader.readString()
      expect(type0.ok && type0.value).toBe("User")
      const name0 = reader.readString()
      expect(name0.ok && name0.value).toBe("alice")

      // owner 1
      const type1 = reader.readString()
      expect(type1.ok && type1.value).toBe("User")
      const name1 = reader.readString()
      expect(name1.ok && name1.value).toBe("bob")

      expect(reader.remaining).toBe(0)
    })

    it("encodes an empty owners array", () => {
      const writer = new BinaryWriter()
      const request: DescribeDelegationTokenRequest = {
        owners: []
      }
      encodeDescribeDelegationTokenRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const arrayLen = reader.readInt32()
      expect(arrayLen.ok && arrayLen.value).toBe(0)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("encodes null owners with compact nullable array", () => {
      const writer = new BinaryWriter()
      const request: DescribeDelegationTokenRequest = {
        owners: null
      }
      encodeDescribeDelegationTokenRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // compact nullable array: 0 means null
      const arrayLen = reader.readUnsignedVarInt()
      expect(arrayLen.ok && arrayLen.value).toBe(0)

      // tagged fields
      const tag = reader.readTaggedFields()
      expect(tag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })

    it("encodes owners with compact strings", () => {
      const writer = new BinaryWriter()
      const request: DescribeDelegationTokenRequest = {
        owners: [{ principalType: "User", principalName: "carol" }]
      }
      encodeDescribeDelegationTokenRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // compact array: length + 1
      const arrayLen = reader.readUnsignedVarInt()
      expect(arrayLen.ok && arrayLen.value).toBe(2) // 1 + 1

      // owner 0
      const type0 = reader.readCompactString()
      expect(type0.ok && type0.value).toBe("User")
      const name0 = reader.readCompactString()
      expect(name0.ok && name0.value).toBe("carol")

      // per-element tagged fields
      const elemTag = reader.readTaggedFields()
      expect(elemTag.ok).toBe(true)

      // request tagged fields
      const tag = reader.readTaggedFields()
      expect(tag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

describe("buildDescribeDelegationTokenRequest", () => {
  it("produces a framed request with correct header", () => {
    const request: DescribeDelegationTokenRequest = {
      owners: null
    }
    const frame = buildDescribeDelegationTokenRequest(99, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.DescribeDelegationToken)

    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(0)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(99)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeDelegationTokenResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes a response with one token and one renewer", () => {
      const writer = new BinaryWriter()

      // error_code
      writer.writeInt16(0)

      // tokens array (length 1)
      writer.writeInt32(1)

      // --- token 0 ---
      writer.writeString("User") // principal_type
      writer.writeString("alice") // principal_name
      writer.writeInt64(1700000000000n) // issue_timestamp_ms
      writer.writeInt64(1700086400000n) // expiry_timestamp_ms
      writer.writeInt64(1700172800000n) // max_timestamp_ms
      writer.writeString("token-abc") // token_id
      writer.writeBytes(new Uint8Array([0xde, 0xad])) // hmac

      // renewers array (length 1)
      writer.writeInt32(1)
      writer.writeString("User") // renewer principal_type
      writer.writeString("bob") // renewer principal_name

      // throttle_time_ms
      writer.writeInt32(10)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDescribeDelegationTokenResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.throttleTimeMs).toBe(10)
      expect(result.value.tokens).toHaveLength(1)

      const token = result.value.tokens[0]
      expect(token.principalType).toBe("User")
      expect(token.principalName).toBe("alice")
      expect(token.tokenRequesterPrincipalType).toBe("")
      expect(token.tokenRequesterPrincipalName).toBe("")
      expect(token.issueTimestampMs).toBe(1700000000000n)
      expect(token.expiryTimestampMs).toBe(1700086400000n)
      expect(token.maxTimestampMs).toBe(1700172800000n)
      expect(token.tokenId).toBe("token-abc")
      expect(token.hmac).toEqual(new Uint8Array([0xde, 0xad]))
      expect(token.renewers).toHaveLength(1)
      expect(token.renewers[0].principalType).toBe("User")
      expect(token.renewers[0].principalName).toBe("bob")
    })

    it("decodes an empty tokens array", () => {
      const writer = new BinaryWriter()

      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // empty tokens array
      writer.writeInt32(0) // throttle_time_ms

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDescribeDelegationTokenResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.tokens).toHaveLength(0)
    })

    it("decodes an error response", () => {
      const writer = new BinaryWriter()

      writer.writeInt16(65) // DELEGATION_TOKEN_NOT_FOUND
      writer.writeInt32(0) // empty tokens
      writer.writeInt32(0) // throttle

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDescribeDelegationTokenResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(65)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

      // error_code
      writer.writeInt16(0)

      // tokens compact array (length + 1 = 2)
      writer.writeUnsignedVarInt(2)

      // --- token 0 ---
      writer.writeCompactString("User")
      writer.writeCompactString("carol")
      writer.writeInt64(1700000000000n)
      writer.writeInt64(1700086400000n)
      writer.writeInt64(1700172800000n)
      writer.writeCompactString("tok-1")
      writer.writeCompactBytes(new Uint8Array([0xca, 0xfe]))

      // renewers compact array (length + 1 = 1 → 0 renewers)
      writer.writeUnsignedVarInt(1)

      // token tagged fields
      writer.writeTaggedFields([])

      // throttle_time_ms
      writer.writeInt32(5)

      // response tagged fields
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDescribeDelegationTokenResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.tokens).toHaveLength(1)

      const token = result.value.tokens[0]
      expect(token.principalType).toBe("User")
      expect(token.principalName).toBe("carol")
      expect(token.tokenId).toBe("tok-1")
      expect(token.hmac).toEqual(new Uint8Array([0xca, 0xfe]))
      expect(token.renewers).toHaveLength(0)
      expect(token.taggedFields).toEqual([])
    })
  })

  describe("v3 — with tokenRequester", () => {
    it("decodes token with requester principal fields", () => {
      const writer = new BinaryWriter()

      // error_code
      writer.writeInt16(0)

      // tokens compact array (length + 1 = 2)
      writer.writeUnsignedVarInt(2)

      // --- token 0 ---
      writer.writeCompactString("User")
      writer.writeCompactString("alice")
      writer.writeCompactString("User") // token_requester_principal_type
      writer.writeCompactString("service-account") // token_requester_principal_name
      writer.writeInt64(1700000000000n)
      writer.writeInt64(1700086400000n)
      writer.writeInt64(1700172800000n)
      writer.writeCompactString("tok-v3")
      writer.writeCompactBytes(new Uint8Array([0xaa, 0xbb, 0xcc]))

      // renewers compact array (length + 1 = 2 → 1 renewer)
      writer.writeUnsignedVarInt(2)
      writer.writeCompactString("User")
      writer.writeCompactString("dave")
      writer.writeTaggedFields([]) // renewer tagged fields

      // token tagged fields
      writer.writeTaggedFields([])

      // throttle_time_ms
      writer.writeInt32(20)

      // response tagged fields
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDescribeDelegationTokenResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.throttleTimeMs).toBe(20)

      const token = result.value.tokens[0]
      expect(token.principalType).toBe("User")
      expect(token.principalName).toBe("alice")
      expect(token.tokenRequesterPrincipalType).toBe("User")
      expect(token.tokenRequesterPrincipalName).toBe("service-account")
      expect(token.tokenId).toBe("tok-v3")
      expect(token.hmac).toEqual(new Uint8Array([0xaa, 0xbb, 0xcc]))
      expect(token.renewers).toHaveLength(1)
      expect(token.renewers[0].principalName).toBe("dave")
    })
  })

  it("returns failure on truncated input", () => {
    const reader = new BinaryReader(new Uint8Array([0, 0]))
    const result = decodeDescribeDelegationTokenResponse(reader, 0)
    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated token entry", () => {
    const w = new BinaryWriter()
    w.writeInt16(0) // error_code
    w.writeInt32(1) // tokens array (1 entry)
    w.writeString("User") // principal_type — but principal_name missing
    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeDelegationTokenResponse(reader, 0)
    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated token timestamps", () => {
    const w = new BinaryWriter()
    w.writeInt16(0) // error_code
    w.writeInt32(1) // tokens array (1 entry)
    w.writeString("User") // principal_type
    w.writeString("alice") // principal_name
    // issue_timestamp_ms missing
    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeDelegationTokenResponse(reader, 0)
    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated v2 token entry", () => {
    const w = new BinaryWriter()
    w.writeInt16(0) // error_code
    w.writeUnsignedVarInt(2) // tokens compact array (1 entry)
    w.writeCompactString("User") // principal_type
    // principal_name missing
    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeDelegationTokenResponse(reader, 2)
    expect(result.ok).toBe(false)
  })
})
