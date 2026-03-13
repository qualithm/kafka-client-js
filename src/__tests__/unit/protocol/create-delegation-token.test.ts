import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildCreateDelegationTokenRequest,
  type CreateDelegationTokenRequest,
  decodeCreateDelegationTokenResponse,
  encodeCreateDelegationTokenRequest
} from "../../../protocol/create-delegation-token"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeCreateDelegationTokenRequest", () => {
  describe("v0–v1 — non-flexible encoding", () => {
    it("encodes renewers array and max lifetime", () => {
      const writer = new BinaryWriter()
      const request: CreateDelegationTokenRequest = {
        renewers: [
          { principalType: "User", principalName: "alice" },
          { principalType: "User", principalName: "bob" }
        ],
        maxLifetimeMs: 86400000n
      }
      encodeCreateDelegationTokenRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // renewers array length (INT32)
      const renewersLen = reader.readInt32()
      expect(renewersLen.ok && renewersLen.value).toBe(2)

      // renewer 0
      const r0Type = reader.readString()
      expect(r0Type.ok && r0Type.value).toBe("User")
      const r0Name = reader.readString()
      expect(r0Name.ok && r0Name.value).toBe("alice")

      // renewer 1
      const r1Type = reader.readString()
      expect(r1Type.ok && r1Type.value).toBe("User")
      const r1Name = reader.readString()
      expect(r1Name.ok && r1Name.value).toBe("bob")

      // max_lifetime_ms (INT64)
      const maxLifetime = reader.readInt64()
      expect(maxLifetime.ok && maxLifetime.value).toBe(86400000n)

      expect(reader.remaining).toBe(0)
    })

    it("encodes empty renewers array", () => {
      const writer = new BinaryWriter()
      const request: CreateDelegationTokenRequest = {
        renewers: [],
        maxLifetimeMs: -1n
      }
      encodeCreateDelegationTokenRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const renewersLen = reader.readInt32()
      expect(renewersLen.ok && renewersLen.value).toBe(0)

      const maxLifetime = reader.readInt64()
      expect(maxLifetime.ok && maxLifetime.value).toBe(-1n)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: CreateDelegationTokenRequest = {
        renewers: [{ principalType: "User", principalName: "charlie" }],
        maxLifetimeMs: 3600000n
      }
      encodeCreateDelegationTokenRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // renewers array (compact: length + 1)
      const renewersLen = reader.readUnsignedVarInt()
      expect(renewersLen.ok && renewersLen.value).toBe(2)

      // renewer 0 (compact strings)
      const r0Type = reader.readCompactString()
      expect(r0Type.ok && r0Type.value).toBe("User")
      const r0Name = reader.readCompactString()
      expect(r0Name.ok && r0Name.value).toBe("charlie")

      // renewer tagged fields
      const rTag = reader.readTaggedFields()
      expect(rTag.ok).toBe(true)

      // max_lifetime_ms
      const maxLifetime = reader.readInt64()
      expect(maxLifetime.ok && maxLifetime.value).toBe(3600000n)

      // request tagged fields
      const reqTag = reader.readTaggedFields()
      expect(reqTag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v3 — adds owner principal fields", () => {
    it("encodes owner principal type and name", () => {
      const writer = new BinaryWriter()
      const request: CreateDelegationTokenRequest = {
        ownerPrincipalType: "User",
        ownerPrincipalName: "admin",
        renewers: [{ principalType: "User", principalName: "dave" }],
        maxLifetimeMs: 7200000n
      }
      encodeCreateDelegationTokenRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // owner_principal_type (compact string)
      const ownerType = reader.readCompactString()
      expect(ownerType.ok && ownerType.value).toBe("User")

      // owner_principal_name (compact string)
      const ownerName = reader.readCompactString()
      expect(ownerName.ok && ownerName.value).toBe("admin")

      // renewers array (compact: length + 1)
      const renewersLen = reader.readUnsignedVarInt()
      expect(renewersLen.ok && renewersLen.value).toBe(2)

      // renewer 0
      const r0Type = reader.readCompactString()
      expect(r0Type.ok && r0Type.value).toBe("User")
      const r0Name = reader.readCompactString()
      expect(r0Name.ok && r0Name.value).toBe("dave")

      // renewer tagged fields
      const rTag = reader.readTaggedFields()
      expect(rTag.ok).toBe(true)

      // max_lifetime_ms
      const maxLifetime = reader.readInt64()
      expect(maxLifetime.ok && maxLifetime.value).toBe(7200000n)

      // request tagged fields
      const reqTag = reader.readTaggedFields()
      expect(reqTag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })

    it("encodes null owner principal as null compact strings", () => {
      const writer = new BinaryWriter()
      const request: CreateDelegationTokenRequest = {
        ownerPrincipalType: null,
        ownerPrincipalName: null,
        renewers: [],
        maxLifetimeMs: -1n
      }
      encodeCreateDelegationTokenRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // owner_principal_type null
      const ownerType = reader.readCompactString()
      expect(ownerType.ok && ownerType.value).toBeNull()

      // owner_principal_name null
      const ownerName = reader.readCompactString()
      expect(ownerName.ok && ownerName.value).toBeNull()

      // empty renewers
      const renewersLen = reader.readUnsignedVarInt()
      expect(renewersLen.ok && renewersLen.value).toBe(1) // 0 + 1

      // max_lifetime_ms
      const maxLifetime = reader.readInt64()
      expect(maxLifetime.ok && maxLifetime.value).toBe(-1n)

      // tagged fields
      const tag = reader.readTaggedFields()
      expect(tag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

describe("buildCreateDelegationTokenRequest", () => {
  it("produces a framed request with correct header", () => {
    const request: CreateDelegationTokenRequest = {
      renewers: [{ principalType: "User", principalName: "alice" }],
      maxLifetimeMs: 86400000n
    }
    const frame = buildCreateDelegationTokenRequest(42, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    // size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    // API key
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.CreateDelegationToken)

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

describe("decodeCreateDelegationTokenResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes a successful response", () => {
      const writer = new BinaryWriter()

      // error_code
      writer.writeInt16(0)
      // principal_type
      writer.writeString("User")
      // principal_name
      writer.writeString("alice")
      // issue_timestamp_ms
      writer.writeInt64(1700000000000n)
      // expiry_timestamp_ms
      writer.writeInt64(1700086400000n)
      // max_timestamp_ms
      writer.writeInt64(1700172800000n)
      // token_id
      writer.writeString("token-123")
      // hmac
      writer.writeBytes(new Uint8Array([0xaa, 0xbb, 0xcc]))
      // throttle_time_ms
      writer.writeInt32(50)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeCreateDelegationTokenResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.principalType).toBe("User")
      expect(result.value.principalName).toBe("alice")
      expect(result.value.tokenRequesterPrincipalType).toBe("")
      expect(result.value.tokenRequesterPrincipalName).toBe("")
      expect(result.value.issueTimestampMs).toBe(1700000000000n)
      expect(result.value.expiryTimestampMs).toBe(1700086400000n)
      expect(result.value.maxTimestampMs).toBe(1700172800000n)
      expect(result.value.tokenId).toBe("token-123")
      expect(result.value.hmac).toEqual(new Uint8Array([0xaa, 0xbb, 0xcc]))
      expect(result.value.throttleTimeMs).toBe(50)
    })

    it("decodes an error response", () => {
      const writer = new BinaryWriter()

      writer.writeInt16(58) // DELEGATION_TOKEN_AUTH_DISABLED
      writer.writeString("")
      writer.writeString("")
      writer.writeInt64(0n)
      writer.writeInt64(0n)
      writer.writeInt64(0n)
      writer.writeString("")
      writer.writeBytes(new Uint8Array(0))
      writer.writeInt32(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeCreateDelegationTokenResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(58)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

      writer.writeInt16(0)
      writer.writeCompactString("User")
      writer.writeCompactString("bob")
      writer.writeInt64(1700000000000n)
      writer.writeInt64(1700086400000n)
      writer.writeInt64(1700172800000n)
      writer.writeCompactString("token-456")
      writer.writeCompactBytes(new Uint8Array([0xdd, 0xee]))
      writer.writeInt32(10)
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeCreateDelegationTokenResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.principalName).toBe("bob")
      expect(result.value.tokenId).toBe("token-456")
      expect(result.value.hmac).toEqual(new Uint8Array([0xdd, 0xee]))
      expect(result.value.throttleTimeMs).toBe(10)
    })
  })

  describe("v3 — adds token requester fields", () => {
    it("decodes token requester principal", () => {
      const writer = new BinaryWriter()

      writer.writeInt16(0)
      writer.writeCompactString("User")
      writer.writeCompactString("owner")
      // token_requester fields (v3+)
      writer.writeCompactString("User")
      writer.writeCompactString("requester")
      writer.writeInt64(1700000000000n)
      writer.writeInt64(1700086400000n)
      writer.writeInt64(1700172800000n)
      writer.writeCompactString("token-789")
      writer.writeCompactBytes(new Uint8Array([0xff]))
      writer.writeInt32(0)
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeCreateDelegationTokenResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.principalType).toBe("User")
      expect(result.value.principalName).toBe("owner")
      expect(result.value.tokenRequesterPrincipalType).toBe("User")
      expect(result.value.tokenRequesterPrincipalName).toBe("requester")
      expect(result.value.tokenId).toBe("token-789")
    })
  })

  it("returns failure on truncated input", () => {
    const reader = new BinaryReader(new Uint8Array([0, 0]))
    const result = decodeCreateDelegationTokenResponse(reader, 0)
    expect(result.ok).toBe(false)
  })
})
