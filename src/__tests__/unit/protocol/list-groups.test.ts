import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildListGroupsRequest,
  decodeListGroupsResponse,
  encodeListGroupsRequest,
  type ListGroupsRequest
} from "../../../protocol/list-groups"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeListGroupsRequest", () => {
  describe("v0 — empty body", () => {
    it("encodes nothing for v0", () => {
      const writer = new BinaryWriter()
      encodeListGroupsRequest(writer, {}, 0)
      const buf = writer.finish()
      expect(buf.byteLength).toBe(0)
    })
  })

  describe("v3 — flexible encoding with tagged fields", () => {
    it("encodes tagged fields", () => {
      const writer = new BinaryWriter()
      encodeListGroupsRequest(writer, {}, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // tagged fields (empty)
      const tagResult = reader.readTaggedFields()
      expect(tagResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v4 — adds states_filter", () => {
    it("encodes states_filter array", () => {
      const writer = new BinaryWriter()
      const request: ListGroupsRequest = {
        statesFilter: ["Stable", "Empty"]
      }
      encodeListGroupsRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // compact array count (unsigned varint, length + 1)
      const countResult = reader.readUnsignedVarInt()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(3) // 2 + 1

      // compact strings
      const s1Result = reader.readCompactString()
      expect(s1Result.ok).toBe(true)
      expect(s1Result.ok && s1Result.value).toBe("Stable")

      const s2Result = reader.readCompactString()
      expect(s2Result.ok).toBe(true)
      expect(s2Result.ok && s2Result.value).toBe("Empty")
    })

    it("encodes empty states_filter", () => {
      const writer = new BinaryWriter()
      encodeListGroupsRequest(writer, { statesFilter: [] }, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // compact array: 0 + 1 = 1
      const countResult = reader.readUnsignedVarInt()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(1)
    })
  })
})

// ---------------------------------------------------------------------------
// buildListGroupsRequest (framed)
// ---------------------------------------------------------------------------

describe("buildListGroupsRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildListGroupsRequest(1, 0, {}, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.ListGroups)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeListGroupsResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes a response with groups", () => {
      const w = new BinaryWriter()
      w.writeInt16(0) // error_code
      w.writeInt32(2) // groups count
      // group 1
      w.writeString("group-1")
      w.writeString("consumer")
      // group 2
      w.writeString("group-2")
      w.writeString("connect")

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeListGroupsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.groups).toHaveLength(2)
      expect(result.value.groups[0].groupId).toBe("group-1")
      expect(result.value.groups[0].protocolType).toBe("consumer")
      expect(result.value.groups[1].groupId).toBe("group-2")
      expect(result.value.groups[1].protocolType).toBe("connect")
    })

    it("decodes an empty groups list", () => {
      const w = new BinaryWriter()
      w.writeInt16(0) // error_code
      w.writeInt32(0) // groups count

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeListGroupsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.groups).toHaveLength(0)
    })

    it("decodes an error response", () => {
      const w = new BinaryWriter()
      w.writeInt16(29) // CLUSTER_AUTHORIZATION_FAILED
      w.writeInt32(0) // groups count

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeListGroupsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(29)
    })
  })

  describe("v1 — with throttle time", () => {
    it("decodes throttle_time_ms", () => {
      const w = new BinaryWriter()
      w.writeInt32(200) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeInt32(0) // groups count

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeListGroupsResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(200)
    })
  })

  describe("v3 — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      // groups compact array (count + 1)
      w.writeUnsignedVarInt(2) // 1 group + 1
      w.writeCompactString("flex-group")
      w.writeCompactString("consumer")
      w.writeUnsignedVarInt(0) // group tagged fields
      w.writeUnsignedVarInt(0) // top-level tagged fields

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeListGroupsResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.groups).toHaveLength(1)
      expect(result.value.groups[0].groupId).toBe("flex-group")
      expect(result.value.groups[0].protocolType).toBe("consumer")
      expect(result.value.groups[0].groupState).toBe("")
    })
  })

  describe("v4 — with group_state", () => {
    it("decodes group_state per group", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      // groups compact array (count + 1)
      w.writeUnsignedVarInt(2) // 1 group + 1
      w.writeCompactString("my-group")
      w.writeCompactString("consumer")
      w.writeCompactString("Stable") // group_state
      w.writeUnsignedVarInt(0) // group tagged fields
      w.writeUnsignedVarInt(0) // top-level tagged fields

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeListGroupsResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.groups[0].groupState).toBe("Stable")
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeListGroupsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v1 input", () => {
      const reader = new BinaryReader(new Uint8Array(3))
      const result = decodeListGroupsResponse(reader, 1)
      expect(result.ok).toBe(false)
    })
  })
})
