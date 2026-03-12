import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDeleteGroupsRequest,
  decodeDeleteGroupsResponse,
  type DeleteGroupsRequest,
  encodeDeleteGroupsRequest
} from "../../../protocol/delete-groups"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDeleteGroupsRequest", () => {
  describe("v0 — basic fields", () => {
    it("encodes groups_names array", () => {
      const writer = new BinaryWriter()
      const request: DeleteGroupsRequest = {
        groupsNames: ["group-1", "group-2"]
      }
      encodeDeleteGroupsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // array count (INT32)
      const countResult = reader.readInt32()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(2)

      // group IDs
      const g1Result = reader.readString()
      expect(g1Result.ok).toBe(true)
      expect(g1Result.ok && g1Result.value).toBe("group-1")

      const g2Result = reader.readString()
      expect(g2Result.ok).toBe(true)
      expect(g2Result.ok && g2Result.value).toBe("group-2")

      expect(reader.remaining).toBe(0)
    })

    it("encodes empty groups_names array", () => {
      const writer = new BinaryWriter()
      encodeDeleteGroupsRequest(writer, { groupsNames: [] }, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const countResult = reader.readInt32()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(0)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: DeleteGroupsRequest = {
        groupsNames: ["flex-group"]
      }
      encodeDeleteGroupsRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // compact array count (unsigned varint, length + 1)
      const countResult = reader.readUnsignedVarInt()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(2) // 1 + 1

      // compact string
      const groupResult = reader.readCompactString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("flex-group")

      // tagged fields
      const tagResult = reader.readTaggedFields()
      expect(tagResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// buildDeleteGroupsRequest (framed)
// ---------------------------------------------------------------------------

describe("buildDeleteGroupsRequest", () => {
  it("builds a framed v0 request", () => {
    const request: DeleteGroupsRequest = { groupsNames: ["grp"] }
    const framed = buildDeleteGroupsRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.DeleteGroups)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDeleteGroupsResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes successful deletion results", () => {
      const w = new BinaryWriter()
      // results array
      w.writeInt32(2) // count
      // result 1
      w.writeString("group-1")
      w.writeInt16(0) // no error
      // result 2
      w.writeString("group-2")
      w.writeInt16(0) // no error

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDeleteGroupsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.results).toHaveLength(2)
      expect(result.value.results[0].groupId).toBe("group-1")
      expect(result.value.results[0].errorCode).toBe(0)
      expect(result.value.results[1].groupId).toBe("group-2")
      expect(result.value.results[1].errorCode).toBe(0)
    })

    it("decodes error results", () => {
      const w = new BinaryWriter()
      w.writeInt32(1) // count
      w.writeString("active-group")
      w.writeInt16(68) // NON_EMPTY_GROUP

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDeleteGroupsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.results[0].errorCode).toBe(68)
    })

    it("decodes empty results", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // count

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDeleteGroupsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.results).toHaveLength(0)
    })
  })

  describe("v1 — with throttle time", () => {
    it("decodes throttle_time_ms", () => {
      const w = new BinaryWriter()
      w.writeInt32(150) // throttle_time_ms
      w.writeInt32(1) // count
      w.writeString("grp")
      w.writeInt16(0)

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDeleteGroupsResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(150)
    })
  })

  describe("v2 — flexible format", () => {
    it("decodes with compact strings and tagged fields", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      // results compact array (count + 1)
      w.writeUnsignedVarInt(2) // 1 result + 1
      w.writeCompactString("flex-group")
      w.writeInt16(0) // error_code
      w.writeUnsignedVarInt(0) // result tagged fields
      w.writeUnsignedVarInt(0) // top-level tagged fields

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDeleteGroupsResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.results).toHaveLength(1)
      expect(result.value.results[0].groupId).toBe("flex-group")
      expect(result.value.results[0].errorCode).toBe(0)
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeDeleteGroupsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v1 input", () => {
      const reader = new BinaryReader(new Uint8Array(3))
      const result = decodeDeleteGroupsResponse(reader, 1)
      expect(result.ok).toBe(false)
    })
  })
})
