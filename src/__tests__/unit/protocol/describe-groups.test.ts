import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDescribeGroupsRequest,
  decodeDescribeGroupsResponse,
  type DescribeGroupsRequest,
  encodeDescribeGroupsRequest
} from "../../../protocol/describe-groups"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeGroupsRequest", () => {
  describe("v0 — basic fields", () => {
    it("encodes groups array", () => {
      const writer = new BinaryWriter()
      const request: DescribeGroupsRequest = {
        groups: ["group-1", "group-2"]
      }
      encodeDescribeGroupsRequest(writer, request, 0)
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

    it("encodes empty groups array", () => {
      const writer = new BinaryWriter()
      encodeDescribeGroupsRequest(writer, { groups: [] }, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const countResult = reader.readInt32()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(0)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v3 — adds include_authorized_operations", () => {
    it("encodes include_authorized_operations = true", () => {
      const writer = new BinaryWriter()
      const request: DescribeGroupsRequest = {
        groups: ["grp"],
        includeAuthorizedOperations: true
      }
      encodeDescribeGroupsRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // array count
      reader.readString() // group ID

      // include_authorized_operations (INT8)
      const authResult = reader.readInt8()
      expect(authResult.ok).toBe(true)
      expect(authResult.ok && authResult.value).toBe(1)

      expect(reader.remaining).toBe(0)
    })

    it("encodes include_authorized_operations = false by default", () => {
      const writer = new BinaryWriter()
      encodeDescribeGroupsRequest(writer, { groups: ["grp"] }, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // count
      reader.readString() // group ID

      const authResult = reader.readInt8()
      expect(authResult.ok).toBe(true)
      expect(authResult.ok && authResult.value).toBe(0)
    })
  })

  describe("v5 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: DescribeGroupsRequest = {
        groups: ["flex-group"],
        includeAuthorizedOperations: false
      }
      encodeDescribeGroupsRequest(writer, request, 5)
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
    })
  })
})

// ---------------------------------------------------------------------------
// buildDescribeGroupsRequest (framed)
// ---------------------------------------------------------------------------

describe("buildDescribeGroupsRequest", () => {
  it("builds a framed v0 request", () => {
    const request: DescribeGroupsRequest = { groups: ["grp"] }
    const framed = buildDescribeGroupsRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.DescribeGroups)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeGroupsResponse", () => {
  describe("v0 — non-flexible, single group", () => {
    it("decodes a group with one member", () => {
      const w = new BinaryWriter()
      // groups array count
      w.writeInt32(1)
      // group entry
      w.writeInt16(0) // error_code
      w.writeString("test-group") // group_id
      w.writeString("Stable") // group_state
      w.writeString("consumer") // protocol_type
      w.writeString("range") // protocol_data
      // members array
      w.writeInt32(1) // count
      w.writeString("member-1") // member_id
      w.writeString("client-1") // client_id
      w.writeString("/127.0.0.1") // client_host
      w.writeBytes(new Uint8Array([1, 2, 3])) // member_metadata
      w.writeBytes(new Uint8Array([4, 5])) // member_assignment

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeGroupsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.groups).toHaveLength(1)

      const group = result.value.groups[0]
      expect(group.errorCode).toBe(0)
      expect(group.groupId).toBe("test-group")
      expect(group.groupState).toBe("Stable")
      expect(group.protocolType).toBe("consumer")
      expect(group.protocolData).toBe("range")
      expect(group.members).toHaveLength(1)

      const member = group.members[0]
      expect(member.memberId).toBe("member-1")
      expect(member.groupInstanceId).toBe(null)
      expect(member.clientId).toBe("client-1")
      expect(member.clientHost).toBe("/127.0.0.1")
      expect(member.memberMetadata).toEqual(new Uint8Array([1, 2, 3]))
      expect(member.memberAssignment).toEqual(new Uint8Array([4, 5]))
    })

    it("decodes an empty group with no members", () => {
      const w = new BinaryWriter()
      w.writeInt32(1) // groups count
      w.writeInt16(0) // error_code
      w.writeString("empty-group")
      w.writeString("Empty")
      w.writeString("consumer")
      w.writeString("")
      w.writeInt32(0) // members count

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeGroupsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      const group = result.value.groups[0]
      expect(group.groupState).toBe("Empty")
      expect(group.members).toHaveLength(0)
    })

    it("decodes an error response", () => {
      const w = new BinaryWriter()
      w.writeInt32(1) // groups count
      w.writeInt16(69) // GROUP_ID_NOT_FOUND
      w.writeString("bad-group")
      w.writeString("")
      w.writeString("")
      w.writeString("")
      w.writeInt32(0) // members count

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeGroupsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.groups[0].errorCode).toBe(69)
    })
  })

  describe("v1 — with throttle time", () => {
    it("decodes throttle_time_ms", () => {
      const w = new BinaryWriter()
      w.writeInt32(100) // throttle_time_ms
      w.writeInt32(1) // groups count
      w.writeInt16(0) // error_code
      w.writeString("grp")
      w.writeString("Stable")
      w.writeString("consumer")
      w.writeString("range")
      w.writeInt32(0) // members count

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeGroupsResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(100)
    })
  })

  describe("v3 — with authorized_operations", () => {
    it("decodes authorized_operations per group", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(1) // groups count
      w.writeInt16(0) // error_code
      w.writeString("grp")
      w.writeString("Stable")
      w.writeString("consumer")
      w.writeString("range")
      w.writeInt32(0) // members count
      w.writeInt32(2048) // authorized_operations

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeGroupsResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.groups[0].authorizedOperations).toBe(2048)
    })
  })

  describe("v4 — with group_instance_id per member", () => {
    it("decodes group_instance_id", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(1) // groups count
      w.writeInt16(0) // error_code
      w.writeString("grp")
      w.writeString("Stable")
      w.writeString("consumer")
      w.writeString("range")
      w.writeInt32(1) // members count
      w.writeString("member-1")
      w.writeString("instance-1") // group_instance_id
      w.writeString("client-1")
      w.writeString("/127.0.0.1")
      w.writeBytes(new Uint8Array(0))
      w.writeBytes(new Uint8Array(0))
      w.writeInt32(-2_147_483_648) // authorized_operations

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeGroupsResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.groups[0].members[0].groupInstanceId).toBe("instance-1")
    })

    it("decodes null group_instance_id", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(1) // groups count
      w.writeInt16(0) // error_code
      w.writeString("grp")
      w.writeString("Stable")
      w.writeString("consumer")
      w.writeString("range")
      w.writeInt32(1) // members count
      w.writeString("member-1")
      w.writeString(null) // null group_instance_id
      w.writeString("client-1")
      w.writeString("/127.0.0.1")
      w.writeBytes(new Uint8Array(0))
      w.writeBytes(new Uint8Array(0))
      w.writeInt32(-2_147_483_648) // authorized_operations

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeGroupsResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.groups[0].members[0].groupInstanceId).toBe(null)
    })
  })

  describe("v5 — flexible format", () => {
    it("decodes a v5 response with compact strings and tagged fields", () => {
      const w = new BinaryWriter()
      w.writeInt32(50) // throttle_time_ms
      // groups array (compact: count + 1)
      w.writeUnsignedVarInt(2) // 1 group + 1
      w.writeInt16(0) // error_code
      w.writeCompactString("flex-grp") // group_id
      w.writeCompactString("Stable") // group_state
      w.writeCompactString("consumer") // protocol_type
      w.writeCompactString("range") // protocol_data
      // members array (compact: count + 1)
      w.writeUnsignedVarInt(1) // 0 members + 1
      w.writeInt32(0) // authorized_operations
      w.writeUnsignedVarInt(0) // group tagged fields
      w.writeUnsignedVarInt(0) // top-level tagged fields

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeGroupsResponse(reader, 5)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.groups[0].groupId).toBe("flex-grp")
      expect(result.value.groups[0].groupState).toBe("Stable")
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeDescribeGroupsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v1 input", () => {
      const reader = new BinaryReader(new Uint8Array(3))
      const result = decodeDescribeGroupsResponse(reader, 1)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated group member", () => {
      const w = new BinaryWriter()
      // v0 — no throttle_time
      w.writeInt32(1) // groups array (1 entry)
      w.writeInt16(0) // error_code
      w.writeString("group-1") // group_id
      w.writeString("Stable") // group_state
      w.writeString("consumer") // protocol_type
      w.writeString("range") // protocol_data
      w.writeInt32(1) // members array (1 entry)
      w.writeString("member-1") // member_id — but client_id missing
      const buf = w.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDescribeGroupsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v5 group entry", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeUnsignedVarInt(2) // groups compact array (1 entry)
      w.writeInt16(0) // error_code
      w.writeCompactString("group-1") // group_id
      // group_state missing
      const buf = w.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDescribeGroupsResponse(reader, 5)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated group protocol_type", () => {
      const w = new BinaryWriter()
      w.writeInt32(1) // groups array (1 entry)
      w.writeInt16(0) // error_code
      w.writeString("group-1") // group_id
      w.writeString("Stable") // group_state
      // protocol_type missing
      const buf = w.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDescribeGroupsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })
  })
})
