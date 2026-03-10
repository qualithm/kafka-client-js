import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  buildLeaveGroupRequest,
  decodeLeaveGroupResponse,
  encodeLeaveGroupRequest,
  type LeaveGroupRequest
} from "../../leave-group"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeLeaveGroupRequest", () => {
  describe("v0 — single member leave", () => {
    it("encodes group_id and member_id", () => {
      const writer = new BinaryWriter()
      const request: LeaveGroupRequest = {
        groupId: "test-group",
        memberId: "member-0"
      }
      encodeLeaveGroupRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id
      const groupResult = reader.readString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("test-group")

      // member_id
      const memberResult = reader.readString()
      expect(memberResult.ok).toBe(true)
      expect(memberResult.ok && memberResult.value).toBe("member-0")

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v3 — batched member leave", () => {
    it("encodes members array", () => {
      const writer = new BinaryWriter()
      const request: LeaveGroupRequest = {
        groupId: "test-group",
        members: [
          { memberId: "member-0", groupInstanceId: null },
          { memberId: "member-1", groupInstanceId: "instance-1" }
        ]
      }
      encodeLeaveGroupRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id
      const groupResult = reader.readString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("test-group")

      // members array length
      const countResult = reader.readInt32()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(2)

      // member 0: member_id
      const m0IdResult = reader.readString()
      expect(m0IdResult.ok).toBe(true)
      expect(m0IdResult.ok && m0IdResult.value).toBe("member-0")

      // member 0: group_instance_id (null)
      const m0InstanceResult = reader.readString()
      expect(m0InstanceResult.ok).toBe(true)
      expect(m0InstanceResult.ok && m0InstanceResult.value).toBe(null)

      // member 1: member_id
      const m1IdResult = reader.readString()
      expect(m1IdResult.ok).toBe(true)
      expect(m1IdResult.ok && m1IdResult.value).toBe("member-1")

      // member 1: group_instance_id
      const m1InstanceResult = reader.readString()
      expect(m1InstanceResult.ok).toBe(true)
      expect(m1InstanceResult.ok && m1InstanceResult.value).toBe("instance-1")
    })
  })

  describe("v4 — flexible encoding", () => {
    it("encodes with compact strings", () => {
      const writer = new BinaryWriter()
      const request: LeaveGroupRequest = {
        groupId: "flex-group",
        members: [{ memberId: "m-1", groupInstanceId: null }]
      }
      encodeLeaveGroupRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id (compact string)
      const groupResult = reader.readCompactString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("flex-group")
    })
  })

  describe("v5 — adds reason per member", () => {
    it("encodes reason for each member", () => {
      const writer = new BinaryWriter()
      const request: LeaveGroupRequest = {
        groupId: "grp",
        members: [{ memberId: "m-0", groupInstanceId: null, reason: "shutting down" }]
      }
      encodeLeaveGroupRequest(writer, request, 5)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id (compact, v5 >= 4)
      reader.readCompactString()

      // members compact array length (length + 1 as unsigned varint)
      const countResult = reader.readUnsignedVarInt()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(2) // 1 member + 1

      // member_id (compact)
      reader.readCompactString()
      // group_instance_id (compact, null)
      reader.readCompactString()
      // reason (compact string)
      const reasonResult = reader.readCompactString()
      expect(reasonResult.ok).toBe(true)
      expect(reasonResult.ok && reasonResult.value).toBe("shutting down")
    })
  })
})

// ---------------------------------------------------------------------------
// buildLeaveGroupRequest (framed)
// ---------------------------------------------------------------------------

describe("buildLeaveGroupRequest", () => {
  it("builds a framed v0 request", () => {
    const request: LeaveGroupRequest = {
      groupId: "grp",
      memberId: "m-0"
    }
    const framed = buildLeaveGroupRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.LeaveGroup)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeLeaveGroupResponse", () => {
  function buildResponseV0(errorCode: number): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt16(errorCode)
    return w.finish()
  }

  function buildResponseV1(throttleTimeMs: number, errorCode: number): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(throttleTimeMs)
    w.writeInt16(errorCode)
    return w.finish()
  }

  function buildResponseV3(
    throttleTimeMs: number,
    errorCode: number,
    members: { memberId: string; groupInstanceId: string | null; errorCode: number }[]
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(throttleTimeMs)
    w.writeInt16(errorCode)
    w.writeInt32(members.length)
    for (const m of members) {
      w.writeString(m.memberId)
      w.writeString(m.groupInstanceId)
      w.writeInt16(m.errorCode)
    }
    return w.finish()
  }

  describe("v0 — non-flexible", () => {
    it("decodes a successful response", () => {
      const body = buildResponseV0(0)
      const reader = new BinaryReader(body)
      const result = decodeLeaveGroupResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.members).toHaveLength(0)
    })

    it("decodes an error response", () => {
      const body = buildResponseV0(25)
      const reader = new BinaryReader(body)
      const result = decodeLeaveGroupResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(25)
    })
  })

  describe("v1 — with throttle time", () => {
    it("decodes throttle_time_ms", () => {
      const body = buildResponseV1(100, 0)
      const reader = new BinaryReader(body)
      const result = decodeLeaveGroupResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(100)
      expect(result.value.errorCode).toBe(0)
    })
  })

  describe("v3 — with per-member results", () => {
    it("decodes per-member leave results", () => {
      const body = buildResponseV3(0, 0, [
        { memberId: "m-0", groupInstanceId: null, errorCode: 0 },
        { memberId: "m-1", groupInstanceId: "inst-1", errorCode: 0 }
      ])
      const reader = new BinaryReader(body)
      const result = decodeLeaveGroupResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.members).toHaveLength(2)
      expect(result.value.members[0].memberId).toBe("m-0")
      expect(result.value.members[0].groupInstanceId).toBe(null)
      expect(result.value.members[1].memberId).toBe("m-1")
      expect(result.value.members[1].groupInstanceId).toBe("inst-1")
    })

    it("decodes per-member errors", () => {
      const body = buildResponseV3(0, 0, [
        { memberId: "m-0", groupInstanceId: null, errorCode: 25 }
      ])
      const reader = new BinaryReader(body)
      const result = decodeLeaveGroupResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.members[0].errorCode).toBe(25)
    })
  })

  describe("v4 — flexible format response with members", () => {
    function buildResponseV4(
      throttleTimeMs: number,
      errorCode: number,
      members: { memberId: string; groupInstanceId: string | null; errorCode: number }[]
    ): Uint8Array {
      const w = new BinaryWriter()
      w.writeInt32(throttleTimeMs)
      w.writeInt16(errorCode)
      // members (compact array: length+1)
      w.writeUnsignedVarInt(members.length + 1)
      for (const m of members) {
        w.writeCompactString(m.memberId)
        w.writeCompactString(m.groupInstanceId)
        w.writeInt16(m.errorCode)
        w.writeUnsignedVarInt(0) // member tagged fields
      }
      // response tagged fields
      w.writeUnsignedVarInt(0)
      return w.finish()
    }

    it("decodes flexible format with compact strings and tagged fields", () => {
      const body = buildResponseV4(50, 0, [
        { memberId: "m-0", groupInstanceId: null, errorCode: 0 },
        { memberId: "m-1", groupInstanceId: "inst-1", errorCode: 0 }
      ])
      const reader = new BinaryReader(body)
      const result = decodeLeaveGroupResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.members).toHaveLength(2)
      expect(result.value.members[0].memberId).toBe("m-0")
      expect(result.value.members[0].groupInstanceId).toBe(null)
      expect(result.value.members[1].groupInstanceId).toBe("inst-1")
    })

    it("decodes per-member error in flexible format", () => {
      const body = buildResponseV4(0, 0, [
        { memberId: "m-0", groupInstanceId: null, errorCode: 25 }
      ])
      const reader = new BinaryReader(body)
      const result = decodeLeaveGroupResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.members[0].errorCode).toBe(25)
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeLeaveGroupResponse(reader, 0)
      expect(result.ok).toBe(false)
    })
  })
})
