/**
 * JoinGroup (API key 11) request/response tests.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_JoinGroup
 */

import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildJoinGroupRequest,
  decodeJoinGroupResponse,
  encodeJoinGroupRequest,
  type JoinGroupRequest
} from "../../../protocol/join-group"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeJoinGroupRequest", () => {
  describe("v0 — basic fields", () => {
    it("encodes group_id, session_timeout_ms, member_id, protocol_type, protocols", () => {
      const writer = new BinaryWriter()
      const metadata = new Uint8Array([1, 2, 3])
      const request: JoinGroupRequest = {
        groupId: "test-group",
        sessionTimeoutMs: 30000,
        memberId: "",
        protocolType: "consumer",
        protocols: [{ name: "range", metadata }]
      }
      encodeJoinGroupRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id
      const groupResult = reader.readString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("test-group")

      // session_timeout_ms
      const sessionResult = reader.readInt32()
      expect(sessionResult.ok).toBe(true)
      expect(sessionResult.ok && sessionResult.value).toBe(30000)

      // member_id
      const memberResult = reader.readString()
      expect(memberResult.ok).toBe(true)
      expect(memberResult.ok && memberResult.value).toBe("")

      // protocol_type
      const ptResult = reader.readString()
      expect(ptResult.ok).toBe(true)
      expect(ptResult.ok && ptResult.value).toBe("consumer")

      // protocols array length
      const protocolCountResult = reader.readInt32()
      expect(protocolCountResult.ok).toBe(true)
      expect(protocolCountResult.ok && protocolCountResult.value).toBe(1)

      // protocol name
      const nameResult = reader.readString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("range")

      // protocol metadata (bytes)
      const metaResult = reader.readBytes()
      expect(metaResult.ok).toBe(true)
      if (metaResult.ok) {
        expect(metaResult.value).toEqual(metadata)
      }

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v1 — adds rebalance_timeout_ms", () => {
    it("encodes rebalance timeout", () => {
      const writer = new BinaryWriter()
      const request: JoinGroupRequest = {
        groupId: "grp",
        sessionTimeoutMs: 30000,
        rebalanceTimeoutMs: 60000,
        memberId: "",
        protocolType: "consumer",
        protocols: [{ name: "range", metadata: new Uint8Array(0) }]
      }
      encodeJoinGroupRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id
      reader.readString()
      // session_timeout_ms
      reader.readInt32()
      // rebalance_timeout_ms
      const rebalanceResult = reader.readInt32()
      expect(rebalanceResult.ok).toBe(true)
      expect(rebalanceResult.ok && rebalanceResult.value).toBe(60000)
    })
  })

  describe("v6 — flexible encoding", () => {
    it("encodes with compact strings", () => {
      const writer = new BinaryWriter()
      const request: JoinGroupRequest = {
        groupId: "flex-group",
        sessionTimeoutMs: 10000,
        rebalanceTimeoutMs: 20000,
        memberId: "member-1",
        groupInstanceId: null,
        protocolType: "consumer",
        protocols: [{ name: "range", metadata: new Uint8Array([0xab]) }]
      }
      encodeJoinGroupRequest(writer, request, 6)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id (compact string)
      const groupResult = reader.readCompactString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("flex-group")
    })
  })
})

// ---------------------------------------------------------------------------
// buildJoinGroupRequest (framed)
// ---------------------------------------------------------------------------

describe("buildJoinGroupRequest", () => {
  it("builds a framed v0 request", () => {
    const request: JoinGroupRequest = {
      groupId: "grp",
      sessionTimeoutMs: 30000,
      memberId: "",
      protocolType: "consumer",
      protocols: [{ name: "range", metadata: new Uint8Array(0) }]
    }
    const framed = buildJoinGroupRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.JoinGroup)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeJoinGroupResponse", () => {
  function buildResponseV0(
    errorCode: number,
    generationId: number,
    protocolName: string,
    leader: string,
    memberId: string,
    members: { memberId: string; metadata: Uint8Array }[]
  ): Uint8Array {
    const w = new BinaryWriter()
    // v0: no throttle_time_ms
    w.writeInt16(errorCode)
    w.writeInt32(generationId)
    w.writeString(protocolName)
    w.writeString(leader)
    w.writeString(memberId)
    w.writeInt32(members.length)
    for (const m of members) {
      w.writeString(m.memberId)
      w.writeBytes(m.metadata)
    }
    return w.finish()
  }

  function buildResponseV2(
    throttleTimeMs: number,
    errorCode: number,
    generationId: number,
    protocolName: string,
    leader: string,
    memberId: string,
    members: { memberId: string; metadata: Uint8Array }[]
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(throttleTimeMs)
    w.writeInt16(errorCode)
    w.writeInt32(generationId)
    w.writeString(protocolName)
    w.writeString(leader)
    w.writeString(memberId)
    w.writeInt32(members.length)
    for (const m of members) {
      w.writeString(m.memberId)
      w.writeBytes(m.metadata)
    }
    return w.finish()
  }

  describe("v0 — non-flexible", () => {
    it("decodes a successful response", () => {
      const meta = new Uint8Array([0x01, 0x02])
      const body = buildResponseV0(0, 1, "range", "member-0", "member-0", [
        { memberId: "member-0", metadata: meta }
      ])
      const reader = new BinaryReader(body)
      const result = decodeJoinGroupResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.generationId).toBe(1)
      expect(result.value.protocolName).toBe("range")
      expect(result.value.leader).toBe("member-0")
      expect(result.value.memberId).toBe("member-0")
      expect(result.value.members).toHaveLength(1)
      expect(result.value.members[0].memberId).toBe("member-0")
      expect(result.value.members[0].metadata).toEqual(meta)
      expect(result.value.throttleTimeMs).toBe(0)
    })

    it("decodes an error response", () => {
      const body = buildResponseV0(79, -1, "", "", "", [])
      const reader = new BinaryReader(body)
      const result = decodeJoinGroupResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(79)
    })
  })

  describe("v2 — with throttle time", () => {
    it("decodes throttle time", () => {
      const body = buildResponseV2(150, 0, 3, "roundrobin", "leader-1", "member-1", [])
      const reader = new BinaryReader(body)
      const result = decodeJoinGroupResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(150)
      expect(result.value.generationId).toBe(3)
      expect(result.value.protocolName).toBe("roundrobin")
    })
  })

  describe("v6 — flexible response decoding", () => {
    function buildResponseV6(
      throttleTimeMs: number,
      errorCode: number,
      generationId: number,
      protocolName: string,
      leader: string,
      memberId: string,
      members: { memberId: string; metadata: Uint8Array }[]
    ): Uint8Array {
      const w = new BinaryWriter()
      w.writeInt32(throttleTimeMs)
      w.writeInt16(errorCode)
      w.writeInt32(generationId)
      // protocol_name (compact string, v6+)
      w.writeCompactString(protocolName)
      // leader (compact string)
      w.writeCompactString(leader)
      // member_id (compact string)
      w.writeCompactString(memberId)
      // members (compact array: length+1)
      w.writeUnsignedVarInt(members.length + 1)
      for (const m of members) {
        w.writeCompactString(m.memberId)
        // no group_instance_id in v6 without v5 flag, but v6 > v5 so include it
        w.writeCompactString(null) // group_instance_id (nullable)
        w.writeCompactBytes(m.metadata)
        w.writeUnsignedVarInt(0) // member tagged fields
      }
      // response tagged fields
      w.writeUnsignedVarInt(0)
      return w.finish()
    }

    it("decodes flexible format with compact strings and arrays", () => {
      const meta = new Uint8Array([0x01, 0x02])
      const body = buildResponseV6(0, 0, 2, "range", "member-0", "member-0", [
        { memberId: "member-0", metadata: meta }
      ])
      const reader = new BinaryReader(body)
      const result = decodeJoinGroupResponse(reader, 6)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.generationId).toBe(2)
      expect(result.value.protocolName).toBe("range")
      expect(result.value.leader).toBe("member-0")
      expect(result.value.memberId).toBe("member-0")
      expect(result.value.members).toHaveLength(1)
      expect(result.value.members[0].memberId).toBe("member-0")
      expect(result.value.members[0].metadata).toEqual(meta)
    })

    it("decodes v6 response with empty members array", () => {
      const body = buildResponseV6(100, 0, 3, "roundrobin", "leader-1", "member-1", [])
      const reader = new BinaryReader(body)
      const result = decodeJoinGroupResponse(reader, 6)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(100)
      expect(result.value.members).toHaveLength(0)
    })
  })

  describe("v7 — with protocol_type", () => {
    it("decodes protocol_type compact string", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeInt32(1) // generation_id
      // protocol_type (v7+, compact string)
      w.writeCompactString("consumer")
      // protocol_name (compact string)
      w.writeCompactString("range")
      // leader (compact string)
      w.writeCompactString("leader-1")
      // member_id (compact string)
      w.writeCompactString("member-1")
      // members (compact array, empty)
      w.writeUnsignedVarInt(1) // 0 members + 1
      // response tagged fields
      w.writeUnsignedVarInt(0)
      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeJoinGroupResponse(reader, 7)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.protocolType).toBe("consumer")
      expect(result.value.protocolName).toBe("range")
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeJoinGroupResponse(reader, 2)
      expect(result.ok).toBe(false)
    })
    it("returns failure on truncated v6 member metadata", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeInt32(1) // generation_id
      w.writeCompactString("range") // protocol_name
      w.writeCompactString("leader-0") // leader
      w.writeCompactString("member-0") // member_id
      w.writeUnsignedVarInt(2) // members (1+1)
      w.writeCompactString("member-0") // member_id
      w.writeCompactString(null) // group_instance_id
      // Missing metadata bytes
      const reader = new BinaryReader(w.finish())
      const result = decodeJoinGroupResponse(reader, 6)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v5 group_instance_id", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeInt32(1) // generation_id
      w.writeCompactString("range") // protocol_name
      w.writeCompactString("leader-0") // leader
      w.writeCompactString("member-0") // member_id
      w.writeUnsignedVarInt(2) // members (1+1)
      w.writeCompactString("member-0") // member_id
      // Missing group_instance_id (v5+)
      const reader = new BinaryReader(w.finish())
      const result = decodeJoinGroupResponse(reader, 6)
      expect(result.ok).toBe(false)
    })
  })

  describe("v7 — protocol_type and protocol_name", () => {
    it("decodes response with protocol_type and protocol_name", () => {
      const meta = new Uint8Array([0x01])
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeInt32(5) // generation_id
      w.writeCompactString("consumer") // protocol_type (v7+)
      w.writeCompactString("range") // protocol_name
      w.writeCompactString("leader-0") // leader
      w.writeCompactString("member-0") // member_id
      w.writeUnsignedVarInt(2) // members (1+1)
      w.writeCompactString("member-0")
      w.writeCompactString(null) // group_instance_id
      w.writeCompactBytes(meta)
      w.writeUnsignedVarInt(0) // member tagged fields
      w.writeUnsignedVarInt(0) // response tagged fields
      const reader = new BinaryReader(w.finish())
      const result = decodeJoinGroupResponse(reader, 7)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }
      expect(result.value.generationId).toBe(5)
      expect(result.value.protocolType).toBe("consumer")
      expect(result.value.protocolName).toBe("range")
      expect(result.value.members[0].groupInstanceId).toBeNull()
    })
  })
})
