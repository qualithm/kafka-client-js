import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildSyncGroupRequest,
  decodeSyncGroupResponse,
  encodeSyncGroupRequest,
  type SyncGroupRequest
} from "../../../protocol/sync-group"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeSyncGroupRequest", () => {
  describe("v0 — basic fields", () => {
    it("encodes group_id, generation_id, member_id, assignments", () => {
      const writer = new BinaryWriter()
      const assignment = new Uint8Array([0xde, 0xad])
      const request: SyncGroupRequest = {
        groupId: "test-group",
        generationId: 1,
        memberId: "member-0",
        assignments: [{ memberId: "member-0", assignment }]
      }
      encodeSyncGroupRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id
      const groupResult = reader.readString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("test-group")

      // generation_id
      const genResult = reader.readInt32()
      expect(genResult.ok).toBe(true)
      expect(genResult.ok && genResult.value).toBe(1)

      // member_id
      const memberResult = reader.readString()
      expect(memberResult.ok).toBe(true)
      expect(memberResult.ok && memberResult.value).toBe("member-0")

      // assignments array length
      const assignCountResult = reader.readInt32()
      expect(assignCountResult.ok).toBe(true)
      expect(assignCountResult.ok && assignCountResult.value).toBe(1)

      // assignment member_id
      const aMemberResult = reader.readString()
      expect(aMemberResult.ok).toBe(true)
      expect(aMemberResult.ok && aMemberResult.value).toBe("member-0")

      // assignment data (bytes)
      const aDataResult = reader.readBytes()
      expect(aDataResult.ok).toBe(true)
      if (aDataResult.ok) {
        expect(aDataResult.value).toEqual(assignment)
      }

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v4 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: SyncGroupRequest = {
        groupId: "flex-group",
        generationId: 5,
        memberId: "m-1",
        groupInstanceId: null,
        assignments: []
      }
      encodeSyncGroupRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id (compact string)
      const groupResult = reader.readCompactString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("flex-group")
    })
  })

  describe("v0 — empty assignments for non-leader", () => {
    it("encodes empty assignments array", () => {
      const writer = new BinaryWriter()
      const request: SyncGroupRequest = {
        groupId: "grp",
        generationId: 1,
        memberId: "follower-1",
        assignments: []
      }
      encodeSyncGroupRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readString() // group_id
      reader.readInt32() // generation_id
      reader.readString() // member_id

      // assignments count should be 0
      const countResult = reader.readInt32()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// buildSyncGroupRequest (framed)
// ---------------------------------------------------------------------------

describe("buildSyncGroupRequest", () => {
  it("builds a framed v0 request", () => {
    const request: SyncGroupRequest = {
      groupId: "grp",
      generationId: 1,
      memberId: "m-0",
      assignments: []
    }
    const framed = buildSyncGroupRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.SyncGroup)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeSyncGroupResponse", () => {
  function buildResponseV0(errorCode: number, assignment: Uint8Array): Uint8Array {
    const w = new BinaryWriter()
    // v0: no throttle_time_ms
    w.writeInt16(errorCode)
    w.writeBytes(assignment)
    return w.finish()
  }

  function buildResponseV1(
    throttleTimeMs: number,
    errorCode: number,
    assignment: Uint8Array
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(throttleTimeMs)
    w.writeInt16(errorCode)
    w.writeBytes(assignment)
    return w.finish()
  }

  describe("v0 — non-flexible", () => {
    it("decodes a successful response", () => {
      const assignment = new Uint8Array([0x01, 0x02, 0x03])
      const body = buildResponseV0(0, assignment)
      const reader = new BinaryReader(body)
      const result = decodeSyncGroupResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.assignment).toEqual(assignment)
      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.protocolType).toBe(null)
      expect(result.value.protocolName).toBe(null)
    })

    it("decodes an error response with empty assignment", () => {
      const body = buildResponseV0(25, new Uint8Array(0))
      const reader = new BinaryReader(body)
      const result = decodeSyncGroupResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(25)
    })
  })

  describe("v1 — with throttle time", () => {
    it("decodes throttle_time_ms", () => {
      const assignment = new Uint8Array([0xab])
      const body = buildResponseV1(100, 0, assignment)
      const reader = new BinaryReader(body)
      const result = decodeSyncGroupResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(100)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.assignment).toEqual(assignment)
    })
  })

  describe("v4 — flexible encoding with compact bytes and tagged fields", () => {
    it("decodes a v4 response with compact assignment and tagged fields", () => {
      const assignment = new Uint8Array([0x01, 0x02, 0x03])
      const w = new BinaryWriter()
      w.writeInt32(50) // throttle_time_ms
      w.writeInt16(0) // error_code
      // assignment (compact bytes: length+1 as varint, then data)
      w.writeCompactBytes(assignment)
      // tagged fields (empty)
      w.writeUnsignedVarInt(0)
      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeSyncGroupResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.assignment).toEqual(assignment)
      expect(result.value.protocolType).toBe(null)
      expect(result.value.protocolName).toBe(null)
    })
  })

  describe("v5 — with protocol_type and protocol_name", () => {
    it("decodes protocol_type and protocol_name compact strings", () => {
      const assignment = new Uint8Array([0xab])
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeCompactString("consumer") // protocol_type (v5+)
      w.writeCompactString("range") // protocol_name (v5+)
      w.writeCompactBytes(assignment) // assignment (compact bytes)
      w.writeUnsignedVarInt(0) // tagged fields
      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeSyncGroupResponse(reader, 5)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.protocolType).toBe("consumer")
      expect(result.value.protocolName).toBe("range")
      expect(result.value.assignment).toEqual(assignment)
    })
  })

  describe("v4 — flexible request encoding with assignments", () => {
    it("encodes assignments with compact strings and bytes", () => {
      const writer = new BinaryWriter()
      const assignment = new Uint8Array([0xde, 0xad])
      const request: SyncGroupRequest = {
        groupId: "flex-group",
        generationId: 5,
        memberId: "m-1",
        groupInstanceId: null,
        assignments: [{ memberId: "m-1", assignment }]
      }
      encodeSyncGroupRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id (compact string)
      const groupResult = reader.readCompactString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("flex-group")
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeSyncGroupResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v4 compact bytes", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      // Missing compact bytes (assignment)
      const reader = new BinaryReader(w.finish())
      const result = decodeSyncGroupResponse(reader, 4)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v5 protocol_type", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      // Missing protocol_type (v5+)
      const reader = new BinaryReader(w.finish())
      const result = decodeSyncGroupResponse(reader, 5)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v5 protocol_name", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeCompactString("consumer") // protocol_type
      // Missing protocol_name (v5+)
      const reader = new BinaryReader(w.finish())
      const result = decodeSyncGroupResponse(reader, 5)
      expect(result.ok).toBe(false)
    })
  })

  describe("v5 — null protocol fields", () => {
    it("decodes null protocol_type and protocol_name", () => {
      const assignment = new Uint8Array([0x01, 0x02])
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeCompactString(null) // protocol_type (nullable)
      w.writeCompactString(null) // protocol_name (nullable)
      w.writeCompactBytes(assignment)
      w.writeUnsignedVarInt(0) // tagged fields
      const reader = new BinaryReader(w.finish())
      const result = decodeSyncGroupResponse(reader, 5)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }
      expect(result.value.protocolType).toBeNull()
      expect(result.value.protocolName).toBeNull()
      expect(result.value.assignment).toEqual(assignment)
    })
  })
})
