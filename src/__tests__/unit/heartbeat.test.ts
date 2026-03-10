import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  buildHeartbeatRequest,
  decodeHeartbeatResponse,
  encodeHeartbeatRequest,
  type HeartbeatRequest
} from "../../heartbeat"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeHeartbeatRequest", () => {
  describe("v0 — basic fields", () => {
    it("encodes group_id, generation_id, member_id", () => {
      const writer = new BinaryWriter()
      const request: HeartbeatRequest = {
        groupId: "test-group",
        generationId: 1,
        memberId: "member-0"
      }
      encodeHeartbeatRequest(writer, request, 0)
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

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v3 — adds group_instance_id", () => {
    it("encodes group_instance_id", () => {
      const writer = new BinaryWriter()
      const request: HeartbeatRequest = {
        groupId: "grp",
        generationId: 2,
        memberId: "m-1",
        groupInstanceId: "instance-123"
      }
      encodeHeartbeatRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readString() // group_id
      reader.readInt32() // generation_id
      reader.readString() // member_id

      // group_instance_id (nullable string)
      const instanceResult = reader.readString()
      expect(instanceResult.ok).toBe(true)
      expect(instanceResult.ok && instanceResult.value).toBe("instance-123")

      expect(reader.remaining).toBe(0)
    })

    it("encodes null group_instance_id", () => {
      const writer = new BinaryWriter()
      const request: HeartbeatRequest = {
        groupId: "grp",
        generationId: 2,
        memberId: "m-1",
        groupInstanceId: null
      }
      encodeHeartbeatRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readString() // group_id
      reader.readInt32() // generation_id
      reader.readString() // member_id

      // null group_instance_id
      const instanceResult = reader.readString()
      expect(instanceResult.ok).toBe(true)
      expect(instanceResult.ok && instanceResult.value).toBe(null)
    })
  })

  describe("v4 — flexible encoding", () => {
    it("encodes with compact strings", () => {
      const writer = new BinaryWriter()
      const request: HeartbeatRequest = {
        groupId: "flex-group",
        generationId: 10,
        memberId: "member-99",
        groupInstanceId: null
      }
      encodeHeartbeatRequest(writer, request, 4)
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
// buildHeartbeatRequest (framed)
// ---------------------------------------------------------------------------

describe("buildHeartbeatRequest", () => {
  it("builds a framed v0 request", () => {
    const request: HeartbeatRequest = {
      groupId: "grp",
      generationId: 1,
      memberId: "m-0"
    }
    const framed = buildHeartbeatRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.Heartbeat)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeHeartbeatResponse", () => {
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

  describe("v0 — non-flexible", () => {
    it("decodes a successful response", () => {
      const body = buildResponseV0(0)
      const reader = new BinaryReader(body)
      const result = decodeHeartbeatResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(0)
      expect(result.value.throttleTimeMs).toBe(0)
    })

    it("decodes a REBALANCE_IN_PROGRESS error", () => {
      const body = buildResponseV0(27)
      const reader = new BinaryReader(body)
      const result = decodeHeartbeatResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(27)
    })
  })

  describe("v1 — with throttle time", () => {
    it("decodes throttle_time_ms", () => {
      const body = buildResponseV1(50, 0)
      const reader = new BinaryReader(body)
      const result = decodeHeartbeatResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.errorCode).toBe(0)
    })
  })

  describe("v4 — flexible format with tagged fields", () => {
    it("decodes a v4 response with tagged fields", () => {
      const w = new BinaryWriter()
      w.writeInt32(75) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeUnsignedVarInt(0) // empty tagged fields
      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeHeartbeatResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(75)
      expect(result.value.errorCode).toBe(0)
    })

    it("decodes a v4 error response", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(27) // REBALANCE_IN_PROGRESS
      w.writeUnsignedVarInt(0) // tagged fields
      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeHeartbeatResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(27)
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeHeartbeatResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v1 input", () => {
      const reader = new BinaryReader(new Uint8Array(3))
      const result = decodeHeartbeatResponse(reader, 1)
      expect(result.ok).toBe(false)
    })
  })
})
