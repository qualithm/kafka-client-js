import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  buildCreatePartitionsRequest,
  type CreatePartitionsRequest,
  decodeCreatePartitionsResponse,
  encodeCreatePartitionsRequest
} from "../../create-partitions"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeCreatePartitionsRequest", () => {
  describe("v0 — topics with count and assignments", () => {
    it("encodes a single topic with automatic assignment", () => {
      const writer = new BinaryWriter()
      const request: CreatePartitionsRequest = {
        topics: [{ name: "my-topic", count: 6, assignments: null }],
        timeoutMs: 30000
      }
      encodeCreatePartitionsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // topics array (INT32)
      const topicsLen = reader.readInt32()
      expect(topicsLen.ok && topicsLen.value).toBe(1)

      // name
      const name = reader.readString()
      expect(name.ok && name.value).toBe("my-topic")

      // count
      const count = reader.readInt32()
      expect(count.ok && count.value).toBe(6)

      // assignments (null => -1)
      const assignLen = reader.readInt32()
      expect(assignLen.ok && assignLen.value).toBe(-1)

      // timeout_ms
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(30000)

      expect(reader.remaining).toBe(0)
    })

    it("encodes topic with explicit assignments", () => {
      const writer = new BinaryWriter()
      const request: CreatePartitionsRequest = {
        topics: [
          {
            name: "assigned-topic",
            count: 4,
            assignments: [{ brokerIds: [1, 2] }, { brokerIds: [2, 3] }]
          }
        ],
        timeoutMs: 10000
      }
      encodeCreatePartitionsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // topics array
      reader.readString() // name
      reader.readInt32() // count

      // assignments array
      const assignLen = reader.readInt32()
      expect(assignLen.ok && assignLen.value).toBe(2)

      // assignment 0
      const brokersLen0 = reader.readInt32()
      expect(brokersLen0.ok && brokersLen0.value).toBe(2)
      expect((reader.readInt32() as { ok: true; value: number }).value).toBe(1)
      expect((reader.readInt32() as { ok: true; value: number }).value).toBe(2)

      // assignment 1
      const brokersLen1 = reader.readInt32()
      expect(brokersLen1.ok && brokersLen1.value).toBe(2)
      expect((reader.readInt32() as { ok: true; value: number }).value).toBe(2)
      expect((reader.readInt32() as { ok: true; value: number }).value).toBe(3)
    })
  })

  describe("v1+ — adds validate_only", () => {
    it("encodes validate_only = true", () => {
      const writer = new BinaryWriter()
      const request: CreatePartitionsRequest = {
        topics: [],
        timeoutMs: 5000,
        validateOnly: true
      }
      encodeCreatePartitionsRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // topics
      reader.readInt32() // timeout

      const validate = reader.readBoolean()
      expect(validate.ok && validate.value).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2+ — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: CreatePartitionsRequest = {
        topics: [{ name: "flex-topic", count: 10, assignments: null }],
        timeoutMs: 5000,
        validateOnly: false
      }
      encodeCreatePartitionsRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // topics array (compact: 1+1)
      const topicsLen = reader.readUnsignedVarInt()
      expect(topicsLen.ok && topicsLen.value).toBe(2)

      // name (compact)
      const name = reader.readCompactString()
      expect(name.ok && name.value).toBe("flex-topic")

      // count
      const count = reader.readInt32()
      expect(count.ok && count.value).toBe(10)

      // assignments (compact null: 0)
      const assignLen = reader.readUnsignedVarInt()
      expect(assignLen.ok && assignLen.value).toBe(0)

      // topic tagged fields
      const topicTag = reader.readTaggedFields()
      expect(topicTag.ok).toBe(true)

      // timeout
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(5000)

      // validate_only
      const validate = reader.readBoolean()
      expect(validate.ok && validate.value).toBe(false)

      // top-level tagged fields
      const tag = reader.readTaggedFields()
      expect(tag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

describe("buildCreatePartitionsRequest", () => {
  it("builds a framed v0 request", () => {
    const frame = buildCreatePartitionsRequest(3, 0, {
      topics: [{ name: "t", count: 5, assignments: null }],
      timeoutMs: 5000
    })

    const reader = new BinaryReader(frame)

    const size = reader.readInt32()
    expect(size.ok && size.value).toBe(frame.length - 4)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.CreatePartitions)

    const version = reader.readInt16()
    expect(version.ok && version.value).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeCreatePartitionsResponse", () => {
  describe("v0 — throttle, topics with error code and error message", () => {
    it("decodes success", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(0)
      // topics array (1)
      writer.writeInt32(1)
      // name
      writer.writeString("my-topic")
      // error_code
      writer.writeInt16(0)
      // error_message
      writer.writeString(null)

      const reader = new BinaryReader(writer.finish())
      const result = decodeCreatePartitionsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("my-topic")
      expect(result.value.topics[0].errorCode).toBe(0)
      expect(result.value.topics[0].errorMessage).toBeNull()
    })

    it("decodes error", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0)
      writer.writeInt32(1)
      writer.writeString("bad-topic")
      writer.writeInt16(37) // INVALID_PARTITIONS
      writer.writeString("partition count must increase")

      const reader = new BinaryReader(writer.finish())
      const result = decodeCreatePartitionsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].errorCode).toBe(37)
      expect(result.value.topics[0].errorMessage).toBe("partition count must increase")
    })
  })

  describe("v2+ — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(50) // throttle
      writer.writeUnsignedVarInt(2) // topics (1+1)
      writer.writeCompactString("flex-topic")
      writer.writeInt16(0) // error
      writer.writeCompactString(null) // error_message
      writer.writeTaggedFields([]) // topic tagged
      writer.writeTaggedFields([]) // top-level tagged

      const reader = new BinaryReader(writer.finish())
      const result = decodeCreatePartitionsResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.topics[0].name).toBe("flex-topic")
    })
  })

  it("decodes multiple topics", () => {
    const writer = new BinaryWriter()

    writer.writeInt32(0) // throttle
    writer.writeInt32(2) // topics
    writer.writeString("topic-a")
    writer.writeInt16(0)
    writer.writeString(null)
    writer.writeString("topic-b")
    writer.writeInt16(3)
    writer.writeString("unknown topic")

    const reader = new BinaryReader(writer.finish())
    const result = decodeCreatePartitionsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.topics).toHaveLength(2)
    expect(result.value.topics[0].errorCode).toBe(0)
    expect(result.value.topics[1].errorCode).toBe(3)
    expect(result.value.topics[1].errorMessage).toBe("unknown topic")
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeCreatePartitionsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v2 flexible response", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // topics (1+1)
      writer.writeCompactString("trunc-topic")
      writer.writeInt16(0) // error_code
      // Missing error_message and tagged fields
      const reader = new BinaryReader(writer.finish())
      const result = decodeCreatePartitionsResponse(reader, 2)
      expect(result.ok).toBe(false)
    })
  })
})
