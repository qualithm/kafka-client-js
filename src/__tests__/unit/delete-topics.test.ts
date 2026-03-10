import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  buildDeleteTopicsRequest,
  decodeDeleteTopicsResponse,
  type DeleteTopicsRequest,
  encodeDeleteTopicsRequest
} from "../../delete-topics"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDeleteTopicsRequest", () => {
  describe("v0–v3 — topic names array + timeout", () => {
    it("encodes topic names", () => {
      const writer = new BinaryWriter()
      const request: DeleteTopicsRequest = {
        topicNames: ["topic-a", "topic-b"],
        timeoutMs: 15000
      }
      encodeDeleteTopicsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // names array (INT32)
      const namesLen = reader.readInt32()
      expect(namesLen.ok && namesLen.value).toBe(2)

      const name1 = reader.readString()
      expect(name1.ok && name1.value).toBe("topic-a")
      const name2 = reader.readString()
      expect(name2.ok && name2.value).toBe("topic-b")

      // timeout_ms
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(15000)

      expect(reader.remaining).toBe(0)
    })

    it("encodes empty topic list", () => {
      const writer = new BinaryWriter()
      const request: DeleteTopicsRequest = {
        topicNames: [],
        timeoutMs: 5000
      }
      encodeDeleteTopicsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const namesLen = reader.readInt32()
      expect(namesLen.ok && namesLen.value).toBe(0)

      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(5000)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v4+ — flexible encoding", () => {
    it("encodes with compact strings", () => {
      const writer = new BinaryWriter()
      const request: DeleteTopicsRequest = {
        topicNames: ["flex-topic"],
        timeoutMs: 10000
      }
      encodeDeleteTopicsRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // names array (compact: 1 + 1)
      const namesLen = reader.readUnsignedVarInt()
      expect(namesLen.ok && namesLen.value).toBe(2)

      const name = reader.readCompactString()
      expect(name.ok && name.value).toBe("flex-topic")

      // timeout
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(10000)

      // tagged fields
      const tag = reader.readTaggedFields()
      expect(tag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v6+ — topics array with name and topic_id", () => {
    it("encodes topics with UUID", () => {
      const topicId = new Uint8Array(16)
      topicId[0] = 0xff
      const writer = new BinaryWriter()
      const request: DeleteTopicsRequest = {
        topics: [{ name: "uuid-topic", topicId }],
        timeoutMs: 5000
      }
      encodeDeleteTopicsRequest(writer, request, 6)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // topics array (compact: 1 + 1)
      const topicsLen = reader.readUnsignedVarInt()
      expect(topicsLen.ok && topicsLen.value).toBe(2)

      // name
      const name = reader.readCompactString()
      expect(name.ok && name.value).toBe("uuid-topic")

      // topic_id (16 bytes)
      const uuid = reader.readRawBytes(16)
      expect(uuid.ok).toBe(true)
      if (uuid.ok) {
        expect(uuid.value[0]).toBe(0xff)
      }

      // tagged fields per topic
      const topicTag = reader.readTaggedFields()
      expect(topicTag.ok).toBe(true)

      // timeout
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(5000)

      // top-level tagged fields
      const tag = reader.readTaggedFields()
      expect(tag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

describe("buildDeleteTopicsRequest", () => {
  it("builds a framed v0 request", () => {
    const frame = buildDeleteTopicsRequest(2, 0, {
      topicNames: ["test"],
      timeoutMs: 5000
    })

    const reader = new BinaryReader(frame)

    // Size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)
    expect(size.ok && size.value).toBe(frame.length - 4)

    // API key
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.DeleteTopics)

    // API version
    const version = reader.readInt16()
    expect(version.ok && version.value).toBe(0)

    // Correlation ID
    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(2)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDeleteTopicsResponse", () => {
  describe("v0 — topic name + error code", () => {
    it("decodes success", () => {
      const writer = new BinaryWriter()

      // topics array (1)
      writer.writeInt32(1)
      writer.writeString("deleted-topic")
      writer.writeInt16(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeDeleteTopicsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("deleted-topic")
      expect(result.value.topics[0].errorCode).toBe(0)
    })

    it("decodes error", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(1)
      writer.writeString("missing-topic")
      writer.writeInt16(3) // UNKNOWN_TOPIC_OR_PARTITION

      const reader = new BinaryReader(writer.finish())
      const result = decodeDeleteTopicsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].errorCode).toBe(3)
    })
  })

  describe("v1+ — adds throttle_time_ms", () => {
    it("decodes throttle time", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(200) // throttle
      writer.writeInt32(0) // empty topics

      const reader = new BinaryReader(writer.finish())
      const result = decodeDeleteTopicsResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(200)
    })
  })

  describe("v4+ — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle
      // topics (compact: 1 + 1)
      writer.writeUnsignedVarInt(2)
      writer.writeCompactString("flex-deleted")
      writer.writeInt16(0)
      // tagged fields per topic
      writer.writeTaggedFields([])
      // top-level tagged fields
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeDeleteTopicsResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("flex-deleted")
      expect(result.value.topics[0].errorCode).toBe(0)
    })
  })

  describe("v5+ — adds error_message", () => {
    it("decodes error message", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // topics
      writer.writeCompactString("fail-topic")
      writer.writeInt16(41) // NOT_CONTROLLER
      writer.writeCompactString("this node is not the controller")
      writer.writeTaggedFields([])
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeDeleteTopicsResponse(reader, 5)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].errorMessage).toBe("this node is not the controller")
    })
  })

  describe("v6+ — adds topic_id", () => {
    it("decodes topic ID in response", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // topics (1+1)
      writer.writeCompactString("id-topic")
      // topic_id (16 bytes)
      const topicId = new Uint8Array(16)
      topicId[0] = 0xde
      topicId[15] = 0xad
      writer.writeRawBytes(topicId)
      writer.writeInt16(0) // error_code
      writer.writeCompactString(null) // error_message
      writer.writeTaggedFields([])
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeDeleteTopicsResponse(reader, 6)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].topicId).toBeDefined()
      expect(result.value.topics[0].topicId![0]).toBe(0xde)
      expect(result.value.topics[0].topicId![15]).toBe(0xad)
    })
  })

  it("decodes multiple topics", () => {
    const writer = new BinaryWriter()

    writer.writeInt32(10) // throttle
    writer.writeInt32(2) // topics
    writer.writeString("topic-1")
    writer.writeInt16(0)
    writer.writeString("topic-2")
    writer.writeInt16(3)

    const reader = new BinaryReader(writer.finish())
    const result = decodeDeleteTopicsResponse(reader, 1)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.topics).toHaveLength(2)
    expect(result.value.topics[0].errorCode).toBe(0)
    expect(result.value.topics[1].errorCode).toBe(3)
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeDeleteTopicsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v4 flexible response", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // topics (1+1)
      writer.writeCompactString("trunc-topic")
      // Missing error_code and tagged fields
      const reader = new BinaryReader(writer.finish())
      const result = decodeDeleteTopicsResponse(reader, 4)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v5 error message", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // topics (1+1)
      writer.writeCompactString("trunc-topic")
      writer.writeInt16(41) // error_code
      // Missing error_message (v5+)
      const reader = new BinaryReader(writer.finish())
      const result = decodeDeleteTopicsResponse(reader, 5)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v6 topic_id", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // topics (1+1)
      writer.writeCompactString("trunc-topic")
      // Missing 16-byte topic_id (v6+)
      const reader = new BinaryReader(writer.finish())
      const result = decodeDeleteTopicsResponse(reader, 6)
      expect(result.ok).toBe(false)
    })
  })
})
