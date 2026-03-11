import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildCreateTopicsRequest,
  type CreateTopicsRequest,
  decodeCreateTopicsResponse,
  encodeCreateTopicsRequest
} from "../../../protocol/create-topics"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeCreateTopicsRequest", () => {
  describe("v0 — topics array, timeout", () => {
    it("encodes a single topic", () => {
      const writer = new BinaryWriter()
      const request: CreateTopicsRequest = {
        topics: [
          {
            name: "test-topic",
            numPartitions: 3,
            replicationFactor: 1,
            assignments: [],
            configs: []
          }
        ],
        timeoutMs: 30000
      }
      encodeCreateTopicsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // topics array length (INT32)
      const topicsLenResult = reader.readInt32()
      expect(topicsLenResult.ok).toBe(true)
      expect(topicsLenResult.ok && topicsLenResult.value).toBe(1)

      // topic name
      const nameResult = reader.readString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("test-topic")

      // num_partitions
      const numPartResult = reader.readInt32()
      expect(numPartResult.ok).toBe(true)
      expect(numPartResult.ok && numPartResult.value).toBe(3)

      // replication_factor
      const rfResult = reader.readInt16()
      expect(rfResult.ok).toBe(true)
      expect(rfResult.ok && rfResult.value).toBe(1)

      // assignments array (empty)
      const assignLenResult = reader.readInt32()
      expect(assignLenResult.ok).toBe(true)
      expect(assignLenResult.ok && assignLenResult.value).toBe(0)

      // configs array (empty)
      const configsLenResult = reader.readInt32()
      expect(configsLenResult.ok).toBe(true)
      expect(configsLenResult.ok && configsLenResult.value).toBe(0)

      // timeout_ms
      const timeoutResult = reader.readInt32()
      expect(timeoutResult.ok).toBe(true)
      expect(timeoutResult.ok && timeoutResult.value).toBe(30000)

      expect(reader.remaining).toBe(0)
    })

    it("encodes topic with configs", () => {
      const writer = new BinaryWriter()
      const request: CreateTopicsRequest = {
        topics: [
          {
            name: "my-topic",
            numPartitions: -1,
            replicationFactor: -1,
            configs: [
              { name: "cleanup.policy", value: "compact" },
              { name: "retention.ms", value: "86400000" }
            ]
          }
        ],
        timeoutMs: 5000
      }
      encodeCreateTopicsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // topics array
      reader.readInt32()
      // name
      reader.readString()
      // num_partitions
      reader.readInt32()
      // replication_factor
      reader.readInt16()
      // assignments (empty default)
      const assignLen = reader.readInt32()
      expect(assignLen.ok && assignLen.value).toBe(0)

      // configs array
      const configsLen = reader.readInt32()
      expect(configsLen.ok && configsLen.value).toBe(2)

      // config 1
      const name1 = reader.readString()
      expect(name1.ok && name1.value).toBe("cleanup.policy")
      const val1 = reader.readString()
      expect(val1.ok && val1.value).toBe("compact")

      // config 2
      const name2 = reader.readString()
      expect(name2.ok && name2.value).toBe("retention.ms")
      const val2 = reader.readString()
      expect(val2.ok && val2.value).toBe("86400000")

      // timeout
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(5000)

      expect(reader.remaining).toBe(0)
    })

    it("encodes topic with replica assignments", () => {
      const writer = new BinaryWriter()
      const request: CreateTopicsRequest = {
        topics: [
          {
            name: "assigned-topic",
            numPartitions: -1,
            replicationFactor: -1,
            assignments: [
              { partitionIndex: 0, brokerIds: [1, 2] },
              { partitionIndex: 1, brokerIds: [2, 3] }
            ]
          }
        ],
        timeoutMs: 10000
      }
      encodeCreateTopicsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // topics array (1)
      reader.readInt32()
      // name
      reader.readString()
      // num_partitions, replication_factor
      reader.readInt32()
      reader.readInt16()

      // assignments array length
      const assignLen = reader.readInt32()
      expect(assignLen.ok && assignLen.value).toBe(2)

      // assignment 0
      const part0 = reader.readInt32()
      expect(part0.ok && part0.value).toBe(0)
      const brokerLen0 = reader.readInt32()
      expect(brokerLen0.ok && brokerLen0.value).toBe(2)
      const b0a = reader.readInt32()
      expect(b0a.ok && b0a.value).toBe(1)
      const b0b = reader.readInt32()
      expect(b0b.ok && b0b.value).toBe(2)

      // assignment 1
      const part1 = reader.readInt32()
      expect(part1.ok && part1.value).toBe(1)
      const brokerLen1 = reader.readInt32()
      expect(brokerLen1.ok && brokerLen1.value).toBe(2)
      const b1a = reader.readInt32()
      expect(b1a.ok && b1a.value).toBe(2)
      const b1b = reader.readInt32()
      expect(b1b.ok && b1b.value).toBe(3)
    })
  })

  describe("v1+ — adds validate_only", () => {
    it("encodes validate_only = true", () => {
      const writer = new BinaryWriter()
      const request: CreateTopicsRequest = {
        topics: [],
        timeoutMs: 5000,
        validateOnly: true
      }
      encodeCreateTopicsRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // topics array (empty)
      const topicsLen = reader.readInt32()
      expect(topicsLen.ok && topicsLen.value).toBe(0)

      // timeout
      reader.readInt32()

      // validate_only
      const validate = reader.readBoolean()
      expect(validate.ok).toBe(true)
      expect(validate.ok && validate.value).toBe(true)

      expect(reader.remaining).toBe(0)
    })

    it("defaults validate_only to false", () => {
      const writer = new BinaryWriter()
      const request: CreateTopicsRequest = {
        topics: [],
        timeoutMs: 5000
      }
      encodeCreateTopicsRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // topics
      reader.readInt32() // timeout

      const validate = reader.readBoolean()
      expect(validate.ok && validate.value).toBe(false)
    })
  })

  describe("v5+ — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: CreateTopicsRequest = {
        topics: [
          {
            name: "flex-topic",
            numPartitions: 1,
            replicationFactor: 1,
            assignments: [],
            configs: [{ name: "key", value: "val" }]
          }
        ],
        timeoutMs: 10000,
        validateOnly: false
      }
      encodeCreateTopicsRequest(writer, request, 5)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // topics array (compact: length + 1)
      const topicsLen = reader.readUnsignedVarInt()
      expect(topicsLen.ok && topicsLen.value).toBe(2)

      // name (compact string)
      const name = reader.readCompactString()
      expect(name.ok && name.value).toBe("flex-topic")

      // num_partitions
      const np = reader.readInt32()
      expect(np.ok && np.value).toBe(1)

      // replication_factor
      const rf = reader.readInt16()
      expect(rf.ok && rf.value).toBe(1)

      // assignments (compact array: 0 + 1 = 1)
      const assignLen = reader.readUnsignedVarInt()
      expect(assignLen.ok && assignLen.value).toBe(1)

      // configs (compact array: 1 + 1 = 2)
      const configsLen = reader.readUnsignedVarInt()
      expect(configsLen.ok && configsLen.value).toBe(2)

      // config entry
      const cName = reader.readCompactString()
      expect(cName.ok && cName.value).toBe("key")
      const cVal = reader.readCompactString()
      expect(cVal.ok && cVal.value).toBe("val")

      // config tagged fields
      const configTag = reader.readTaggedFields()
      expect(configTag.ok).toBe(true)

      // topic tagged fields
      const topicTag = reader.readTaggedFields()
      expect(topicTag.ok).toBe(true)

      // timeout
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(10000)

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

describe("buildCreateTopicsRequest", () => {
  it("builds a framed v0 request", () => {
    const frame = buildCreateTopicsRequest(1, 0, {
      topics: [{ name: "t", numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 5000
    })

    const reader = new BinaryReader(frame)

    // Size prefix
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)
    expect(sizeResult.ok && sizeResult.value).toBe(frame.length - 4)

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.CreateTopics)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok && versionResult.value).toBe(0)

    // Correlation ID
    const corrResult = reader.readInt32()
    expect(corrResult.ok && corrResult.value).toBe(1)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeCreateTopicsResponse", () => {
  describe("v0 — topic error codes only", () => {
    it("decodes single topic success", () => {
      const writer = new BinaryWriter()

      // topics array (1)
      writer.writeInt32(1)
      // topic name
      writer.writeString("test-topic")
      // error_code
      writer.writeInt16(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeCreateTopicsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("test-topic")
      expect(result.value.topics[0].errorCode).toBe(0)
      expect(result.value.topics[0].errorMessage).toBeNull()
    })

    it("decodes topic with error code", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(1)
      writer.writeString("existing-topic")
      writer.writeInt16(36) // TOPIC_ALREADY_EXISTS

      const reader = new BinaryReader(writer.finish())
      const result = decodeCreateTopicsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].errorCode).toBe(36)
    })
  })

  describe("v1+ — adds error_message", () => {
    it("decodes with error message", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(1) // topics array
      writer.writeString("bad-topic")
      writer.writeInt16(37) // INVALID_PARTITIONS
      writer.writeString("partition count must be positive")

      const reader = new BinaryReader(writer.finish())
      const result = decodeCreateTopicsResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].errorCode).toBe(37)
      expect(result.value.topics[0].errorMessage).toBe("partition count must be positive")
    })
  })

  describe("v2+ — adds throttle_time_ms", () => {
    it("decodes throttle time", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(100)
      // topics array (0)
      writer.writeInt32(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeCreateTopicsResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(100)
      expect(result.value.topics).toHaveLength(0)
    })
  })

  describe("v5+ — flexible encoding with config entries", () => {
    it("decodes with num_partitions, replication_factor, and configs", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(0)
      // topics array (compact: 1 + 1 = 2)
      writer.writeUnsignedVarInt(2)
      // topic name (compact)
      writer.writeCompactString("created-topic")
      // error_code
      writer.writeInt16(0)
      // error_message (compact nullable: null)
      writer.writeCompactString(null)
      // num_partitions
      writer.writeInt32(3)
      // replication_factor
      writer.writeInt16(2)
      // configs array (compact: 1 + 1 = 2)
      writer.writeUnsignedVarInt(2)
      // config entry
      writer.writeCompactString("cleanup.policy")
      writer.writeCompactString("delete")
      writer.writeBoolean(false) // read_only
      writer.writeInt8(5) // config_source (DEFAULT)
      writer.writeBoolean(false) // is_sensitive
      writer.writeTaggedFields([]) // config tagged fields
      // topic tagged fields
      writer.writeTaggedFields([])
      // top-level tagged fields
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeCreateTopicsResponse(reader, 5)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics).toHaveLength(1)
      const topic = result.value.topics[0]
      expect(topic.name).toBe("created-topic")
      expect(topic.errorCode).toBe(0)
      expect(topic.numPartitions).toBe(3)
      expect(topic.replicationFactor).toBe(2)
      expect(topic.configs).toHaveLength(1)
      expect(topic.configs[0].name).toBe("cleanup.policy")
      expect(topic.configs[0].value).toBe("delete")
      expect(topic.configs[0].readOnly).toBe(false)
      expect(topic.configs[0].configSource).toBe(5)
      expect(topic.configs[0].isSensitive).toBe(false)
    })
  })

  describe("v7+ — adds topic_id", () => {
    it("decodes topic ID", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(0)
      // topics array (compact: 1 + 1 = 2)
      writer.writeUnsignedVarInt(2)
      // topic name (compact)
      writer.writeCompactString("uuid-topic")
      // topic_id (16 bytes)
      const topicId = new Uint8Array(16)
      topicId[0] = 0xab
      topicId[15] = 0xcd
      writer.writeRawBytes(topicId)
      // error_code
      writer.writeInt16(0)
      // error_message
      writer.writeCompactString(null)
      // num_partitions
      writer.writeInt32(1)
      // replication_factor
      writer.writeInt16(1)
      // configs (empty compact array)
      writer.writeUnsignedVarInt(1)
      // topic tagged fields
      writer.writeTaggedFields([])
      // top-level tagged fields
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeCreateTopicsResponse(reader, 7)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].topicId).toBeDefined()
      expect(result.value.topics[0].topicId![0]).toBe(0xab)
      expect(result.value.topics[0].topicId![15]).toBe(0xcd)
    })
  })

  it("decodes multiple topics", () => {
    const writer = new BinaryWriter()

    // throttle_time_ms
    writer.writeInt32(50)
    // topics array (2)
    writer.writeInt32(2)
    // topic 1
    writer.writeString("topic-a")
    writer.writeInt16(0) // success
    writer.writeString(null) // error_message
    // topic 2
    writer.writeString("topic-b")
    writer.writeInt16(36) // TOPIC_ALREADY_EXISTS
    writer.writeString("topic already exists")

    const reader = new BinaryReader(writer.finish())
    const result = decodeCreateTopicsResponse(reader, 2)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(50)
    expect(result.value.topics).toHaveLength(2)
    expect(result.value.topics[0].name).toBe("topic-a")
    expect(result.value.topics[0].errorCode).toBe(0)
    expect(result.value.topics[1].name).toBe("topic-b")
    expect(result.value.topics[1].errorCode).toBe(36)
    expect(result.value.topics[1].errorMessage).toBe("topic already exists")
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeCreateTopicsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v5 config entry", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // topics (1+1)
      writer.writeCompactString("trunc-topic")
      writer.writeInt16(0) // error_code
      writer.writeCompactString(null) // error_message
      writer.writeInt32(3) // num_partitions
      writer.writeInt16(1) // replication_factor
      writer.writeUnsignedVarInt(2) // configs (1+1)
      writer.writeCompactString("key")
      // Missing config value and remaining fields
      const reader = new BinaryReader(writer.finish())
      const result = decodeCreateTopicsResponse(reader, 5)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v7 topic_id", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // topics (1+1)
      writer.writeCompactString("trunc-topic")
      // Missing 16-byte topic_id (v7+)
      const reader = new BinaryReader(writer.finish())
      const result = decodeCreateTopicsResponse(reader, 7)
      expect(result.ok).toBe(false)
    })
  })
})
