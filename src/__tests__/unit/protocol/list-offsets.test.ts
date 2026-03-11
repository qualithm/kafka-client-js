import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildListOffsetsRequest,
  decodeListOffsetsResponse,
  encodeListOffsetsRequest,
  IsolationLevel,
  type ListOffsetsRequest,
  OffsetTimestamp
} from "../../../protocol/list-offsets"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeListOffsetsRequest", () => {
  describe("v0 — replica ID, topics, partitions with max_num_offsets", () => {
    it("encodes single partition request", () => {
      const writer = new BinaryWriter()
      const request: ListOffsetsRequest = {
        replicaId: -1,
        topics: [
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, timestamp: OffsetTimestamp.Latest }]
          }
        ]
      }
      encodeListOffsetsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // replica_id (INT32)
      const replicaIdResult = reader.readInt32()
      expect(replicaIdResult.ok).toBe(true)
      expect(replicaIdResult.ok && replicaIdResult.value).toBe(-1)

      // topics array length (INT32)
      const topicsLengthResult = reader.readInt32()
      expect(topicsLengthResult.ok).toBe(true)
      expect(topicsLengthResult.ok && topicsLengthResult.value).toBe(1)

      // topic name
      const topicNameResult = reader.readString()
      expect(topicNameResult.ok).toBe(true)
      expect(topicNameResult.ok && topicNameResult.value).toBe("test-topic")

      // partitions array length
      const partitionsLengthResult = reader.readInt32()
      expect(partitionsLengthResult.ok).toBe(true)
      expect(partitionsLengthResult.ok && partitionsLengthResult.value).toBe(1)

      // partition_index
      const partitionIndexResult = reader.readInt32()
      expect(partitionIndexResult.ok).toBe(true)
      expect(partitionIndexResult.ok && partitionIndexResult.value).toBe(0)

      // timestamp
      const timestampResult = reader.readInt64()
      expect(timestampResult.ok).toBe(true)
      expect(timestampResult.ok && timestampResult.value).toBe(-1n)

      // max_num_offsets (v0 only)
      const maxOffsetsResult = reader.readInt32()
      expect(maxOffsetsResult.ok).toBe(true)
      expect(maxOffsetsResult.ok && maxOffsetsResult.value).toBe(1)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — adds isolation_level", () => {
    it("encodes with READ_UNCOMMITTED isolation", () => {
      const writer = new BinaryWriter()
      const request: ListOffsetsRequest = {
        isolationLevel: IsolationLevel.ReadUncommitted,
        topics: [
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, timestamp: OffsetTimestamp.Earliest }]
          }
        ]
      }
      encodeListOffsetsRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // replica_id
      reader.readInt32()

      // isolation_level (INT8)
      const isolationResult = reader.readInt8()
      expect(isolationResult.ok).toBe(true)
      expect(isolationResult.ok && isolationResult.value).toBe(0)

      // Rest of payload exists
      expect(reader.remaining).toBeGreaterThan(0)
    })

    it("encodes with READ_COMMITTED isolation", () => {
      const writer = new BinaryWriter()
      const request: ListOffsetsRequest = {
        isolationLevel: IsolationLevel.ReadCommitted,
        topics: [
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, timestamp: OffsetTimestamp.Earliest }]
          }
        ]
      }
      encodeListOffsetsRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // replica_id
      reader.readInt32()

      // isolation_level
      const isolationResult = reader.readInt8()
      expect(isolationResult.ok).toBe(true)
      expect(isolationResult.ok && isolationResult.value).toBe(1)
    })
  })

  describe("v4 — adds current_leader_epoch", () => {
    it("encodes partition with leader epoch", () => {
      const writer = new BinaryWriter()
      const request: ListOffsetsRequest = {
        topics: [
          {
            name: "test-topic",
            partitions: [
              { partitionIndex: 0, currentLeaderEpoch: 5, timestamp: OffsetTimestamp.Latest }
            ]
          }
        ]
      }
      encodeListOffsetsRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // replica_id
      reader.readInt32()
      // isolation_level
      reader.readInt8()
      // topics array length
      reader.readInt32()
      // topic name
      reader.readString()
      // partitions array length
      reader.readInt32()

      // partition_index
      const partitionIndexResult = reader.readInt32()
      expect(partitionIndexResult.ok).toBe(true)
      expect(partitionIndexResult.ok && partitionIndexResult.value).toBe(0)

      // current_leader_epoch
      const epochResult = reader.readInt32()
      expect(epochResult.ok).toBe(true)
      expect(epochResult.ok && epochResult.value).toBe(5)

      // timestamp
      const timestampResult = reader.readInt64()
      expect(timestampResult.ok).toBe(true)
      expect(timestampResult.ok && timestampResult.value).toBe(-1n)

      expect(reader.remaining).toBe(0)
    })

    it("encodes -1 when leader epoch not provided", () => {
      const writer = new BinaryWriter()
      const request: ListOffsetsRequest = {
        topics: [
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, timestamp: OffsetTimestamp.Latest }]
          }
        ]
      }
      encodeListOffsetsRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Skip to leader epoch
      reader.readInt32() // replica_id
      reader.readInt8() // isolation_level
      reader.readInt32() // topics length
      reader.readString() // topic name
      reader.readInt32() // partitions length
      reader.readInt32() // partition_index

      const epochResult = reader.readInt32()
      expect(epochResult.ok).toBe(true)
      expect(epochResult.ok && epochResult.value).toBe(-1)
    })
  })

  describe("v6+ — flexible encoding", () => {
    it("encodes with compact strings and arrays", () => {
      const writer = new BinaryWriter()
      const request: ListOffsetsRequest = {
        topics: [
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, timestamp: OffsetTimestamp.Latest }]
          }
        ]
      }
      encodeListOffsetsRequest(writer, request, 6)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // replica_id
      reader.readInt32()
      // isolation_level
      reader.readInt8()

      // topics array (compact: length + 1)
      const topicsLengthResult = reader.readUnsignedVarInt()
      expect(topicsLengthResult.ok).toBe(true)
      expect(topicsLengthResult.ok && topicsLengthResult.value).toBe(2) // 1 + 1

      // topic name (compact string)
      const topicNameResult = reader.readCompactString()
      expect(topicNameResult.ok).toBe(true)
      expect(topicNameResult.ok && topicNameResult.value).toBe("test-topic")

      // partitions array (compact)
      const partitionsLengthResult = reader.readUnsignedVarInt()
      expect(partitionsLengthResult.ok).toBe(true)
      expect(partitionsLengthResult.ok && partitionsLengthResult.value).toBe(2) // 1 + 1

      // partition_index
      reader.readInt32()
      // current_leader_epoch (v4+)
      reader.readInt32()
      // timestamp
      reader.readInt64()

      // partition tagged fields
      const partitionTagsResult = reader.readTaggedFields()
      expect(partitionTagsResult.ok).toBe(true)

      // topic tagged fields
      const topicTagsResult = reader.readTaggedFields()
      expect(topicTagsResult.ok).toBe(true)

      // request tagged fields
      const requestTagsResult = reader.readTaggedFields()
      expect(requestTagsResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })

    it("encodes multiple topics and partitions", () => {
      const writer = new BinaryWriter()
      const request: ListOffsetsRequest = {
        topics: [
          {
            name: "topic-a",
            partitions: [
              { partitionIndex: 0, timestamp: OffsetTimestamp.Earliest },
              { partitionIndex: 1, timestamp: OffsetTimestamp.Latest }
            ]
          },
          {
            name: "topic-b",
            partitions: [{ partitionIndex: 0, timestamp: 1234567890000n }]
          }
        ]
      }
      encodeListOffsetsRequest(writer, request, 6)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // replica_id
      reader.readInt32()
      // isolation_level
      reader.readInt8()

      // topics array
      const topicsLengthResult = reader.readUnsignedVarInt()
      expect(topicsLengthResult.ok && topicsLengthResult.value).toBe(3) // 2 + 1

      // topic-a
      expect(reader.readCompactString().ok && reader.readCompactString()).toBeTruthy()
    })
  })
})

describe("buildListOffsetsRequest", () => {
  it("builds a framed v0 request", () => {
    const frame = buildListOffsetsRequest(1, 0, {
      topics: [
        {
          name: "test-topic",
          partitions: [{ partitionIndex: 0, timestamp: OffsetTimestamp.Latest }]
        }
      ]
    })

    const reader = new BinaryReader(frame)

    // Size prefix
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)
    expect(sizeResult.ok && sizeResult.value).toBe(frame.length - 4)

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.ListOffsets)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok).toBe(true)
    expect(versionResult.ok && versionResult.value).toBe(0)

    // Correlation ID
    const corrIdResult = reader.readInt32()
    expect(corrIdResult.ok).toBe(true)
    expect(corrIdResult.ok && corrIdResult.value).toBe(1)
  })

  it("builds a framed v6 request with client ID", () => {
    const frame = buildListOffsetsRequest(
      42,
      6,
      {
        topics: [
          {
            name: "my-topic",
            partitions: [{ partitionIndex: 0, timestamp: OffsetTimestamp.Earliest }]
          }
        ]
      },
      "my-client"
    )

    const reader = new BinaryReader(frame)

    // Size prefix
    reader.readInt32()

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.ListOffsets)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok && versionResult.value).toBe(6)

    // Correlation ID
    const corrIdResult = reader.readInt32()
    expect(corrIdResult.ok && corrIdResult.value).toBe(42)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeListOffsetsResponse", () => {
  describe("v0 — old_style_offsets array", () => {
    it("decodes response with single offset", () => {
      const writer = new BinaryWriter()

      // topics array length
      writer.writeInt32(1)
      // topic name
      writer.writeString("test-topic")
      // partitions array length
      writer.writeInt32(1)
      // partition_index
      writer.writeInt32(0)
      // error_code
      writer.writeInt16(0)
      // old_style_offsets array length
      writer.writeInt32(1)
      // offset
      writer.writeInt64(12345n)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.throttleTimeMs).toBe(0)
        expect(result.value.topics).toHaveLength(1)
        expect(result.value.topics[0].name).toBe("test-topic")
        expect(result.value.topics[0].partitions).toHaveLength(1)
        expect(result.value.topics[0].partitions[0].partitionIndex).toBe(0)
        expect(result.value.topics[0].partitions[0].errorCode).toBe(0)
        expect(result.value.topics[0].partitions[0].offset).toBe(12345n)
        expect(result.value.topics[0].partitions[0].timestamp).toBe(-1n)
        expect(result.value.topics[0].partitions[0].leaderEpoch).toBe(-1)
      }
    })

    it("decodes response with empty offsets array", () => {
      const writer = new BinaryWriter()

      // topics array length
      writer.writeInt32(1)
      // topic name
      writer.writeString("test-topic")
      // partitions array length
      writer.writeInt32(1)
      // partition_index
      writer.writeInt32(0)
      // error_code (UNKNOWN_TOPIC_OR_PARTITION)
      writer.writeInt16(3)
      // old_style_offsets array length (empty)
      writer.writeInt32(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].partitions[0].errorCode).toBe(3)
        expect(result.value.topics[0].partitions[0].offset).toBe(-1n)
      }
    })
  })

  describe("v1–v3 — single offset with timestamp", () => {
    it("decodes response with timestamp and offset", () => {
      const writer = new BinaryWriter()

      // topics array length
      writer.writeInt32(1)
      // topic name
      writer.writeString("test-topic")
      // partitions array length
      writer.writeInt32(1)
      // partition_index
      writer.writeInt32(0)
      // error_code
      writer.writeInt16(0)
      // timestamp
      writer.writeInt64(1609459200000n)
      // offset
      writer.writeInt64(100n)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].partitions[0].timestamp).toBe(1609459200000n)
        expect(result.value.topics[0].partitions[0].offset).toBe(100n)
        expect(result.value.topics[0].partitions[0].leaderEpoch).toBe(-1)
      }
    })
  })

  describe("v2–v3 — adds throttle_time_ms", () => {
    it("decodes throttle time", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(100)
      // topics array length
      writer.writeInt32(1)
      // topic name
      writer.writeString("test-topic")
      // partitions array length
      writer.writeInt32(1)
      // partition_index
      writer.writeInt32(0)
      // error_code
      writer.writeInt16(0)
      // timestamp
      writer.writeInt64(-1n)
      // offset
      writer.writeInt64(500n)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.throttleTimeMs).toBe(100)
      }
    })
  })

  describe("v4–v5 — adds leader_epoch", () => {
    it("decodes leader epoch", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(0)
      // topics array length
      writer.writeInt32(1)
      // topic name
      writer.writeString("test-topic")
      // partitions array length
      writer.writeInt32(1)
      // partition_index
      writer.writeInt32(0)
      // error_code
      writer.writeInt16(0)
      // timestamp
      writer.writeInt64(1609459200000n)
      // offset
      writer.writeInt64(200n)
      // leader_epoch
      writer.writeInt32(7)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].partitions[0].leaderEpoch).toBe(7)
        expect(result.value.topics[0].partitions[0].offset).toBe(200n)
      }
    })
  })

  describe("v6+ — flexible encoding", () => {
    it("decodes compact arrays and tagged fields", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(50)
      // topics array (compact: length + 1)
      writer.writeUnsignedVarInt(2) // 1 topic + 1
      // topic name (compact string)
      writer.writeCompactString("test-topic")
      // partitions array (compact)
      writer.writeUnsignedVarInt(2) // 1 partition + 1
      // partition_index
      writer.writeInt32(0)
      // error_code
      writer.writeInt16(0)
      // timestamp
      writer.writeInt64(1609459200000n)
      // offset
      writer.writeInt64(1000n)
      // leader_epoch
      writer.writeInt32(10)
      // partition tagged fields
      writer.writeUnsignedVarInt(0)
      // topic tagged fields
      writer.writeUnsignedVarInt(0)
      // response tagged fields
      writer.writeUnsignedVarInt(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 6)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.throttleTimeMs).toBe(50)
        expect(result.value.topics).toHaveLength(1)
        expect(result.value.topics[0].name).toBe("test-topic")
        expect(result.value.topics[0].partitions[0].offset).toBe(1000n)
        expect(result.value.topics[0].partitions[0].leaderEpoch).toBe(10)
        expect(result.value.taggedFields).toHaveLength(0)
      }
    })

    it("decodes multiple topics and partitions", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(0)
      // topics array (3 = 2 topics + 1)
      writer.writeUnsignedVarInt(3)

      // Topic A
      writer.writeCompactString("topic-a")
      writer.writeUnsignedVarInt(3) // 2 partitions + 1
      // Partition 0
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeInt64(-1n)
      writer.writeInt64(100n)
      writer.writeInt32(1)
      writer.writeUnsignedVarInt(0)
      // Partition 1
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeInt64(-1n)
      writer.writeInt64(200n)
      writer.writeInt32(1)
      writer.writeUnsignedVarInt(0)
      // Topic A tagged fields
      writer.writeUnsignedVarInt(0)

      // Topic B
      writer.writeCompactString("topic-b")
      writer.writeUnsignedVarInt(2) // 1 partition + 1
      // Partition 0
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeInt64(-1n)
      writer.writeInt64(50n)
      writer.writeInt32(2)
      writer.writeUnsignedVarInt(0)
      // Topic B tagged fields
      writer.writeUnsignedVarInt(0)

      // Response tagged fields
      writer.writeUnsignedVarInt(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 6)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics).toHaveLength(2)
        expect(result.value.topics[0].name).toBe("topic-a")
        expect(result.value.topics[0].partitions).toHaveLength(2)
        expect(result.value.topics[1].name).toBe("topic-b")
        expect(result.value.topics[1].partitions).toHaveLength(1)
      }
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const buf = new Uint8Array([0, 0, 0]) // Too short
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 2)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated topic name (non-flexible)", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1) // topics array length = 1
      // topic name is too short (only 2 bytes of length prefix, no string data)
      const buf = writer.finish()
      // Append partial topic name length
      const truncated = new Uint8Array(buf.length + 2)
      truncated.set(buf)
      truncated.set([0, 5], buf.length) // says string length=5 but no data
      const reader = new BinaryReader(truncated)
      const result = decodeListOffsetsResponse(reader, 2)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated topic name (flexible)", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeUnsignedVarInt(2) // topics array = 1 topic
      // compact string says length=10 but no data follows
      writer.writeUnsignedVarInt(11) // compact string length (10 + 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 6)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated partition data (non-flexible)", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms (v2+)
      writer.writeInt32(1) // 1 topic
      writer.writeString("test-topic")
      writer.writeInt32(1) // 1 partition
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      // v2 needs timestamp (INT64) + offset (INT64) but we stop here
      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 2)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated partitions array (flexible)", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeUnsignedVarInt(2) // 1 topic
      writer.writeCompactString("test-topic")
      // partitions array compact length says 10 but only partial data
      writer.writeUnsignedVarInt(2) // 1 partition
      // partition_index only, truncated
      writer.writeInt32(0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 6)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v1+ timestamp/offset", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(1) // 1 topic
      writer.writeString("test-topic")
      writer.writeInt32(1) // 1 partition
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      // Missing timestamp and offset
      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 1)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v4+ leader_epoch", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1)
      writer.writeString("test-topic")
      writer.writeInt32(1)
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(1000n) // timestamp
      writer.writeInt64(500n) // offset
      // Missing leader_epoch
      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 4)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v6+ tagged fields", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeUnsignedVarInt(2) // 1 topic
      writer.writeCompactString("test-topic")
      writer.writeUnsignedVarInt(2) // 1 partition
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(1000n) // timestamp
      writer.writeInt64(500n) // offset
      writer.writeInt32(1) // leader_epoch
      writer.writeUnsignedVarInt(0) // partition tagged fields
      writer.writeUnsignedVarInt(0) // topic tagged fields
      // Missing response tagged fields
      const buf = writer.finish()
      // Truncate the last byte
      const truncated = buf.slice(0, buf.length - 1)
      const reader = new BinaryReader(truncated)
      const result = decodeListOffsetsResponse(reader, 6)
      // If we truncate 1 byte, the last tagged fields read will fail
      // (one of the tagged fields will be missing)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v0 old_style_offsets", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(1) // 1 topic
      writer.writeString("test-topic")
      writer.writeInt32(1) // 1 partition
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt32(1) // old_style_offsets length = 1
      // Missing the offset INT64
      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("correctly reports bytesRead", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(0)
      // topics array (compact: 1 = empty)
      writer.writeUnsignedVarInt(1)
      // response tagged fields
      writer.writeUnsignedVarInt(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 6)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.bytesRead).toBe(buf.length)
      }
    })
  })

  describe("v0 — multiple old_style_offsets", () => {
    it("decodes response with multiple offsets (takes first, skips rest)", () => {
      const writer = new BinaryWriter()

      // topics array length
      writer.writeInt32(1)
      // topic name
      writer.writeString("test-topic")
      // partitions array length
      writer.writeInt32(1)
      // partition_index
      writer.writeInt32(0)
      // error_code
      writer.writeInt16(0)
      // old_style_offsets array length = 3
      writer.writeInt32(3)
      // first offset (used)
      writer.writeInt64(100n)
      // second offset (skipped)
      writer.writeInt64(200n)
      // third offset (skipped)
      writer.writeInt64(300n)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].partitions[0].offset).toBe(100n)
      }
    })

    it("returns failure when skip offset read fails in v0", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(1) // topics
      writer.writeString("test-topic")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt32(3) // old_style_offsets length (3 offsets)
      writer.writeInt64(100n) // first offset OK
      writer.writeInt64(200n) // second offset OK
      // third offset missing — truncated

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeListOffsetsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })
  })
})

// ---------------------------------------------------------------------------
// Special timestamp constants
// ---------------------------------------------------------------------------

describe("OffsetTimestamp constants", () => {
  it("has correct values", () => {
    expect(OffsetTimestamp.Latest).toBe(-1n)
    expect(OffsetTimestamp.Earliest).toBe(-2n)
    expect(OffsetTimestamp.MaxTimestamp).toBe(-3n)
  })
})

describe("IsolationLevel constants", () => {
  it("has correct values", () => {
    expect(IsolationLevel.ReadUncommitted).toBe(0)
    expect(IsolationLevel.ReadCommitted).toBe(1)
  })
})
