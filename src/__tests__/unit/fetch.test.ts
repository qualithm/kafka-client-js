import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  buildFetchRequest,
  decodeFetchResponse,
  encodeFetchRequest,
  FetchIsolationLevel,
  type FetchRequest
} from "../../fetch"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeFetchRequest", () => {
  describe("v0 — replica_id, max_wait_ms, min_bytes, topics", () => {
    it("encodes single topic with one partition", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        replicaId: -1,
        maxWaitMs: 500,
        minBytes: 1,
        topics: [
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, fetchOffset: 100n, partitionMaxBytes: 1048576 }]
          }
        ]
      }
      encodeFetchRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // replica_id (INT32)
      const replicaResult = reader.readInt32()
      expect(replicaResult.ok).toBe(true)
      expect(replicaResult.ok && replicaResult.value).toBe(-1)

      // max_wait_ms (INT32)
      const waitResult = reader.readInt32()
      expect(waitResult.ok).toBe(true)
      expect(waitResult.ok && waitResult.value).toBe(500)

      // min_bytes (INT32)
      const minBytesResult = reader.readInt32()
      expect(minBytesResult.ok).toBe(true)
      expect(minBytesResult.ok && minBytesResult.value).toBe(1)

      // topics array length (INT32)
      const topicsLenResult = reader.readInt32()
      expect(topicsLenResult.ok).toBe(true)
      expect(topicsLenResult.ok && topicsLenResult.value).toBe(1)

      // topic name
      const nameResult = reader.readString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("test-topic")

      // partitions array length
      const partLenResult = reader.readInt32()
      expect(partLenResult.ok).toBe(true)
      expect(partLenResult.ok && partLenResult.value).toBe(1)

      // partition_index
      const partIndexResult = reader.readInt32()
      expect(partIndexResult.ok).toBe(true)
      expect(partIndexResult.ok && partIndexResult.value).toBe(0)

      // fetch_offset (INT64)
      const offsetResult = reader.readInt64()
      expect(offsetResult.ok).toBe(true)
      expect(offsetResult.ok && offsetResult.value).toBe(100n)

      // partition_max_bytes (INT32)
      const maxBytesResult = reader.readInt32()
      expect(maxBytesResult.ok).toBe(true)
      expect(maxBytesResult.ok && maxBytesResult.value).toBe(1048576)

      expect(reader.remaining).toBe(0)
    })

    it("encodes empty topics list", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 100,
        minBytes: 0,
        topics: []
      }
      encodeFetchRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes

      const topicsLenResult = reader.readInt32()
      expect(topicsLenResult.ok && topicsLenResult.value).toBe(0)
      expect(reader.remaining).toBe(0)
    })

    it("encodes multiple topics with multiple partitions", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        topics: [
          {
            name: "topic-a",
            partitions: [
              { partitionIndex: 0, fetchOffset: 10n, partitionMaxBytes: 1024 },
              { partitionIndex: 1, fetchOffset: 20n, partitionMaxBytes: 1024 }
            ]
          },
          {
            name: "topic-b",
            partitions: [{ partitionIndex: 0, fetchOffset: 0n, partitionMaxBytes: 2048 }]
          }
        ]
      }
      encodeFetchRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes

      const topicsLenResult = reader.readInt32()
      expect(topicsLenResult.ok && topicsLenResult.value).toBe(2)
    })

    it("defaults replicaId to -1 when omitted", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        topics: []
      }
      encodeFetchRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const replicaResult = reader.readInt32()
      expect(replicaResult.ok && replicaResult.value).toBe(-1)
    })
  })

  describe("v3+ — adds max_bytes at request level", () => {
    it("encodes max_bytes", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        maxBytes: 52428800,
        topics: []
      }
      encodeFetchRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes

      const maxBytesResult = reader.readInt32()
      expect(maxBytesResult.ok && maxBytesResult.value).toBe(52428800)
    })

    it("defaults max_bytes to MAX_INT32", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        topics: []
      }
      encodeFetchRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes

      const maxBytesResult = reader.readInt32()
      expect(maxBytesResult.ok && maxBytesResult.value).toBe(0x7fffffff)
    })
  })

  describe("v4+ — adds isolation_level", () => {
    it("encodes READ_COMMITTED isolation", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        isolationLevel: FetchIsolationLevel.ReadCommitted,
        topics: []
      }
      encodeFetchRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes
      reader.readInt32() // max_bytes

      const isolationResult = reader.readInt8()
      expect(isolationResult.ok && isolationResult.value).toBe(1)
    })

    it("defaults to READ_UNCOMMITTED", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        topics: []
      }
      encodeFetchRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes
      reader.readInt32() // max_bytes

      const isolationResult = reader.readInt8()
      expect(isolationResult.ok && isolationResult.value).toBe(0)
    })
  })

  describe("v7+ — adds session_id, session_epoch, forgotten_topics", () => {
    it("encodes session fields and forgotten topics", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        sessionId: 42,
        sessionEpoch: 1,
        topics: [],
        forgottenTopics: [{ name: "old-topic", partitions: [0, 1, 2] }]
      }
      encodeFetchRequest(writer, request, 7)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes
      reader.readInt32() // max_bytes
      reader.readInt8() // isolation_level

      // session_id
      const sessionIdResult = reader.readInt32()
      expect(sessionIdResult.ok && sessionIdResult.value).toBe(42)

      // session_epoch
      const sessionEpochResult = reader.readInt32()
      expect(sessionEpochResult.ok && sessionEpochResult.value).toBe(1)

      // topics array (empty)
      const topicsLenResult = reader.readInt32()
      expect(topicsLenResult.ok && topicsLenResult.value).toBe(0)

      // forgotten_topics_data array
      const forgottenLenResult = reader.readInt32()
      expect(forgottenLenResult.ok && forgottenLenResult.value).toBe(1)

      // forgotten topic name
      const nameResult = reader.readString()
      expect(nameResult.ok && nameResult.value).toBe("old-topic")

      // forgotten partitions
      const partLenResult = reader.readInt32()
      expect(partLenResult.ok && partLenResult.value).toBe(3)

      const p0 = reader.readInt32()
      expect(p0.ok && p0.value).toBe(0)
      const p1 = reader.readInt32()
      expect(p1.ok && p1.value).toBe(1)
      const p2 = reader.readInt32()
      expect(p2.ok && p2.value).toBe(2)

      expect(reader.remaining).toBe(0)
    })

    it("defaults session fields and empty forgotten topics", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        topics: []
      }
      encodeFetchRequest(writer, request, 7)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes
      reader.readInt32() // max_bytes
      reader.readInt8() // isolation_level

      const sessionIdResult = reader.readInt32()
      expect(sessionIdResult.ok && sessionIdResult.value).toBe(0)

      const sessionEpochResult = reader.readInt32()
      expect(sessionEpochResult.ok && sessionEpochResult.value).toBe(-1)

      reader.readInt32() // topics array

      const forgottenLenResult = reader.readInt32()
      expect(forgottenLenResult.ok && forgottenLenResult.value).toBe(0)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v9+ — adds current_leader_epoch per partition", () => {
    it("encodes current_leader_epoch", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        topics: [
          {
            name: "test-topic",
            partitions: [
              {
                partitionIndex: 0,
                currentLeaderEpoch: 5,
                fetchOffset: 100n,
                partitionMaxBytes: 1024
              }
            ]
          }
        ]
      }
      encodeFetchRequest(writer, request, 9)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes
      reader.readInt32() // max_bytes
      reader.readInt8() // isolation_level
      reader.readInt32() // session_id
      reader.readInt32() // session_epoch

      // topics array
      reader.readInt32()
      // topic name
      reader.readString()
      // partitions array
      reader.readInt32()
      // partition_index
      reader.readInt32()

      // current_leader_epoch (INT32)
      const epochResult = reader.readInt32()
      expect(epochResult.ok && epochResult.value).toBe(5)

      // fetch_offset
      const offsetResult = reader.readInt64()
      expect(offsetResult.ok && offsetResult.value).toBe(100n)
    })
  })

  describe("v11+ — adds rack_id", () => {
    it("encodes rack_id", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        topics: [],
        rackId: "us-east-1a"
      }
      encodeFetchRequest(writer, request, 11)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes
      reader.readInt32() // max_bytes
      reader.readInt8() // isolation_level
      reader.readInt32() // session_id
      reader.readInt32() // session_epoch
      reader.readInt32() // topics array
      reader.readInt32() // forgotten topics array

      const rackResult = reader.readString()
      expect(rackResult.ok && rackResult.value).toBe("us-east-1a")

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v12+ — flexible encoding", () => {
    it("encodes with compact strings and arrays", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        topics: [
          {
            name: "test-topic",
            partitions: [
              {
                partitionIndex: 0,
                currentLeaderEpoch: -1,
                fetchOffset: 0n,
                logStartOffset: -1n,
                partitionMaxBytes: 1048576
              }
            ]
          }
        ],
        rackId: ""
      }
      encodeFetchRequest(writer, request, 12)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // replica_id
      const replicaResult = reader.readInt32()
      expect(replicaResult.ok && replicaResult.value).toBe(-1)

      // max_wait_ms
      reader.readInt32()
      // min_bytes
      reader.readInt32()
      // max_bytes
      reader.readInt32()
      // isolation_level
      reader.readInt8()
      // session_id
      reader.readInt32()
      // session_epoch
      reader.readInt32()

      // topics array (compact: 1 + 1 = 2)
      const topicsLenResult = reader.readUnsignedVarInt()
      expect(topicsLenResult.ok && topicsLenResult.value).toBe(2)

      // topic name (compact string)
      const nameResult = reader.readCompactString()
      expect(nameResult.ok && nameResult.value).toBe("test-topic")

      // partitions array (compact: 1 + 1 = 2)
      const partLenResult = reader.readUnsignedVarInt()
      expect(partLenResult.ok && partLenResult.value).toBe(2)

      // partition_index
      reader.readInt32()
      // current_leader_epoch
      reader.readInt32()
      // fetch_offset
      reader.readInt64()
      // log_start_offset
      reader.readInt64()
      // partition_max_bytes
      reader.readInt32()

      // partition tagged fields
      const partTagsResult = reader.readTaggedFields()
      expect(partTagsResult.ok).toBe(true)

      // topic tagged fields
      const topicTagsResult = reader.readTaggedFields()
      expect(topicTagsResult.ok).toBe(true)

      // forgotten topics (compact: 0 + 1 = 1)
      const forgottenLenResult = reader.readUnsignedVarInt()
      expect(forgottenLenResult.ok && forgottenLenResult.value).toBe(1)

      // rack_id (compact string)
      const rackResult = reader.readCompactString()
      expect(rackResult.ok && rackResult.value).toBe("")

      // request tagged fields
      const requestTagsResult = reader.readTaggedFields()
      expect(requestTagsResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v13+ — topic_id (UUID) replaces topic name", () => {
    it("encodes topics with UUID", () => {
      const topicId = new Uint8Array([
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f
      ])
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        topics: [
          {
            topicId,
            partitions: [{ partitionIndex: 0, fetchOffset: 0n, partitionMaxBytes: 1048576 }]
          }
        ],
        forgottenTopics: [{ topicId, partitions: [1] }]
      }
      encodeFetchRequest(writer, request, 13)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes
      reader.readInt32() // max_bytes
      reader.readInt8() // isolation_level
      reader.readInt32() // session_id
      reader.readInt32() // session_epoch

      // topics array (compact)
      reader.readUnsignedVarInt()

      // topic_id (UUID - 16 bytes)
      const uuidResult = reader.readUuid()
      expect(uuidResult.ok).toBe(true)
      if (uuidResult.ok) {
        expect(uuidResult.value).toEqual(topicId)
      }

      // partitions array (compact)
      reader.readUnsignedVarInt()
      reader.readInt32() // partition_index
      reader.readInt32() // current_leader_epoch
      reader.readInt64() // fetch_offset
      reader.readInt64() // log_start_offset
      reader.readInt32() // partition_max_bytes
      reader.readTaggedFields() // partition tags
      reader.readTaggedFields() // topic tags

      // forgotten_topics_data array (compact)
      const forgottenLenResult = reader.readUnsignedVarInt()
      expect(forgottenLenResult.ok && forgottenLenResult.value).toBe(2) // 1 + 1

      // forgotten topic_id (UUID)
      const forgottenUuidResult = reader.readUuid()
      expect(forgottenUuidResult.ok).toBe(true)
      if (forgottenUuidResult.ok) {
        expect(forgottenUuidResult.value).toEqual(topicId)
      }
    })
  })

  describe("v12+ — forgotten topics with flexible encoding", () => {
    it("encodes forgotten topics with compact strings (non-UUID)", () => {
      const writer = new BinaryWriter()
      const request: FetchRequest = {
        maxWaitMs: 500,
        minBytes: 1,
        topics: [],
        forgottenTopics: [{ name: "old-topic", partitions: [0, 2] }]
      }
      encodeFetchRequest(writer, request, 12)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // replica_id
      reader.readInt32() // max_wait_ms
      reader.readInt32() // min_bytes
      reader.readInt32() // max_bytes
      reader.readInt8() // isolation_level
      reader.readInt32() // session_id
      reader.readInt32() // session_epoch

      // topics array (compact: 0 + 1 = 1)
      const topicsLenResult = reader.readUnsignedVarInt()
      expect(topicsLenResult.ok && topicsLenResult.value).toBe(1)

      // forgotten_topics_data array (compact: 1 + 1 = 2)
      const forgottenLenResult = reader.readUnsignedVarInt()
      expect(forgottenLenResult.ok && forgottenLenResult.value).toBe(2)

      // forgotten topic name (compact string)
      const nameResult = reader.readCompactString()
      expect(nameResult.ok && nameResult.value).toBe("old-topic")

      // forgotten partitions (compact: 2 + 1 = 3)
      const partLenResult = reader.readUnsignedVarInt()
      expect(partLenResult.ok && partLenResult.value).toBe(3)

      const p0 = reader.readInt32()
      expect(p0.ok && p0.value).toBe(0)
      const p1 = reader.readInt32()
      expect(p1.ok && p1.value).toBe(2)

      // forgotten topic tagged fields
      const tagsResult = reader.readTaggedFields()
      expect(tagsResult.ok).toBe(true)

      // rack_id (compact string)
      reader.readCompactString()
      // request tagged fields
      reader.readTaggedFields()

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// Build framed request
// ---------------------------------------------------------------------------

describe("buildFetchRequest", () => {
  it("builds a framed v0 request", () => {
    const frame = buildFetchRequest(1, 0, {
      maxWaitMs: 500,
      minBytes: 1,
      topics: []
    })

    const reader = new BinaryReader(frame)

    // Size prefix
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)
    expect(sizeResult.ok && sizeResult.value).toBe(frame.length - 4)

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.Fetch)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok && versionResult.value).toBe(0)

    // Correlation ID
    const corrIdResult = reader.readInt32()
    expect(corrIdResult.ok && corrIdResult.value).toBe(1)
  })

  it("builds a framed v12 request with client ID", () => {
    const frame = buildFetchRequest(
      42,
      12,
      {
        maxWaitMs: 500,
        minBytes: 1,
        topics: []
      },
      "my-client"
    )

    const reader = new BinaryReader(frame)

    // Size prefix
    reader.readInt32()

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.Fetch)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok && versionResult.value).toBe(12)

    // Correlation ID
    const corrIdResult = reader.readInt32()
    expect(corrIdResult.ok && corrIdResult.value).toBe(42)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeFetchResponse", () => {
  describe("v0 — high_watermark and records per partition", () => {
    it("decodes response with single topic and partition", () => {
      const records = new Uint8Array([0x01, 0x02, 0x03])
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
      // high_watermark
      writer.writeInt64(100n)
      // records (length-prefixed bytes)
      writer.writeBytes(records)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.throttleTimeMs).toBe(0)
        expect(result.value.errorCode).toBe(0)
        expect(result.value.sessionId).toBe(0)
        expect(result.value.topics).toHaveLength(1)
        expect(result.value.topics[0].name).toBe("test-topic")
        expect(result.value.topics[0].partitions).toHaveLength(1)

        const partition = result.value.topics[0].partitions[0]
        expect(partition.partitionIndex).toBe(0)
        expect(partition.errorCode).toBe(0)
        expect(partition.highWatermark).toBe(100n)
        expect(partition.lastStableOffset).toBe(-1n)
        expect(partition.logStartOffset).toBe(-1n)
        expect(partition.abortedTransactions).toEqual([])
        expect(partition.preferredReadReplica).toBe(-1)
        expect(partition.records).toEqual(records)
      }
    })

    it("decodes response with null records", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(1)
      writer.writeString("test-topic")
      writer.writeInt32(1)
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeInt64(0n)
      writer.writeBytes(null)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].partitions[0].records).toBeNull()
      }
    })

    it("decodes response with error", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(1)
      writer.writeString("test-topic")
      writer.writeInt32(1)
      writer.writeInt32(0)
      writer.writeInt16(1) // OFFSET_OUT_OF_RANGE
      writer.writeInt64(0n)
      writer.writeBytes(null)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].partitions[0].errorCode).toBe(1)
      }
    })

    it("decodes response with multiple topics and partitions", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(2)

      // topic-a with 2 partitions
      writer.writeString("topic-a")
      writer.writeInt32(2)
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeInt64(10n)
      writer.writeBytes(new Uint8Array([0x01]))
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeInt64(20n)
      writer.writeBytes(new Uint8Array([0x02]))

      // topic-b with 1 partition
      writer.writeString("topic-b")
      writer.writeInt32(1)
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeInt64(30n)
      writer.writeBytes(null)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics).toHaveLength(2)
        expect(result.value.topics[0].name).toBe("topic-a")
        expect(result.value.topics[0].partitions).toHaveLength(2)
        expect(result.value.topics[0].partitions[0].highWatermark).toBe(10n)
        expect(result.value.topics[0].partitions[1].highWatermark).toBe(20n)
        expect(result.value.topics[1].name).toBe("topic-b")
        expect(result.value.topics[1].partitions[0].highWatermark).toBe(30n)
      }
    })
  })

  describe("v1+ — adds throttle_time_ms", () => {
    it("decodes throttle time", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(500)
      // topics array (empty)
      writer.writeInt32(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.throttleTimeMs).toBe(500)
        expect(result.value.topics).toHaveLength(0)
      }
    })
  })

  describe("v4+ — adds last_stable_offset and aborted_transactions", () => {
    it("decodes last_stable_offset and empty aborted transactions", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1) // topics
      writer.writeString("test-topic")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      writer.writeInt32(-1) // aborted_transactions (null = -1)
      writer.writeBytes(null) // records

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (result.ok) {
        const partition = result.value.topics[0].partitions[0]
        expect(partition.lastStableOffset).toBe(90n)
        expect(partition.abortedTransactions).toEqual([])
        expect(partition.logStartOffset).toBe(-1n)
      }
    })

    it("decodes aborted transactions", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1) // topics
      writer.writeString("test-topic")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      // aborted_transactions array (2 entries)
      writer.writeInt32(2)
      writer.writeInt64(1000n) // producer_id
      writer.writeInt64(50n) // first_offset
      writer.writeInt64(2000n) // producer_id
      writer.writeInt64(70n) // first_offset
      writer.writeBytes(null) // records

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (result.ok) {
        const partition = result.value.topics[0].partitions[0]
        expect(partition.abortedTransactions).toHaveLength(2)
        expect(partition.abortedTransactions[0].producerId).toBe(1000n)
        expect(partition.abortedTransactions[0].firstOffset).toBe(50n)
        expect(partition.abortedTransactions[1].producerId).toBe(2000n)
        expect(partition.abortedTransactions[1].firstOffset).toBe(70n)
      }
    })
  })

  describe("v5+ — adds log_start_offset", () => {
    it("decodes log start offset", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1) // topics
      writer.writeString("test-topic")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      writer.writeInt64(5n) // log_start_offset
      writer.writeInt32(-1) // aborted_transactions (null)
      writer.writeBytes(new Uint8Array([0xaa])) // records

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 5)

      expect(result.ok).toBe(true)
      if (result.ok) {
        const partition = result.value.topics[0].partitions[0]
        expect(partition.logStartOffset).toBe(5n)
        expect(partition.records).toEqual(new Uint8Array([0xaa]))
      }
    })
  })

  describe("v7+ — adds top-level error_code and session_id", () => {
    it("decodes top-level error and session ID", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(99) // session_id
      writer.writeInt32(0) // topics (empty)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 7)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.errorCode).toBe(0)
        expect(result.value.sessionId).toBe(99)
        expect(result.value.topics).toHaveLength(0)
      }
    })

    it("decodes top-level error code", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(70) // FETCH_SESSION_ID_NOT_FOUND
      writer.writeInt32(0) // session_id
      writer.writeInt32(0) // topics

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 7)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.errorCode).toBe(70)
      }
    })
  })

  describe("v11+ — adds preferred_read_replica", () => {
    it("decodes preferred read replica", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      writer.writeInt32(1) // topics
      writer.writeString("test-topic")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      writer.writeInt64(0n) // log_start_offset
      writer.writeInt32(-1) // aborted_transactions (null)
      writer.writeInt32(3) // preferred_read_replica
      writer.writeBytes(null) // records

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 11)

      expect(result.ok).toBe(true)
      if (result.ok) {
        const partition = result.value.topics[0].partitions[0]
        expect(partition.preferredReadReplica).toBe(3)
      }
    })
  })

  describe("v12+ — flexible encoding", () => {
    it("decodes response with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(100) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(5) // session_id
      // topics array (compact: 1 + 1 = 2)
      writer.writeUnsignedVarInt(2)
      // topic name (compact string)
      writer.writeCompactString("test-topic")
      // partitions array (compact: 1 + 1 = 2)
      writer.writeUnsignedVarInt(2)
      // partition_index
      writer.writeInt32(0)
      // error_code
      writer.writeInt16(0)
      // high_watermark
      writer.writeInt64(42n)
      // last_stable_offset
      writer.writeInt64(40n)
      // log_start_offset
      writer.writeInt64(0n)
      // aborted_transactions (compact: 0 + 1 = 1 = empty)
      writer.writeUnsignedVarInt(1)
      // preferred_read_replica
      writer.writeInt32(-1)
      // records (compact nullable bytes: 0 = null)
      writer.writeCompactBytes(null)
      // partition tagged fields
      writer.writeTaggedFields([])
      // topic tagged fields
      writer.writeTaggedFields([])
      // response tagged fields
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 12)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.throttleTimeMs).toBe(100)
        expect(result.value.errorCode).toBe(0)
        expect(result.value.sessionId).toBe(5)
        expect(result.value.topics).toHaveLength(1)
        expect(result.value.topics[0].name).toBe("test-topic")

        const partition = result.value.topics[0].partitions[0]
        expect(partition.partitionIndex).toBe(0)
        expect(partition.errorCode).toBe(0)
        expect(partition.highWatermark).toBe(42n)
        expect(partition.lastStableOffset).toBe(40n)
        expect(partition.logStartOffset).toBe(0n)
        expect(partition.abortedTransactions).toEqual([])
        expect(partition.preferredReadReplica).toBe(-1)
        expect(partition.records).toBeNull()
        expect(result.value.taggedFields).toEqual([])
      }
    })

    it("decodes v12 aborted transactions with compact encoding", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      // topics (compact)
      writer.writeUnsignedVarInt(2)
      writer.writeCompactString("test-topic")
      // partitions (compact)
      writer.writeUnsignedVarInt(2)
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      writer.writeInt64(0n) // log_start_offset
      // aborted_transactions (compact: 1 + 1 = 2)
      writer.writeUnsignedVarInt(2)
      writer.writeInt64(500n) // producer_id
      writer.writeInt64(10n) // first_offset
      writer.writeTaggedFields([]) // aborted txn tags
      // preferred_read_replica
      writer.writeInt32(-1)
      // records
      writer.writeCompactBytes(null)
      // partition tagged fields
      writer.writeTaggedFields([])
      // topic tagged fields
      writer.writeTaggedFields([])
      // response tagged fields
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 12)

      expect(result.ok).toBe(true)
      if (result.ok) {
        const partition = result.value.topics[0].partitions[0]
        expect(partition.abortedTransactions).toHaveLength(1)
        expect(partition.abortedTransactions[0].producerId).toBe(500n)
        expect(partition.abortedTransactions[0].firstOffset).toBe(10n)
      }
    })
  })

  describe("v13+ — topic_id (UUID) replaces topic name", () => {
    it("decodes response with topic UUID", () => {
      const topicId = new Uint8Array([
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f
      ])
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      // topics (compact: 1 + 1 = 2)
      writer.writeUnsignedVarInt(2)
      // topic_id (UUID)
      writer.writeUuid(topicId)
      // partitions (compact: 1 + 1 = 2)
      writer.writeUnsignedVarInt(2)
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(50n) // high_watermark
      writer.writeInt64(50n) // last_stable_offset
      writer.writeInt64(0n) // log_start_offset
      writer.writeUnsignedVarInt(1) // aborted_transactions (empty)
      writer.writeInt32(-1) // preferred_read_replica
      writer.writeCompactBytes(null) // records
      writer.writeTaggedFields([]) // partition tags
      writer.writeTaggedFields([]) // topic tags
      writer.writeTaggedFields([]) // response tags

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 13)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].name).toBe("")
        expect(result.value.topics[0].topicId).toEqual(topicId)

        const partition = result.value.topics[0].partitions[0]
        expect(partition.highWatermark).toBe(50n)
      }
    })
  })

  describe("empty response", () => {
    it("decodes v0 response with no topics", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics).toHaveLength(0)
        expect(result.value.throttleTimeMs).toBe(0)
      }
    })

    it("decodes v7 empty response with session ID", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(12) // session_id
      writer.writeInt32(0) // topics (empty)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeFetchResponse(reader, 7)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.sessionId).toBe(12)
        expect(result.value.topics).toHaveLength(0)
      }
    })
  })

  describe("truncated response — buffer underflow errors", () => {
    it("returns failure when v1 response is truncated before throttle_time_ms", () => {
      const reader = new BinaryReader(new Uint8Array(0))
      const result = decodeFetchResponse(reader, 1)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v7 response is truncated before error_code", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms only
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 7)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v7 response is truncated before session_id", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 7)
      expect(result.ok).toBe(false)
    })

    it("returns failure when response is truncated before topics array", () => {
      const reader = new BinaryReader(
        new Uint8Array([
          0x00,
          0x00,
          0x00,
          0x00, // throttle_time_ms
          0x00,
          0x00, // error_code
          0x00,
          0x00,
          0x00,
          0x00 // session_id
          // topics array length missing
        ])
      )
      const result = decodeFetchResponse(reader, 7)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v12 response is truncated before tagged fields", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      writer.writeUnsignedVarInt(1) // topics array (empty)
      // no trailing tagged fields
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 12)
      expect(result.ok).toBe(false)
    })

    it("returns failure when topic name is truncated in v0", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(1) // topics count
      writer.writeInt16(5) // string length = 5 but no string data follows
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure when topic name is truncated in v12 flexible", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      writer.writeUnsignedVarInt(2) // 1 topic
      writer.writeUnsignedVarInt(100) // compact string length = 99 chars, but no data
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 12)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v13 topic UUID is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      writer.writeUnsignedVarInt(2) // 1 topic
      // Only 4 bytes of UUID instead of 16
      writer.writeRawBytes(new Uint8Array([0x01, 0x02, 0x03, 0x04]))
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 13)
      expect(result.ok).toBe(false)
    })

    it("returns failure when partition data is truncated in topic", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(1) // topics count
      writer.writeString("test-topic")
      writer.writeInt32(1) // partitions count
      // only partition_index, nothing else
      writer.writeInt32(0)
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure when partition_index is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(1) // topics count
      writer.writeString("t")
      writer.writeInt32(1) // partitions count
      // no partition data
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure when high_watermark is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(1) // topics count
      writer.writeString("t")
      writer.writeInt32(1) // partitions count
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      // no high_watermark
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v4 last_stable_offset is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1) // topics
      writer.writeString("t")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      // no last_stable_offset
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 4)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v5 log_start_offset is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1) // topics
      writer.writeString("t")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      // no log_start_offset
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 5)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v4 aborted_transactions array count is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1) // topics
      writer.writeString("t")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      // missing aborted_transactions array count
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 4)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v4 aborted_transactions array entry is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1) // topics
      writer.writeString("t")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      // aborted_transactions with count but truncated entry
      writer.writeInt32(1) // 1 transaction
      // only producer_id, no first_offset
      writer.writeInt64(1000n)
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 4)
      expect(result.ok).toBe(false)
    })

    it("returns failure when aborted_transactions producer_id is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1) // topics
      writer.writeString("t")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      writer.writeInt32(1) // 1 aborted transaction
      // no producer_id data
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 4)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v11 preferred_read_replica is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      writer.writeInt32(1) // topics
      writer.writeString("t")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      writer.writeInt64(0n) // log_start_offset
      writer.writeInt32(-1) // aborted_transactions (null)
      // no preferred_read_replica
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 11)
      expect(result.ok).toBe(false)
    })

    it("returns failure when records bytes are truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(1) // topics
      writer.writeString("t")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt32(999) // records length = 999 but no data
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v12 partition tagged fields are truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      writer.writeUnsignedVarInt(2) // 1 topic
      writer.writeCompactString("t")
      writer.writeUnsignedVarInt(2) // 1 partition
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      writer.writeInt64(0n) // log_start_offset
      writer.writeUnsignedVarInt(1) // aborted_transactions (empty)
      writer.writeInt32(-1) // preferred_read_replica
      writer.writeCompactBytes(null) // records
      // no partition tagged fields
      const buf = writer.finish()
      // Truncate so tagged fields can't be read
      const reader2 = new BinaryReader(buf.subarray(0, buf.length - 1))
      const result = decodeFetchResponse(reader2, 12)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v12 topic tagged fields are truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      writer.writeUnsignedVarInt(2) // 1 topic
      writer.writeCompactString("t")
      writer.writeUnsignedVarInt(1) // 0 partitions
      // no topic tagged fields
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 12)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v12 aborted transaction tagged fields are truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      writer.writeUnsignedVarInt(2) // 1 topic
      writer.writeCompactString("t")
      writer.writeUnsignedVarInt(2) // 1 partition
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      writer.writeInt64(0n) // log_start_offset
      // 1 aborted transaction
      writer.writeUnsignedVarInt(2)
      writer.writeInt64(500n) // producer_id
      writer.writeInt64(10n) // first_offset
      // no tagged fields for aborted transaction
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 12)
      expect(result.ok).toBe(false)
    })

    it("returns failure when partitions array length is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(1) // topics count
      writer.writeString("t")
      // no partitions array length
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v12 topics compact array length is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      // no topics compact array
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 12)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v12 partitions compact array length is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      writer.writeUnsignedVarInt(2) // 1 topic
      writer.writeCompactString("t")
      // no partitions compact array length
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 12)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v12 partition tagged fields are missing", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      writer.writeUnsignedVarInt(2) // 1 topic
      writer.writeCompactString("t")
      writer.writeUnsignedVarInt(2) // 1 partition
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      writer.writeInt64(0n) // log_start_offset
      writer.writeUnsignedVarInt(1) // aborted_transactions (empty)
      writer.writeInt32(-1) // preferred_read_replica
      writer.writeCompactBytes(new Uint8Array([0x01])) // records (non-null)
      // missing partition tagged fields — truncated
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 12)
      expect(result.ok).toBe(false)
    })

    it("returns failure when v12 aborted_transactions compact array is truncated", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(0) // error_code
      writer.writeInt32(0) // session_id
      writer.writeUnsignedVarInt(2) // 1 topic
      writer.writeCompactString("t")
      writer.writeUnsignedVarInt(2) // 1 partition
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // high_watermark
      writer.writeInt64(90n) // last_stable_offset
      writer.writeInt64(0n) // log_start_offset
      // missing aborted_transactions compact array length
      const reader = new BinaryReader(writer.finish())
      const result = decodeFetchResponse(reader, 12)
      expect(result.ok).toBe(false)
    })
  })
})
