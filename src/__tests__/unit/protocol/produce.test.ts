/**
 * Produce (API key 0) request/response tests.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_Produce
 */

import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  Acks,
  buildProduceRequest,
  decodeProduceResponse,
  encodeProduceRequest,
  type ProduceRequest
} from "../../../protocol/produce"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeProduceRequest", () => {
  describe("v0 — acks, timeout, topic_data with partition records", () => {
    it("encodes single topic with one partition", () => {
      const records = new Uint8Array([0x01, 0x02, 0x03])
      const writer = new BinaryWriter()
      const request: ProduceRequest = {
        acks: Acks.Leader,
        timeoutMs: 30000,
        topics: [
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, records }]
          }
        ]
      }
      encodeProduceRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // acks (INT16)
      const acksResult = reader.readInt16()
      expect(acksResult.ok).toBe(true)
      expect(acksResult.ok && acksResult.value).toBe(1)

      // timeout_ms (INT32)
      const timeoutResult = reader.readInt32()
      expect(timeoutResult.ok).toBe(true)
      expect(timeoutResult.ok && timeoutResult.value).toBe(30000)

      // topic_data array length (INT32)
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

      // records (length-prefixed bytes)
      const recordsLenResult = reader.readInt32()
      expect(recordsLenResult.ok).toBe(true)
      expect(recordsLenResult.ok && recordsLenResult.value).toBe(3)

      const recordsBytesResult = reader.readRawBytes(3)
      expect(recordsBytesResult.ok).toBe(true)
      if (recordsBytesResult.ok) {
        expect(recordsBytesResult.value).toEqual(records)
      }

      expect(reader.remaining).toBe(0)
    })

    it("encodes acks = -1 (all)", () => {
      const writer = new BinaryWriter()
      const request: ProduceRequest = {
        acks: Acks.All,
        timeoutMs: 5000,
        topics: []
      }
      encodeProduceRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const acksResult = reader.readInt16()
      expect(acksResult.ok).toBe(true)
      expect(acksResult.ok && acksResult.value).toBe(-1)
    })

    it("encodes acks = 0 (none)", () => {
      const writer = new BinaryWriter()
      const request: ProduceRequest = {
        acks: Acks.None,
        timeoutMs: 5000,
        topics: []
      }
      encodeProduceRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const acksResult = reader.readInt16()
      expect(acksResult.ok).toBe(true)
      expect(acksResult.ok && acksResult.value).toBe(0)
    })

    it("encodes null records for a partition", () => {
      const writer = new BinaryWriter()
      const request: ProduceRequest = {
        acks: Acks.Leader,
        timeoutMs: 1000,
        topics: [
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, records: null }]
          }
        ]
      }
      encodeProduceRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Skip acks + timeout + topics length + topic name + partitions length + partition_index
      reader.readInt16()
      reader.readInt32()
      reader.readInt32()
      reader.readString()
      reader.readInt32()
      reader.readInt32()

      // records length = -1 (null)
      const recordsLenResult = reader.readInt32()
      expect(recordsLenResult.ok).toBe(true)
      expect(recordsLenResult.ok && recordsLenResult.value).toBe(-1)

      expect(reader.remaining).toBe(0)
    })

    it("encodes multiple topics with multiple partitions", () => {
      const recordsA = new Uint8Array([0x10, 0x20])
      const recordsB = new Uint8Array([0x30])
      const writer = new BinaryWriter()
      const request: ProduceRequest = {
        acks: Acks.All,
        timeoutMs: 10000,
        topics: [
          {
            name: "topic-a",
            partitions: [
              { partitionIndex: 0, records: recordsA },
              { partitionIndex: 1, records: recordsB }
            ]
          },
          {
            name: "topic-b",
            partitions: [{ partitionIndex: 0, records: null }]
          }
        ]
      }
      encodeProduceRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // acks
      reader.readInt16()
      // timeout
      reader.readInt32()

      // topics array length
      const topicsLenResult = reader.readInt32()
      expect(topicsLenResult.ok && topicsLenResult.value).toBe(2)

      // topic-a
      expect(reader.readString().ok && reader.readString()).toBeTruthy()
    })
  })

  describe("v3+ — adds transactional_id", () => {
    it("encodes null transactional_id", () => {
      const writer = new BinaryWriter()
      const request: ProduceRequest = {
        transactionalId: null,
        acks: Acks.All,
        timeoutMs: 5000,
        topics: []
      }
      encodeProduceRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id (nullable string, null = -1 length)
      const txnIdResult = reader.readString()
      expect(txnIdResult.ok).toBe(true)
      expect(txnIdResult.ok && txnIdResult.value).toBeNull()

      // acks
      const acksResult = reader.readInt16()
      expect(acksResult.ok && acksResult.value).toBe(-1)
    })

    it("encodes non-null transactional_id", () => {
      const writer = new BinaryWriter()
      const request: ProduceRequest = {
        transactionalId: "my-txn-id",
        acks: Acks.All,
        timeoutMs: 5000,
        topics: []
      }
      encodeProduceRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id
      const txnIdResult = reader.readString()
      expect(txnIdResult.ok).toBe(true)
      expect(txnIdResult.ok && txnIdResult.value).toBe("my-txn-id")
    })

    it("defaults transactional_id to null when omitted", () => {
      const writer = new BinaryWriter()
      const request: ProduceRequest = {
        acks: Acks.Leader,
        timeoutMs: 5000,
        topics: []
      }
      encodeProduceRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const txnIdResult = reader.readString()
      expect(txnIdResult.ok).toBe(true)
      expect(txnIdResult.ok && txnIdResult.value).toBeNull()
    })
  })

  describe("v9+ — flexible encoding", () => {
    it("encodes with compact strings and arrays", () => {
      const records = new Uint8Array([0xaa, 0xbb])
      const writer = new BinaryWriter()
      const request: ProduceRequest = {
        transactionalId: null,
        acks: Acks.All,
        timeoutMs: 30000,
        topics: [
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, records }]
          }
        ]
      }
      encodeProduceRequest(writer, request, 9)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id (compact nullable string: 0 = null)
      const txnIdResult = reader.readCompactString()
      expect(txnIdResult.ok).toBe(true)
      expect(txnIdResult.ok && txnIdResult.value).toBeNull()

      // acks
      const acksResult = reader.readInt16()
      expect(acksResult.ok && acksResult.value).toBe(-1)

      // timeout_ms
      const timeoutResult = reader.readInt32()
      expect(timeoutResult.ok && timeoutResult.value).toBe(30000)

      // topic_data array (compact: length + 1)
      const topicsLenResult = reader.readUnsignedVarInt()
      expect(topicsLenResult.ok).toBe(true)
      expect(topicsLenResult.ok && topicsLenResult.value).toBe(2) // 1 + 1

      // topic name (compact string)
      const nameResult = reader.readCompactString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("test-topic")

      // partitions array (compact)
      const partLenResult = reader.readUnsignedVarInt()
      expect(partLenResult.ok && partLenResult.value).toBe(2) // 1 + 1

      // partition_index
      const partIndexResult = reader.readInt32()
      expect(partIndexResult.ok && partIndexResult.value).toBe(0)

      // records (compact bytes: length + 1)
      const recordsLenResult = reader.readUnsignedVarInt()
      expect(recordsLenResult.ok && recordsLenResult.value).toBe(3) // 2 + 1

      const recordsBytesResult = reader.readRawBytes(2)
      expect(recordsBytesResult.ok).toBe(true)
      if (recordsBytesResult.ok) {
        expect(recordsBytesResult.value).toEqual(records)
      }

      // partition tagged fields
      const partTagsResult = reader.readTaggedFields()
      expect(partTagsResult.ok).toBe(true)

      // topic tagged fields
      const topicTagsResult = reader.readTaggedFields()
      expect(topicTagsResult.ok).toBe(true)

      // request tagged fields
      const requestTagsResult = reader.readTaggedFields()
      expect(requestTagsResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })

    it("encodes non-null transactional_id with compact string", () => {
      const writer = new BinaryWriter()
      const request: ProduceRequest = {
        transactionalId: "txn-1",
        acks: Acks.All,
        timeoutMs: 5000,
        topics: []
      }
      encodeProduceRequest(writer, request, 9)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id (compact string)
      const txnIdResult = reader.readCompactString()
      expect(txnIdResult.ok).toBe(true)
      expect(txnIdResult.ok && txnIdResult.value).toBe("txn-1")
    })

    it("encodes null records with compact bytes encoding", () => {
      const writer = new BinaryWriter()
      const request: ProduceRequest = {
        acks: Acks.Leader,
        timeoutMs: 5000,
        topics: [
          {
            name: "t",
            partitions: [{ partitionIndex: 0, records: null }]
          }
        ]
      }
      encodeProduceRequest(writer, request, 9)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id
      reader.readCompactString()
      // acks
      reader.readInt16()
      // timeout_ms
      reader.readInt32()
      // topics array
      reader.readUnsignedVarInt()
      // topic name
      reader.readCompactString()
      // partitions array
      reader.readUnsignedVarInt()
      // partition_index
      reader.readInt32()

      // records (compact nullable bytes: 0 = null)
      const recordsLenResult = reader.readUnsignedVarInt()
      expect(recordsLenResult.ok).toBe(true)
      expect(recordsLenResult.ok && recordsLenResult.value).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// Build framed request
// ---------------------------------------------------------------------------

describe("buildProduceRequest", () => {
  it("builds a framed v0 request", () => {
    const frame = buildProduceRequest(1, 0, {
      acks: Acks.Leader,
      timeoutMs: 5000,
      topics: []
    })

    const reader = new BinaryReader(frame)

    // Size prefix
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)
    expect(sizeResult.ok && sizeResult.value).toBe(frame.length - 4)

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.Produce)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok).toBe(true)
    expect(versionResult.ok && versionResult.value).toBe(0)

    // Correlation ID
    const corrIdResult = reader.readInt32()
    expect(corrIdResult.ok).toBe(true)
    expect(corrIdResult.ok && corrIdResult.value).toBe(1)
  })

  it("builds a framed v9 request with client ID", () => {
    const frame = buildProduceRequest(
      42,
      9,
      {
        acks: Acks.All,
        timeoutMs: 30000,
        topics: []
      },
      "my-client"
    )

    const reader = new BinaryReader(frame)

    // Size prefix
    reader.readInt32()

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.Produce)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok && versionResult.value).toBe(9)

    // Correlation ID
    const corrIdResult = reader.readInt32()
    expect(corrIdResult.ok && corrIdResult.value).toBe(42)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeProduceResponse", () => {
  describe("v0 — base_offset and error_code per partition", () => {
    it("decodes response with single topic and partition", () => {
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
      // base_offset
      writer.writeInt64(100n)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeProduceResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.throttleTimeMs).toBe(0)
        expect(result.value.topics).toHaveLength(1)
        expect(result.value.topics[0].name).toBe("test-topic")
        expect(result.value.topics[0].partitions).toHaveLength(1)

        const partition = result.value.topics[0].partitions[0]
        expect(partition.partitionIndex).toBe(0)
        expect(partition.errorCode).toBe(0)
        expect(partition.baseOffset).toBe(100n)
        expect(partition.logAppendTimeMs).toBe(-1n)
        expect(partition.logStartOffset).toBe(-1n)
        expect(partition.recordErrors).toEqual([])
        expect(partition.errorMessage).toBeNull()
      }
    })

    it("decodes response with error", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(1)
      writer.writeString("test-topic")
      writer.writeInt32(1)
      writer.writeInt32(0)
      writer.writeInt16(3) // UNKNOWN_TOPIC_OR_PARTITION
      writer.writeInt64(-1n)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeProduceResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].partitions[0].errorCode).toBe(3)
        expect(result.value.topics[0].partitions[0].baseOffset).toBe(-1n)
      }
    })

    it("decodes response with multiple topics and partitions", () => {
      const writer = new BinaryWriter()

      // 2 topics
      writer.writeInt32(2)

      // topic-a with 2 partitions
      writer.writeString("topic-a")
      writer.writeInt32(2)
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeInt64(10n)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeInt64(20n)

      // topic-b with 1 partition
      writer.writeString("topic-b")
      writer.writeInt32(1)
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeInt64(30n)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeProduceResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics).toHaveLength(2)
        expect(result.value.topics[0].name).toBe("topic-a")
        expect(result.value.topics[0].partitions).toHaveLength(2)
        expect(result.value.topics[0].partitions[0].baseOffset).toBe(10n)
        expect(result.value.topics[0].partitions[1].baseOffset).toBe(20n)
        expect(result.value.topics[1].name).toBe("topic-b")
        expect(result.value.topics[1].partitions[0].baseOffset).toBe(30n)
      }
    })
  })

  describe("v1+ — adds throttle_time_ms", () => {
    it("decodes throttle time", () => {
      const writer = new BinaryWriter()

      // topics array (empty)
      writer.writeInt32(0)
      // throttle_time_ms
      writer.writeInt32(500)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeProduceResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.throttleTimeMs).toBe(500)
        expect(result.value.topics).toHaveLength(0)
      }
    })
  })

  describe("v2+ — adds log_append_time_ms", () => {
    it("decodes log append time", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(1) // topics
      writer.writeString("test-topic")
      writer.writeInt32(1) // partitions
      writer.writeInt32(0) // partition_index
      writer.writeInt16(0) // error_code
      writer.writeInt64(100n) // base_offset
      writer.writeInt64(1709856000000n) // log_append_time_ms
      writer.writeInt32(0) // throttle_time_ms

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeProduceResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (result.ok) {
        const partition = result.value.topics[0].partitions[0]
        expect(partition.logAppendTimeMs).toBe(1709856000000n)
        expect(partition.logStartOffset).toBe(-1n) // not present in v2
      }
    })
  })

  describe("v5+ — adds log_start_offset", () => {
    it("decodes log start offset", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(1)
      writer.writeString("test-topic")
      writer.writeInt32(1)
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeInt64(100n)
      writer.writeInt64(-1n) // log_append_time_ms
      writer.writeInt64(50n) // log_start_offset
      writer.writeInt32(0) // throttle_time_ms

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeProduceResponse(reader, 5)

      expect(result.ok).toBe(true)
      if (result.ok) {
        const partition = result.value.topics[0].partitions[0]
        expect(partition.logStartOffset).toBe(50n)
      }
    })
  })

  describe("v8+ — adds record_errors and error_message", () => {
    it("decodes empty record errors and null error message", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(1)
      writer.writeString("test-topic")
      writer.writeInt32(1)
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeInt64(100n)
      writer.writeInt64(-1n) // log_append_time_ms
      writer.writeInt64(0n) // log_start_offset
      writer.writeInt32(0) // record_errors array (empty)
      writer.writeString(null) // error_message (null)
      writer.writeInt32(0) // throttle_time_ms

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeProduceResponse(reader, 8)

      expect(result.ok).toBe(true)
      if (result.ok) {
        const partition = result.value.topics[0].partitions[0]
        expect(partition.recordErrors).toEqual([])
        expect(partition.errorMessage).toBeNull()
      }
    })

    it("decodes record errors with messages", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(1)
      writer.writeString("test-topic")
      writer.writeInt32(1)
      writer.writeInt32(0)
      writer.writeInt16(87) // INVALID_RECORD
      writer.writeInt64(-1n)
      writer.writeInt64(-1n)
      writer.writeInt64(-1n)
      // record_errors array
      writer.writeInt32(2)
      // error 1
      writer.writeInt32(0) // batch_index
      writer.writeString("invalid timestamp") // message
      // error 2
      writer.writeInt32(3)
      writer.writeString(null) // null message
      // error_message
      writer.writeString("batch contains invalid records")
      writer.writeInt32(0) // throttle_time_ms

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeProduceResponse(reader, 8)

      expect(result.ok).toBe(true)
      if (result.ok) {
        const partition = result.value.topics[0].partitions[0]
        expect(partition.errorCode).toBe(87)
        expect(partition.recordErrors).toHaveLength(2)
        expect(partition.recordErrors[0].batchIndex).toBe(0)
        expect(partition.recordErrors[0].message).toBe("invalid timestamp")
        expect(partition.recordErrors[1].batchIndex).toBe(3)
        expect(partition.recordErrors[1].message).toBeNull()
        expect(partition.errorMessage).toBe("batch contains invalid records")
      }
    })
  })

  describe("v9+ — flexible encoding", () => {
    it("decodes response with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

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
      // base_offset
      writer.writeInt64(42n)
      // log_append_time_ms
      writer.writeInt64(-1n)
      // log_start_offset
      writer.writeInt64(0n)
      // record_errors (compact array: 0 + 1 = 1)
      writer.writeUnsignedVarInt(1)
      // error_message (compact nullable: 0 = null)
      writer.writeCompactString(null)
      // partition tagged fields
      writer.writeTaggedFields([])
      // topic tagged fields
      writer.writeTaggedFields([])
      // throttle_time_ms
      writer.writeInt32(100)
      // response tagged fields
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeProduceResponse(reader, 9)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.throttleTimeMs).toBe(100)
        expect(result.value.topics).toHaveLength(1)
        expect(result.value.topics[0].name).toBe("test-topic")

        const partition = result.value.topics[0].partitions[0]
        expect(partition.partitionIndex).toBe(0)
        expect(partition.errorCode).toBe(0)
        expect(partition.baseOffset).toBe(42n)
        expect(partition.logAppendTimeMs).toBe(-1n)
        expect(partition.logStartOffset).toBe(0n)
        expect(partition.recordErrors).toEqual([])
        expect(partition.errorMessage).toBeNull()
        expect(result.value.taggedFields).toEqual([])
      }
    })

    it("decodes v9 record errors with compact encoding", () => {
      const writer = new BinaryWriter()

      // topics array (compact)
      writer.writeUnsignedVarInt(2) // 1 topic
      writer.writeCompactString("test-topic")
      // partitions array (compact)
      writer.writeUnsignedVarInt(2) // 1 partition
      writer.writeInt32(0)
      writer.writeInt16(87)
      writer.writeInt64(-1n)
      writer.writeInt64(-1n)
      writer.writeInt64(-1n)
      // record_errors (compact: 1 + 1 = 2)
      writer.writeUnsignedVarInt(2)
      writer.writeInt32(5) // batch_index
      writer.writeCompactString("bad record") // error message
      writer.writeTaggedFields([]) // record error tagged fields
      // partition error_message
      writer.writeCompactString("partial failure")
      // partition tagged fields
      writer.writeTaggedFields([])
      // topic tagged fields
      writer.writeTaggedFields([])
      // throttle_time_ms
      writer.writeInt32(0)
      // response tagged fields
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeProduceResponse(reader, 9)

      expect(result.ok).toBe(true)
      if (result.ok) {
        const partition = result.value.topics[0].partitions[0]
        expect(partition.recordErrors).toHaveLength(1)
        expect(partition.recordErrors[0].batchIndex).toBe(5)
        expect(partition.recordErrors[0].message).toBe("bad record")
        expect(partition.errorMessage).toBe("partial failure")
      }
    })
  })

  describe("empty response", () => {
    it("decodes v0 response with no topics", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeProduceResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics).toHaveLength(0)
        expect(result.value.throttleTimeMs).toBe(0)
      }
    })
  })
})
