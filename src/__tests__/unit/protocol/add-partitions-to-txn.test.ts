import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  type AddPartitionsToTxnRequest,
  buildAddPartitionsToTxnRequest,
  decodeAddPartitionsToTxnResponse,
  encodeAddPartitionsToTxnRequest
} from "../../../protocol/add-partitions-to-txn"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeAddPartitionsToTxnRequest", () => {
  describe("v0 — non-flexible encoding", () => {
    it("encodes transactional_id, producer_id, epoch, and topics", () => {
      const writer = new BinaryWriter()
      const request: AddPartitionsToTxnRequest = {
        transactionalId: "my-txn",
        producerId: 42n,
        producerEpoch: 1,
        topics: [
          { name: "topic-a", partitions: [0, 1, 2] },
          { name: "topic-b", partitions: [0] }
        ]
      }
      encodeAddPartitionsToTxnRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id
      const tidResult = reader.readString()
      expect(tidResult.ok).toBe(true)
      expect(tidResult.ok && tidResult.value).toBe("my-txn")

      // producer_id
      const pidResult = reader.readInt64()
      expect(pidResult.ok).toBe(true)
      expect(pidResult.ok && pidResult.value).toBe(42n)

      // producer_epoch
      const epochResult = reader.readInt16()
      expect(epochResult.ok).toBe(true)
      expect(epochResult.ok && epochResult.value).toBe(1)

      // topics array length
      const topicCountResult = reader.readInt32()
      expect(topicCountResult.ok).toBe(true)
      expect(topicCountResult.ok && topicCountResult.value).toBe(2)

      // topic-a
      const nameAResult = reader.readString()
      expect(nameAResult.ok).toBe(true)
      expect(nameAResult.ok && nameAResult.value).toBe("topic-a")

      const partCountAResult = reader.readInt32()
      expect(partCountAResult.ok).toBe(true)
      expect(partCountAResult.ok && partCountAResult.value).toBe(3)

      for (const expected of [0, 1, 2]) {
        const partResult = reader.readInt32()
        expect(partResult.ok).toBe(true)
        expect(partResult.ok && partResult.value).toBe(expected)
      }

      // topic-b
      const nameBResult = reader.readString()
      expect(nameBResult.ok).toBe(true)
      expect(nameBResult.ok && nameBResult.value).toBe("topic-b")

      const partCountBResult = reader.readInt32()
      expect(partCountBResult.ok).toBe(true)
      expect(partCountBResult.ok && partCountBResult.value).toBe(1)

      const part0Result = reader.readInt32()
      expect(part0Result.ok).toBe(true)
      expect(part0Result.ok && part0Result.value).toBe(0)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v3 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: AddPartitionsToTxnRequest = {
        transactionalId: "my-txn",
        producerId: 100n,
        producerEpoch: 3,
        topics: [{ name: "topic-x", partitions: [5] }]
      }
      encodeAddPartitionsToTxnRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id (compact)
      const tidResult = reader.readCompactString()
      expect(tidResult.ok).toBe(true)
      expect(tidResult.ok && tidResult.value).toBe("my-txn")

      // producer_id
      const pidResult = reader.readInt64()
      expect(pidResult.ok).toBe(true)
      expect(pidResult.ok && pidResult.value).toBe(100n)

      // producer_epoch
      const epochResult = reader.readInt16()
      expect(epochResult.ok).toBe(true)
      expect(epochResult.ok && epochResult.value).toBe(3)

      // topics array (compact: length + 1)
      const topicCountResult = reader.readUnsignedVarInt()
      expect(topicCountResult.ok).toBe(true)
      expect(topicCountResult.ok && topicCountResult.value).toBe(2) // 1 + 1

      // topic name (compact)
      const nameResult = reader.readCompactString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("topic-x")

      // partitions array (compact: length + 1)
      const partCountResult = reader.readUnsignedVarInt()
      expect(partCountResult.ok).toBe(true)
      expect(partCountResult.ok && partCountResult.value).toBe(2) // 1 + 1

      const partResult = reader.readInt32()
      expect(partResult.ok).toBe(true)
      expect(partResult.ok && partResult.value).toBe(5)

      // tagged fields (topic level)
      const topicTagResult = reader.readTaggedFields()
      expect(topicTagResult.ok).toBe(true)

      // tagged fields (top level)
      const topTagResult = reader.readTaggedFields()
      expect(topTagResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// Build (framed request)
// ---------------------------------------------------------------------------

describe("buildAddPartitionsToTxnRequest", () => {
  it("produces a size-prefixed frame with correct API key", () => {
    const request: AddPartitionsToTxnRequest = {
      transactionalId: "txn-1",
      producerId: 1n,
      producerEpoch: 0,
      topics: [{ name: "t", partitions: [0] }]
    }
    const frame = buildAddPartitionsToTxnRequest(1, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    // size prefix
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.AddPartitionsToTxn)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeAddPartitionsToTxnResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes throttle_time_ms and per-topic/partition results", () => {
      const writer = new BinaryWriter()
      // throttle_time_ms
      writer.writeInt32(50)
      // topics array length
      writer.writeInt32(1)
      // topic name
      writer.writeString("topic-a")
      // partitions array length
      writer.writeInt32(2)
      // partition 0: index, error_code
      writer.writeInt32(0)
      writer.writeInt16(0)
      // partition 1: index, error_code
      writer.writeInt32(1)
      writer.writeInt16(3) // UNKNOWN_TOPIC_OR_PARTITION

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeAddPartitionsToTxnResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("topic-a")
      expect(result.value.topics[0].partitions).toHaveLength(2)
      expect(result.value.topics[0].partitions[0]).toEqual({
        partitionIndex: 0,
        partitionErrorCode: 0
      })
      expect(result.value.topics[0].partitions[1]).toEqual({
        partitionIndex: 1,
        partitionErrorCode: 3
      })
    })
  })

  describe("v3 — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      // throttle_time_ms
      writer.writeInt32(0)
      // topics array (compact: length + 1)
      writer.writeUnsignedVarInt(2) // 1 topic + 1
      // topic name (compact)
      writer.writeCompactString("topic-b")
      // partitions array (compact)
      writer.writeUnsignedVarInt(2) // 1 partition + 1
      // partition: index, error_code
      writer.writeInt32(0)
      writer.writeInt16(0)
      // tagged fields (partition level)
      writer.writeTaggedFields([])
      // tagged fields (topic level)
      writer.writeTaggedFields([])
      // tagged fields (top level)
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeAddPartitionsToTxnResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("topic-b")
      expect(result.value.topics[0].partitions[0].partitionErrorCode).toBe(0)
      expect(result.value.taggedFields).toEqual([])
    })
  })
})
