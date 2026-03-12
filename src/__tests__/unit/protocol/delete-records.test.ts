import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDeleteRecordsRequest,
  decodeDeleteRecordsResponse,
  type DeleteRecordsRequest,
  encodeDeleteRecordsRequest
} from "../../../protocol/delete-records"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDeleteRecordsRequest", () => {
  describe("v0–v1 — non-flexible encoding", () => {
    it("encodes topics with partitions and offsets", () => {
      const writer = new BinaryWriter()
      const request: DeleteRecordsRequest = {
        topics: [
          {
            name: "topic-a",
            partitions: [
              { partitionIndex: 0, offset: 100n },
              { partitionIndex: 1, offset: 200n }
            ]
          }
        ],
        timeoutMs: 30000
      }
      encodeDeleteRecordsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // topics array length (INT32)
      const topicsLen = reader.readInt32()
      expect(topicsLen.ok && topicsLen.value).toBe(1)

      // topic name
      const name = reader.readString()
      expect(name.ok && name.value).toBe("topic-a")

      // partitions array length (INT32)
      const partsLen = reader.readInt32()
      expect(partsLen.ok && partsLen.value).toBe(2)

      // partition 0
      const p0Idx = reader.readInt32()
      expect(p0Idx.ok && p0Idx.value).toBe(0)
      const p0Offset = reader.readInt64()
      expect(p0Offset.ok && p0Offset.value).toBe(100n)

      // partition 1
      const p1Idx = reader.readInt32()
      expect(p1Idx.ok && p1Idx.value).toBe(1)
      const p1Offset = reader.readInt64()
      expect(p1Offset.ok && p1Offset.value).toBe(200n)

      // timeout_ms
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(30000)

      expect(reader.remaining).toBe(0)
    })

    it("encodes empty topics list", () => {
      const writer = new BinaryWriter()
      const request: DeleteRecordsRequest = {
        topics: [],
        timeoutMs: 5000
      }
      encodeDeleteRecordsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const topicsLen = reader.readInt32()
      expect(topicsLen.ok && topicsLen.value).toBe(0)

      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(5000)

      expect(reader.remaining).toBe(0)
    })

    it("encodes offset -1 for high watermark deletion", () => {
      const writer = new BinaryWriter()
      const request: DeleteRecordsRequest = {
        topics: [
          {
            name: "topic-b",
            partitions: [{ partitionIndex: 0, offset: -1n }]
          }
        ],
        timeoutMs: 10000
      }
      encodeDeleteRecordsRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const topicsLen = reader.readInt32()
      expect(topicsLen.ok && topicsLen.value).toBe(1)

      const name = reader.readString()
      expect(name.ok && name.value).toBe("topic-b")

      const partsLen = reader.readInt32()
      expect(partsLen.ok && partsLen.value).toBe(1)

      const pIdx = reader.readInt32()
      expect(pIdx.ok && pIdx.value).toBe(0)
      const pOffset = reader.readInt64()
      expect(pOffset.ok && pOffset.value).toBe(-1n)

      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(10000)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: DeleteRecordsRequest = {
        topics: [
          {
            name: "flex-topic",
            partitions: [{ partitionIndex: 0, offset: 50n }]
          }
        ],
        timeoutMs: 15000
      }
      encodeDeleteRecordsRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // topics array (compact: length + 1)
      const topicsLen = reader.readUnsignedVarInt()
      expect(topicsLen.ok && topicsLen.value).toBe(2)

      // topic name (compact string)
      const name = reader.readCompactString()
      expect(name.ok && name.value).toBe("flex-topic")

      // partitions array (compact)
      const partsLen = reader.readUnsignedVarInt()
      expect(partsLen.ok && partsLen.value).toBe(2)

      // partition 0
      const pIdx = reader.readInt32()
      expect(pIdx.ok && pIdx.value).toBe(0)
      const pOffset = reader.readInt64()
      expect(pOffset.ok && pOffset.value).toBe(50n)

      // partition tagged fields
      const pTag = reader.readTaggedFields()
      expect(pTag.ok).toBe(true)

      // topic tagged fields
      const tTag = reader.readTaggedFields()
      expect(tTag.ok).toBe(true)

      // timeout_ms
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(15000)

      // request tagged fields
      const rTag = reader.readTaggedFields()
      expect(rTag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

describe("buildDeleteRecordsRequest", () => {
  it("produces a framed request with correct header", () => {
    const request: DeleteRecordsRequest = {
      topics: [
        {
          name: "test-topic",
          partitions: [{ partitionIndex: 0, offset: 10n }]
        }
      ],
      timeoutMs: 5000
    }
    const frame = buildDeleteRecordsRequest(42, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    // size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    // API key
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.DeleteRecords)

    // API version
    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(0)

    // correlation id
    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(42)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDeleteRecordsResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes throttle, topics and partition results", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(50)
      // topics array (INT32)
      writer.writeInt32(1)
      // topic name
      writer.writeString("topic-a")
      // partitions array (INT32)
      writer.writeInt32(2)
      // partition 0: index, low_watermark, error_code
      writer.writeInt32(0)
      writer.writeInt64(100n)
      writer.writeInt16(0)
      // partition 1: index, low_watermark, error_code
      writer.writeInt32(1)
      writer.writeInt64(200n)
      writer.writeInt16(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDeleteRecordsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("topic-a")
      expect(result.value.topics[0].partitions).toHaveLength(2)
      expect(result.value.topics[0].partitions[0].partitionIndex).toBe(0)
      expect(result.value.topics[0].partitions[0].lowWatermark).toBe(100n)
      expect(result.value.topics[0].partitions[0].errorCode).toBe(0)
      expect(result.value.topics[0].partitions[1].partitionIndex).toBe(1)
      expect(result.value.topics[0].partitions[1].lowWatermark).toBe(200n)
      expect(result.value.topics[0].partitions[1].errorCode).toBe(0)
    })

    it("decodes error codes", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle
      writer.writeInt32(1) // 1 topic
      writer.writeString("topic-err")
      writer.writeInt32(1) // 1 partition
      writer.writeInt32(0) // partition index
      writer.writeInt64(-1n) // low_watermark
      writer.writeInt16(39) // REPLICA_NOT_AVAILABLE

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDeleteRecordsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].partitions[0].errorCode).toBe(39)
      expect(result.value.topics[0].partitions[0].lowWatermark).toBe(-1n)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(10)
      // topics array (compact: length + 1)
      writer.writeUnsignedVarInt(2)
      // topic name (compact)
      writer.writeCompactString("flex-topic")
      // partitions array (compact)
      writer.writeUnsignedVarInt(2)
      // partition 0
      writer.writeInt32(0)
      writer.writeInt64(500n)
      writer.writeInt16(0)
      writer.writeTaggedFields([]) // partition tagged fields
      // partition tagged fields
      writer.writeTaggedFields([]) // topic tagged fields
      // request tagged fields
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDeleteRecordsResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(10)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("flex-topic")
      expect(result.value.topics[0].partitions[0].partitionIndex).toBe(0)
      expect(result.value.topics[0].partitions[0].lowWatermark).toBe(500n)
      expect(result.value.topics[0].partitions[0].errorCode).toBe(0)
    })
  })

  it("returns failure on truncated input", () => {
    const reader = new BinaryReader(new Uint8Array([0, 0]))
    const result = decodeDeleteRecordsResponse(reader, 0)
    expect(result.ok).toBe(false)
  })
})
