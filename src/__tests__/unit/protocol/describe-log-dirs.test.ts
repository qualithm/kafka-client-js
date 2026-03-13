import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDescribeLogDirsRequest,
  decodeDescribeLogDirsResponse,
  type DescribeLogDirsRequest,
  encodeDescribeLogDirsRequest
} from "../../../protocol/describe-log-dirs"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeLogDirsRequest", () => {
  it("encodes v0 request with topics", () => {
    const writer = new BinaryWriter()
    const request: DescribeLogDirsRequest = {
      topics: [{ topic: "test-topic", partitions: [0, 1, 2] }]
    }
    encodeDescribeLogDirsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // topics array (non-flexible: INT32 length)
    const len = reader.readInt32()
    expect(len.ok && len.value).toBe(1)

    // topic name
    const name = reader.readString()
    expect(name.ok && name.value).toBe("test-topic")

    // partitions array
    const partLen = reader.readInt32()
    expect(partLen.ok && partLen.value).toBe(3)

    const p0 = reader.readInt32()
    expect(p0.ok && p0.value).toBe(0)
    const p1 = reader.readInt32()
    expect(p1.ok && p1.value).toBe(1)
    const p2 = reader.readInt32()
    expect(p2.ok && p2.value).toBe(2)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 request with null topics (describe all)", () => {
    const writer = new BinaryWriter()
    const request: DescribeLogDirsRequest = { topics: null }
    encodeDescribeLogDirsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    const len = reader.readInt32()
    expect(len.ok && len.value).toBe(-1)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v2 flexible request with topics", () => {
    const writer = new BinaryWriter()
    const request: DescribeLogDirsRequest = {
      topics: [{ topic: "my-topic", partitions: [0] }]
    }
    encodeDescribeLogDirsRequest(writer, request, 2)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // compact array (length + 1)
    const len = reader.readUnsignedVarInt()
    expect(len.ok && len.value).toBe(2) // 1 + 1

    // topic name (compact string)
    const name = reader.readCompactString()
    expect(name.ok && name.value).toBe("my-topic")

    // partitions compact array
    const partLen = reader.readUnsignedVarInt()
    expect(partLen.ok && partLen.value).toBe(2) // 1 + 1

    const p0 = reader.readInt32()
    expect(p0.ok && p0.value).toBe(0)

    // topic tagged fields
    const topicTag = reader.readTaggedFields()
    expect(topicTag.ok).toBe(true)

    // request tagged fields
    const reqTag = reader.readTaggedFields()
    expect(reqTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v2 flexible request with null topics", () => {
    const writer = new BinaryWriter()
    const request: DescribeLogDirsRequest = { topics: null }
    encodeDescribeLogDirsRequest(writer, request, 2)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // null compact array
    const len = reader.readUnsignedVarInt()
    expect(len.ok && len.value).toBe(0)

    // request tagged fields
    const reqTag = reader.readTaggedFields()
    expect(reqTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 request with empty topics array", () => {
    const writer = new BinaryWriter()
    const request: DescribeLogDirsRequest = { topics: [] }
    encodeDescribeLogDirsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    const len = reader.readInt32()
    expect(len.ok && len.value).toBe(0)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

describe("buildDescribeLogDirsRequest", () => {
  it("produces a framed request with correct header (v0)", () => {
    const request: DescribeLogDirsRequest = {
      topics: [{ topic: "test", partitions: [0] }]
    }
    const frame = buildDescribeLogDirsRequest(42, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    // size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    // API key
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.DescribeLogDirs)

    // API version
    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(0)

    // correlation id
    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(42)
  })

  it("produces a framed request with correct header (v2)", () => {
    const request: DescribeLogDirsRequest = { topics: null }
    const frame = buildDescribeLogDirsRequest(7, 2, request, "test-client")
    const reader = new BinaryReader(frame)

    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.DescribeLogDirs)

    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(2)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(7)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeLogDirsResponse", () => {
  it("decodes v0 response with log dirs", () => {
    const w = new BinaryWriter()
    // throttle_time_ms
    w.writeInt32(0)
    // results array (1 log dir)
    w.writeInt32(1)

    // result 0
    w.writeInt16(0) // error_code
    w.writeString("/var/kafka/data") // log_dir
    // topics array (1 topic)
    w.writeInt32(1)
    w.writeString("test-topic") // name
    // partitions array (1 partition)
    w.writeInt32(1)
    w.writeInt32(0) // partition_index
    w.writeInt64(BigInt(1048576)) // partition_size
    w.writeInt64(BigInt(100)) // offset_lag

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeLogDirsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.results).toHaveLength(1)

    const dir = result.value.results[0]
    expect(dir.errorCode).toBe(0)
    expect(dir.logDir).toBe("/var/kafka/data")
    expect(dir.topics).toHaveLength(1)

    const topic = dir.topics[0]
    expect(topic.name).toBe("test-topic")
    expect(topic.partitions).toHaveLength(1)

    const partition = topic.partitions[0]
    expect(partition.partitionIndex).toBe(0)
    expect(partition.partitionSize).toBe(BigInt(1048576))
    expect(partition.offsetLag).toBe(BigInt(100))
    expect(partition.isFutureKey).toBe(false) // v0 does not have is_future_key
  })

  it("decodes v1 response with is_future_key", () => {
    const w = new BinaryWriter()
    w.writeInt32(10) // throttle_time_ms
    w.writeInt32(1) // results array

    w.writeInt16(0) // error_code
    w.writeString("/data/kafka") // log_dir
    w.writeInt32(1) // topics array
    w.writeString("topic-a") // name
    w.writeInt32(1) // partitions array
    w.writeInt32(3) // partition_index
    w.writeInt64(BigInt(2048)) // partition_size
    w.writeInt64(BigInt(0)) // offset_lag
    w.writeBoolean(true) // is_future_key (v1+)

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeLogDirsResponse(reader, 1)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(10)
    const partition = result.value.results[0].topics[0].partitions[0]
    expect(partition.partitionIndex).toBe(3)
    expect(partition.isFutureKey).toBe(true)
  })

  it("decodes v2 flexible response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // results compact array (1 + 1)
    w.writeUnsignedVarInt(2)

    w.writeInt16(0) // error_code
    w.writeCompactString("/kafka-data") // log_dir
    // topics compact array (1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeCompactString("flex-topic") // name
    // partitions compact array (1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeInt32(0) // partition_index
    w.writeInt64(BigInt(512)) // partition_size
    w.writeInt64(BigInt(5)) // offset_lag
    w.writeBoolean(false) // is_future_key
    w.writeTaggedFields([]) // partition tagged fields
    w.writeTaggedFields([]) // topic tagged fields
    w.writeTaggedFields([]) // result tagged fields

    // response tagged fields
    w.writeTaggedFields([])

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeLogDirsResponse(reader, 2)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    const dir = result.value.results[0]
    expect(dir.logDir).toBe("/kafka-data")
    expect(dir.topics[0].name).toBe("flex-topic")
    expect(dir.topics[0].partitions[0].partitionSize).toBe(BigInt(512))
  })

  it("decodes v3 response with total/usable bytes per log dir", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // results compact array (1 + 1)
    w.writeUnsignedVarInt(2)

    w.writeInt16(0) // error_code
    w.writeCompactString("/data") // log_dir
    // topics compact array (empty)
    w.writeUnsignedVarInt(1) // 0 + 1
    // total_bytes (v3+)
    w.writeInt64(BigInt(107374182400))
    // usable_bytes (v3+)
    w.writeInt64(BigInt(53687091200))
    w.writeTaggedFields([]) // result tagged fields

    // response tagged fields
    w.writeTaggedFields([])

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeLogDirsResponse(reader, 3)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    const dir = result.value.results[0]
    expect(dir.totalBytes).toBe(BigInt(107374182400))
    expect(dir.usableBytes).toBe(BigInt(53687091200))
  })

  it("decodes v4 response with per-partition total/usable bytes", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // results compact array (1 + 1)
    w.writeUnsignedVarInt(2)

    w.writeInt16(0) // error_code
    w.writeCompactString("/data") // log_dir
    // topics compact array (1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeCompactString("topic-x") // name
    // partitions compact array (1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeInt32(0) // partition_index
    w.writeInt64(BigInt(4096)) // partition_size
    w.writeInt64(BigInt(10)) // offset_lag
    w.writeBoolean(false) // is_future_key
    w.writeInt64(BigInt(1000000)) // total_bytes (v4+)
    w.writeInt64(BigInt(500000)) // usable_bytes (v4+)
    w.writeTaggedFields([]) // partition tagged fields
    w.writeTaggedFields([]) // topic tagged fields
    // total_bytes per log dir (v3+)
    w.writeInt64(BigInt(107374182400))
    // usable_bytes per log dir (v3+)
    w.writeInt64(BigInt(53687091200))
    w.writeTaggedFields([]) // result tagged fields

    // response tagged fields
    w.writeTaggedFields([])

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeLogDirsResponse(reader, 4)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    const partition = result.value.results[0].topics[0].partitions[0]
    expect(partition.totalBytes).toBe(BigInt(1000000))
    expect(partition.usableBytes).toBe(BigInt(500000))

    const dir = result.value.results[0]
    expect(dir.totalBytes).toBe(BigInt(107374182400))
    expect(dir.usableBytes).toBe(BigInt(53687091200))
  })

  it("decodes v0 response with empty results", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt32(0) // empty results array

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeLogDirsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.results).toEqual([])
  })

  it("decodes v0 response with error code per log dir", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt32(1) // results array

    w.writeInt16(58) // error_code (KAFKA_STORAGE_ERROR)
    w.writeString("/failed-disk") // log_dir
    w.writeInt32(0) // empty topics

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeLogDirsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.results[0].errorCode).toBe(58)
    expect(result.value.results[0].logDir).toBe("/failed-disk")
    expect(result.value.results[0].topics).toEqual([])
  })

  it("handles truncated response gracefully", () => {
    const buf = new Uint8Array([0x00, 0x00]) // too short
    const reader = new BinaryReader(buf)
    const result = decodeDescribeLogDirsResponse(reader, 0)

    expect(result.ok).toBe(false)
  })

  it("decodes v0 response with multiple log dirs and topics", () => {
    const w = new BinaryWriter()
    w.writeInt32(50) // throttle_time_ms
    w.writeInt32(2) // 2 log dirs

    // log dir 0
    w.writeInt16(0) // error_code
    w.writeString("/data1") // log_dir
    w.writeInt32(1) // 1 topic
    w.writeString("topic-a") // name
    w.writeInt32(2) // 2 partitions
    w.writeInt32(0) // partition_index
    w.writeInt64(BigInt(1000)) // partition_size
    w.writeInt64(BigInt(0)) // offset_lag
    w.writeInt32(1) // partition_index
    w.writeInt64(BigInt(2000)) // partition_size
    w.writeInt64(BigInt(5)) // offset_lag

    // log dir 1
    w.writeInt16(0) // error_code
    w.writeString("/data2") // log_dir
    w.writeInt32(1) // 1 topic
    w.writeString("topic-b") // name
    w.writeInt32(1) // 1 partition
    w.writeInt32(0) // partition_index
    w.writeInt64(BigInt(500)) // partition_size
    w.writeInt64(BigInt(10)) // offset_lag

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeDescribeLogDirsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(50)
    expect(result.value.results).toHaveLength(2)

    expect(result.value.results[0].logDir).toBe("/data1")
    expect(result.value.results[0].topics[0].partitions).toHaveLength(2)
    expect(result.value.results[0].topics[0].partitions[1].partitionSize).toBe(BigInt(2000))

    expect(result.value.results[1].logDir).toBe("/data2")
    expect(result.value.results[1].topics[0].name).toBe("topic-b")
  })
})
