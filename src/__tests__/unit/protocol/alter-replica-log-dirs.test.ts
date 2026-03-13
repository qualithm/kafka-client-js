import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  type AlterReplicaLogDirsRequest,
  buildAlterReplicaLogDirsRequest,
  decodeAlterReplicaLogDirsResponse,
  encodeAlterReplicaLogDirsRequest
} from "../../../protocol/alter-replica-log-dirs"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeAlterReplicaLogDirsRequest", () => {
  it("encodes v0 request with dirs", () => {
    const writer = new BinaryWriter()
    const request: AlterReplicaLogDirsRequest = {
      dirs: [
        {
          logDir: "/data/kafka-logs-1",
          topics: [{ topic: "test-topic", partitions: [0, 1] }]
        }
      ]
    }
    encodeAlterReplicaLogDirsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // dirs array (INT32)
    const dirsLen = reader.readInt32()
    expect(dirsLen.ok && dirsLen.value).toBe(1)

    // log_dir
    const logDir = reader.readString()
    expect(logDir.ok && logDir.value).toBe("/data/kafka-logs-1")

    // topics array
    const topicsLen = reader.readInt32()
    expect(topicsLen.ok && topicsLen.value).toBe(1)

    // topic name
    const topicName = reader.readString()
    expect(topicName.ok && topicName.value).toBe("test-topic")

    // partitions array
    const partLen = reader.readInt32()
    expect(partLen.ok && partLen.value).toBe(2)

    const p0 = reader.readInt32()
    expect(p0.ok && p0.value).toBe(0)
    const p1 = reader.readInt32()
    expect(p1.ok && p1.value).toBe(1)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v2 flexible request", () => {
    const writer = new BinaryWriter()
    const request: AlterReplicaLogDirsRequest = {
      dirs: [
        {
          logDir: "/new-dir",
          topics: [{ topic: "my-topic", partitions: [0] }]
        }
      ]
    }
    encodeAlterReplicaLogDirsRequest(writer, request, 2)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // dirs compact array (1 + 1)
    const dirsLen = reader.readUnsignedVarInt()
    expect(dirsLen.ok && dirsLen.value).toBe(2)

    // log_dir compact string
    const logDir = reader.readCompactString()
    expect(logDir.ok && logDir.value).toBe("/new-dir")

    // topics compact array (1 + 1)
    const topicsLen = reader.readUnsignedVarInt()
    expect(topicsLen.ok && topicsLen.value).toBe(2)

    // topic name
    const topicName = reader.readCompactString()
    expect(topicName.ok && topicName.value).toBe("my-topic")

    // partitions compact array (1 + 1)
    const partLen = reader.readUnsignedVarInt()
    expect(partLen.ok && partLen.value).toBe(2)

    const p0 = reader.readInt32()
    expect(p0.ok && p0.value).toBe(0)

    // topic tagged fields
    const topicTag = reader.readTaggedFields()
    expect(topicTag.ok).toBe(true)

    // dir tagged fields
    const dirTag = reader.readTaggedFields()
    expect(dirTag.ok).toBe(true)

    // request tagged fields
    const reqTag = reader.readTaggedFields()
    expect(reqTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 request with multiple dirs", () => {
    const writer = new BinaryWriter()
    const request: AlterReplicaLogDirsRequest = {
      dirs: [
        {
          logDir: "/dir-a",
          topics: [{ topic: "topic-1", partitions: [0] }]
        },
        {
          logDir: "/dir-b",
          topics: [{ topic: "topic-2", partitions: [1, 2] }]
        }
      ]
    }
    encodeAlterReplicaLogDirsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    const dirsLen = reader.readInt32()
    expect(dirsLen.ok && dirsLen.value).toBe(2)

    // dir 0
    const dir0 = reader.readString()
    expect(dir0.ok && dir0.value).toBe("/dir-a")
    const topics0Len = reader.readInt32()
    expect(topics0Len.ok && topics0Len.value).toBe(1)
    const topic0Name = reader.readString()
    expect(topic0Name.ok && topic0Name.value).toBe("topic-1")
    const parts0Len = reader.readInt32()
    expect(parts0Len.ok && parts0Len.value).toBe(1)
    const p00 = reader.readInt32()
    expect(p00.ok && p00.value).toBe(0)

    // dir 1
    const dir1 = reader.readString()
    expect(dir1.ok && dir1.value).toBe("/dir-b")
    const topics1Len = reader.readInt32()
    expect(topics1Len.ok && topics1Len.value).toBe(1)
    const topic1Name = reader.readString()
    expect(topic1Name.ok && topic1Name.value).toBe("topic-2")
    const parts1Len = reader.readInt32()
    expect(parts1Len.ok && parts1Len.value).toBe(2)
    const p10 = reader.readInt32()
    expect(p10.ok && p10.value).toBe(1)
    const p11 = reader.readInt32()
    expect(p11.ok && p11.value).toBe(2)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

describe("buildAlterReplicaLogDirsRequest", () => {
  it("produces a framed request with correct header (v0)", () => {
    const request: AlterReplicaLogDirsRequest = {
      dirs: [{ logDir: "/data", topics: [{ topic: "t", partitions: [0] }] }]
    }
    const frame = buildAlterReplicaLogDirsRequest(99, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.AlterReplicaLogDirs)

    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(0)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(99)
  })

  it("produces a framed request with correct header (v2)", () => {
    const request: AlterReplicaLogDirsRequest = {
      dirs: []
    }
    const frame = buildAlterReplicaLogDirsRequest(5, 2, request, "test-client")
    const reader = new BinaryReader(frame)

    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.AlterReplicaLogDirs)

    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(2)

    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(5)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeAlterReplicaLogDirsResponse", () => {
  it("decodes v0 response with results", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // results array (1 topic)
    w.writeInt32(1)
    w.writeString("test-topic") // topic_name
    // partitions array (2 partitions)
    w.writeInt32(2)
    w.writeInt32(0) // partition_index
    w.writeInt16(0) // error_code (success)
    w.writeInt32(1) // partition_index
    w.writeInt16(0) // error_code (success)

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterReplicaLogDirsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.results).toHaveLength(1)

    const topic = result.value.results[0]
    expect(topic.topicName).toBe("test-topic")
    expect(topic.partitions).toHaveLength(2)
    expect(topic.partitions[0].partitionIndex).toBe(0)
    expect(topic.partitions[0].errorCode).toBe(0)
    expect(topic.partitions[1].partitionIndex).toBe(1)
    expect(topic.partitions[1].errorCode).toBe(0)
  })

  it("decodes v0 response with error codes", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt32(1) // results array
    w.writeString("topic-x") // topic_name
    w.writeInt32(1) // partitions array
    w.writeInt32(0) // partition_index
    w.writeInt16(58) // error_code (KAFKA_STORAGE_ERROR)

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterReplicaLogDirsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.results[0].partitions[0].errorCode).toBe(58)
  })

  it("decodes v2 flexible response", () => {
    const w = new BinaryWriter()
    w.writeInt32(25) // throttle_time_ms
    // results compact array (1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeCompactString("flex-topic") // topic_name
    // partitions compact array (1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeInt32(5) // partition_index
    w.writeInt16(0) // error_code
    w.writeTaggedFields([]) // partition tagged fields
    w.writeTaggedFields([]) // topic tagged fields

    // response tagged fields
    w.writeTaggedFields([])

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterReplicaLogDirsResponse(reader, 2)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(25)
    const topic = result.value.results[0]
    expect(topic.topicName).toBe("flex-topic")
    expect(topic.partitions[0].partitionIndex).toBe(5)
    expect(topic.partitions[0].errorCode).toBe(0)
  })

  it("decodes v0 response with empty results", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt32(0) // empty results array

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterReplicaLogDirsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.results).toEqual([])
  })

  it("handles truncated response gracefully", () => {
    const buf = new Uint8Array([0x00, 0x00]) // too short
    const reader = new BinaryReader(buf)
    const result = decodeAlterReplicaLogDirsResponse(reader, 0)

    expect(result.ok).toBe(false)
  })

  it("decodes v0 response with multiple topics", () => {
    const w = new BinaryWriter()
    w.writeInt32(100) // throttle_time_ms
    w.writeInt32(2) // 2 topics

    // topic 0
    w.writeString("topic-a")
    w.writeInt32(1) // 1 partition
    w.writeInt32(0) // partition_index
    w.writeInt16(0) // error_code

    // topic 1
    w.writeString("topic-b")
    w.writeInt32(2) // 2 partitions
    w.writeInt32(0) // partition_index
    w.writeInt16(0) // error_code
    w.writeInt32(1) // partition_index
    w.writeInt16(58) // error_code

    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterReplicaLogDirsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(100)
    expect(result.value.results).toHaveLength(2)
    expect(result.value.results[0].topicName).toBe("topic-a")
    expect(result.value.results[1].topicName).toBe("topic-b")
    expect(result.value.results[1].partitions[1].errorCode).toBe(58)
  })
})
