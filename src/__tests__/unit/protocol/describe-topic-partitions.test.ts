import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDescribeTopicPartitionsRequest,
  decodeDescribeTopicPartitionsResponse,
  type DescribeTopicPartitionsRequest,
  encodeDescribeTopicPartitionsRequest
} from "../../../protocol/describe-topic-partitions"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeTopicPartitionsRequest", () => {
  it("encodes v0 request with topics and no cursor", () => {
    const writer = new BinaryWriter()
    const request: DescribeTopicPartitionsRequest = {
      topics: [{ name: "topic-a" }, { name: "topic-b" }],
      responsePartitionLimit: 100
    }
    encodeDescribeTopicPartitionsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // topics compact array (2 + 1 = 3)
    const topicCount = reader.readUnsignedVarInt()
    expect(topicCount.ok && topicCount.value).toBe(3)

    // topic 1
    const name1 = reader.readCompactString()
    expect(name1.ok && name1.value).toBe("topic-a")
    reader.readTaggedFields() // topic tagged

    // topic 2
    const name2 = reader.readCompactString()
    expect(name2.ok && name2.value).toBe("topic-b")
    reader.readTaggedFields() // topic tagged

    // response_partition_limit
    const limit = reader.readInt32()
    expect(limit.ok && limit.value).toBe(100)

    // cursor: null (-1)
    const cursor = reader.readInt8()
    expect(cursor.ok && cursor.value).toBe(-1)

    // tagged fields
    const tag = reader.readTaggedFields()
    expect(tag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 request with cursor", () => {
    const writer = new BinaryWriter()
    const request: DescribeTopicPartitionsRequest = {
      topics: [{ name: "topic-a" }],
      responsePartitionLimit: 50,
      cursor: { topicName: "topic-a", partitionIndex: 10 }
    }
    encodeDescribeTopicPartitionsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // topics
    reader.readUnsignedVarInt()
    reader.readCompactString()
    reader.readTaggedFields()

    // response_partition_limit
    reader.readInt32()

    // cursor: non-null (0)
    const indicator = reader.readInt8()
    expect(indicator.ok && indicator.value).toBe(0)

    const cursorTopic = reader.readCompactString()
    expect(cursorTopic.ok && cursorTopic.value).toBe("topic-a")

    const cursorPart = reader.readInt32()
    expect(cursorPart.ok && cursorPart.value).toBe(10)

    // cursor tagged
    reader.readTaggedFields()

    // request tagged
    const tag = reader.readTaggedFields()
    expect(tag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// buildDescribeTopicPartitionsRequest (framed)
// ---------------------------------------------------------------------------

describe("buildDescribeTopicPartitionsRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildDescribeTopicPartitionsRequest(
      1,
      0,
      { topics: [{ name: "t" }], responsePartitionLimit: 100 },
      "test-client"
    )
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.DescribeTopicPartitions)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeTopicPartitionsResponse", () => {
  it("decodes v0 response with topic partitions", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // topics (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // topic
    w.writeInt16(0) // error_code
    w.writeCompactString("test-topic") // name
    // topic_id (UUID = 16 bytes)
    const uuid = new Uint8Array(16)
    uuid[0] = 0x01
    uuid[15] = 0xff
    w.writeRawBytes(uuid)
    w.writeBoolean(false) // is_internal
    // partitions (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // partition
    w.writeInt16(0) // error_code
    w.writeInt32(0) // partition_index
    w.writeInt32(1) // leader_id
    w.writeInt32(5) // leader_epoch
    // replica_nodes (compact: 2 + 1)
    w.writeUnsignedVarInt(3)
    w.writeInt32(1)
    w.writeInt32(2)
    // isr_nodes (compact: 2 + 1)
    w.writeUnsignedVarInt(3)
    w.writeInt32(1)
    w.writeInt32(2)
    // eligible_leader_replicas (compact: 0 + 1)
    w.writeUnsignedVarInt(1)
    // last_known_elr (compact: 0 + 1)
    w.writeUnsignedVarInt(1)
    // offline_replicas (compact: 0 + 1)
    w.writeUnsignedVarInt(1)
    w.writeTaggedFields([]) // partition tagged
    // end partitions
    w.writeInt32(0) // topic_authorized_operations
    w.writeTaggedFields([]) // topic tagged
    // end topics
    // next_cursor: null
    w.writeInt8(-1)
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeTopicPartitionsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.topics).toHaveLength(1)

    const topic = result.value.topics[0]
    expect(topic.errorCode).toBe(0)
    expect(topic.name).toBe("test-topic")
    expect(topic.topicId[0]).toBe(0x01)
    expect(topic.topicId[15]).toBe(0xff)
    expect(topic.isInternal).toBe(false)
    expect(topic.partitions).toHaveLength(1)

    const part = topic.partitions[0]
    expect(part.errorCode).toBe(0)
    expect(part.partitionIndex).toBe(0)
    expect(part.leaderId).toBe(1)
    expect(part.leaderEpoch).toBe(5)
    expect(part.replicaNodes).toEqual([1, 2])
    expect(part.isrNodes).toEqual([1, 2])
    expect(part.eligibleLeaderReplicas).toHaveLength(0)
    expect(part.lastKnownElr).toHaveLength(0)
    expect(part.offlineReplicas).toHaveLength(0)

    expect(result.value.nextCursor).toBeNull()
  })

  it("decodes v0 response with next cursor", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // topics (compact: 0 + 1)
    w.writeUnsignedVarInt(1)
    // next_cursor: non-null
    w.writeInt8(0) // indicator
    w.writeCompactString("topic-a") // cursor topic
    w.writeInt32(5) // cursor partition
    w.writeTaggedFields([]) // cursor tagged
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeTopicPartitionsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.topics).toHaveLength(0)
    expect(result.value.nextCursor).not.toBeNull()
    expect(result.value.nextCursor?.topicName).toBe("topic-a")
    expect(result.value.nextCursor?.partitionIndex).toBe(5)
  })
})
