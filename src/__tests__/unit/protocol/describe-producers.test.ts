import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDescribeProducersRequest,
  decodeDescribeProducersResponse,
  type DescribeProducersRequest,
  encodeDescribeProducersRequest
} from "../../../protocol/describe-producers"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeProducersRequest", () => {
  it("encodes v0 request with topics", () => {
    const writer = new BinaryWriter()
    const request: DescribeProducersRequest = {
      topics: [{ name: "test-topic", partitionIndexes: [0, 1] }]
    }
    encodeDescribeProducersRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // topics compact array (1 + 1 = 2)
    const topicCount = reader.readUnsignedVarInt()
    expect(topicCount.ok && topicCount.value).toBe(2)

    // topic name
    const name = reader.readCompactString()
    expect(name.ok && name.value).toBe("test-topic")

    // partition_indexes compact array (2 + 1 = 3)
    const partCount = reader.readUnsignedVarInt()
    expect(partCount.ok && partCount.value).toBe(3)

    const p0 = reader.readInt32()
    expect(p0.ok && p0.value).toBe(0)
    const p1 = reader.readInt32()
    expect(p1.ok && p1.value).toBe(1)

    // topic tagged fields
    const topicTag = reader.readTaggedFields()
    expect(topicTag.ok).toBe(true)

    // request tagged fields
    const reqTag = reader.readTaggedFields()
    expect(reqTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// buildDescribeProducersRequest (framed)
// ---------------------------------------------------------------------------

describe("buildDescribeProducersRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildDescribeProducersRequest(
      1,
      0,
      { topics: [{ name: "t", partitionIndexes: [0] }] },
      "test-client"
    )
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.DescribeProducers)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeProducersResponse", () => {
  it("decodes v0 response with active producers", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // topics array (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // topic
    w.writeCompactString("test-topic")
    // partitions array (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // partition
    w.writeInt32(0) // partition_index
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    // active_producers array (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // producer
    w.writeInt64(1000n) // producer_id
    w.writeInt32(5) // producer_epoch
    w.writeInt32(42) // last_sequence
    w.writeInt64(1234567890000n) // last_timestamp
    w.writeInt32(0) // coordinator_epoch
    w.writeInt64(-1n) // current_txn_start_offset
    w.writeTaggedFields([]) // producer tagged fields
    // end active_producers
    w.writeTaggedFields([]) // partition tagged fields
    // end partitions
    w.writeTaggedFields([]) // topic tagged fields
    // end topics
    w.writeTaggedFields([]) // response tagged fields

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeProducersResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.topics).toHaveLength(1)

    const topic = result.value.topics[0]
    expect(topic.name).toBe("test-topic")
    expect(topic.partitions).toHaveLength(1)

    const partition = topic.partitions[0]
    expect(partition.partitionIndex).toBe(0)
    expect(partition.errorCode).toBe(0)
    expect(partition.activeProducers).toHaveLength(1)

    const producer = partition.activeProducers[0]
    expect(producer.producerId).toBe(1000n)
    expect(producer.producerEpoch).toBe(5)
    expect(producer.lastSequence).toBe(42)
    expect(producer.lastTimestamp).toBe(1234567890000n)
    expect(producer.coordinatorEpoch).toBe(0)
    expect(producer.currentTxnStartOffset).toBe(-1n)
  })

  it("decodes response with no producers", () => {
    const w = new BinaryWriter()
    w.writeInt32(5) // throttle_time_ms
    w.writeUnsignedVarInt(2) // 1 topic + 1
    w.writeCompactString("empty-topic")
    w.writeUnsignedVarInt(2) // 1 partition + 1
    w.writeInt32(0) // partition_index
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeUnsignedVarInt(1) // 0 producers + 1
    w.writeTaggedFields([]) // partition tagged
    w.writeTaggedFields([]) // topic tagged
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeProducersResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(5)
    expect(result.value.topics[0].partitions[0].activeProducers).toHaveLength(0)
  })
})
