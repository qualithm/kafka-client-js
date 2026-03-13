import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDescribeQuorumRequest,
  decodeDescribeQuorumResponse,
  type DescribeQuorumRequest,
  encodeDescribeQuorumRequest
} from "../../../protocol/describe-quorum"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeQuorumRequest", () => {
  it("encodes v0 request with topics and partitions", () => {
    const writer = new BinaryWriter()
    const request: DescribeQuorumRequest = {
      topics: [
        {
          topicName: "__cluster_metadata",
          partitions: [{ partitionIndex: 0 }]
        }
      ]
    }
    encodeDescribeQuorumRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // topics compact array (1 + 1)
    const topicCount = reader.readUnsignedVarInt()
    expect(topicCount.ok && topicCount.value).toBe(2)

    // topic name
    const name = reader.readCompactString()
    expect(name.ok && name.value).toBe("__cluster_metadata")

    // partitions compact array (1 + 1)
    const partCount = reader.readUnsignedVarInt()
    expect(partCount.ok && partCount.value).toBe(2)

    // partition_index
    const partIdx = reader.readInt32()
    expect(partIdx.ok && partIdx.value).toBe(0)

    // partition tagged
    reader.readTaggedFields()
    // topic tagged
    reader.readTaggedFields()
    // request tagged
    const tag = reader.readTaggedFields()
    expect(tag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// buildDescribeQuorumRequest (framed)
// ---------------------------------------------------------------------------

describe("buildDescribeQuorumRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildDescribeQuorumRequest(
      1,
      0,
      { topics: [{ topicName: "t", partitions: [{ partitionIndex: 0 }] }] },
      "test-client"
    )
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.DescribeQuorum)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeQuorumResponse", () => {
  it("decodes v0 response with quorum info", () => {
    const w = new BinaryWriter()
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    // topics (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // topic
    w.writeCompactString("__cluster_metadata") // topic_name
    // partitions (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // partition
    w.writeInt16(0) // error_code
    w.writeInt32(0) // partition_index
    w.writeInt32(1) // leader_id
    w.writeInt32(5) // leader_epoch
    w.writeInt64(100n) // high_watermark
    // current_voters (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // voter
    w.writeInt32(1) // replica_id
    w.writeInt64(100n) // log_end_offset
    w.writeTaggedFields([]) // voter tagged
    // end voters
    // observers (compact: 0 + 1)
    w.writeUnsignedVarInt(1)
    // partition tagged
    w.writeTaggedFields([])
    // end partitions
    // topic tagged
    w.writeTaggedFields([])
    // end topics
    // response tagged
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeQuorumResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.errorCode).toBe(0)
    expect(result.value.errorMessage).toBeNull()
    expect(result.value.topics).toHaveLength(1)

    const topic = result.value.topics[0]
    expect(topic.topicName).toBe("__cluster_metadata")
    expect(topic.partitions).toHaveLength(1)

    const partition = topic.partitions[0]
    expect(partition.errorCode).toBe(0)
    expect(partition.partitionIndex).toBe(0)
    expect(partition.leaderId).toBe(1)
    expect(partition.leaderEpoch).toBe(5)
    expect(partition.highWatermark).toBe(100n)
    expect(partition.currentVoters).toHaveLength(1)
    expect(partition.currentVoters[0].replicaId).toBe(1)
    expect(partition.currentVoters[0].logEndOffset).toBe(100n)
    expect(partition.currentVoters[0].lastCaughtUpTimestamp).toBe(-1n)
    expect(partition.currentVoters[0].lastFetchTimestamp).toBe(-1n)
    expect(partition.observers).toHaveLength(0)
    expect(result.value.nodes).toHaveLength(0)
  })

  it("decodes v1 response with timestamps and nodes", () => {
    const w = new BinaryWriter()
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    // topics (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeCompactString("__cluster_metadata")
    // partitions (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeInt16(0) // error_code
    w.writeInt32(0) // partition_index
    w.writeInt32(1) // leader_id
    w.writeInt32(10) // leader_epoch
    w.writeInt64(500n) // high_watermark
    // current_voters (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeInt32(1) // replica_id
    w.writeInt64(500n) // log_end_offset
    w.writeInt64(1700000000000n) // last_caught_up_timestamp (v1)
    w.writeInt64(1700000001000n) // last_fetch_timestamp (v1)
    w.writeTaggedFields([])
    // end voters
    // observers (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeInt32(2) // replica_id
    w.writeInt64(490n) // log_end_offset
    w.writeInt64(1699999999000n) // last_caught_up_timestamp
    w.writeInt64(1700000000500n) // last_fetch_timestamp
    w.writeTaggedFields([])
    // end observers
    w.writeTaggedFields([]) // partition tagged
    w.writeTaggedFields([]) // topic tagged
    // nodes (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // node
    w.writeInt32(1) // node_id
    // listeners (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeCompactString("CONTROLLER") // name
    w.writeCompactString("broker1.example.com") // host
    w.writeInt16(9093) // port (UINT16 read as INT16)
    w.writeTaggedFields([]) // listener tagged
    w.writeTaggedFields([]) // node tagged
    // end nodes
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeQuorumResponse(reader, 1)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    const partition = result.value.topics[0].partitions[0]
    expect(partition.currentVoters[0].lastCaughtUpTimestamp).toBe(1700000000000n)
    expect(partition.currentVoters[0].lastFetchTimestamp).toBe(1700000001000n)

    expect(partition.observers).toHaveLength(1)
    expect(partition.observers[0].replicaId).toBe(2)
    expect(partition.observers[0].logEndOffset).toBe(490n)

    expect(result.value.nodes).toHaveLength(1)
    expect(result.value.nodes[0].nodeId).toBe(1)
    expect(result.value.nodes[0].listeners).toHaveLength(1)
    expect(result.value.nodes[0].listeners[0].name).toBe("CONTROLLER")
    expect(result.value.nodes[0].listeners[0].host).toBe("broker1.example.com")
    expect(result.value.nodes[0].listeners[0].port).toBe(9093)
  })
})
