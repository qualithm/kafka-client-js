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

  it("decodes v0 response with multiple voters and observers", () => {
    const w = new BinaryWriter()
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeUnsignedVarInt(2) // 1 topic + 1
    w.writeCompactString("__cluster_metadata")
    w.writeUnsignedVarInt(2) // 1 partition + 1
    w.writeInt16(0) // error_code
    w.writeInt32(0) // partition_index
    w.writeInt32(1) // leader_id
    w.writeInt32(3) // leader_epoch
    w.writeInt64(200n) // high_watermark
    // 3 voters
    w.writeUnsignedVarInt(4)
    for (const [id, offset] of [
      [1, 200n],
      [2, 195n],
      [3, 198n]
    ] as const) {
      w.writeInt32(id)
      w.writeInt64(offset)
      w.writeTaggedFields([])
    }
    // 2 observers
    w.writeUnsignedVarInt(3)
    for (const [id, offset] of [
      [4, 180n],
      [5, 175n]
    ] as const) {
      w.writeInt32(id)
      w.writeInt64(offset)
      w.writeTaggedFields([])
    }
    w.writeTaggedFields([]) // partition tagged
    w.writeTaggedFields([]) // topic tagged
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeQuorumResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    const part = result.value.topics[0].partitions[0]
    expect(part.currentVoters).toHaveLength(3)
    expect(part.currentVoters[0].replicaId).toBe(1)
    expect(part.currentVoters[2].logEndOffset).toBe(198n)
    expect(part.observers).toHaveLength(2)
    expect(part.observers[1].replicaId).toBe(5)
  })

  it("decodes v1 response with multiple nodes and listeners", () => {
    const w = new BinaryWriter()
    w.writeInt16(0)
    w.writeCompactString(null)
    w.writeUnsignedVarInt(2) // 1 topic + 1
    w.writeCompactString("__cluster_metadata")
    w.writeUnsignedVarInt(2)
    w.writeInt16(0)
    w.writeInt32(0)
    w.writeInt32(1)
    w.writeInt32(1)
    w.writeInt64(50n)
    // 1 voter
    w.writeUnsignedVarInt(2)
    w.writeInt32(1)
    w.writeInt64(50n)
    w.writeInt64(1000n)
    w.writeInt64(2000n)
    w.writeTaggedFields([])
    // 0 observers
    w.writeUnsignedVarInt(1)
    w.writeTaggedFields([])
    w.writeTaggedFields([])
    // 2 nodes
    w.writeUnsignedVarInt(3)
    // node 1 with 2 listeners
    w.writeInt32(1)
    w.writeUnsignedVarInt(3) // 2 listeners + 1
    w.writeCompactString("CONTROLLER")
    w.writeCompactString("host1")
    w.writeInt16(9093)
    w.writeTaggedFields([])
    w.writeCompactString("BROKER")
    w.writeCompactString("host1")
    w.writeInt16(9092)
    w.writeTaggedFields([])
    w.writeTaggedFields([]) // node tagged
    // node 2 with 1 listener
    w.writeInt32(2)
    w.writeUnsignedVarInt(2) // 1 listener + 1
    w.writeCompactString("BROKER")
    w.writeCompactString("host2")
    w.writeInt16(9092)
    w.writeTaggedFields([])
    w.writeTaggedFields([])
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeQuorumResponse(reader, 1)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.nodes).toHaveLength(2)
    expect(result.value.nodes[0].listeners).toHaveLength(2)
    expect(result.value.nodes[0].listeners[0].name).toBe("CONTROLLER")
    expect(result.value.nodes[0].listeners[1].name).toBe("BROKER")
    expect(result.value.nodes[1].nodeId).toBe(2)
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeDescribeQuorumResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated partition entry", () => {
      const w = new BinaryWriter()
      w.writeInt16(0) // error_code
      w.writeCompactString(null) // error_message
      w.writeUnsignedVarInt(2) // 1 topic + 1
      w.writeCompactString("__cluster_metadata")
      w.writeUnsignedVarInt(2) // 1 partition + 1
      w.writeInt16(0) // error_code
      w.writeInt32(0) // partition_index
      // Missing leader_id and beyond

      const reader = new BinaryReader(w.finish())
      const result = decodeDescribeQuorumResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v1 replica timestamps", () => {
      const w = new BinaryWriter()
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeUnsignedVarInt(2)
      w.writeCompactString("__cluster_metadata")
      w.writeUnsignedVarInt(2)
      w.writeInt16(0)
      w.writeInt32(0)
      w.writeInt32(1)
      w.writeInt32(1)
      w.writeInt64(50n)
      w.writeUnsignedVarInt(2) // 1 voter
      w.writeInt32(1) // replica_id
      w.writeInt64(50n) // log_end_offset
      // Missing last_caught_up_timestamp and last_fetch_timestamp for v1

      const reader = new BinaryReader(w.finish())
      const result = decodeDescribeQuorumResponse(reader, 1)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v1 nodes array", () => {
      const w = new BinaryWriter()
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeUnsignedVarInt(1) // 0 topics
      // Missing nodes array for v1

      const reader = new BinaryReader(w.finish())
      const result = decodeDescribeQuorumResponse(reader, 1)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated listener entry", () => {
      const w = new BinaryWriter()
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeUnsignedVarInt(1) // 0 topics
      w.writeUnsignedVarInt(2) // 1 node + 1
      w.writeInt32(1) // node_id
      w.writeUnsignedVarInt(2) // 1 listener + 1
      w.writeCompactString("CONTROLLER") // name
      // Missing host and port

      const reader = new BinaryReader(w.finish())
      const result = decodeDescribeQuorumResponse(reader, 1)
      expect(result.ok).toBe(false)
    })
  })
})
