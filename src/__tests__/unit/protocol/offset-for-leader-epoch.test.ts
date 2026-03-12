import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildOffsetForLeaderEpochRequest,
  decodeOffsetForLeaderEpochResponse,
  encodeOffsetForLeaderEpochRequest,
  type OffsetForLeaderEpochRequest
} from "../../../protocol/offset-for-leader-epoch"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeOffsetForLeaderEpochRequest", () => {
  describe("v0–v2 — non-flexible, no replica_id", () => {
    it("encodes topics with partitions and leader epochs", () => {
      const writer = new BinaryWriter()
      const request: OffsetForLeaderEpochRequest = {
        topics: [
          {
            name: "topic-a",
            partitions: [
              { partitionIndex: 0, leaderEpoch: 5 },
              { partitionIndex: 1, leaderEpoch: 3 }
            ]
          }
        ]
      }
      encodeOffsetForLeaderEpochRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // topics array (INT32)
      const topicsLen = reader.readInt32()
      expect(topicsLen.ok && topicsLen.value).toBe(1)

      // topic name
      const name = reader.readString()
      expect(name.ok && name.value).toBe("topic-a")

      // partitions array (INT32)
      const partsLen = reader.readInt32()
      expect(partsLen.ok && partsLen.value).toBe(2)

      // partition 0
      const p0Idx = reader.readInt32()
      expect(p0Idx.ok && p0Idx.value).toBe(0)
      const p0Epoch = reader.readInt32()
      expect(p0Epoch.ok && p0Epoch.value).toBe(5)

      // partition 1
      const p1Idx = reader.readInt32()
      expect(p1Idx.ok && p1Idx.value).toBe(1)
      const p1Epoch = reader.readInt32()
      expect(p1Epoch.ok && p1Epoch.value).toBe(3)

      expect(reader.remaining).toBe(0)
    })

    it("encodes empty topics list", () => {
      const writer = new BinaryWriter()
      const request: OffsetForLeaderEpochRequest = {
        topics: []
      }
      encodeOffsetForLeaderEpochRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const topicsLen = reader.readInt32()
      expect(topicsLen.ok && topicsLen.value).toBe(0)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v3 — adds replica_id and current_leader_epoch", () => {
    it("encodes replica_id and current_leader_epoch", () => {
      const writer = new BinaryWriter()
      const request: OffsetForLeaderEpochRequest = {
        replicaId: -1,
        topics: [
          {
            name: "topic-b",
            partitions: [{ partitionIndex: 0, currentLeaderEpoch: 2, leaderEpoch: 1 }]
          }
        ]
      }
      encodeOffsetForLeaderEpochRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // replica_id (INT32)
      const replicaId = reader.readInt32()
      expect(replicaId.ok && replicaId.value).toBe(-1)

      // topics array (INT32)
      const topicsLen = reader.readInt32()
      expect(topicsLen.ok && topicsLen.value).toBe(1)

      const name = reader.readString()
      expect(name.ok && name.value).toBe("topic-b")

      const partsLen = reader.readInt32()
      expect(partsLen.ok && partsLen.value).toBe(1)

      // partition 0: index, current_leader_epoch, leader_epoch
      const pIdx = reader.readInt32()
      expect(pIdx.ok && pIdx.value).toBe(0)
      const curEpoch = reader.readInt32()
      expect(curEpoch.ok && curEpoch.value).toBe(2)
      const epoch = reader.readInt32()
      expect(epoch.ok && epoch.value).toBe(1)

      expect(reader.remaining).toBe(0)
    })

    it("defaults replica_id to -1 when not specified", () => {
      const writer = new BinaryWriter()
      const request: OffsetForLeaderEpochRequest = {
        topics: [
          {
            name: "topic-c",
            partitions: [{ partitionIndex: 0, leaderEpoch: 0 }]
          }
        ]
      }
      encodeOffsetForLeaderEpochRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const replicaId = reader.readInt32()
      expect(replicaId.ok && replicaId.value).toBe(-1)
    })
  })

  describe("v4 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: OffsetForLeaderEpochRequest = {
        replicaId: -1,
        topics: [
          {
            name: "flex-topic",
            partitions: [{ partitionIndex: 0, currentLeaderEpoch: 5, leaderEpoch: 4 }]
          }
        ]
      }
      encodeOffsetForLeaderEpochRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // replica_id
      const replicaId = reader.readInt32()
      expect(replicaId.ok && replicaId.value).toBe(-1)

      // topics array (compact: length + 1)
      const topicsLen = reader.readUnsignedVarInt()
      expect(topicsLen.ok && topicsLen.value).toBe(2)

      // topic name (compact)
      const name = reader.readCompactString()
      expect(name.ok && name.value).toBe("flex-topic")

      // partitions (compact)
      const partsLen = reader.readUnsignedVarInt()
      expect(partsLen.ok && partsLen.value).toBe(2)

      const pIdx = reader.readInt32()
      expect(pIdx.ok && pIdx.value).toBe(0)
      const curEpoch = reader.readInt32()
      expect(curEpoch.ok && curEpoch.value).toBe(5)
      const epoch = reader.readInt32()
      expect(epoch.ok && epoch.value).toBe(4)

      // partition tagged fields
      const pTag = reader.readTaggedFields()
      expect(pTag.ok).toBe(true)

      // topic tagged fields
      const tTag = reader.readTaggedFields()
      expect(tTag.ok).toBe(true)

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

describe("buildOffsetForLeaderEpochRequest", () => {
  it("produces a framed request with correct header", () => {
    const request: OffsetForLeaderEpochRequest = {
      topics: [
        {
          name: "test-topic",
          partitions: [{ partitionIndex: 0, leaderEpoch: 1 }]
        }
      ]
    }
    const frame = buildOffsetForLeaderEpochRequest(99, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    // size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    // API key
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.OffsetForLeaderEpoch)

    // API version
    const apiVersion = reader.readInt16()
    expect(apiVersion.ok && apiVersion.value).toBe(0)

    // correlation id
    const corrId = reader.readInt32()
    expect(corrId.ok && corrId.value).toBe(99)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeOffsetForLeaderEpochResponse", () => {
  describe("v0 — non-flexible, no throttle, no leader epoch in response", () => {
    it("decodes topics and partition results", () => {
      const writer = new BinaryWriter()

      // topics array (INT32)
      writer.writeInt32(1)
      // topic name
      writer.writeString("topic-a")
      // partitions array (INT32)
      writer.writeInt32(1)
      // error_code, partition_index, end_offset
      writer.writeInt16(0)
      writer.writeInt32(0)
      writer.writeInt64(42n)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeOffsetForLeaderEpochResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("topic-a")
      expect(result.value.topics[0].partitions).toHaveLength(1)
      expect(result.value.topics[0].partitions[0].errorCode).toBe(0)
      expect(result.value.topics[0].partitions[0].partitionIndex).toBe(0)
      expect(result.value.topics[0].partitions[0].leaderEpoch).toBe(-1) // not present in v0
      expect(result.value.topics[0].partitions[0].endOffset).toBe(42n)
    })
  })

  describe("v1 — adds leader epoch in response", () => {
    it("decodes leader epoch per partition", () => {
      const writer = new BinaryWriter()

      // topics array
      writer.writeInt32(1)
      writer.writeString("topic-b")
      // partitions array
      writer.writeInt32(1)
      // error_code, partition_index, leader_epoch, end_offset
      writer.writeInt16(0)
      writer.writeInt32(0)
      writer.writeInt32(7)
      writer.writeInt64(100n)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeOffsetForLeaderEpochResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].partitions[0].leaderEpoch).toBe(7)
      expect(result.value.topics[0].partitions[0].endOffset).toBe(100n)
    })
  })

  describe("v2 — adds throttle_time_ms", () => {
    it("decodes throttle time", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(25)
      // topics array
      writer.writeInt32(1)
      writer.writeString("topic-c")
      // partitions array
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeInt32(0)
      writer.writeInt32(3)
      writer.writeInt64(50n)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeOffsetForLeaderEpochResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(25)
      expect(result.value.topics[0].partitions[0].leaderEpoch).toBe(3)
      expect(result.value.topics[0].partitions[0].endOffset).toBe(50n)
    })
  })

  describe("v4 — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(5)
      // topics array (compact)
      writer.writeUnsignedVarInt(2)
      // topic name (compact)
      writer.writeCompactString("flex-topic")
      // partitions array (compact)
      writer.writeUnsignedVarInt(2)
      // error_code, partition_index, leader_epoch, end_offset
      writer.writeInt16(0)
      writer.writeInt32(0)
      writer.writeInt32(10)
      writer.writeInt64(999n)
      writer.writeTaggedFields([]) // partition tagged fields
      // topic tagged fields
      writer.writeTaggedFields([])
      // response tagged fields
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeOffsetForLeaderEpochResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(5)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("flex-topic")
      expect(result.value.topics[0].partitions[0].leaderEpoch).toBe(10)
      expect(result.value.topics[0].partitions[0].endOffset).toBe(999n)
    })
  })

  it("decodes multiple topics and partitions", () => {
    const writer = new BinaryWriter()

    // throttle_time_ms
    writer.writeInt32(0)
    // topics array
    writer.writeInt32(2)

    // topic 1
    writer.writeString("topic-x")
    writer.writeInt32(2) // 2 partitions
    writer.writeInt16(0) // error
    writer.writeInt32(0) // partition index
    writer.writeInt32(1) // leader epoch
    writer.writeInt64(10n) // end offset
    writer.writeInt16(0)
    writer.writeInt32(1)
    writer.writeInt32(1)
    writer.writeInt64(20n)

    // topic 2
    writer.writeString("topic-y")
    writer.writeInt32(1) // 1 partition
    writer.writeInt16(6) // NOT_LEADER_OR_FOLLOWER
    writer.writeInt32(0)
    writer.writeInt32(0)
    writer.writeInt64(-1n)

    const buf = writer.finish()
    const reader = new BinaryReader(buf)
    const result = decodeOffsetForLeaderEpochResponse(reader, 2)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.topics).toHaveLength(2)
    expect(result.value.topics[0].partitions).toHaveLength(2)
    expect(result.value.topics[0].partitions[1].endOffset).toBe(20n)
    expect(result.value.topics[1].partitions[0].errorCode).toBe(6)
    expect(result.value.topics[1].partitions[0].endOffset).toBe(-1n)
  })

  it("returns failure on truncated input", () => {
    const reader = new BinaryReader(new Uint8Array([0, 0]))
    const result = decodeOffsetForLeaderEpochResponse(reader, 2)
    expect(result.ok).toBe(false)
  })
})
