import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildTxnOffsetCommitRequest,
  decodeTxnOffsetCommitResponse,
  encodeTxnOffsetCommitRequest,
  type TxnOffsetCommitRequest
} from "../../../protocol/txn-offset-commit"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeTxnOffsetCommitRequest", () => {
  describe("v0 — non-flexible encoding", () => {
    it("encodes transactional fields and topic/partition offsets", () => {
      const writer = new BinaryWriter()
      const request: TxnOffsetCommitRequest = {
        transactionalId: "my-txn",
        groupId: "my-group",
        producerId: 42n,
        producerEpoch: 1,
        topics: [
          {
            name: "topic-a",
            partitions: [
              { partitionIndex: 0, committedOffset: 100n, committedMetadata: null },
              { partitionIndex: 1, committedOffset: 200n, committedMetadata: "meta" }
            ]
          }
        ]
      }
      encodeTxnOffsetCommitRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id
      const tidResult = reader.readString()
      expect(tidResult.ok).toBe(true)
      expect(tidResult.ok && tidResult.value).toBe("my-txn")

      // group_id
      const gidResult = reader.readString()
      expect(gidResult.ok).toBe(true)
      expect(gidResult.ok && gidResult.value).toBe("my-group")

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
      expect(topicCountResult.ok && topicCountResult.value).toBe(1)

      // topic name
      const nameResult = reader.readString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("topic-a")

      // partitions array length
      const partCountResult = reader.readInt32()
      expect(partCountResult.ok).toBe(true)
      expect(partCountResult.ok && partCountResult.value).toBe(2)

      // partition 0
      const p0IndexResult = reader.readInt32()
      expect(p0IndexResult.ok).toBe(true)
      expect(p0IndexResult.ok && p0IndexResult.value).toBe(0)

      const p0OffsetResult = reader.readInt64()
      expect(p0OffsetResult.ok).toBe(true)
      expect(p0OffsetResult.ok && p0OffsetResult.value).toBe(100n)

      // v0 has no committed_leader_epoch

      const p0MetaResult = reader.readString()
      expect(p0MetaResult.ok).toBe(true)
      expect(p0MetaResult.ok && p0MetaResult.value).toBe(null)

      // partition 1
      const p1IndexResult = reader.readInt32()
      expect(p1IndexResult.ok).toBe(true)
      expect(p1IndexResult.ok && p1IndexResult.value).toBe(1)

      const p1OffsetResult = reader.readInt64()
      expect(p1OffsetResult.ok).toBe(true)
      expect(p1OffsetResult.ok && p1OffsetResult.value).toBe(200n)

      const p1MetaResult = reader.readString()
      expect(p1MetaResult.ok).toBe(true)
      expect(p1MetaResult.ok && p1MetaResult.value).toBe("meta")

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — adds committed_leader_epoch", () => {
    it("encodes leader epoch per partition", () => {
      const writer = new BinaryWriter()
      const request: TxnOffsetCommitRequest = {
        transactionalId: "txn-v2",
        groupId: "group-v2",
        producerId: 10n,
        producerEpoch: 0,
        topics: [
          {
            name: "t",
            partitions: [
              {
                partitionIndex: 0,
                committedOffset: 50n,
                committedLeaderEpoch: 7,
                committedMetadata: null
              }
            ]
          }
        ]
      }
      encodeTxnOffsetCommitRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readString() // transactional_id
      reader.readString() // group_id
      reader.readInt64() // producer_id
      reader.readInt16() // producer_epoch
      reader.readInt32() // topics count
      reader.readString() // topic name
      reader.readInt32() // partitions count

      reader.readInt32() // partition_index

      const offsetResult = reader.readInt64()
      expect(offsetResult.ok).toBe(true)
      expect(offsetResult.ok && offsetResult.value).toBe(50n)

      // committed_leader_epoch (v2+)
      const epochResult = reader.readInt32()
      expect(epochResult.ok).toBe(true)
      expect(epochResult.ok && epochResult.value).toBe(7)

      reader.readString() // committed_metadata

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v3 — flexible encoding with generation/member", () => {
    it("encodes with compact strings, generation, member, and instance ID", () => {
      const writer = new BinaryWriter()
      const request: TxnOffsetCommitRequest = {
        transactionalId: "txn-flex",
        groupId: "grp-flex",
        producerId: 100n,
        producerEpoch: 3,
        generationId: 5,
        memberId: "member-1",
        groupInstanceId: "instance-1",
        topics: [
          {
            name: "tp",
            partitions: [
              {
                partitionIndex: 0,
                committedOffset: 999n,
                committedLeaderEpoch: 2,
                committedMetadata: "m"
              }
            ]
          }
        ]
      }
      encodeTxnOffsetCommitRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id (compact)
      const tidResult = reader.readCompactString()
      expect(tidResult.ok).toBe(true)
      expect(tidResult.ok && tidResult.value).toBe("txn-flex")

      // group_id (compact)
      const gidResult = reader.readCompactString()
      expect(gidResult.ok).toBe(true)
      expect(gidResult.ok && gidResult.value).toBe("grp-flex")

      // producer_id
      const pidResult = reader.readInt64()
      expect(pidResult.ok).toBe(true)
      expect(pidResult.ok && pidResult.value).toBe(100n)

      // producer_epoch
      const peResult = reader.readInt16()
      expect(peResult.ok).toBe(true)
      expect(peResult.ok && peResult.value).toBe(3)

      // generation_id (v3+)
      const genResult = reader.readInt32()
      expect(genResult.ok).toBe(true)
      expect(genResult.ok && genResult.value).toBe(5)

      // member_id (compact, v3+)
      const memResult = reader.readCompactString()
      expect(memResult.ok).toBe(true)
      expect(memResult.ok && memResult.value).toBe("member-1")

      // group_instance_id (compact nullable, v3+)
      const giResult = reader.readCompactString()
      expect(giResult.ok).toBe(true)
      expect(giResult.ok && giResult.value).toBe("instance-1")

      // topics count (compact)
      const tcResult = reader.readUnsignedVarInt()
      expect(tcResult.ok).toBe(true)
      expect(tcResult.ok && tcResult.value).toBe(2) // 1 + 1

      // topic name (compact)
      const tnResult = reader.readCompactString()
      expect(tnResult.ok).toBe(true)
      expect(tnResult.ok && tnResult.value).toBe("tp")

      // partitions count (compact)
      const pcResult = reader.readUnsignedVarInt()
      expect(pcResult.ok).toBe(true)
      expect(pcResult.ok && pcResult.value).toBe(2) // 1 + 1

      // partition fields
      const piResult = reader.readInt32()
      expect(piResult.ok).toBe(true)
      expect(piResult.ok && piResult.value).toBe(0)

      const offResult = reader.readInt64()
      expect(offResult.ok).toBe(true)
      expect(offResult.ok && offResult.value).toBe(999n)

      const leResult = reader.readInt32()
      expect(leResult.ok).toBe(true)
      expect(leResult.ok && leResult.value).toBe(2)

      const cmResult = reader.readCompactString()
      expect(cmResult.ok).toBe(true)
      expect(cmResult.ok && cmResult.value).toBe("m")

      // tagged fields at partition/topic/top level
      reader.readTaggedFields()
      reader.readTaggedFields()
      reader.readTaggedFields()

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// Build (framed request)
// ---------------------------------------------------------------------------

describe("buildTxnOffsetCommitRequest", () => {
  it("produces a size-prefixed frame with correct API key", () => {
    const request: TxnOffsetCommitRequest = {
      transactionalId: "txn-1",
      groupId: "g",
      producerId: 1n,
      producerEpoch: 0,
      topics: []
    }
    const frame = buildTxnOffsetCommitRequest(1, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)

    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.TxnOffsetCommit)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeTxnOffsetCommitResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes per-topic/partition error codes", () => {
      const writer = new BinaryWriter()
      // throttle_time_ms
      writer.writeInt32(15)
      // topics array
      writer.writeInt32(1)
      // topic name
      writer.writeString("topic-a")
      // partitions array
      writer.writeInt32(2)
      // partition 0
      writer.writeInt32(0)
      writer.writeInt16(0) // success
      // partition 1
      writer.writeInt32(1)
      writer.writeInt16(49) // INVALID_PRODUCER_ID_MAPPING

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeTxnOffsetCommitResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(15)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("topic-a")
      expect(result.value.topics[0].partitions).toHaveLength(2)
      expect(result.value.topics[0].partitions[0].errorCode).toBe(0)
      expect(result.value.topics[0].partitions[1].errorCode).toBe(49)
    })
  })

  describe("v3 — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeUnsignedVarInt(2) // topics: 1 + 1
      writer.writeCompactString("topic-b")
      writer.writeUnsignedVarInt(2) // partitions: 1 + 1
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeTaggedFields([]) // partition tags
      writer.writeTaggedFields([]) // topic tags
      writer.writeTaggedFields([]) // top-level tags

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeTxnOffsetCommitResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("topic-b")
      expect(result.value.topics[0].partitions[0].errorCode).toBe(0)
      expect(result.value.taggedFields).toEqual([])
    })
  })
})
