import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildElectLeadersRequest,
  decodeElectLeadersResponse,
  type ElectLeadersRequest,
  encodeElectLeadersRequest
} from "../../../protocol/elect-leaders"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeElectLeadersRequest", () => {
  describe("v0–v1 — non-flexible encoding", () => {
    it("encodes election type, topics, and timeout", () => {
      const writer = new BinaryWriter()
      const request: ElectLeadersRequest = {
        electionType: 0,
        topicPartitions: [
          {
            topic: "topic-a",
            partitions: [{ partitionId: 0 }, { partitionId: 1 }]
          }
        ],
        timeoutMs: 30000
      }
      encodeElectLeadersRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // election_type (INT8)
      const electionType = reader.readInt8()
      expect(electionType.ok && electionType.value).toBe(0)

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
      const p0 = reader.readInt32()
      expect(p0.ok && p0.value).toBe(0)

      // partition 1
      const p1 = reader.readInt32()
      expect(p1.ok && p1.value).toBe(1)

      // timeout_ms
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(30000)

      expect(reader.remaining).toBe(0)
    })

    it("encodes null topic_partitions for all-partitions election", () => {
      const writer = new BinaryWriter()
      const request: ElectLeadersRequest = {
        electionType: 1,
        topicPartitions: null,
        timeoutMs: 10000
      }
      encodeElectLeadersRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // election_type (INT8)
      const electionType = reader.readInt8()
      expect(electionType.ok && electionType.value).toBe(1)

      // topics array (null = -1)
      const topicsLen = reader.readInt32()
      expect(topicsLen.ok && topicsLen.value).toBe(-1)

      // timeout_ms
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(10000)

      expect(reader.remaining).toBe(0)
    })

    it("encodes empty topics list", () => {
      const writer = new BinaryWriter()
      const request: ElectLeadersRequest = {
        electionType: 0,
        topicPartitions: [],
        timeoutMs: 5000
      }
      encodeElectLeadersRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // election_type
      const electionType = reader.readInt8()
      expect(electionType.ok && electionType.value).toBe(0)

      // topics array length
      const topicsLen = reader.readInt32()
      expect(topicsLen.ok && topicsLen.value).toBe(0)

      // timeout_ms
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(5000)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: ElectLeadersRequest = {
        electionType: 0,
        topicPartitions: [
          {
            topic: "flex-topic",
            partitions: [{ partitionId: 0 }]
          }
        ],
        timeoutMs: 15000
      }
      encodeElectLeadersRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // election_type (INT8)
      const electionType = reader.readInt8()
      expect(electionType.ok && electionType.value).toBe(0)

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

    it("encodes null topic_partitions in flexible encoding", () => {
      const writer = new BinaryWriter()
      const request: ElectLeadersRequest = {
        electionType: 1,
        topicPartitions: null,
        timeoutMs: 20000
      }
      encodeElectLeadersRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // election_type
      const electionType = reader.readInt8()
      expect(electionType.ok && electionType.value).toBe(1)

      // topics array (compact null = 0)
      const topicsLen = reader.readUnsignedVarInt()
      expect(topicsLen.ok && topicsLen.value).toBe(0)

      // timeout_ms
      const timeout = reader.readInt32()
      expect(timeout.ok && timeout.value).toBe(20000)

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

describe("buildElectLeadersRequest", () => {
  it("produces a framed request with correct header", () => {
    const request: ElectLeadersRequest = {
      electionType: 0,
      topicPartitions: [
        {
          topic: "test-topic",
          partitions: [{ partitionId: 0 }]
        }
      ],
      timeoutMs: 5000
    }
    const frame = buildElectLeadersRequest(42, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    // size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    // API key
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.ElectLeaders)

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

describe("decodeElectLeadersResponse", () => {
  describe("v0 — non-flexible, no top-level error_code", () => {
    it("decodes throttle and election results", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(50)
      // replica_election_results array (INT32)
      writer.writeInt32(1)
      // topic name
      writer.writeString("topic-a")
      // partitions array (INT32)
      writer.writeInt32(2)
      // partition 0: partition_id, error_code, error_message
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeString(null)
      // partition 1
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeString(null)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeElectLeadersResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.replicaElectionResults).toHaveLength(1)
      expect(result.value.replicaElectionResults[0].topic).toBe("topic-a")
      expect(result.value.replicaElectionResults[0].partitions).toHaveLength(2)
      expect(result.value.replicaElectionResults[0].partitions[0].partitionId).toBe(0)
      expect(result.value.replicaElectionResults[0].partitions[0].errorCode).toBe(0)
      expect(result.value.replicaElectionResults[0].partitions[0].errorMessage).toBeNull()
      expect(result.value.replicaElectionResults[0].partitions[1].partitionId).toBe(1)
    })

    it("decodes error codes and messages", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle
      writer.writeInt32(1) // 1 topic
      writer.writeString("topic-err")
      writer.writeInt32(1) // 1 partition
      writer.writeInt32(0) // partition index
      writer.writeInt16(84) // ELECTION_NOT_NEEDED
      writer.writeString("election not needed")

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeElectLeadersResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.replicaElectionResults[0].partitions[0].errorCode).toBe(84)
      expect(result.value.replicaElectionResults[0].partitions[0].errorMessage).toBe(
        "election not needed"
      )
    })
  })

  describe("v1 — includes top-level error_code", () => {
    it("decodes top-level error code", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(10)
      // error_code (top-level, v1+)
      writer.writeInt16(87) // ELIGIBLE_LEADERS_NOT_AVAILABLE
      // replica_election_results array
      writer.writeInt32(0)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeElectLeadersResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(10)
      expect(result.value.errorCode).toBe(87)
      expect(result.value.replicaElectionResults).toHaveLength(0)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(10)
      // error_code (top-level, v1+)
      writer.writeInt16(0)
      // replica_election_results array (compact: length + 1)
      writer.writeUnsignedVarInt(2)
      // topic name (compact)
      writer.writeCompactString("flex-topic")
      // partitions array (compact)
      writer.writeUnsignedVarInt(2)
      // partition 0
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeCompactString(null)
      writer.writeTaggedFields([]) // partition tagged fields
      // topic tagged fields
      writer.writeTaggedFields([])
      // response tagged fields
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeElectLeadersResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(10)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.replicaElectionResults).toHaveLength(1)
      expect(result.value.replicaElectionResults[0].topic).toBe("flex-topic")
      expect(result.value.replicaElectionResults[0].partitions[0].partitionId).toBe(0)
      expect(result.value.replicaElectionResults[0].partitions[0].errorCode).toBe(0)
      expect(result.value.replicaElectionResults[0].partitions[0].errorMessage).toBeNull()
    })
  })

  it("returns failure on truncated input", () => {
    const reader = new BinaryReader(new Uint8Array([0, 0]))
    const result = decodeElectLeadersResponse(reader, 0)
    expect(result.ok).toBe(false)
  })
})
