import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildListPartitionReassignmentsRequest,
  decodeListPartitionReassignmentsResponse,
  encodeListPartitionReassignmentsRequest,
  type ListPartitionReassignmentsRequest
} from "../../../protocol/list-partition-reassignments"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeListPartitionReassignmentsRequest", () => {
  it("encodes timeout and topic filters", () => {
    const writer = new BinaryWriter()
    const request: ListPartitionReassignmentsRequest = {
      timeoutMs: 30000,
      topics: [
        {
          name: "topic-a",
          partitionIndexes: [0, 1, 2]
        }
      ]
    }
    encodeListPartitionReassignmentsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // timeout_ms (INT32)
    const timeout = reader.readInt32()
    expect(timeout.ok && timeout.value).toBe(30000)

    // topics compact array (1 + 1 = 2)
    const topicsLen = reader.readUnsignedVarInt()
    expect(topicsLen.ok && topicsLen.value).toBe(2)

    // topic name (compact string)
    const name = reader.readCompactString()
    expect(name.ok && name.value).toBe("topic-a")

    // partition_indexes compact array (3 + 1 = 4)
    const partsLen = reader.readUnsignedVarInt()
    expect(partsLen.ok && partsLen.value).toBe(4)

    const p0 = reader.readInt32()
    expect(p0.ok && p0.value).toBe(0)
    const p1 = reader.readInt32()
    expect(p1.ok && p1.value).toBe(1)
    const p2 = reader.readInt32()
    expect(p2.ok && p2.value).toBe(2)

    // topic tagged fields
    const tTag = reader.readTaggedFields()
    expect(tTag.ok).toBe(true)

    // request tagged fields
    const rTag = reader.readTaggedFields()
    expect(rTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes null topics for all-partition listing", () => {
    const writer = new BinaryWriter()
    const request: ListPartitionReassignmentsRequest = {
      timeoutMs: 10000,
      topics: null
    }
    encodeListPartitionReassignmentsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // timeout_ms
    const timeout = reader.readInt32()
    expect(timeout.ok && timeout.value).toBe(10000)

    // topics compact null (0)
    const topicsLen = reader.readUnsignedVarInt()
    expect(topicsLen.ok && topicsLen.value).toBe(0)

    // request tagged fields
    const rTag = reader.readTaggedFields()
    expect(rTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes empty topics list", () => {
    const writer = new BinaryWriter()
    const request: ListPartitionReassignmentsRequest = {
      timeoutMs: 5000,
      topics: []
    }
    encodeListPartitionReassignmentsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // timeout_ms
    const timeout = reader.readInt32()
    expect(timeout.ok && timeout.value).toBe(5000)

    // topics compact array (0 + 1 = 1)
    const topicsLen = reader.readUnsignedVarInt()
    expect(topicsLen.ok && topicsLen.value).toBe(1)

    // request tagged fields
    const rTag = reader.readTaggedFields()
    expect(rTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

describe("buildListPartitionReassignmentsRequest", () => {
  it("produces a framed request with correct header", () => {
    const request: ListPartitionReassignmentsRequest = {
      timeoutMs: 30000,
      topics: [
        {
          name: "test-topic",
          partitionIndexes: [0]
        }
      ]
    }
    const frame = buildListPartitionReassignmentsRequest(42, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    // size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    // API key
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.ListPartitionReassignments)

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

describe("decodeListPartitionReassignmentsResponse", () => {
  it("decodes throttle, error, and per-topic/partition reassignment state", () => {
    const writer = new BinaryWriter()

    // throttle_time_ms
    writer.writeInt32(50)
    // error_code
    writer.writeInt16(0)
    // error_message (compact nullable string — null)
    writer.writeCompactString(null)
    // topics compact array (1 topic → 1 + 1 = 2)
    writer.writeUnsignedVarInt(2)
    // topic name
    writer.writeCompactString("topic-a")
    // partitions compact array (1 partition → 1 + 1 = 2)
    writer.writeUnsignedVarInt(2)
    // partition_index
    writer.writeInt32(0)
    // replicas compact array [1, 2, 3] → 3 + 1 = 4
    writer.writeUnsignedVarInt(4)
    writer.writeInt32(1)
    writer.writeInt32(2)
    writer.writeInt32(3)
    // adding_replicas compact array [3] → 1 + 1 = 2
    writer.writeUnsignedVarInt(2)
    writer.writeInt32(3)
    // removing_replicas compact array [1] → 1 + 1 = 2
    writer.writeUnsignedVarInt(2)
    writer.writeInt32(1)
    // partition tagged fields
    writer.writeTaggedFields([])
    // topic tagged fields
    writer.writeTaggedFields([])
    // response tagged fields
    writer.writeTaggedFields([])

    const buf = writer.finish()
    const reader = new BinaryReader(buf)
    const result = decodeListPartitionReassignmentsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(50)
    expect(result.value.errorCode).toBe(0)
    expect(result.value.errorMessage).toBeNull()
    expect(result.value.topics).toHaveLength(1)
    expect(result.value.topics[0].name).toBe("topic-a")
    expect(result.value.topics[0].partitions).toHaveLength(1)

    const partition = result.value.topics[0].partitions[0]
    expect(partition.partitionIndex).toBe(0)
    expect(partition.replicas).toEqual([1, 2, 3])
    expect(partition.addingReplicas).toEqual([3])
    expect(partition.removingReplicas).toEqual([1])
  })

  it("decodes top-level error code and message", () => {
    const writer = new BinaryWriter()

    writer.writeInt32(0) // throttle
    writer.writeInt16(87) // NOT_CONTROLLER
    writer.writeCompactString("not the controller")
    // topics — empty compact array (0 + 1 = 1)
    writer.writeUnsignedVarInt(1)
    // response tagged fields
    writer.writeTaggedFields([])

    const buf = writer.finish()
    const reader = new BinaryReader(buf)
    const result = decodeListPartitionReassignmentsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.errorCode).toBe(87)
    expect(result.value.errorMessage).toBe("not the controller")
    expect(result.value.topics).toHaveLength(0)
  })

  it("decodes multiple topics with multiple partitions", () => {
    const writer = new BinaryWriter()

    writer.writeInt32(0) // throttle
    writer.writeInt16(0) // error_code
    writer.writeCompactString(null) // error_message
    // topics compact array (2 topics → 2 + 1 = 3)
    writer.writeUnsignedVarInt(3)

    // topic 1
    writer.writeCompactString("topic-x")
    // partitions compact array (1 partition)
    writer.writeUnsignedVarInt(2)
    writer.writeInt32(0) // partition_index
    // replicas [1, 2] → 2 + 1 = 3
    writer.writeUnsignedVarInt(3)
    writer.writeInt32(1)
    writer.writeInt32(2)
    // adding_replicas [] → 0 + 1 = 1
    writer.writeUnsignedVarInt(1)
    // removing_replicas [] → 0 + 1 = 1
    writer.writeUnsignedVarInt(1)
    writer.writeTaggedFields([]) // partition tagged fields
    writer.writeTaggedFields([]) // topic tagged fields

    // topic 2
    writer.writeCompactString("topic-y")
    // partitions compact array (1 partition)
    writer.writeUnsignedVarInt(2)
    writer.writeInt32(3) // partition_index
    // replicas [4, 5] → 2 + 1 = 3
    writer.writeUnsignedVarInt(3)
    writer.writeInt32(4)
    writer.writeInt32(5)
    // adding_replicas [5] → 1 + 1 = 2
    writer.writeUnsignedVarInt(2)
    writer.writeInt32(5)
    // removing_replicas [4] → 1 + 1 = 2
    writer.writeUnsignedVarInt(2)
    writer.writeInt32(4)
    writer.writeTaggedFields([]) // partition tagged fields
    writer.writeTaggedFields([]) // topic tagged fields

    // response tagged fields
    writer.writeTaggedFields([])

    const buf = writer.finish()
    const reader = new BinaryReader(buf)
    const result = decodeListPartitionReassignmentsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.topics).toHaveLength(2)
    expect(result.value.topics[0].name).toBe("topic-x")
    expect(result.value.topics[0].partitions[0].replicas).toEqual([1, 2])
    expect(result.value.topics[0].partitions[0].addingReplicas).toEqual([])
    expect(result.value.topics[0].partitions[0].removingReplicas).toEqual([])
    expect(result.value.topics[1].name).toBe("topic-y")
    expect(result.value.topics[1].partitions[0].partitionIndex).toBe(3)
    expect(result.value.topics[1].partitions[0].addingReplicas).toEqual([5])
    expect(result.value.topics[1].partitions[0].removingReplicas).toEqual([4])
  })

  it("returns failure on truncated input", () => {
    const reader = new BinaryReader(new Uint8Array([0, 0]))
    const result = decodeListPartitionReassignmentsResponse(reader, 0)
    expect(result.ok).toBe(false)
  })
})
