import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  type AlterPartitionReassignmentsRequest,
  buildAlterPartitionReassignmentsRequest,
  decodeAlterPartitionReassignmentsResponse,
  encodeAlterPartitionReassignmentsRequest
} from "../../../protocol/alter-partition-reassignments"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeAlterPartitionReassignmentsRequest", () => {
  it("encodes timeout, topics with partitions and replicas", () => {
    const writer = new BinaryWriter()
    const request: AlterPartitionReassignmentsRequest = {
      timeoutMs: 30000,
      topics: [
        {
          name: "topic-a",
          partitions: [
            { partitionIndex: 0, replicas: [1, 2, 3] },
            { partitionIndex: 1, replicas: [2, 3, 4] }
          ]
        }
      ]
    }
    encodeAlterPartitionReassignmentsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // timeout_ms (INT32)
    const timeout = reader.readInt32()
    expect(timeout.ok && timeout.value).toBe(30000)

    // topics compact array (length + 1)
    const topicsLen = reader.readUnsignedVarInt()
    expect(topicsLen.ok && topicsLen.value).toBe(2) // 1 + 1

    // topic name (compact string)
    const name = reader.readCompactString()
    expect(name.ok && name.value).toBe("topic-a")

    // partitions compact array (length + 1)
    const partsLen = reader.readUnsignedVarInt()
    expect(partsLen.ok && partsLen.value).toBe(3) // 2 + 1

    // partition 0
    const p0Idx = reader.readInt32()
    expect(p0Idx.ok && p0Idx.value).toBe(0)

    // replicas compact array (length + 1)
    const rep0Len = reader.readUnsignedVarInt()
    expect(rep0Len.ok && rep0Len.value).toBe(4) // 3 + 1

    const r0 = reader.readInt32()
    expect(r0.ok && r0.value).toBe(1)
    const r1 = reader.readInt32()
    expect(r1.ok && r1.value).toBe(2)
    const r2 = reader.readInt32()
    expect(r2.ok && r2.value).toBe(3)

    // partition tagged fields
    const pTag0 = reader.readTaggedFields()
    expect(pTag0.ok).toBe(true)

    // partition 1
    const p1Idx = reader.readInt32()
    expect(p1Idx.ok && p1Idx.value).toBe(1)

    const rep1Len = reader.readUnsignedVarInt()
    expect(rep1Len.ok && rep1Len.value).toBe(4) // 3 + 1

    const r3 = reader.readInt32()
    expect(r3.ok && r3.value).toBe(2)
    const r4 = reader.readInt32()
    expect(r4.ok && r4.value).toBe(3)
    const r5 = reader.readInt32()
    expect(r5.ok && r5.value).toBe(4)

    // partition tagged fields
    const pTag1 = reader.readTaggedFields()
    expect(pTag1.ok).toBe(true)

    // topic tagged fields
    const tTag = reader.readTaggedFields()
    expect(tTag.ok).toBe(true)

    // request tagged fields
    const rTag = reader.readTaggedFields()
    expect(rTag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes null replicas to cancel a pending reassignment", () => {
    const writer = new BinaryWriter()
    const request: AlterPartitionReassignmentsRequest = {
      timeoutMs: 10000,
      topics: [
        {
          name: "topic-b",
          partitions: [{ partitionIndex: 2, replicas: null }]
        }
      ]
    }
    encodeAlterPartitionReassignmentsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // timeout_ms
    const timeout = reader.readInt32()
    expect(timeout.ok && timeout.value).toBe(10000)

    // topics compact array
    const topicsLen = reader.readUnsignedVarInt()
    expect(topicsLen.ok && topicsLen.value).toBe(2)

    // topic name
    const name = reader.readCompactString()
    expect(name.ok && name.value).toBe("topic-b")

    // partitions compact array
    const partsLen = reader.readUnsignedVarInt()
    expect(partsLen.ok && partsLen.value).toBe(2)

    // partition index
    const pIdx = reader.readInt32()
    expect(pIdx.ok && pIdx.value).toBe(2)

    // replicas compact null (0)
    const repLen = reader.readUnsignedVarInt()
    expect(repLen.ok && repLen.value).toBe(0)

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

  it("encodes empty topics list", () => {
    const writer = new BinaryWriter()
    const request: AlterPartitionReassignmentsRequest = {
      timeoutMs: 5000,
      topics: []
    }
    encodeAlterPartitionReassignmentsRequest(writer, request, 0)
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

describe("buildAlterPartitionReassignmentsRequest", () => {
  it("produces a framed request with correct header", () => {
    const request: AlterPartitionReassignmentsRequest = {
      timeoutMs: 30000,
      topics: [
        {
          name: "test-topic",
          partitions: [{ partitionIndex: 0, replicas: [1, 2] }]
        }
      ]
    }
    const frame = buildAlterPartitionReassignmentsRequest(42, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    // size prefix
    const size = reader.readInt32()
    expect(size.ok).toBe(true)

    // API key
    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.AlterPartitionReassignments)

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

describe("decodeAlterPartitionReassignmentsResponse", () => {
  it("decodes throttle, error code, and per-topic/partition results", () => {
    const writer = new BinaryWriter()

    // throttle_time_ms
    writer.writeInt32(50)
    // error_code
    writer.writeInt16(0)
    // error_message (compact nullable string — null)
    writer.writeCompactString(null)
    // responses compact array (1 topic → length + 1 = 2)
    writer.writeUnsignedVarInt(2)
    // topic name
    writer.writeCompactString("topic-a")
    // partitions compact array (2 partitions → length + 1 = 3)
    writer.writeUnsignedVarInt(3)
    // partition 0
    writer.writeInt32(0)
    writer.writeInt16(0) // error_code
    writer.writeCompactString(null) // error_message
    writer.writeTaggedFields([])
    // partition 1
    writer.writeInt32(1)
    writer.writeInt16(0)
    writer.writeCompactString(null)
    writer.writeTaggedFields([])
    // topic tagged fields
    writer.writeTaggedFields([])
    // response tagged fields
    writer.writeTaggedFields([])

    const buf = writer.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterPartitionReassignmentsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(50)
    expect(result.value.errorCode).toBe(0)
    expect(result.value.errorMessage).toBeNull()
    expect(result.value.responses).toHaveLength(1)
    expect(result.value.responses[0].name).toBe("topic-a")
    expect(result.value.responses[0].partitions).toHaveLength(2)
    expect(result.value.responses[0].partitions[0].partitionIndex).toBe(0)
    expect(result.value.responses[0].partitions[0].errorCode).toBe(0)
    expect(result.value.responses[0].partitions[0].errorMessage).toBeNull()
    expect(result.value.responses[0].partitions[1].partitionIndex).toBe(1)
  })

  it("decodes top-level error code and message", () => {
    const writer = new BinaryWriter()

    writer.writeInt32(0) // throttle
    writer.writeInt16(87) // NOT_CONTROLLER
    writer.writeCompactString("not the controller")
    // responses — empty compact array (0 + 1 = 1)
    writer.writeUnsignedVarInt(1)
    // response tagged fields
    writer.writeTaggedFields([])

    const buf = writer.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterPartitionReassignmentsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.errorCode).toBe(87)
    expect(result.value.errorMessage).toBe("not the controller")
    expect(result.value.responses).toHaveLength(0)
  })

  it("decodes per-partition error codes and messages", () => {
    const writer = new BinaryWriter()

    writer.writeInt32(0) // throttle
    writer.writeInt16(0) // top-level error
    writer.writeCompactString(null) // top-level error message
    // responses compact array (1 topic)
    writer.writeUnsignedVarInt(2)
    writer.writeCompactString("topic-err")
    // partitions compact array (1 partition)
    writer.writeUnsignedVarInt(2)
    writer.writeInt32(0) // partition index
    writer.writeInt16(40) // INVALID_REPLICA_ASSIGNMENT
    writer.writeCompactString("invalid replica assignment")
    writer.writeTaggedFields([]) // partition tagged fields
    writer.writeTaggedFields([]) // topic tagged fields
    writer.writeTaggedFields([]) // response tagged fields

    const buf = writer.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterPartitionReassignmentsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.responses[0].partitions[0].errorCode).toBe(40)
    expect(result.value.responses[0].partitions[0].errorMessage).toBe("invalid replica assignment")
  })

  it("returns failure on truncated input", () => {
    const reader = new BinaryReader(new Uint8Array([0, 0]))
    const result = decodeAlterPartitionReassignmentsResponse(reader, 0)
    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated topic response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeUnsignedVarInt(2) // responses array (1 topic)
    w.writeCompactString("topic-a") // topic name
    // partitions missing
    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterPartitionReassignmentsResponse(reader, 0)
    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated partition response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeUnsignedVarInt(2) // responses array (1 topic)
    w.writeCompactString("topic-a")
    w.writeUnsignedVarInt(2) // partitions (1 partition)
    w.writeInt32(0) // partition_index
    // error_code missing
    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterPartitionReassignmentsResponse(reader, 0)
    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated error_message field", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    // error_message missing
    const buf = w.finish()
    const reader = new BinaryReader(buf)
    const result = decodeAlterPartitionReassignmentsResponse(reader, 0)
    expect(result.ok).toBe(false)
  })
})
