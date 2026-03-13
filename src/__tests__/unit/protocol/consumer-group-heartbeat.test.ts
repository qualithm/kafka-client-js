import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildConsumerGroupHeartbeatRequest,
  type ConsumerGroupHeartbeatRequest,
  decodeConsumerGroupHeartbeatResponse,
  encodeConsumerGroupHeartbeatRequest
} from "../../../protocol/consumer-group-heartbeat"

const TEST_UUID = new Uint8Array([
  0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10
])

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeConsumerGroupHeartbeatRequest", () => {
  it("encodes v0 join request (member_epoch = 0)", () => {
    const writer = new BinaryWriter()
    const request: ConsumerGroupHeartbeatRequest = {
      groupId: "my-group",
      memberId: "",
      memberEpoch: 0,
      instanceId: null,
      rackId: null,
      rebalanceTimeoutMs: 300000,
      subscribedTopicNames: ["topic-a", "topic-b"],
      serverAssignor: null,
      topicPartitions: null
    }
    encodeConsumerGroupHeartbeatRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // group_id
    const groupResult = reader.readCompactString()
    expect(groupResult.ok).toBe(true)
    expect(groupResult.ok && groupResult.value).toBe("my-group")

    // member_id
    const memberResult = reader.readCompactString()
    expect(memberResult.ok).toBe(true)
    expect(memberResult.ok && memberResult.value).toBe("")

    // member_epoch
    const epochResult = reader.readInt32()
    expect(epochResult.ok).toBe(true)
    expect(epochResult.ok && epochResult.value).toBe(0)

    // instance_id (null)
    const instanceResult = reader.readCompactString()
    expect(instanceResult.ok).toBe(true)
    expect(instanceResult.ok && instanceResult.value).toBeNull()

    // rack_id (null)
    const rackResult = reader.readCompactString()
    expect(rackResult.ok).toBe(true)
    expect(rackResult.ok && rackResult.value).toBeNull()

    // rebalance_timeout_ms
    const timeoutResult = reader.readInt32()
    expect(timeoutResult.ok).toBe(true)
    expect(timeoutResult.ok && timeoutResult.value).toBe(300000)

    // subscribed_topic_names array (compact: count + 1 = 3)
    const topicCountResult = reader.readUnsignedVarInt()
    expect(topicCountResult.ok).toBe(true)
    expect(topicCountResult.ok && topicCountResult.value).toBe(3) // 2 + 1

    const t1 = reader.readCompactString()
    expect(t1.ok).toBe(true)
    expect(t1.ok && t1.value).toBe("topic-a")
    const t2 = reader.readCompactString()
    expect(t2.ok).toBe(true)
    expect(t2.ok && t2.value).toBe("topic-b")

    // server_assignor (null)
    const assignorResult = reader.readCompactString()
    expect(assignorResult.ok).toBe(true)
    expect(assignorResult.ok && assignorResult.value).toBeNull()

    // topic_partitions (null array = varint 0)
    const tpCountResult = reader.readUnsignedVarInt()
    expect(tpCountResult.ok).toBe(true)
    expect(tpCountResult.ok && tpCountResult.value).toBe(0)

    // tagged fields
    const tagResult = reader.readTaggedFields()
    expect(tagResult.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 leave request (member_epoch = -1)", () => {
    const writer = new BinaryWriter()
    const request: ConsumerGroupHeartbeatRequest = {
      groupId: "my-group",
      memberId: "member-123",
      memberEpoch: -1,
      instanceId: null,
      rackId: null,
      rebalanceTimeoutMs: -1,
      subscribedTopicNames: null,
      serverAssignor: null,
      topicPartitions: null
    }
    encodeConsumerGroupHeartbeatRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // group_id
    const groupResult = reader.readCompactString()
    expect(groupResult.ok).toBe(true)
    expect(groupResult.ok && groupResult.value).toBe("my-group")

    // member_id
    const memberResult = reader.readCompactString()
    expect(memberResult.ok).toBe(true)
    expect(memberResult.ok && memberResult.value).toBe("member-123")

    // member_epoch (-1 = leave)
    const epochResult = reader.readInt32()
    expect(epochResult.ok).toBe(true)
    expect(epochResult.ok && epochResult.value).toBe(-1)

    // instance_id
    reader.readCompactString()
    // rack_id
    reader.readCompactString()
    // rebalance_timeout_ms
    reader.readInt32()

    // subscribed_topic_names (null array = varint 0)
    const topicsResult = reader.readUnsignedVarInt()
    expect(topicsResult.ok).toBe(true)
    expect(topicsResult.ok && topicsResult.value).toBe(0)

    // server_assignor (null)
    reader.readCompactString()

    // topic_partitions (null array)
    const tpResult = reader.readUnsignedVarInt()
    expect(tpResult.ok).toBe(true)
    expect(tpResult.ok && tpResult.value).toBe(0)

    // tagged fields
    const tagResult = reader.readTaggedFields()
    expect(tagResult.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 request with topic partitions", () => {
    const writer = new BinaryWriter()
    const request: ConsumerGroupHeartbeatRequest = {
      groupId: "group-1",
      memberId: "member-1",
      memberEpoch: 5,
      instanceId: "instance-1",
      rackId: "rack-a",
      rebalanceTimeoutMs: 60000,
      subscribedTopicNames: ["topic-x"],
      serverAssignor: "uniform",
      topicPartitions: [
        {
          topicId: TEST_UUID,
          partitions: [0, 1, 2]
        }
      ]
    }
    encodeConsumerGroupHeartbeatRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // group_id
    const groupResult = reader.readCompactString()
    expect(groupResult.ok && groupResult.value).toBe("group-1")

    // member_id
    const memberResult = reader.readCompactString()
    expect(memberResult.ok && memberResult.value).toBe("member-1")

    // member_epoch
    const epochResult = reader.readInt32()
    expect(epochResult.ok && epochResult.value).toBe(5)

    // instance_id
    const instanceResult = reader.readCompactString()
    expect(instanceResult.ok && instanceResult.value).toBe("instance-1")

    // rack_id
    const rackResult = reader.readCompactString()
    expect(rackResult.ok && rackResult.value).toBe("rack-a")

    // rebalance_timeout_ms
    const timeoutResult = reader.readInt32()
    expect(timeoutResult.ok && timeoutResult.value).toBe(60000)

    // subscribed_topic_names (1 topic → varint 2)
    const topicsCountResult = reader.readUnsignedVarInt()
    expect(topicsCountResult.ok && topicsCountResult.value).toBe(2)
    const t1 = reader.readCompactString()
    expect(t1.ok && t1.value).toBe("topic-x")

    // server_assignor
    const assignorResult = reader.readCompactString()
    expect(assignorResult.ok && assignorResult.value).toBe("uniform")

    // topic_partitions (1 entry → varint 2)
    const tpCountResult = reader.readUnsignedVarInt()
    expect(tpCountResult.ok && tpCountResult.value).toBe(2)

    // topic_id (UUID)
    const topicIdResult = reader.readUuid()
    expect(topicIdResult.ok).toBe(true)
    if (topicIdResult.ok) {
      expect(topicIdResult.value).toEqual(TEST_UUID)
    }

    // partitions (3 entries → varint 4)
    const partCountResult = reader.readUnsignedVarInt()
    expect(partCountResult.ok && partCountResult.value).toBe(4)
    for (let i = 0; i < 3; i++) {
      const pResult = reader.readInt32()
      expect(pResult.ok).toBe(true)
      expect(pResult.ok && pResult.value).toBe(i)
    }

    // tp tagged fields
    const tpTagResult = reader.readTaggedFields()
    expect(tpTagResult.ok).toBe(true)

    // request tagged fields
    const tagResult = reader.readTaggedFields()
    expect(tagResult.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// buildConsumerGroupHeartbeatRequest (framed)
// ---------------------------------------------------------------------------

describe("buildConsumerGroupHeartbeatRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildConsumerGroupHeartbeatRequest(
      42,
      0,
      {
        groupId: "test-group",
        memberId: "",
        memberEpoch: 0,
        instanceId: null,
        rackId: null,
        rebalanceTimeoutMs: 300000,
        subscribedTopicNames: ["test-topic"],
        serverAssignor: null,
        topicPartitions: null
      },
      "test-client"
    )
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)

    // size prefix
    reader.readInt32()

    // api key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.ConsumerGroupHeartbeat)

    // api version
    const versionResult = reader.readInt16()
    expect(versionResult.ok).toBe(true)
    expect(versionResult.ok && versionResult.value).toBe(0)

    // correlation id
    const corrResult = reader.readInt32()
    expect(corrResult.ok).toBe(true)
    expect(corrResult.ok && corrResult.value).toBe(42)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeConsumerGroupHeartbeatResponse", () => {
  it("decodes v0 response with assignment", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeCompactString("member-abc") // member_id
    w.writeInt32(3) // member_epoch
    w.writeInt32(5000) // heartbeat_interval_ms
    // assignment: topic_partitions array (1 entry → varint 2)
    w.writeUnsignedVarInt(2)
    // topic_id (UUID)
    w.writeUuid(TEST_UUID)
    // partitions (2 entries → varint 3)
    w.writeUnsignedVarInt(3)
    w.writeInt32(0)
    w.writeInt32(1)
    // tp tagged fields
    w.writeTaggedFields([])
    // assignment tagged fields
    w.writeTaggedFields([])
    // response tagged fields
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeConsumerGroupHeartbeatResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.errorCode).toBe(0)
    expect(result.value.errorMessage).toBeNull()
    expect(result.value.memberId).toBe("member-abc")
    expect(result.value.memberEpoch).toBe(3)
    expect(result.value.heartbeatIntervalMs).toBe(5000)
    expect(result.value.assignment).not.toBeNull()
    expect(result.value.assignment!.topicPartitions).toHaveLength(1)
    expect(result.value.assignment!.topicPartitions[0].topicId).toEqual(TEST_UUID)
    expect(result.value.assignment!.topicPartitions[0].partitions).toEqual([0, 1])
  })

  it("decodes v0 response with null assignment", () => {
    const w = new BinaryWriter()
    w.writeInt32(10) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeCompactString("member-xyz") // member_id
    w.writeInt32(7) // member_epoch
    w.writeInt32(3000) // heartbeat_interval_ms
    // assignment: null topic_partitions (varint 0)
    w.writeUnsignedVarInt(0)
    // assignment tagged fields
    w.writeTaggedFields([])
    // response tagged fields
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeConsumerGroupHeartbeatResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(10)
    expect(result.value.memberId).toBe("member-xyz")
    expect(result.value.memberEpoch).toBe(7)
    expect(result.value.heartbeatIntervalMs).toBe(3000)
    expect(result.value.assignment).toBeNull()
  })

  it("decodes v0 error response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(69) // error_code (UNKNOWN_MEMBER_ID)
    w.writeCompactString("unknown member") // error_message
    w.writeCompactString(null) // member_id
    w.writeInt32(-1) // member_epoch
    w.writeInt32(5000) // heartbeat_interval_ms
    // null assignment
    w.writeUnsignedVarInt(0)
    w.writeTaggedFields([])
    // response tagged fields
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeConsumerGroupHeartbeatResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.errorCode).toBe(69)
    expect(result.value.errorMessage).toBe("unknown member")
    expect(result.value.memberId).toBeNull()
    expect(result.value.memberEpoch).toBe(-1)
  })

  it("returns error for truncated response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // truncated — missing error_code and rest

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeConsumerGroupHeartbeatResponse(reader, 0)

    expect(result.ok).toBe(false)
  })

  it("decodes v0 response with multiple topic partitions in assignment", () => {
    const uuid2 = new Uint8Array([
      0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20
    ])

    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeCompactString("member-multi") // member_id
    w.writeInt32(1) // member_epoch
    w.writeInt32(5000) // heartbeat_interval_ms
    // assignment: 2 topic partitions (varint 3)
    w.writeUnsignedVarInt(3)
    // first topic partition
    w.writeUuid(TEST_UUID)
    w.writeUnsignedVarInt(2) // 1 partition
    w.writeInt32(0)
    w.writeTaggedFields([])
    // second topic partition
    w.writeUuid(uuid2)
    w.writeUnsignedVarInt(4) // 3 partitions
    w.writeInt32(0)
    w.writeInt32(1)
    w.writeInt32(2)
    w.writeTaggedFields([])
    // assignment tagged fields
    w.writeTaggedFields([])
    // response tagged fields
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeConsumerGroupHeartbeatResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.assignment).not.toBeNull()
    expect(result.value.assignment!.topicPartitions).toHaveLength(2)
    expect(result.value.assignment!.topicPartitions[0].topicId).toEqual(TEST_UUID)
    expect(result.value.assignment!.topicPartitions[0].partitions).toEqual([0])
    expect(result.value.assignment!.topicPartitions[1].topicId).toEqual(uuid2)
    expect(result.value.assignment!.topicPartitions[1].partitions).toEqual([0, 1, 2])
  })
})
