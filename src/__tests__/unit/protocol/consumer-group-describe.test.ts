import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildConsumerGroupDescribeRequest,
  type ConsumerGroupDescribeRequest,
  decodeConsumerGroupDescribeResponse,
  encodeConsumerGroupDescribeRequest
} from "../../../protocol/consumer-group-describe"

const TEST_UUID = new Uint8Array([
  0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10
])

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeConsumerGroupDescribeRequest", () => {
  it("encodes v0 request with single group", () => {
    const writer = new BinaryWriter()
    const request: ConsumerGroupDescribeRequest = {
      groupIds: ["my-group"],
      includeAuthorizedOperations: false
    }
    encodeConsumerGroupDescribeRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // group_ids (compact array: 1 + 1 = 2)
    const countResult = reader.readUnsignedVarInt()
    expect(countResult.ok).toBe(true)
    expect(countResult.ok && countResult.value).toBe(2)

    const g1 = reader.readCompactString()
    expect(g1.ok).toBe(true)
    expect(g1.ok && g1.value).toBe("my-group")

    // include_authorized_operations (false = 0)
    const boolResult = reader.readInt8()
    expect(boolResult.ok).toBe(true)
    expect(boolResult.ok && boolResult.value).toBe(0)

    // tagged fields
    const tagResult = reader.readTaggedFields()
    expect(tagResult.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 request with multiple groups and authorized operations", () => {
    const writer = new BinaryWriter()
    const request: ConsumerGroupDescribeRequest = {
      groupIds: ["group-a", "group-b", "group-c"],
      includeAuthorizedOperations: true
    }
    encodeConsumerGroupDescribeRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // group_ids (compact array: 3 + 1 = 4)
    const countResult = reader.readUnsignedVarInt()
    expect(countResult.ok).toBe(true)
    expect(countResult.ok && countResult.value).toBe(4)

    const g1 = reader.readCompactString()
    expect(g1.ok && g1.value).toBe("group-a")
    const g2 = reader.readCompactString()
    expect(g2.ok && g2.value).toBe("group-b")
    const g3 = reader.readCompactString()
    expect(g3.ok && g3.value).toBe("group-c")

    // include_authorized_operations (true = 1)
    const boolResult = reader.readInt8()
    expect(boolResult.ok).toBe(true)
    expect(boolResult.ok && boolResult.value).toBe(1)

    // tagged fields
    const tagResult = reader.readTaggedFields()
    expect(tagResult.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 request with empty group list", () => {
    const writer = new BinaryWriter()
    const request: ConsumerGroupDescribeRequest = {
      groupIds: [],
      includeAuthorizedOperations: false
    }
    encodeConsumerGroupDescribeRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // group_ids (compact array: 0 + 1 = 1)
    const countResult = reader.readUnsignedVarInt()
    expect(countResult.ok).toBe(true)
    expect(countResult.ok && countResult.value).toBe(1)

    // include_authorized_operations
    const boolResult = reader.readInt8()
    expect(boolResult.ok).toBe(true)
    expect(boolResult.ok && boolResult.value).toBe(0)

    // tagged fields
    const tagResult = reader.readTaggedFields()
    expect(tagResult.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// buildConsumerGroupDescribeRequest (framed)
// ---------------------------------------------------------------------------

describe("buildConsumerGroupDescribeRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildConsumerGroupDescribeRequest(
      42,
      0,
      {
        groupIds: ["test-group"],
        includeAuthorizedOperations: true
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
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.ConsumerGroupDescribe)

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

describe("decodeConsumerGroupDescribeResponse", () => {
  /**
   * Helper to write a TopicPartitions struct.
   */
  function writeTopicPartition(
    w: BinaryWriter,
    topicId: Uint8Array,
    topicName: string,
    partitions: number[]
  ): void {
    w.writeUuid(topicId)
    w.writeCompactString(topicName)
    w.writeUnsignedVarInt(partitions.length + 1)
    for (const p of partitions) {
      w.writeInt32(p)
    }
    w.writeTaggedFields([])
  }

  /**
   * Helper to write an Assignment struct.
   */
  function writeAssignment(
    w: BinaryWriter,
    topicPartitions: { topicId: Uint8Array; topicName: string; partitions: number[] }[]
  ): void {
    w.writeUnsignedVarInt(topicPartitions.length + 1)
    for (const tp of topicPartitions) {
      writeTopicPartition(w, tp.topicId, tp.topicName, tp.partitions)
    }
    w.writeTaggedFields([])
  }

  it("decodes v0 response with one group and one member", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // groups array (1 group → varint 2)
    w.writeUnsignedVarInt(2)

    // --- group 0 ---
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeCompactString("my-group") // group_id
    w.writeCompactString("stable") // group_state
    w.writeInt32(5) // group_epoch
    w.writeInt32(5) // assignment_epoch
    w.writeCompactString("uniform") // assignor_name

    // members (1 member → varint 2)
    w.writeUnsignedVarInt(2)
    // member 0
    w.writeCompactString("member-1") // member_id
    w.writeCompactString(null) // instance_id
    w.writeCompactString("rack-a") // rack_id
    w.writeInt32(5) // member_epoch
    w.writeCompactString("client-1") // client_id
    w.writeCompactString("/192.168.1.1") // client_host
    // subscribed_topic_names (1 topic → varint 2)
    w.writeUnsignedVarInt(2)
    w.writeCompactString("topic-a")
    // subscribed_topic_regex
    w.writeCompactString(null)
    // assignment
    writeAssignment(w, [{ topicId: TEST_UUID, topicName: "topic-a", partitions: [0, 1] }])
    // target_assignment
    writeAssignment(w, [{ topicId: TEST_UUID, topicName: "topic-a", partitions: [0, 1, 2] }])
    // member tagged fields
    w.writeTaggedFields([])

    w.writeInt32(-2147483648) // authorized_operations
    // group tagged fields
    w.writeTaggedFields([])

    // response tagged fields
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeConsumerGroupDescribeResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.groups).toHaveLength(1)

    const group = result.value.groups[0]
    expect(group.errorCode).toBe(0)
    expect(group.errorMessage).toBeNull()
    expect(group.groupId).toBe("my-group")
    expect(group.groupState).toBe("stable")
    expect(group.groupEpoch).toBe(5)
    expect(group.assignmentEpoch).toBe(5)
    expect(group.assignorName).toBe("uniform")
    expect(group.authorizedOperations).toBe(-2147483648)

    expect(group.members).toHaveLength(1)
    const member = group.members[0]
    expect(member.memberId).toBe("member-1")
    expect(member.instanceId).toBeNull()
    expect(member.rackId).toBe("rack-a")
    expect(member.memberEpoch).toBe(5)
    expect(member.clientId).toBe("client-1")
    expect(member.clientHost).toBe("/192.168.1.1")
    expect(member.subscribedTopicNames).toEqual(["topic-a"])
    expect(member.subscribedTopicRegex).toBeNull()

    // current assignment
    expect(member.assignment.topicPartitions).toHaveLength(1)
    expect(member.assignment.topicPartitions[0].topicId).toEqual(TEST_UUID)
    expect(member.assignment.topicPartitions[0].topicName).toBe("topic-a")
    expect(member.assignment.topicPartitions[0].partitions).toEqual([0, 1])

    // target assignment
    expect(member.targetAssignment.topicPartitions).toHaveLength(1)
    expect(member.targetAssignment.topicPartitions[0].partitions).toEqual([0, 1, 2])
  })

  it("decodes v0 response with empty group (no members)", () => {
    const w = new BinaryWriter()
    w.writeInt32(5) // throttle_time_ms
    // groups array (1 group → varint 2)
    w.writeUnsignedVarInt(2)

    // --- group 0 ---
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeCompactString("empty-group") // group_id
    w.writeCompactString("empty") // group_state
    w.writeInt32(0) // group_epoch
    w.writeInt32(0) // assignment_epoch
    w.writeCompactString("") // assignor_name

    // members (0 members → varint 1)
    w.writeUnsignedVarInt(1)

    w.writeInt32(0x0f) // authorized_operations
    // group tagged fields
    w.writeTaggedFields([])

    // response tagged fields
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeConsumerGroupDescribeResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(5)
    expect(result.value.groups).toHaveLength(1)

    const group = result.value.groups[0]
    expect(group.errorCode).toBe(0)
    expect(group.groupId).toBe("empty-group")
    expect(group.groupState).toBe("empty")
    expect(group.members).toHaveLength(0)
    expect(group.authorizedOperations).toBe(0x0f)
  })

  it("decodes v0 error response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // groups array (1 group → varint 2)
    w.writeUnsignedVarInt(2)

    // --- group 0 with error ---
    w.writeInt16(69) // error_code (GROUP_ID_NOT_FOUND)
    w.writeCompactString("group not found") // error_message
    w.writeCompactString("missing-group") // group_id
    w.writeCompactString("") // group_state
    w.writeInt32(0) // group_epoch
    w.writeInt32(0) // assignment_epoch
    w.writeCompactString("") // assignor_name

    // members (0 members → varint 1)
    w.writeUnsignedVarInt(1)

    w.writeInt32(-2147483648) // authorized_operations
    // group tagged fields
    w.writeTaggedFields([])

    // response tagged fields
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeConsumerGroupDescribeResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.groups).toHaveLength(1)
    const group = result.value.groups[0]
    expect(group.errorCode).toBe(69)
    expect(group.errorMessage).toBe("group not found")
    expect(group.groupId).toBe("missing-group")
  })

  it("decodes v0 response with multiple groups", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // groups array (2 groups → varint 3)
    w.writeUnsignedVarInt(3)

    // --- group 0 ---
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeCompactString("group-a") // group_id
    w.writeCompactString("stable") // group_state
    w.writeInt32(3) // group_epoch
    w.writeInt32(3) // assignment_epoch
    w.writeCompactString("uniform") // assignor_name
    w.writeUnsignedVarInt(1) // 0 members
    w.writeInt32(-2147483648) // authorized_operations
    w.writeTaggedFields([])

    // --- group 1 ---
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeCompactString("group-b") // group_id
    w.writeCompactString("reconciling") // group_state
    w.writeInt32(7) // group_epoch
    w.writeInt32(6) // assignment_epoch
    w.writeCompactString("range") // assignor_name
    w.writeUnsignedVarInt(1) // 0 members
    w.writeInt32(-2147483648) // authorized_operations
    w.writeTaggedFields([])

    // response tagged fields
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeConsumerGroupDescribeResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.groups).toHaveLength(2)
    expect(result.value.groups[0].groupId).toBe("group-a")
    expect(result.value.groups[0].groupState).toBe("stable")
    expect(result.value.groups[0].groupEpoch).toBe(3)
    expect(result.value.groups[1].groupId).toBe("group-b")
    expect(result.value.groups[1].groupState).toBe("reconciling")
    expect(result.value.groups[1].assignmentEpoch).toBe(6)
    expect(result.value.groups[1].assignorName).toBe("range")
  })

  it("decodes v0 response with member having empty assignments", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // groups array (1 group → varint 2)
    w.writeUnsignedVarInt(2)

    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeCompactString("my-group") // group_id
    w.writeCompactString("reconciling") // group_state
    w.writeInt32(2) // group_epoch
    w.writeInt32(1) // assignment_epoch
    w.writeCompactString("uniform") // assignor_name

    // members (1 member → varint 2)
    w.writeUnsignedVarInt(2)
    w.writeCompactString("member-x") // member_id
    w.writeCompactString("instance-1") // instance_id
    w.writeCompactString(null) // rack_id
    w.writeInt32(1) // member_epoch
    w.writeCompactString("my-client") // client_id
    w.writeCompactString("/10.0.0.1") // client_host
    // subscribed_topic_names (2 topics → varint 3)
    w.writeUnsignedVarInt(3)
    w.writeCompactString("topic-x")
    w.writeCompactString("topic-y")
    // subscribed_topic_regex
    w.writeCompactString("topic-.*")
    // assignment (empty)
    writeAssignment(w, [])
    // target_assignment (has partitions)
    writeAssignment(w, [{ topicId: TEST_UUID, topicName: "topic-x", partitions: [0] }])
    // member tagged fields
    w.writeTaggedFields([])

    w.writeInt32(-2147483648) // authorized_operations
    w.writeTaggedFields([])

    // response tagged fields
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeConsumerGroupDescribeResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    const member = result.value.groups[0].members[0]
    expect(member.memberId).toBe("member-x")
    expect(member.instanceId).toBe("instance-1")
    expect(member.rackId).toBeNull()
    expect(member.subscribedTopicNames).toEqual(["topic-x", "topic-y"])
    expect(member.subscribedTopicRegex).toBe("topic-.*")
    expect(member.assignment.topicPartitions).toHaveLength(0)
    expect(member.targetAssignment.topicPartitions).toHaveLength(1)
    expect(member.targetAssignment.topicPartitions[0].partitions).toEqual([0])
  })
})
