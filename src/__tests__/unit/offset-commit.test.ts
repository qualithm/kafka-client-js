/**
 * OffsetCommit (API key 8) request/response tests.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_OffsetCommit
 */

import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  buildOffsetCommitRequest,
  decodeOffsetCommitResponse,
  encodeOffsetCommitRequest,
  type OffsetCommitRequest
} from "../../offset-commit"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeOffsetCommitRequest", () => {
  describe("v0 — group_id, topics with offset + metadata", () => {
    it("encodes a basic offset commit", () => {
      const writer = new BinaryWriter()
      const request: OffsetCommitRequest = {
        groupId: "test-group",
        topics: [
          {
            name: "topic-a",
            partitions: [{ partitionIndex: 0, committedOffset: 42n, committedMetadata: null }]
          }
        ]
      }
      encodeOffsetCommitRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id
      const groupResult = reader.readString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("test-group")

      // topics array length
      const topicCountResult = reader.readInt32()
      expect(topicCountResult.ok).toBe(true)
      expect(topicCountResult.ok && topicCountResult.value).toBe(1)

      // topic name
      const topicNameResult = reader.readString()
      expect(topicNameResult.ok).toBe(true)
      expect(topicNameResult.ok && topicNameResult.value).toBe("topic-a")

      // partitions array length
      const partCountResult = reader.readInt32()
      expect(partCountResult.ok).toBe(true)
      expect(partCountResult.ok && partCountResult.value).toBe(1)

      // partition_index
      const partResult = reader.readInt32()
      expect(partResult.ok).toBe(true)
      expect(partResult.ok && partResult.value).toBe(0)

      // committed_offset
      const offsetResult = reader.readInt64()
      expect(offsetResult.ok).toBe(true)
      expect(offsetResult.ok && offsetResult.value).toBe(42n)

      // committed_metadata (nullable string)
      const metaResult = reader.readString()
      expect(metaResult.ok).toBe(true)
      expect(metaResult.ok && metaResult.value).toBe(null)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v1 — adds generation_id, member_id", () => {
    it("encodes with generation and member", () => {
      const writer = new BinaryWriter()
      const request: OffsetCommitRequest = {
        groupId: "grp",
        generationIdOrMemberEpoch: 5,
        memberId: "member-1",
        topics: [
          {
            name: "t",
            partitions: [{ partitionIndex: 0, committedOffset: 100n, committedMetadata: "meta" }]
          }
        ]
      }
      encodeOffsetCommitRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id
      reader.readString()
      // generation_id
      const genResult = reader.readInt32()
      expect(genResult.ok).toBe(true)
      expect(genResult.ok && genResult.value).toBe(5)
      // member_id
      const memberResult = reader.readString()
      expect(memberResult.ok).toBe(true)
      expect(memberResult.ok && memberResult.value).toBe("member-1")
    })
  })

  describe("v8 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: OffsetCommitRequest = {
        groupId: "grp-flex",
        generationIdOrMemberEpoch: 1,
        memberId: "m-1",
        topics: [
          {
            name: "topic-1",
            partitions: [{ partitionIndex: 0, committedOffset: 99n, committedMetadata: null }]
          }
        ]
      }
      encodeOffsetCommitRequest(writer, request, 8)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id (compact string)
      const groupResult = reader.readCompactString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("grp-flex")

      // generation_id
      const genResult = reader.readInt32()
      expect(genResult.ok).toBe(true)
      expect(genResult.ok && genResult.value).toBe(1)

      // member_id (compact string)
      const memberResult = reader.readCompactString()
      expect(memberResult.ok).toBe(true)
      expect(memberResult.ok && memberResult.value).toBe("m-1")
    })
  })
})

// ---------------------------------------------------------------------------
// buildOffsetCommitRequest (framed)
// ---------------------------------------------------------------------------

describe("buildOffsetCommitRequest", () => {
  it("builds a framed v0 request", () => {
    const request: OffsetCommitRequest = {
      groupId: "grp",
      topics: [
        {
          name: "t",
          partitions: [{ partitionIndex: 0, committedOffset: 10n, committedMetadata: null }]
        }
      ]
    }
    const framed = buildOffsetCommitRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)
    expect(framed.byteLength).toBeGreaterThan(0)

    const reader = new BinaryReader(framed)
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)

    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.OffsetCommit)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeOffsetCommitResponse", () => {
  function buildResponseV0(
    topics: { name: string; partitions: { partitionIndex: number; errorCode: number }[] }[]
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(topics.length)
    for (const topic of topics) {
      w.writeString(topic.name)
      w.writeInt32(topic.partitions.length)
      for (const p of topic.partitions) {
        w.writeInt32(p.partitionIndex)
        w.writeInt16(p.errorCode)
      }
    }
    return w.finish()
  }

  function buildResponseV3(
    throttleTimeMs: number,
    topics: { name: string; partitions: { partitionIndex: number; errorCode: number }[] }[]
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(throttleTimeMs)
    w.writeInt32(topics.length)
    for (const topic of topics) {
      w.writeString(topic.name)
      w.writeInt32(topic.partitions.length)
      for (const p of topic.partitions) {
        w.writeInt32(p.partitionIndex)
        w.writeInt16(p.errorCode)
      }
    }
    return w.finish()
  }

  function buildResponseV8(
    throttleTimeMs: number,
    topics: { name: string; partitions: { partitionIndex: number; errorCode: number }[] }[]
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(throttleTimeMs)
    w.writeUnsignedVarInt(topics.length + 1)
    for (const topic of topics) {
      w.writeCompactString(topic.name)
      w.writeUnsignedVarInt(topic.partitions.length + 1)
      for (const p of topic.partitions) {
        w.writeInt32(p.partitionIndex)
        w.writeInt16(p.errorCode)
        w.writeTaggedFields([])
      }
      w.writeTaggedFields([])
    }
    w.writeTaggedFields([])
    return w.finish()
  }

  describe("v0 — non-flexible", () => {
    it("decodes a successful response", () => {
      const body = buildResponseV0([
        { name: "topic-a", partitions: [{ partitionIndex: 0, errorCode: 0 }] }
      ])
      const reader = new BinaryReader(body)
      const result = decodeOffsetCommitResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("topic-a")
      expect(result.value.topics[0].partitions).toHaveLength(1)
      expect(result.value.topics[0].partitions[0].partitionIndex).toBe(0)
      expect(result.value.topics[0].partitions[0].errorCode).toBe(0)
    })
  })

  describe("v3 — with throttle time", () => {
    it("decodes throttle time and topics", () => {
      const body = buildResponseV3(100, [
        {
          name: "t1",
          partitions: [
            { partitionIndex: 0, errorCode: 0 },
            { partitionIndex: 1, errorCode: 0 }
          ]
        }
      ])
      const reader = new BinaryReader(body)
      const result = decodeOffsetCommitResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(100)
      expect(result.value.topics[0].partitions).toHaveLength(2)
    })
  })

  describe("v8 — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const body = buildResponseV8(50, [
        { name: "flex-topic", partitions: [{ partitionIndex: 0, errorCode: 0 }] }
      ])
      const reader = new BinaryReader(body)
      const result = decodeOffsetCommitResponse(reader, 8)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.topics[0].name).toBe("flex-topic")
      expect(result.value.topics[0].partitions[0].errorCode).toBe(0)
      expect(result.value.taggedFields).toEqual([])
    })
  })

  describe("error handling", () => {
    it("decodes a response with partition error codes", () => {
      const body = buildResponseV0([
        {
          name: "t",
          partitions: [
            { partitionIndex: 0, errorCode: 25 } // UNKNOWN_MEMBER_ID
          ]
        }
      ])
      const reader = new BinaryReader(body)
      const result = decodeOffsetCommitResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].partitions[0].errorCode).toBe(25)
    })

    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeOffsetCommitResponse(reader, 3)
      expect(result.ok).toBe(false)
    })
  })
})
