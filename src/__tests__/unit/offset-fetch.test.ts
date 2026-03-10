import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  buildOffsetFetchRequest,
  decodeOffsetFetchResponse,
  encodeOffsetFetchRequest,
  type OffsetFetchRequest
} from "../../offset-fetch"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeOffsetFetchRequest", () => {
  describe("v0 — group_id, topics with partition indexes", () => {
    it("encodes a basic offset fetch", () => {
      const writer = new BinaryWriter()
      const request: OffsetFetchRequest = {
        groupId: "test-group",
        topics: [{ name: "topic-a", partitionIndexes: [0, 1, 2] }]
      }
      encodeOffsetFetchRequest(writer, request, 0)
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
      const topicResult = reader.readString()
      expect(topicResult.ok).toBe(true)
      expect(topicResult.ok && topicResult.value).toBe("topic-a")

      // partition_indexes length
      const partCountResult = reader.readInt32()
      expect(partCountResult.ok).toBe(true)
      expect(partCountResult.ok && partCountResult.value).toBe(3)

      // partition indexes
      for (let i = 0; i < 3; i++) {
        const pResult = reader.readInt32()
        expect(pResult.ok).toBe(true)
        expect(pResult.ok && pResult.value).toBe(i)
      }

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v6 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: OffsetFetchRequest = {
        groupId: "flex-group",
        topics: [{ name: "t1", partitionIndexes: [0] }]
      }
      encodeOffsetFetchRequest(writer, request, 6)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id (compact string)
      const groupResult = reader.readCompactString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("flex-group")
    })
  })

  describe("v7 — null topics for all-partition fetch", () => {
    it("encodes null topics", () => {
      const writer = new BinaryWriter()
      const request: OffsetFetchRequest = {
        groupId: "grp",
        topics: null,
        requireStable: true
      }
      encodeOffsetFetchRequest(writer, request, 7)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id (compact for v7, since v6+)
      const groupResult = reader.readCompactString()
      expect(groupResult.ok).toBe(true)

      // null topics (compact null = varint 0)
      const nullArrayResult = reader.readUnsignedVarInt()
      expect(nullArrayResult.ok).toBe(true)
      expect(nullArrayResult.ok && nullArrayResult.value).toBe(0)

      // require_stable (BOOLEAN)
      const stableResult = reader.readBoolean()
      expect(stableResult.ok).toBe(true)
      expect(stableResult.ok && stableResult.value).toBe(true)
    })
  })
})

// ---------------------------------------------------------------------------
// buildOffsetFetchRequest (framed)
// ---------------------------------------------------------------------------

describe("buildOffsetFetchRequest", () => {
  it("builds a framed v0 request", () => {
    const request: OffsetFetchRequest = {
      groupId: "grp",
      topics: [{ name: "t", partitionIndexes: [0] }]
    }
    const framed = buildOffsetFetchRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.OffsetFetch)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeOffsetFetchResponse", () => {
  function buildResponseV0(
    topics: {
      name: string
      partitions: {
        partitionIndex: number
        committedOffset: bigint
        metadata: string | null
        errorCode: number
      }[]
    }[]
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(topics.length)
    for (const topic of topics) {
      w.writeString(topic.name)
      w.writeInt32(topic.partitions.length)
      for (const p of topic.partitions) {
        w.writeInt32(p.partitionIndex)
        w.writeInt64(p.committedOffset)
        w.writeString(p.metadata)
        w.writeInt16(p.errorCode)
      }
    }
    return w.finish()
  }

  function buildResponseV3(
    throttleTimeMs: number,
    topics: {
      name: string
      partitions: {
        partitionIndex: number
        committedOffset: bigint
        metadata: string | null
        errorCode: number
      }[]
    }[],
    errorCode: number
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(throttleTimeMs)
    w.writeInt32(topics.length)
    for (const topic of topics) {
      w.writeString(topic.name)
      w.writeInt32(topic.partitions.length)
      for (const p of topic.partitions) {
        w.writeInt32(p.partitionIndex)
        w.writeInt64(p.committedOffset)
        w.writeString(p.metadata)
        w.writeInt16(p.errorCode)
      }
    }
    w.writeInt16(errorCode)
    return w.finish()
  }

  function buildResponseV5(
    throttleTimeMs: number,
    topics: {
      name: string
      partitions: {
        partitionIndex: number
        committedOffset: bigint
        committedLeaderEpoch: number
        metadata: string | null
        errorCode: number
      }[]
    }[],
    errorCode: number
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(throttleTimeMs)
    w.writeInt32(topics.length)
    for (const topic of topics) {
      w.writeString(topic.name)
      w.writeInt32(topic.partitions.length)
      for (const p of topic.partitions) {
        w.writeInt32(p.partitionIndex)
        w.writeInt64(p.committedOffset)
        w.writeInt32(p.committedLeaderEpoch)
        w.writeString(p.metadata)
        w.writeInt16(p.errorCode)
      }
    }
    w.writeInt16(errorCode)
    return w.finish()
  }

  describe("v0 — non-flexible", () => {
    it("decodes a successful response", () => {
      const body = buildResponseV0([
        {
          name: "topic-a",
          partitions: [{ partitionIndex: 0, committedOffset: 42n, metadata: "meta", errorCode: 0 }]
        }
      ])
      const reader = new BinaryReader(body)
      const result = decodeOffsetFetchResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("topic-a")
      expect(result.value.topics[0].partitions[0].committedOffset).toBe(42n)
      expect(result.value.topics[0].partitions[0].metadata).toBe("meta")
    })

    it("decodes -1 offset as no committed offset", () => {
      const body = buildResponseV0([
        {
          name: "t",
          partitions: [{ partitionIndex: 0, committedOffset: -1n, metadata: null, errorCode: 0 }]
        }
      ])
      const reader = new BinaryReader(body)
      const result = decodeOffsetFetchResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].partitions[0].committedOffset).toBe(-1n)
      expect(result.value.topics[0].partitions[0].metadata).toBe(null)
    })
  })

  describe("v3 — with throttle time and error code", () => {
    it("decodes throttle time and top-level error", () => {
      const body = buildResponseV3(
        200,
        [
          {
            name: "t1",
            partitions: [{ partitionIndex: 0, committedOffset: 100n, metadata: null, errorCode: 0 }]
          }
        ],
        0
      )
      const reader = new BinaryReader(body)
      const result = decodeOffsetFetchResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(200)
      expect(result.value.errorCode).toBe(0)
    })
  })

  describe("v5 — with leader epoch", () => {
    it("decodes committed leader epoch", () => {
      const body = buildResponseV5(
        0,
        [
          {
            name: "t1",
            partitions: [
              {
                partitionIndex: 0,
                committedOffset: 50n,
                committedLeaderEpoch: 3,
                metadata: null,
                errorCode: 0
              }
            ]
          }
        ],
        0
      )
      const reader = new BinaryReader(body)
      const result = decodeOffsetFetchResponse(reader, 5)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].partitions[0].committedLeaderEpoch).toBe(3)
      expect(result.value.topics[0].partitions[0].committedOffset).toBe(50n)
    })
  })

  describe("v6 — flexible format response", () => {
    function buildResponseV6(
      throttleTimeMs: number,
      topics: {
        name: string
        partitions: {
          partitionIndex: number
          committedOffset: bigint
          committedLeaderEpoch: number
          metadata: string | null
          errorCode: number
        }[]
      }[],
      errorCode: number
    ): Uint8Array {
      const w = new BinaryWriter()
      w.writeInt32(throttleTimeMs)
      // topics (compact array: length+1)
      w.writeUnsignedVarInt(topics.length + 1)
      for (const topic of topics) {
        w.writeCompactString(topic.name)
        // partitions (compact array: length+1)
        w.writeUnsignedVarInt(topic.partitions.length + 1)
        for (const p of topic.partitions) {
          w.writeInt32(p.partitionIndex)
          w.writeInt64(p.committedOffset)
          w.writeInt32(p.committedLeaderEpoch) // v5+ leader epoch
          w.writeCompactString(p.metadata) // compact nullable metadata
          w.writeInt16(p.errorCode)
          w.writeUnsignedVarInt(0) // partition tagged fields
        }
        w.writeUnsignedVarInt(0) // topic tagged fields
      }
      w.writeInt16(errorCode) // top-level error code (v3+)
      w.writeUnsignedVarInt(0) // response tagged fields
      return w.finish()
    }

    it("decodes flexible format with compact strings and tagged fields", () => {
      const body = buildResponseV6(
        50,
        [
          {
            name: "topic-a",
            partitions: [
              {
                partitionIndex: 0,
                committedOffset: 100n,
                committedLeaderEpoch: 5,
                metadata: "meta-v6",
                errorCode: 0
              }
            ]
          }
        ],
        0
      )
      const reader = new BinaryReader(body)
      const result = decodeOffsetFetchResponse(reader, 6)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.topics).toHaveLength(1)
      expect(result.value.topics[0].name).toBe("topic-a")
      expect(result.value.topics[0].partitions[0].committedOffset).toBe(100n)
      expect(result.value.topics[0].partitions[0].committedLeaderEpoch).toBe(5)
      expect(result.value.topics[0].partitions[0].metadata).toBe("meta-v6")
    })

    it("decodes v6 with null metadata and multiple partitions", () => {
      const body = buildResponseV6(
        0,
        [
          {
            name: "t1",
            partitions: [
              {
                partitionIndex: 0,
                committedOffset: 42n,
                committedLeaderEpoch: -1,
                metadata: null,
                errorCode: 0
              },
              {
                partitionIndex: 1,
                committedOffset: -1n,
                committedLeaderEpoch: -1,
                metadata: null,
                errorCode: 0
              }
            ]
          }
        ],
        0
      )
      const reader = new BinaryReader(body)
      const result = decodeOffsetFetchResponse(reader, 6)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.topics[0].partitions).toHaveLength(2)
      expect(result.value.topics[0].partitions[0].committedOffset).toBe(42n)
      expect(result.value.topics[0].partitions[1].committedOffset).toBe(-1n)
    })
  })

  describe("v6 — flexible request encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: OffsetFetchRequest = {
        groupId: "compact-group",
        topics: [{ name: "t1", partitionIndexes: [0, 1] }]
      }
      encodeOffsetFetchRequest(writer, request, 6)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // group_id (compact string)
      const groupResult = reader.readCompactString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("compact-group")
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeOffsetFetchResponse(reader, 3)
      expect(result.ok).toBe(false)
    })
  })
})
