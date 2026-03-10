import { describe, expect, it } from "vitest"

import {
  type AddOffsetsToTxnRequest,
  buildAddOffsetsToTxnRequest,
  decodeAddOffsetsToTxnResponse,
  encodeAddOffsetsToTxnRequest
} from "../../add-offsets-to-txn"
import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeAddOffsetsToTxnRequest", () => {
  describe("v0 — non-flexible encoding", () => {
    it("encodes transactional_id, producer_id, epoch, and group_id", () => {
      const writer = new BinaryWriter()
      const request: AddOffsetsToTxnRequest = {
        transactionalId: "my-txn",
        producerId: 42n,
        producerEpoch: 1,
        groupId: "my-group"
      }
      encodeAddOffsetsToTxnRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const tidResult = reader.readString()
      expect(tidResult.ok).toBe(true)
      expect(tidResult.ok && tidResult.value).toBe("my-txn")

      const pidResult = reader.readInt64()
      expect(pidResult.ok).toBe(true)
      expect(pidResult.ok && pidResult.value).toBe(42n)

      const epochResult = reader.readInt16()
      expect(epochResult.ok).toBe(true)
      expect(epochResult.ok && epochResult.value).toBe(1)

      const groupResult = reader.readString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("my-group")

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v3 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: AddOffsetsToTxnRequest = {
        transactionalId: "txn-flex",
        producerId: 100n,
        producerEpoch: 5,
        groupId: "group-flex"
      }
      encodeAddOffsetsToTxnRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const tidResult = reader.readCompactString()
      expect(tidResult.ok).toBe(true)
      expect(tidResult.ok && tidResult.value).toBe("txn-flex")

      const pidResult = reader.readInt64()
      expect(pidResult.ok).toBe(true)
      expect(pidResult.ok && pidResult.value).toBe(100n)

      const epochResult = reader.readInt16()
      expect(epochResult.ok).toBe(true)
      expect(epochResult.ok && epochResult.value).toBe(5)

      const groupResult = reader.readCompactString()
      expect(groupResult.ok).toBe(true)
      expect(groupResult.ok && groupResult.value).toBe("group-flex")

      const tagsResult = reader.readTaggedFields()
      expect(tagsResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// Build (framed request)
// ---------------------------------------------------------------------------

describe("buildAddOffsetsToTxnRequest", () => {
  it("produces a size-prefixed frame with correct API key", () => {
    const request: AddOffsetsToTxnRequest = {
      transactionalId: "txn-1",
      producerId: 1n,
      producerEpoch: 0,
      groupId: "g"
    }
    const frame = buildAddOffsetsToTxnRequest(1, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)

    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.AddOffsetsToTxn)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeAddOffsetsToTxnResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes throttle_time_ms and error_code", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(25) // throttle_time_ms
      writer.writeInt16(0) // error_code

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeAddOffsetsToTxnResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(25)
      expect(result.value.errorCode).toBe(0)
    })
  })

  describe("v3 — flexible encoding", () => {
    it("decodes with tagged fields", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt16(47) // INVALID_PRODUCER_EPOCH
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeAddOffsetsToTxnResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.errorCode).toBe(47)
      expect(result.value.taggedFields).toEqual([])
    })
  })
})
