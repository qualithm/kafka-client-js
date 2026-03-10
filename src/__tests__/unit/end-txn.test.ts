import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  buildEndTxnRequest,
  decodeEndTxnResponse,
  encodeEndTxnRequest,
  type EndTxnRequest
} from "../../end-txn"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeEndTxnRequest", () => {
  describe("v0 — non-flexible encoding", () => {
    it("encodes commit transaction", () => {
      const writer = new BinaryWriter()
      const request: EndTxnRequest = {
        transactionalId: "my-txn",
        producerId: 42n,
        producerEpoch: 1,
        committed: true
      }
      encodeEndTxnRequest(writer, request, 0)
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

      const committedResult = reader.readBoolean()
      expect(committedResult.ok).toBe(true)
      expect(committedResult.ok && committedResult.value).toBe(true)

      expect(reader.remaining).toBe(0)
    })

    it("encodes abort transaction", () => {
      const writer = new BinaryWriter()
      const request: EndTxnRequest = {
        transactionalId: "my-txn",
        producerId: 42n,
        producerEpoch: 1,
        committed: false
      }
      encodeEndTxnRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readString() // skip transactional_id
      reader.readInt64() // skip producer_id
      reader.readInt16() // skip producer_epoch

      const committedResult = reader.readBoolean()
      expect(committedResult.ok).toBe(true)
      expect(committedResult.ok && committedResult.value).toBe(false)
    })
  })

  describe("v3 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: EndTxnRequest = {
        transactionalId: "txn-flex",
        producerId: 100n,
        producerEpoch: 3,
        committed: true
      }
      encodeEndTxnRequest(writer, request, 3)
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
      expect(epochResult.ok && epochResult.value).toBe(3)

      const committedResult = reader.readBoolean()
      expect(committedResult.ok).toBe(true)
      expect(committedResult.ok && committedResult.value).toBe(true)

      const tagsResult = reader.readTaggedFields()
      expect(tagsResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// Build (framed request)
// ---------------------------------------------------------------------------

describe("buildEndTxnRequest", () => {
  it("produces a size-prefixed frame with correct API key", () => {
    const request: EndTxnRequest = {
      transactionalId: "txn-1",
      producerId: 1n,
      producerEpoch: 0,
      committed: true
    }
    const frame = buildEndTxnRequest(1, 0, request, "test-client")
    const reader = new BinaryReader(frame)

    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)

    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.EndTxn)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeEndTxnResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes successful commit response", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(10) // throttle_time_ms
      writer.writeInt16(0) // error_code (success)

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeEndTxnResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(10)
      expect(result.value.errorCode).toBe(0)
    })

    it("decodes error response", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0)
      writer.writeInt16(47) // INVALID_PRODUCER_EPOCH

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeEndTxnResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(47)
    })
  })

  describe("v3 — flexible encoding", () => {
    it("decodes with tagged fields", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0)
      writer.writeInt16(0)
      writer.writeTaggedFields([])

      const buf = writer.finish()
      const reader = new BinaryReader(buf)
      const result = decodeEndTxnResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.taggedFields).toEqual([])
    })
  })
})
