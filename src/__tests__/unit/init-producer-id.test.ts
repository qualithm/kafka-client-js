import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  buildInitProducerIdRequest,
  decodeInitProducerIdResponse,
  encodeInitProducerIdRequest,
  type InitProducerIdRequest
} from "../../init-producer-id"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeInitProducerIdRequest", () => {
  describe("v0 — transactional_id, transaction_timeout_ms", () => {
    it("encodes null transactional_id for idempotent-only producer", () => {
      const writer = new BinaryWriter()
      const request: InitProducerIdRequest = {
        transactionalId: null,
        transactionTimeoutMs: -1
      }
      encodeInitProducerIdRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id (nullable string, -1 = null)
      const tidResult = reader.readString()
      expect(tidResult.ok).toBe(true)
      expect(tidResult.ok && tidResult.value).toBe(null)

      // transaction_timeout_ms
      const timeoutResult = reader.readInt32()
      expect(timeoutResult.ok).toBe(true)
      expect(timeoutResult.ok && timeoutResult.value).toBe(-1)

      expect(reader.remaining).toBe(0)
    })

    it("encodes transactional_id for transactional producer", () => {
      const writer = new BinaryWriter()
      const request: InitProducerIdRequest = {
        transactionalId: "my-txn-id",
        transactionTimeoutMs: 60000
      }
      encodeInitProducerIdRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const tidResult = reader.readString()
      expect(tidResult.ok).toBe(true)
      expect(tidResult.ok && tidResult.value).toBe("my-txn-id")

      const timeoutResult = reader.readInt32()
      expect(timeoutResult.ok).toBe(true)
      expect(timeoutResult.ok && timeoutResult.value).toBe(60000)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("encodes with compact string and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: InitProducerIdRequest = {
        transactionalId: null,
        transactionTimeoutMs: -1
      }
      encodeInitProducerIdRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id (compact nullable string)
      const tidResult = reader.readCompactString()
      expect(tidResult.ok).toBe(true)
      expect(tidResult.ok && tidResult.value).toBe(null)

      // transaction_timeout_ms
      const timeoutResult = reader.readInt32()
      expect(timeoutResult.ok).toBe(true)
      expect(timeoutResult.ok && timeoutResult.value).toBe(-1)

      // tagged fields (empty)
      const tagsResult = reader.readTaggedFields()
      expect(tagsResult.ok).toBe(true)
      expect(tagsResult.ok && tagsResult.value).toEqual([])

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v3 — adds producer_id and producer_epoch", () => {
    it("encodes new producer session with -1 id and epoch", () => {
      const writer = new BinaryWriter()
      const request: InitProducerIdRequest = {
        transactionalId: null,
        transactionTimeoutMs: -1,
        producerId: -1n,
        producerEpoch: -1
      }
      encodeInitProducerIdRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id (compact, v3 is flexible)
      const tidResult = reader.readCompactString()
      expect(tidResult.ok).toBe(true)
      expect(tidResult.ok && tidResult.value).toBe(null)

      // transaction_timeout_ms
      const timeoutResult = reader.readInt32()
      expect(timeoutResult.ok).toBe(true)
      expect(timeoutResult.ok && timeoutResult.value).toBe(-1)

      // producer_id (INT64)
      const pidResult = reader.readInt64()
      expect(pidResult.ok).toBe(true)
      expect(pidResult.ok && pidResult.value).toBe(-1n)

      // producer_epoch (INT16)
      const epochResult = reader.readInt16()
      expect(epochResult.ok).toBe(true)
      expect(epochResult.ok && epochResult.value).toBe(-1)

      // tagged fields
      const tagsResult = reader.readTaggedFields()
      expect(tagsResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })

    it("encodes existing producer session with known id and epoch", () => {
      const writer = new BinaryWriter()
      const request: InitProducerIdRequest = {
        transactionalId: "txn-1",
        transactionTimeoutMs: 30000,
        producerId: 12345n,
        producerEpoch: 5
      }
      encodeInitProducerIdRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const tidResult = reader.readCompactString()
      expect(tidResult.ok).toBe(true)
      expect(tidResult.ok && tidResult.value).toBe("txn-1")

      const timeoutResult = reader.readInt32()
      expect(timeoutResult.ok).toBe(true)
      expect(timeoutResult.ok && timeoutResult.value).toBe(30000)

      const pidResult = reader.readInt64()
      expect(pidResult.ok).toBe(true)
      expect(pidResult.ok && pidResult.value).toBe(12345n)

      const epochResult = reader.readInt16()
      expect(epochResult.ok).toBe(true)
      expect(epochResult.ok && epochResult.value).toBe(5)

      const tagsResult = reader.readTaggedFields()
      expect(tagsResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v0 omits producer_id and producer_epoch", () => {
    it("does not write producer_id or producer_epoch in v0", () => {
      const writer = new BinaryWriter()
      const request: InitProducerIdRequest = {
        transactionalId: null,
        transactionTimeoutMs: -1,
        producerId: 100n, // should be ignored
        producerEpoch: 7 // should be ignored
      }
      encodeInitProducerIdRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // transactional_id
      reader.readString()
      // transaction_timeout_ms
      reader.readInt32()

      // No more data
      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// buildInitProducerIdRequest (framed)
// ---------------------------------------------------------------------------

describe("buildInitProducerIdRequest", () => {
  it("builds a framed v0 request", () => {
    const request: InitProducerIdRequest = {
      transactionalId: null,
      transactionTimeoutMs: -1
    }
    const framed = buildInitProducerIdRequest(42, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)
    expect(framed.byteLength).toBeGreaterThan(0)

    // Verify the outer frame: size prefix (INT32) + api_key (INT16) should be InitProducerId
    const reader = new BinaryReader(framed)
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)

    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.InitProducerId)
  })

  it("builds a framed v3 request with producer_id and epoch", () => {
    const request: InitProducerIdRequest = {
      transactionalId: "txn-abc",
      transactionTimeoutMs: 5000,
      producerId: 999n,
      producerEpoch: 3
    }
    const framed = buildInitProducerIdRequest(100, 3, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)
    expect(framed.byteLength).toBeGreaterThan(0)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeInitProducerIdResponse", () => {
  /** Build a v0 response body. */
  function buildResponseV0(
    throttleTimeMs: number,
    errorCode: number,
    producerId: bigint,
    producerEpoch: number
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(throttleTimeMs)
    w.writeInt16(errorCode)
    w.writeInt64(producerId)
    w.writeInt16(producerEpoch)
    return w.finish()
  }

  /** Build a v2+ flexible response body. */
  function buildResponseV2(
    throttleTimeMs: number,
    errorCode: number,
    producerId: bigint,
    producerEpoch: number
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(throttleTimeMs)
    w.writeInt16(errorCode)
    w.writeInt64(producerId)
    w.writeInt16(producerEpoch)
    w.writeTaggedFields([]) // empty tagged fields
    return w.finish()
  }

  describe("v0 — non-flexible", () => {
    it("decodes a successful response", () => {
      const body = buildResponseV0(0, 0, 1000n, 0)
      const reader = new BinaryReader(body)
      const result = decodeInitProducerIdResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.producerId).toBe(1000n)
      expect(result.value.producerEpoch).toBe(0)
      expect(result.value.taggedFields).toEqual([])
    })

    it("decodes a response with throttle time", () => {
      const body = buildResponseV0(50, 0, 2000n, 1)
      const reader = new BinaryReader(body)
      const result = decodeInitProducerIdResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.producerId).toBe(2000n)
      expect(result.value.producerEpoch).toBe(1)
    })

    it("decodes a response with error code", () => {
      const body = buildResponseV0(0, 15, -1n, -1)
      const reader = new BinaryReader(body)
      const result = decodeInitProducerIdResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(15) // NOT_COORDINATOR
      expect(result.value.producerId).toBe(-1n)
      expect(result.value.producerEpoch).toBe(-1)
    })

    it("reports bytesRead correctly", () => {
      const body = buildResponseV0(0, 0, 42n, 0)
      const reader = new BinaryReader(body)
      const result = decodeInitProducerIdResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }
      expect(result.bytesRead).toBe(body.byteLength)
      expect(reader.remaining).toBe(0)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("decodes a successful flexible response", () => {
      const body = buildResponseV2(0, 0, 5000n, 0)
      const reader = new BinaryReader(body)
      const result = decodeInitProducerIdResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.producerId).toBe(5000n)
      expect(result.value.producerEpoch).toBe(0)
      expect(result.value.taggedFields).toEqual([])
    })

    it("decodes with tagged fields present", () => {
      const body = buildResponseV2(10, 0, 99n, 2)
      const reader = new BinaryReader(body)
      const result = decodeInitProducerIdResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(10)
      expect(result.value.producerId).toBe(99n)
      expect(result.value.producerEpoch).toBe(2)
    })
  })

  describe("v4 — identical structure to v2", () => {
    it("decodes a v4 response", () => {
      const body = buildResponseV2(0, 0, 7777n, 3)
      const reader = new BinaryReader(body)
      const result = decodeInitProducerIdResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.producerId).toBe(7777n)
      expect(result.value.producerEpoch).toBe(3)
    })
  })

  describe("error cases", () => {
    it("returns failure on empty buffer", () => {
      const reader = new BinaryReader(new Uint8Array(0))
      const result = decodeInitProducerIdResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure when truncated after throttle_time_ms", () => {
      // 4 bytes for throttle_time_ms, then nothing for error_code
      const w = new BinaryWriter()
      w.writeInt32(0)
      const reader = new BinaryReader(w.finish())
      const result = decodeInitProducerIdResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure when truncated after error_code", () => {
      // 4 bytes throttle + 2 bytes error_code, then nothing for producer_id
      const w = new BinaryWriter()
      w.writeInt32(0)
      w.writeInt16(0)
      const reader = new BinaryReader(w.finish())
      const result = decodeInitProducerIdResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure when truncated after producer_id", () => {
      // 4 bytes throttle + 2 bytes error_code + 8 bytes producer_id, then nothing for epoch
      const w = new BinaryWriter()
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeInt64(1000n)
      const reader = new BinaryReader(w.finish())
      const result = decodeInitProducerIdResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure when flexible response is missing tagged fields", () => {
      // Complete fixed fields but no trailing tagged fields varint for v2+
      const w = new BinaryWriter()
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeInt64(500n)
      w.writeInt16(0)
      const reader = new BinaryReader(w.finish())
      const result = decodeInitProducerIdResponse(reader, 2)
      expect(result.ok).toBe(false)
    })
  })
})
