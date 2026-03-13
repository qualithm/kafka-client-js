import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildListTransactionsRequest,
  decodeListTransactionsResponse,
  encodeListTransactionsRequest
} from "../../../protocol/list-transactions"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeListTransactionsRequest", () => {
  it("encodes v0 request with no filters", () => {
    const writer = new BinaryWriter()
    encodeListTransactionsRequest(writer, {}, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // state_filters (compact: 0 + 1)
    const stateCount = reader.readUnsignedVarInt()
    expect(stateCount.ok && stateCount.value).toBe(1)

    // producer_id_filters (compact: 0 + 1)
    const pidCount = reader.readUnsignedVarInt()
    expect(pidCount.ok && pidCount.value).toBe(1)

    // tagged fields
    const tag = reader.readTaggedFields()
    expect(tag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v0 request with filters", () => {
    const writer = new BinaryWriter()
    encodeListTransactionsRequest(
      writer,
      { stateFilters: ["Ongoing"], producerIdFilters: [100n, 200n] },
      0
    )
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // state_filters (1 + 1 = 2)
    const stateCount = reader.readUnsignedVarInt()
    expect(stateCount.ok && stateCount.value).toBe(2)
    const state1 = reader.readCompactString()
    expect(state1.ok && state1.value).toBe("Ongoing")

    // producer_id_filters (2 + 1 = 3)
    const pidCount = reader.readUnsignedVarInt()
    expect(pidCount.ok && pidCount.value).toBe(3)
    const pid1 = reader.readInt64()
    expect(pid1.ok && pid1.value).toBe(100n)
    const pid2 = reader.readInt64()
    expect(pid2.ok && pid2.value).toBe(200n)
  })
})

// ---------------------------------------------------------------------------
// buildListTransactionsRequest (framed)
// ---------------------------------------------------------------------------

describe("buildListTransactionsRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildListTransactionsRequest(1, 0, {}, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.ListTransactions)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeListTransactionsResponse", () => {
  it("decodes v0 response with transactions", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    // unknown_state_filters (compact: 0 + 1)
    w.writeUnsignedVarInt(1)
    // transaction_states (compact: 2 + 1)
    w.writeUnsignedVarInt(3)
    // transaction 1
    w.writeCompactString("txn-1")
    w.writeInt64(100n)
    w.writeCompactString("Ongoing")
    w.writeTaggedFields([])
    // transaction 2
    w.writeCompactString("txn-2")
    w.writeInt64(200n)
    w.writeCompactString("PrepareCommit")
    w.writeTaggedFields([])
    // end transactions
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeListTransactionsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.errorCode).toBe(0)
    expect(result.value.unknownStateFilters).toHaveLength(0)
    expect(result.value.transactionStates).toHaveLength(2)
    expect(result.value.transactionStates[0].transactionalId).toBe("txn-1")
    expect(result.value.transactionStates[0].producerId).toBe(100n)
    expect(result.value.transactionStates[0].transactionState).toBe("Ongoing")
    expect(result.value.transactionStates[1].transactionalId).toBe("txn-2")
    expect(result.value.transactionStates[1].producerId).toBe(200n)
    expect(result.value.transactionStates[1].transactionState).toBe("PrepareCommit")
  })

  it("decodes response with unknown state filters", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    // unknown_state_filters (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    w.writeCompactString("InvalidState")
    // transaction_states (compact: 0 + 1)
    w.writeUnsignedVarInt(1)
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeListTransactionsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.unknownStateFilters).toEqual(["InvalidState"])
    expect(result.value.transactionStates).toHaveLength(0)
  })

  it("decodes error response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(29) // CLUSTER_AUTHORIZATION_FAILED
    w.writeUnsignedVarInt(1) // 0 unknown filters
    w.writeUnsignedVarInt(1) // 0 transactions
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeListTransactionsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.errorCode).toBe(29)
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeListTransactionsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated transaction entry", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeUnsignedVarInt(1) // 0 unknown state filters
      w.writeUnsignedVarInt(2) // 1 transaction + 1
      w.writeCompactString("txn-1") // transactional_id
      // Missing producer_id and transaction_state

      const reader = new BinaryReader(w.finish())
      const result = decodeListTransactionsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated unknown state filters", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeUnsignedVarInt(3) // 2 unknown filters + 1
      w.writeCompactString("Bad")
      // Missing second filter entry

      const reader = new BinaryReader(w.finish())
      const result = decodeListTransactionsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })
  })
})
