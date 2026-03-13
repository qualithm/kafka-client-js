import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDescribeTransactionsRequest,
  decodeDescribeTransactionsResponse,
  type DescribeTransactionsRequest,
  encodeDescribeTransactionsRequest
} from "../../../protocol/describe-transactions"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeTransactionsRequest", () => {
  it("encodes v0 request with transactional IDs", () => {
    const writer = new BinaryWriter()
    const request: DescribeTransactionsRequest = {
      transactionalIds: ["txn-1", "txn-2"]
    }
    encodeDescribeTransactionsRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // compact array (2 + 1 = 3)
    const count = reader.readUnsignedVarInt()
    expect(count.ok && count.value).toBe(3)

    const id1 = reader.readCompactString()
    expect(id1.ok && id1.value).toBe("txn-1")

    const id2 = reader.readCompactString()
    expect(id2.ok && id2.value).toBe("txn-2")

    // tagged fields
    const tag = reader.readTaggedFields()
    expect(tag.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes empty request", () => {
    const writer = new BinaryWriter()
    encodeDescribeTransactionsRequest(writer, { transactionalIds: [] }, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    const count = reader.readUnsignedVarInt()
    expect(count.ok && count.value).toBe(1) // 0 + 1
  })
})

// ---------------------------------------------------------------------------
// buildDescribeTransactionsRequest (framed)
// ---------------------------------------------------------------------------

describe("buildDescribeTransactionsRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildDescribeTransactionsRequest(
      1,
      0,
      { transactionalIds: ["txn-1"] },
      "test-client"
    )
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.DescribeTransactions)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeTransactionsResponse", () => {
  it("decodes v0 response with transaction state", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    // transaction_states array (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // transaction state
    w.writeInt16(0) // error_code
    w.writeCompactString("txn-1") // transactional_id
    w.writeCompactString("Ongoing") // state
    w.writeInt32(60000) // transaction_timeout_ms
    w.writeInt64(1700000000000n) // transaction_start_time_ms
    w.writeInt64(1001n) // producer_id
    w.writeInt16(5) // producer_epoch
    // topics array (compact: 1 + 1)
    w.writeUnsignedVarInt(2)
    // topic
    w.writeCompactString("test-topic")
    // partitions (compact: 2 + 1)
    w.writeUnsignedVarInt(3)
    w.writeInt32(0)
    w.writeInt32(1)
    w.writeTaggedFields([]) // topic tagged
    // end topics
    w.writeTaggedFields([]) // state tagged
    // end states
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeTransactionsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.transactionStates).toHaveLength(1)

    const txn = result.value.transactionStates[0]
    expect(txn.errorCode).toBe(0)
    expect(txn.transactionalId).toBe("txn-1")
    expect(txn.state).toBe("Ongoing")
    expect(txn.transactionTimeoutMs).toBe(60000)
    expect(txn.transactionStartTimeMs).toBe(1700000000000n)
    expect(txn.producerId).toBe(1001n)
    expect(txn.producerEpoch).toBe(5)
    expect(txn.topics).toHaveLength(1)
    expect(txn.topics[0].topic).toBe("test-topic")
    expect(txn.topics[0].partitions).toEqual([0, 1])
  })

  it("decodes response with empty transaction states", () => {
    const w = new BinaryWriter()
    w.writeInt32(5) // throttle_time_ms
    w.writeUnsignedVarInt(1) // 0 states + 1
    w.writeTaggedFields([]) // response tagged

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeTransactionsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(5)
    expect(result.value.transactionStates).toHaveLength(0)
  })
})
