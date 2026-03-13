/**
 * DescribeTransactions request/response encoding and decoding.
 *
 * The DescribeTransactions API (key 65) describes the state of active
 * transactions by transactional ID.
 *
 * **Request versions:**
 * - v0: transactional_ids array
 * - All versions use flexible encoding
 *
 * **Response versions:**
 * - v0: throttle_time_ms, transaction_states array
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeTransactions
 *
 * @packageDocumentation
 */

import { ApiKey } from "../codec/api-keys.js"
import type { BinaryReader, TaggedField } from "../codec/binary-reader.js"
import type { BinaryWriter } from "../codec/binary-writer.js"
import { frameRequest, type RequestHeader } from "../codec/protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "../result.js"

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * DescribeTransactions request payload.
 */
export type DescribeTransactionsRequest = {
  /** The transactional IDs to describe. */
  readonly transactionalIds: readonly string[]
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A topic partition involved in a transaction.
 */
export type TransactionTopicPartition = {
  /** The topic name. */
  readonly topic: string
  /** The partition indexes. */
  readonly partitions: readonly number[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * The state of a single transaction.
 */
export type TransactionState = {
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** The transactional ID. */
  readonly transactionalId: string
  /** The transaction state (e.g. "Ongoing", "PrepareCommit", "PrepareAbort", "CompleteCommit", "CompleteAbort", "Empty", "Dead"). */
  readonly state: string
  /** The transaction timeout in milliseconds. */
  readonly transactionTimeoutMs: number
  /** The transaction start timestamp. */
  readonly transactionStartTimeMs: bigint
  /** The producer ID. */
  readonly producerId: bigint
  /** The producer epoch. */
  readonly producerEpoch: number
  /** The topic partitions involved in the transaction. */
  readonly topics: readonly TransactionTopicPartition[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeTransactions response payload.
 */
export type DescribeTransactionsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-transaction state results. */
  readonly transactionStates: readonly TransactionState[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeTransactions request body into the given writer.
 */
export function encodeDescribeTransactionsRequest(
  writer: BinaryWriter,
  request: DescribeTransactionsRequest,
  _apiVersion: number
): void {
  // transactional_ids (compact array of COMPACT_STRING)
  writer.writeUnsignedVarInt(request.transactionalIds.length + 1)
  for (const id of request.transactionalIds) {
    writer.writeCompactString(id)
  }

  // tagged fields
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed DescribeTransactions request ready to send to a broker.
 */
export function buildDescribeTransactionsRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeTransactionsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeTransactions,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeTransactionsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeTransactions response body.
 */
export function decodeDescribeTransactionsResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<DescribeTransactionsResponse> {
  const startOffset = reader.offset

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // transaction_states array
  const statesResult = decodeTransactionStatesArray(reader)
  if (!statesResult.ok) {
    return statesResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      throttleTimeMs: throttleResult.value,
      transactionStates: statesResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeTransactionStatesArray(reader: BinaryReader): DecodeResult<TransactionState[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const states: TransactionState[] = []
  for (let i = 0; i < count; i++) {
    const stateResult = decodeTransactionStateEntry(reader)
    if (!stateResult.ok) {
      return stateResult
    }
    states.push(stateResult.value)
  }

  return decodeSuccess(states, reader.offset - startOffset)
}

function decodeTransactionStateEntry(reader: BinaryReader): DecodeResult<TransactionState> {
  const startOffset = reader.offset

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // transactional_id (COMPACT_STRING)
  const txnIdResult = reader.readCompactString()
  if (!txnIdResult.ok) {
    return txnIdResult
  }

  // state (COMPACT_STRING)
  const stateResult = reader.readCompactString()
  if (!stateResult.ok) {
    return stateResult
  }

  // transaction_timeout_ms (INT32)
  const timeoutResult = reader.readInt32()
  if (!timeoutResult.ok) {
    return timeoutResult
  }

  // transaction_start_time_ms (INT64)
  const startTimeResult = reader.readInt64()
  if (!startTimeResult.ok) {
    return startTimeResult
  }

  // producer_id (INT64)
  const producerIdResult = reader.readInt64()
  if (!producerIdResult.ok) {
    return producerIdResult
  }

  // producer_epoch (INT16)
  const epochResult = reader.readInt16()
  if (!epochResult.ok) {
    return epochResult
  }

  // topics array
  const topicsResult = decodeTransactionTopicsArray(reader)
  if (!topicsResult.ok) {
    return topicsResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      errorCode: errorCodeResult.value,
      transactionalId: txnIdResult.value ?? "",
      state: stateResult.value ?? "",
      transactionTimeoutMs: timeoutResult.value,
      transactionStartTimeMs: startTimeResult.value,
      producerId: producerIdResult.value,
      producerEpoch: epochResult.value,
      topics: topicsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeTransactionTopicsArray(
  reader: BinaryReader
): DecodeResult<TransactionTopicPartition[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const topics: TransactionTopicPartition[] = []
  for (let i = 0; i < count; i++) {
    const topicResult = decodeTransactionTopicEntry(reader)
    if (!topicResult.ok) {
      return topicResult
    }
    topics.push(topicResult.value)
  }

  return decodeSuccess(topics, reader.offset - startOffset)
}

function decodeTransactionTopicEntry(
  reader: BinaryReader
): DecodeResult<TransactionTopicPartition> {
  const startOffset = reader.offset

  // topic (COMPACT_STRING)
  const topicResult = reader.readCompactString()
  if (!topicResult.ok) {
    return topicResult
  }

  // partitions (compact array of INT32)
  const partCountResult = reader.readUnsignedVarInt()
  if (!partCountResult.ok) {
    return partCountResult
  }
  const partCount = partCountResult.value - 1

  const partitions: number[] = []
  for (let i = 0; i < partCount; i++) {
    const partResult = reader.readInt32()
    if (!partResult.ok) {
      return partResult
    }
    partitions.push(partResult.value)
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      topic: topicResult.value ?? "",
      partitions,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
