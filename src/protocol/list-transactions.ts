/**
 * ListTransactions request/response encoding and decoding.
 *
 * The ListTransactions API (key 66) lists active transactions on the broker,
 * optionally filtered by state or producer ID.
 *
 * **Request versions:**
 * - v0: state_filters, producer_id_filters
 * - All versions use flexible encoding
 *
 * **Response versions:**
 * - v0: throttle_time_ms, error_code, unknown_state_filters, transaction_states
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_ListTransactions
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
 * ListTransactions request payload.
 */
export type ListTransactionsRequest = {
  /** Filter by transaction states (e.g. "Ongoing"). Empty means no filter. */
  readonly stateFilters?: readonly string[]
  /** Filter by producer IDs. Empty means no filter. */
  readonly producerIdFilters?: readonly bigint[]
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A listed transaction.
 */
export type ListedTransaction = {
  /** The transactional ID. */
  readonly transactionalId: string
  /** The producer ID. */
  readonly producerId: bigint
  /** The current transaction state. */
  readonly transactionState: string
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * ListTransactions response payload.
 */
export type ListTransactionsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** State filters that were not recognised by the broker. */
  readonly unknownStateFilters: readonly string[]
  /** Listed transaction states. */
  readonly transactionStates: readonly ListedTransaction[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a ListTransactions request body into the given writer.
 */
export function encodeListTransactionsRequest(
  writer: BinaryWriter,
  request: ListTransactionsRequest,
  _apiVersion: number
): void {
  // state_filters (compact array of COMPACT_STRING)
  const states = request.stateFilters ?? []
  writer.writeUnsignedVarInt(states.length + 1)
  for (const state of states) {
    writer.writeCompactString(state)
  }

  // producer_id_filters (compact array of INT64)
  const producerIds = request.producerIdFilters ?? []
  writer.writeUnsignedVarInt(producerIds.length + 1)
  for (const pid of producerIds) {
    writer.writeInt64(pid)
  }

  // tagged fields
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed ListTransactions request ready to send to a broker.
 */
export function buildListTransactionsRequest(
  correlationId: number,
  apiVersion: number,
  request: ListTransactionsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.ListTransactions,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeListTransactionsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a ListTransactions response body.
 */
export function decodeListTransactionsResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<ListTransactionsResponse> {
  const startOffset = reader.offset

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // unknown_state_filters (compact array of COMPACT_STRING)
  const unknownStatesResult = decodeCompactStringArray(reader)
  if (!unknownStatesResult.ok) {
    return unknownStatesResult
  }

  // transaction_states array
  const txnStatesResult = decodeTransactionStatesArray(reader)
  if (!txnStatesResult.ok) {
    return txnStatesResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      throttleTimeMs: throttleResult.value,
      errorCode: errorCodeResult.value,
      unknownStateFilters: unknownStatesResult.value,
      transactionStates: txnStatesResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeCompactStringArray(reader: BinaryReader): DecodeResult<string[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const strings: string[] = []
  for (let i = 0; i < count; i++) {
    const strResult = reader.readCompactString()
    if (!strResult.ok) {
      return strResult
    }
    strings.push(strResult.value ?? "")
  }

  return decodeSuccess(strings, reader.offset - startOffset)
}

function decodeTransactionStatesArray(reader: BinaryReader): DecodeResult<ListedTransaction[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const states: ListedTransaction[] = []
  for (let i = 0; i < count; i++) {
    const stateResult = decodeListedTransaction(reader)
    if (!stateResult.ok) {
      return stateResult
    }
    states.push(stateResult.value)
  }

  return decodeSuccess(states, reader.offset - startOffset)
}

function decodeListedTransaction(reader: BinaryReader): DecodeResult<ListedTransaction> {
  const startOffset = reader.offset

  // transactional_id (COMPACT_STRING)
  const txnIdResult = reader.readCompactString()
  if (!txnIdResult.ok) {
    return txnIdResult
  }

  // producer_id (INT64)
  const producerIdResult = reader.readInt64()
  if (!producerIdResult.ok) {
    return producerIdResult
  }

  // transaction_state (COMPACT_STRING)
  const stateResult = reader.readCompactString()
  if (!stateResult.ok) {
    return stateResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      transactionalId: txnIdResult.value ?? "",
      producerId: producerIdResult.value,
      transactionState: stateResult.value ?? "",
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
