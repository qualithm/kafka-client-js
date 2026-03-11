/**
 * FindCoordinator request/response encoding and decoding.
 *
 * The FindCoordinator API (key 10) locates the coordinator broker for a
 * consumer group or transactional ID. This is the first step before joining
 * a consumer group or starting a transaction.
 *
 * **Request versions:**
 * - v0: key (group ID) only
 * - v1–v3: adds key_type (GROUP=0, TRANSACTION=1)
 * - v4+: flexible encoding, batched coordinator lookup via coordinatorKeys
 *
 * **Response versions:**
 * - v0: error code, node ID, host, port
 * - v1–v3: adds throttle time, error message
 * - v4+: flexible encoding, coordinators array for batched responses
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_FindCoordinator
 *
 * @packageDocumentation
 */

import { ApiKey } from "../codec/api-keys.js"
import type { BinaryReader, TaggedField } from "../codec/binary-reader.js"
import type { BinaryWriter } from "../codec/binary-writer.js"
import { frameRequest, type RequestHeader } from "../codec/protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "../result.js"

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/**
 * Coordinator type for consumer groups.
 */
export const CoordinatorType = {
  /** Consumer group coordinator. */
  Group: 0,
  /** Transaction coordinator. */
  Transaction: 1
} as const

export type CoordinatorType = (typeof CoordinatorType)[keyof typeof CoordinatorType]

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * FindCoordinator request payload.
 */
export type FindCoordinatorRequest = {
  /**
   * The coordinator key (group ID or transactional ID).
   * Used in v0–v3. In v4+ this is ignored if coordinatorKeys is provided.
   */
  readonly key: string
  /**
   * The coordinator type (v1+).
   * - 0 = GROUP (default)
   * - 1 = TRANSACTION
   */
  readonly keyType?: CoordinatorType
  /**
   * Keys to look up coordinators for (v4+, batched lookup).
   * If provided, `key` is ignored.
   */
  readonly coordinatorKeys?: readonly string[]
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Coordinator information from a single coordinator lookup (v0–v3).
 */
export type Coordinator = {
  /** The coordinator key that was looked up (v4+ only). */
  readonly key?: string
  /** The node ID of the coordinator broker. */
  readonly nodeId: number
  /** The hostname of the coordinator broker. */
  readonly host: string
  /** The port of the coordinator broker. */
  readonly port: number
  /** Error code for this coordinator lookup (v4+ only). */
  readonly errorCode?: number
  /** Error message (v1+ for single, v4+ for batched). */
  readonly errorMessage?: string | null
  /** Tagged fields (v4+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * FindCoordinator response payload.
 */
export type FindCoordinatorResponse = {
  /** Time the request was throttled in milliseconds (v1+). */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). Present in v0–v3 responses. */
  readonly errorCode: number
  /** Error message (v1–v3). Null if no error or not available. */
  readonly errorMessage: string | null
  /** The coordinator broker node ID (v0–v3). */
  readonly nodeId: number
  /** The coordinator broker hostname (v0–v3). */
  readonly host: string
  /** The coordinator broker port (v0–v3). */
  readonly port: number
  /** Coordinators array for batched lookups (v4+). */
  readonly coordinators: readonly Coordinator[]
  /** Tagged fields from flexible versions (v4+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a FindCoordinator request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–4).
 */
export function encodeFindCoordinatorRequest(
  writer: BinaryWriter,
  request: FindCoordinatorRequest,
  apiVersion: number
): void {
  if (apiVersion < 4) {
    // v0–v3: single key lookup (always non-flexible)
    writer.writeString(request.key)

    // key_type (v1+)
    if (apiVersion >= 1) {
      writer.writeInt8(request.keyType ?? CoordinatorType.Group)
    }
  } else {
    // v4+: batched lookup
    // key_type
    writer.writeInt8(request.keyType ?? CoordinatorType.Group)

    // coordinator_keys array (compact)
    const keys = request.coordinatorKeys ?? [request.key]
    writer.writeUnsignedVarInt(keys.length + 1)
    for (const key of keys) {
      writer.writeCompactString(key)
    }

    // Tagged fields
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed FindCoordinator request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–4).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildFindCoordinatorRequest(
  correlationId: number,
  apiVersion: number,
  request: FindCoordinatorRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.FindCoordinator,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeFindCoordinatorRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a FindCoordinator response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–4).
 * @returns The decoded response or a failure.
 */
export function decodeFindCoordinatorResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<FindCoordinatorResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 4

  // Throttle time (v1+)
  let throttleTimeMs = 0
  if (apiVersion >= 1) {
    const throttleResult = reader.readInt32()
    if (!throttleResult.ok) {
      return throttleResult
    }
    throttleTimeMs = throttleResult.value
  }

  if (isFlexible) {
    return decodeFindCoordinatorResponseV4(reader, startOffset, throttleTimeMs)
  }

  return decodeFindCoordinatorResponseV0(reader, startOffset, throttleTimeMs, apiVersion)
}

/**
 * Decode v4+ flexible response with coordinators array.
 */
function decodeFindCoordinatorResponseV4(
  reader: BinaryReader,
  startOffset: number,
  throttleTimeMs: number
): DecodeResult<FindCoordinatorResponse> {
  // Coordinators array
  const coordinatorsResult = decodeCoordinatorsArray(reader)
  if (!coordinatorsResult.ok) {
    return coordinatorsResult
  }

  // Tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  const coordinators = coordinatorsResult.value

  // Extract first coordinator for backward-compatible fields
  // Default values for empty coordinators array
  let errorCode = 0
  let errorMessage: string | null = null
  let nodeId = -1
  let host = ""
  let port = 0

  if (coordinators.length > 0) {
    const first = coordinators[0]
    ;({ nodeId, host, port } = first)
    errorCode = first.errorCode ?? 0
    errorMessage = first.errorMessage ?? null
  }

  return decodeSuccess(
    {
      throttleTimeMs,
      errorCode,
      errorMessage,
      nodeId,
      host,
      port,
      coordinators,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

/**
 * Decode v0–v3 non-flexible response with single coordinator.
 */
function decodeFindCoordinatorResponseV0(
  reader: BinaryReader,
  startOffset: number,
  throttleTimeMs: number,
  apiVersion: number
): DecodeResult<FindCoordinatorResponse> {
  // Error code
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // Error message (v1+)
  let errorMessage: string | null = null
  if (apiVersion >= 1) {
    const errorMsgResult = reader.readString()
    if (!errorMsgResult.ok) {
      return errorMsgResult
    }
    errorMessage = errorMsgResult.value
  }

  // Node ID
  const nodeIdResult = reader.readInt32()
  if (!nodeIdResult.ok) {
    return nodeIdResult
  }

  // Host
  const hostResult = reader.readString()
  if (!hostResult.ok) {
    return hostResult
  }

  // Port
  const portResult = reader.readInt32()
  if (!portResult.ok) {
    return portResult
  }

  return decodeSuccess(
    {
      throttleTimeMs,
      errorCode: errorCodeResult.value,
      errorMessage,
      nodeId: nodeIdResult.value,
      host: hostResult.value ?? "",
      port: portResult.value,
      coordinators: [],
      taggedFields: []
    },
    reader.offset - startOffset
  )
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Decode the coordinators array for v4+ responses.
 */
function decodeCoordinatorsArray(reader: BinaryReader): DecodeResult<Coordinator[]> {
  const startOffset = reader.offset

  // Compact array length
  const lengthResult = reader.readUnsignedVarInt()
  if (!lengthResult.ok) {
    return lengthResult
  }
  const arrayLength = lengthResult.value - 1

  if (arrayLength < 0) {
    return decodeSuccess([], reader.offset - startOffset)
  }

  const coordinators: Coordinator[] = []

  for (let i = 0; i < arrayLength; i++) {
    // Key (compact string)
    const keyResult = reader.readCompactString()
    if (!keyResult.ok) {
      return keyResult
    }

    // Node ID
    const nodeIdResult = reader.readInt32()
    if (!nodeIdResult.ok) {
      return nodeIdResult
    }

    // Host (compact string)
    const hostResult = reader.readCompactString()
    if (!hostResult.ok) {
      return hostResult
    }

    // Port
    const portResult = reader.readInt32()
    if (!portResult.ok) {
      return portResult
    }

    // Error code
    const errorCodeResult = reader.readInt16()
    if (!errorCodeResult.ok) {
      return errorCodeResult
    }

    // Error message (compact nullable string)
    const errorMsgResult = reader.readCompactString()
    if (!errorMsgResult.ok) {
      return errorMsgResult
    }

    // Tagged fields per coordinator
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }

    coordinators.push({
      key: keyResult.value ?? "",
      nodeId: nodeIdResult.value,
      host: hostResult.value ?? "",
      port: portResult.value,
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      taggedFields: tagResult.value
    })
  }

  return decodeSuccess(coordinators, reader.offset - startOffset)
}
