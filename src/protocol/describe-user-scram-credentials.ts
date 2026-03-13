/**
 * DescribeUserScramCredentials request/response encoding and decoding.
 *
 * The DescribeUserScramCredentials API (key 50) returns SCRAM credential
 * information for the specified users, or all users if no filter is given.
 *
 * **Request versions:**
 * - v0: optional user list filter, flexible encoding
 *
 * **Response versions:**
 * - v0: per-user credential descriptions with mechanism/iterations, flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeUserScramCredentials
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
 * SCRAM mechanism types used in credential descriptions.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeUserScramCredentials
 */
export const ScramMechanism = {
  /** Unknown mechanism. */
  Unknown: 0,
  /** SCRAM-SHA-256. */
  ScramSha256: 1,
  /** SCRAM-SHA-512. */
  ScramSha512: 2
} as const

export type ScramMechanism = (typeof ScramMechanism)[keyof typeof ScramMechanism]

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * A user to describe SCRAM credentials for.
 */
export type UserScramCredentialsUser = {
  /** The user name. */
  readonly name: string
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * DescribeUserScramCredentials request payload.
 */
export type DescribeUserScramCredentialsRequest = {
  /** The users to describe, or null/empty for all users. */
  readonly users: readonly UserScramCredentialsUser[] | null
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A single SCRAM credential info entry for a user.
 */
export type CredentialInfo = {
  /** The SCRAM mechanism (0=UNKNOWN, 1=SCRAM-SHA-256, 2=SCRAM-SHA-512). */
  readonly mechanism: number
  /** The number of iterations used in the SCRAM credential. */
  readonly iterations: number
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * SCRAM credential description for a single user.
 */
export type DescribeUserScramCredentialsResult = {
  /** The user name. */
  readonly user: string
  /** Error code for this user (0 = no error). */
  readonly errorCode: number
  /** Error message for this user. Null if no error. */
  readonly errorMessage: string | null
  /** The user's SCRAM credentials. */
  readonly credentialInfos: readonly CredentialInfo[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeUserScramCredentials response payload.
 */
export type DescribeUserScramCredentialsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Top-level error code (0 = no error). */
  readonly errorCode: number
  /** Top-level error message. Null if no error. */
  readonly errorMessage: string | null
  /** Per-user credential descriptions. */
  readonly results: readonly DescribeUserScramCredentialsResult[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeUserScramCredentials request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param _apiVersion - The API version to encode for (0).
 */
export function encodeDescribeUserScramCredentialsRequest(
  writer: BinaryWriter,
  request: DescribeUserScramCredentialsRequest,
  _apiVersion: number
): void {
  // v0 is always flexible

  // users array (nullable compact array)
  const { users } = request
  if (users === null) {
    // null compact array: write 0
    writer.writeUnsignedVarInt(0)
  } else {
    writer.writeUnsignedVarInt(users.length + 1)
    for (const user of users) {
      writer.writeCompactString(user.name)
      writer.writeTaggedFields(user.taggedFields ?? [])
    }
  }

  // Request tagged fields
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed DescribeUserScramCredentials request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildDescribeUserScramCredentialsRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeUserScramCredentialsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeUserScramCredentials,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeUserScramCredentialsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeUserScramCredentials response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param _apiVersion - The API version of the response (0).
 * @returns The decoded response or a failure.
 */
export function decodeDescribeUserScramCredentialsResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<DescribeUserScramCredentialsResponse> {
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

  // error_message (compact nullable string)
  const errorMsgResult = reader.readCompactString()
  if (!errorMsgResult.ok) {
    return errorMsgResult
  }

  // results array (compact array)
  const resultsResult = decodeResultsArray(reader)
  if (!resultsResult.ok) {
    return resultsResult
  }

  // Tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      throttleTimeMs: throttleResult.value,
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      results: resultsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeResultsArray(
  reader: BinaryReader
): DecodeResult<DescribeUserScramCredentialsResult[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const results: DescribeUserScramCredentialsResult[] = []
  for (let i = 0; i < count; i++) {
    const resultEntry = decodeResultEntry(reader)
    if (!resultEntry.ok) {
      return resultEntry
    }
    results.push(resultEntry.value)
  }

  return decodeSuccess(results, reader.offset - startOffset)
}

function decodeResultEntry(reader: BinaryReader): DecodeResult<DescribeUserScramCredentialsResult> {
  const startOffset = reader.offset

  // user (compact string)
  const userResult = reader.readCompactString()
  if (!userResult.ok) {
    return userResult
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // error_message (compact nullable string)
  const errorMsgResult = reader.readCompactString()
  if (!errorMsgResult.ok) {
    return errorMsgResult
  }

  // credential_infos (compact array)
  const credentialsResult = decodeCredentialInfoArray(reader)
  if (!credentialsResult.ok) {
    return credentialsResult
  }

  // Tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      user: userResult.value ?? "",
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      credentialInfos: credentialsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeCredentialInfoArray(reader: BinaryReader): DecodeResult<CredentialInfo[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const infos: CredentialInfo[] = []
  for (let i = 0; i < count; i++) {
    const infoResult = decodeCredentialInfo(reader)
    if (!infoResult.ok) {
      return infoResult
    }
    infos.push(infoResult.value)
  }

  return decodeSuccess(infos, reader.offset - startOffset)
}

function decodeCredentialInfo(reader: BinaryReader): DecodeResult<CredentialInfo> {
  const startOffset = reader.offset

  // mechanism (INT8)
  const mechanismResult = reader.readInt8()
  if (!mechanismResult.ok) {
    return mechanismResult
  }

  // iterations (INT32)
  const iterationsResult = reader.readInt32()
  if (!iterationsResult.ok) {
    return iterationsResult
  }

  // Tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      mechanism: mechanismResult.value,
      iterations: iterationsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
