/**
 * AlterUserScramCredentials request/response encoding and decoding.
 *
 * The AlterUserScramCredentials API (key 51) creates, updates, or deletes
 * SCRAM credentials for users.
 *
 * **Request versions:**
 * - v0: upsertions and deletions array, flexible encoding
 *
 * **Response versions:**
 * - v0: per-user results with error codes, flexible encoding
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_AlterUserScramCredentials
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
 * A SCRAM credential deletion entry.
 */
export type ScramCredentialDeletion = {
  /** The user name. */
  readonly name: string
  /** The SCRAM mechanism (1=SCRAM-SHA-256, 2=SCRAM-SHA-512). */
  readonly mechanism: number
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * A SCRAM credential upsertion (create or update) entry.
 */
export type ScramCredentialUpsertion = {
  /** The user name. */
  readonly name: string
  /** The SCRAM mechanism (1=SCRAM-SHA-256, 2=SCRAM-SHA-512). */
  readonly mechanism: number
  /** The number of iterations used in the SCRAM credential. */
  readonly iterations: number
  /** A random salt used to compute the hash of the password. */
  readonly salt: Uint8Array
  /**
   * The salted password. When provided, the broker uses this directly
   * instead of computing it from a plaintext password.
   */
  readonly saltedPassword: Uint8Array
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * AlterUserScramCredentials request payload.
 */
export type AlterUserScramCredentialsRequest = {
  /** SCRAM credential deletions. */
  readonly deletions: readonly ScramCredentialDeletion[]
  /** SCRAM credential upsertions. */
  readonly upsertions: readonly ScramCredentialUpsertion[]
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * Per-user result of SCRAM credential alteration.
 */
export type AlterUserScramCredentialsResultEntry = {
  /** The user name. */
  readonly user: string
  /** Error code for this user (0 = no error). */
  readonly errorCode: number
  /** Error message for this user. Null if no error. */
  readonly errorMessage: string | null
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * AlterUserScramCredentials response payload.
 */
export type AlterUserScramCredentialsResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Per-user alteration results. */
  readonly results: readonly AlterUserScramCredentialsResultEntry[]
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode an AlterUserScramCredentials request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param _apiVersion - The API version to encode for (0).
 */
export function encodeAlterUserScramCredentialsRequest(
  writer: BinaryWriter,
  request: AlterUserScramCredentialsRequest,
  _apiVersion: number
): void {
  // v0 is always flexible

  // deletions (compact array)
  writer.writeUnsignedVarInt(request.deletions.length + 1)
  for (const deletion of request.deletions) {
    writer.writeCompactString(deletion.name)
    writer.writeInt8(deletion.mechanism)
    writer.writeTaggedFields(deletion.taggedFields ?? [])
  }

  // upsertions (compact array)
  writer.writeUnsignedVarInt(request.upsertions.length + 1)
  for (const upsertion of request.upsertions) {
    writer.writeCompactString(upsertion.name)
    writer.writeInt8(upsertion.mechanism)
    writer.writeInt32(upsertion.iterations)
    writer.writeCompactBytes(upsertion.salt)
    writer.writeCompactBytes(upsertion.saltedPassword)
    writer.writeTaggedFields(upsertion.taggedFields ?? [])
  }

  // Request tagged fields
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed AlterUserScramCredentials request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildAlterUserScramCredentialsRequest(
  correlationId: number,
  apiVersion: number,
  request: AlterUserScramCredentialsRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.AlterUserScramCredentials,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeAlterUserScramCredentialsRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode an AlterUserScramCredentials response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param _apiVersion - The API version of the response (0).
 * @returns The decoded response or a failure.
 */
export function decodeAlterUserScramCredentialsResponse(
  reader: BinaryReader,
  _apiVersion: number
): DecodeResult<AlterUserScramCredentialsResponse> {
  const startOffset = reader.offset

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // results (compact array)
  const resultsResult = decodeAlterResultsArray(reader)
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
      results: resultsResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeAlterResultsArray(
  reader: BinaryReader
): DecodeResult<AlterUserScramCredentialsResultEntry[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const results: AlterUserScramCredentialsResultEntry[] = []
  for (let i = 0; i < count; i++) {
    const entryResult = decodeAlterResultEntry(reader)
    if (!entryResult.ok) {
      return entryResult
    }
    results.push(entryResult.value)
  }

  return decodeSuccess(results, reader.offset - startOffset)
}

function decodeAlterResultEntry(
  reader: BinaryReader
): DecodeResult<AlterUserScramCredentialsResultEntry> {
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
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
