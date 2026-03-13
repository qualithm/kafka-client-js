/**
 * CreateDelegationToken request/response encoding and decoding.
 *
 * The CreateDelegationToken API (key 38) creates a new delegation token
 * for authenticating with brokers on behalf of a principal.
 *
 * **Request versions:**
 * - v0: renewers array, max_lifetime_ms
 * - v1: same as v0
 * - v2: flexible encoding (KIP-482)
 * - v3: adds owner_principal_type, owner_principal_name
 *
 * **Response versions:**
 * - v0: error_code, owner fields, issue/expiry/max timestamps, token_id, hmac, throttle_time_ms
 * - v1–v2: same structure
 * - v3: flexible encoding, adds token_requester fields
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_CreateDelegationToken
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
 * A principal that is allowed to renew the delegation token.
 */
export type CreatableRenewers = {
  /** The type of principal (e.g. "User"). */
  readonly principalType: string
  /** The name of the principal. */
  readonly principalName: string
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * CreateDelegationToken request payload.
 */
export type CreateDelegationTokenRequest = {
  /** The principal type of the token owner (v3+). `null` defaults to the requester. */
  readonly ownerPrincipalType?: string | null
  /** The principal name of the token owner (v3+). `null` defaults to the requester. */
  readonly ownerPrincipalName?: string | null
  /** Principals allowed to renew the token. */
  readonly renewers: readonly CreatableRenewers[]
  /** Maximum lifetime of the token in milliseconds. -1 = server default. */
  readonly maxLifetimeMs: bigint
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * CreateDelegationToken response payload.
 */
export type CreateDelegationTokenResponse = {
  /** Error code (0 = success). */
  readonly errorCode: number
  /** The owner principal type. */
  readonly principalType: string
  /** The owner principal name. */
  readonly principalName: string
  /** Token requester principal type (v3+). */
  readonly tokenRequesterPrincipalType: string
  /** Token requester principal name (v3+). */
  readonly tokenRequesterPrincipalName: string
  /** Timestamp when the token was issued (epoch ms). */
  readonly issueTimestampMs: bigint
  /** Timestamp when the token will expire (epoch ms). */
  readonly expiryTimestampMs: bigint
  /** Maximum timestamp until which the token can be renewed (epoch ms). */
  readonly maxTimestampMs: bigint
  /** The token ID string. */
  readonly tokenId: string
  /** The HMAC of the delegation token. */
  readonly hmac: Uint8Array
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a CreateDelegationToken request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–3).
 */
export function encodeCreateDelegationTokenRequest(
  writer: BinaryWriter,
  request: CreateDelegationTokenRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // owner_principal_type / owner_principal_name (v3+)
  if (apiVersion >= 3) {
    if (isFlexible) {
      writer.writeCompactString(request.ownerPrincipalType ?? null)
      writer.writeCompactString(request.ownerPrincipalName ?? null)
    }
  }

  // renewers array
  if (isFlexible) {
    writer.writeUnsignedVarInt(request.renewers.length + 1)
  } else {
    writer.writeInt32(request.renewers.length)
  }

  for (const renewer of request.renewers) {
    if (isFlexible) {
      writer.writeCompactString(renewer.principalType)
      writer.writeCompactString(renewer.principalName)
    } else {
      writer.writeString(renewer.principalType)
      writer.writeString(renewer.principalName)
    }

    if (isFlexible) {
      writer.writeTaggedFields(renewer.taggedFields ?? [])
    }
  }

  // max_lifetime_ms (INT64)
  writer.writeInt64(request.maxLifetimeMs)

  // tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed CreateDelegationToken request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–3).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildCreateDelegationTokenRequest(
  correlationId: number,
  apiVersion: number,
  request: CreateDelegationTokenRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.CreateDelegationToken,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeCreateDelegationTokenRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a CreateDelegationToken response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–3).
 * @returns The decoded response or a failure.
 */
export function decodeCreateDelegationTokenResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<CreateDelegationTokenResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // principal_type (STRING / COMPACT_STRING)
  const principalTypeResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!principalTypeResult.ok) {
    return principalTypeResult
  }

  // principal_name (STRING / COMPACT_STRING)
  const principalNameResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!principalNameResult.ok) {
    return principalNameResult
  }

  // token_requester_principal_type / token_requester_principal_name (v3+)
  const requesterResult = decodeTokenRequesterFields(reader, apiVersion)
  if (!requesterResult.ok) {
    return requesterResult
  }

  // issue_timestamp_ms (INT64)
  const issueResult = reader.readInt64()
  if (!issueResult.ok) {
    return issueResult
  }

  // expiry_timestamp_ms (INT64)
  const expiryResult = reader.readInt64()
  if (!expiryResult.ok) {
    return expiryResult
  }

  // max_timestamp_ms (INT64)
  const maxResult = reader.readInt64()
  if (!maxResult.ok) {
    return maxResult
  }

  // token_id (STRING / COMPACT_STRING)
  const tokenIdResult = isFlexible ? reader.readCompactString() : reader.readString()
  if (!tokenIdResult.ok) {
    return tokenIdResult
  }

  // hmac (BYTES / COMPACT_BYTES)
  const hmacResult = isFlexible ? reader.readCompactBytes() : reader.readBytes()
  if (!hmacResult.ok) {
    return hmacResult
  }

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // tagged fields (v2+)
  const tagFieldsResult = decodeFlexibleTaggedFields(reader, isFlexible)
  if (!tagFieldsResult.ok) {
    return tagFieldsResult
  }

  return decodeSuccess(
    {
      errorCode: errorCodeResult.value,
      principalType: principalTypeResult.value ?? "",
      principalName: principalNameResult.value ?? "",
      tokenRequesterPrincipalType: requesterResult.value.tokenRequesterPrincipalType,
      tokenRequesterPrincipalName: requesterResult.value.tokenRequesterPrincipalName,
      issueTimestampMs: issueResult.value,
      expiryTimestampMs: expiryResult.value,
      maxTimestampMs: maxResult.value,
      tokenId: tokenIdResult.value ?? "",
      hmac: hmacResult.value ?? new Uint8Array(0),
      throttleTimeMs: throttleResult.value,
      taggedFields: tagFieldsResult.value
    },
    reader.offset - startOffset
  )
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

type TokenRequesterFields = {
  readonly tokenRequesterPrincipalType: string
  readonly tokenRequesterPrincipalName: string
}

function decodeTokenRequesterFields(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<TokenRequesterFields> {
  const startOffset = reader.offset

  if (apiVersion < 3) {
    return decodeSuccess({ tokenRequesterPrincipalType: "", tokenRequesterPrincipalName: "" }, 0)
  }

  const reqTypeResult = reader.readCompactString()
  if (!reqTypeResult.ok) {
    return reqTypeResult
  }

  const reqNameResult = reader.readCompactString()
  if (!reqNameResult.ok) {
    return reqNameResult
  }

  return decodeSuccess(
    {
      tokenRequesterPrincipalType: reqTypeResult.value ?? "",
      tokenRequesterPrincipalName: reqNameResult.value ?? ""
    },
    reader.offset - startOffset
  )
}

function decodeFlexibleTaggedFields(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<readonly TaggedField[]> {
  if (!isFlexible) {
    return decodeSuccess([], 0)
  }
  return reader.readTaggedFields()
}
