/**
 * DescribeDelegationToken request/response encoding and decoding.
 *
 * The DescribeDelegationToken API (key 41) lists delegation tokens
 * for given principals, or all tokens if no owners are specified.
 *
 * **Request versions:**
 * - v0: owners (nullable array of principal_type + principal_name)
 * - v1: same as v0
 * - v2: flexible encoding (KIP-482)
 * - v3: flexible encoding
 *
 * **Response versions:**
 * - v0: error_code, tokens array (owner, renewers, timestamps, token_id, hmac), throttle_time_ms
 * - v1–v2: same structure
 * - v3: adds token_requester fields to each token
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeDelegationToken
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
 * An owner principal to filter delegation tokens by.
 */
export type DescribeDelegationTokenOwner = {
  /** The type of principal (e.g. "User"). */
  readonly principalType: string
  /** The name of the principal. */
  readonly principalName: string
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

/**
 * DescribeDelegationToken request payload.
 */
export type DescribeDelegationTokenRequest = {
  /** Owners to filter by. `null` returns all tokens the requester can see. */
  readonly owners: readonly DescribeDelegationTokenOwner[] | null
  /** Tagged fields (v2+). */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A principal allowed to renew a delegation token.
 */
export type DescribedTokenRenewer = {
  /** The type of principal. */
  readonly principalType: string
  /** The name of the principal. */
  readonly principalName: string
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * A described delegation token.
 */
export type DescribedDelegationToken = {
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
  /** Principals allowed to renew the token. */
  readonly renewers: readonly DescribedTokenRenewer[]
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeDelegationToken response payload.
 */
export type DescribeDelegationTokenResponse = {
  /** Error code (0 = success). */
  readonly errorCode: number
  /** The described delegation tokens. */
  readonly tokens: readonly DescribedDelegationToken[]
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Tagged fields (v2+). */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeDelegationToken request body into the given writer.
 *
 * @param writer - The writer to encode into.
 * @param request - The request payload.
 * @param apiVersion - The API version to encode for (0–3).
 */
export function encodeDescribeDelegationTokenRequest(
  writer: BinaryWriter,
  request: DescribeDelegationTokenRequest,
  apiVersion: number
): void {
  const isFlexible = apiVersion >= 2

  // owners — nullable array
  if (request.owners === null) {
    if (isFlexible) {
      writer.writeUnsignedVarInt(0) // compact nullable array: 0 means null
    } else {
      writer.writeInt32(-1)
    }
  } else {
    if (isFlexible) {
      writer.writeUnsignedVarInt(request.owners.length + 1)
    } else {
      writer.writeInt32(request.owners.length)
    }

    for (const owner of request.owners) {
      if (isFlexible) {
        writer.writeCompactString(owner.principalType)
        writer.writeCompactString(owner.principalName)
      } else {
        writer.writeString(owner.principalType)
        writer.writeString(owner.principalName)
      }

      if (isFlexible) {
        writer.writeTaggedFields(owner.taggedFields ?? [])
      }
    }
  }

  // tagged fields (v2+)
  if (isFlexible) {
    writer.writeTaggedFields(request.taggedFields ?? [])
  }
}

/**
 * Build a complete, framed DescribeDelegationToken request ready to send to a broker.
 *
 * @param correlationId - Correlation ID for matching the response.
 * @param apiVersion - API version to use (0–3).
 * @param request - Request payload.
 * @param clientId - Client ID for the header.
 * @returns The framed request as a Uint8Array.
 */
export function buildDescribeDelegationTokenRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeDelegationTokenRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeDelegationToken,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeDelegationTokenRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeDelegationToken response body.
 *
 * @param reader - The reader positioned at the start of the response body.
 * @param apiVersion - The API version of the response (0–3).
 * @returns The decoded response or a failure.
 */
export function decodeDescribeDelegationTokenResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeDelegationTokenResponse> {
  const startOffset = reader.offset
  const isFlexible = apiVersion >= 2

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // tokens array
  const tokensResult = decodeDescribedTokens(reader, apiVersion, isFlexible)
  if (!tokensResult.ok) {
    return tokensResult
  }

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // tagged fields (v2+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess(
    {
      errorCode: errorCodeResult.value,
      tokens: tokensResult.value,
      throttleTimeMs: throttleResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function decodeDescribedTokens(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribedDelegationToken[]> {
  const startOffset = reader.offset
  let arrayLength: number

  if (isFlexible) {
    const lengthResult = reader.readUnsignedVarInt()
    if (!lengthResult.ok) {
      return lengthResult
    }
    arrayLength = lengthResult.value - 1
  } else {
    const lengthResult = reader.readInt32()
    if (!lengthResult.ok) {
      return lengthResult
    }
    arrayLength = lengthResult.value
  }

  const tokens: DescribedDelegationToken[] = []
  for (let i = 0; i < arrayLength; i++) {
    const tokenResult = decodeDescribedToken(reader, apiVersion, isFlexible)
    if (!tokenResult.ok) {
      return tokenResult
    }
    tokens.push(tokenResult.value)
  }

  return decodeSuccess(tokens, reader.offset - startOffset)
}

function decodeDescribedToken(
  reader: BinaryReader,
  apiVersion: number,
  isFlexible: boolean
): DecodeResult<DescribedDelegationToken> {
  const startOffset = reader.offset

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

  // renewers array
  const renewersResult = decodeTokenRenewers(reader, isFlexible)
  if (!renewersResult.ok) {
    return renewersResult
  }

  // tagged fields (v2+)
  let taggedFields: readonly TaggedField[] = []
  if (isFlexible) {
    const tagResult = reader.readTaggedFields()
    if (!tagResult.ok) {
      return tagResult
    }
    taggedFields = tagResult.value
  }

  return decodeSuccess(
    {
      principalType: principalTypeResult.value ?? "",
      principalName: principalNameResult.value ?? "",
      tokenRequesterPrincipalType: requesterResult.value.tokenRequesterPrincipalType,
      tokenRequesterPrincipalName: requesterResult.value.tokenRequesterPrincipalName,
      issueTimestampMs: issueResult.value,
      expiryTimestampMs: expiryResult.value,
      maxTimestampMs: maxResult.value,
      tokenId: tokenIdResult.value ?? "",
      hmac: hmacResult.value ?? new Uint8Array(0),
      renewers: renewersResult.value,
      taggedFields
    },
    reader.offset - startOffset
  )
}

function decodeTokenRenewers(
  reader: BinaryReader,
  isFlexible: boolean
): DecodeResult<DescribedTokenRenewer[]> {
  const startOffset = reader.offset
  let arrayLength: number

  if (isFlexible) {
    const lengthResult = reader.readUnsignedVarInt()
    if (!lengthResult.ok) {
      return lengthResult
    }
    arrayLength = lengthResult.value - 1
  } else {
    const lengthResult = reader.readInt32()
    if (!lengthResult.ok) {
      return lengthResult
    }
    arrayLength = lengthResult.value
  }

  const renewers: DescribedTokenRenewer[] = []
  for (let i = 0; i < arrayLength; i++) {
    // principal_type
    const typeResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!typeResult.ok) {
      return typeResult
    }

    // principal_name
    const nameResult = isFlexible ? reader.readCompactString() : reader.readString()
    if (!nameResult.ok) {
      return nameResult
    }

    // tagged fields (v2+)
    let renewerTaggedFields: readonly TaggedField[] = []
    if (isFlexible) {
      const tagResult = reader.readTaggedFields()
      if (!tagResult.ok) {
        return tagResult
      }
      renewerTaggedFields = tagResult.value
    }

    renewers.push({
      principalType: typeResult.value ?? "",
      principalName: nameResult.value ?? "",
      taggedFields: renewerTaggedFields
    })
  }

  return decodeSuccess(renewers, reader.offset - startOffset)
}

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
