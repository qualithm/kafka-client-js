/**
 * Result types for decode operations.
 *
 * Decode functions return {@link DecodeResult} to avoid exceptions in hot paths.
 * This follows the pattern from Kafka protocol spec where decoding can fail
 * due to truncated messages, corrupt data, or unsupported versions.
 *
 * @packageDocumentation
 */

/**
 * Successful decode result containing the decoded value and how many bytes were consumed.
 */
export type DecodeSuccess<T> = {
  readonly ok: true
  /** The decoded value. */
  readonly value: T
  /** Number of bytes consumed from the input buffer. */
  readonly bytesRead: number
}

/**
 * Failed decode result containing an error describing what went wrong.
 */
export type DecodeFailure = {
  readonly ok: false
  /** Error describing the decode failure. */
  readonly error: DecodeError
}

/**
 * Result of a decode operation. Either a success with the decoded value
 * and bytes consumed, or a failure with an error.
 */
export type DecodeResult<T> = DecodeSuccess<T> | DecodeFailure

/**
 * Describes why a decode operation failed.
 */
export type DecodeError = {
  /** Machine-readable error code. */
  readonly code: DecodeErrorCode
  /** Human-readable error message. */
  readonly message: string
  /** Byte offset where the error occurred, if known. */
  readonly offset?: number
}

/**
 * Error codes for decode failures.
 */
export type DecodeErrorCode =
  | "BUFFER_UNDERFLOW"
  | "INVALID_DATA"
  | "UNSUPPORTED_VERSION"
  | "CRC_MISMATCH"
  | "INVALID_VARINT"

/** Create a successful decode result. */
export function decodeSuccess<T>(value: T, bytesRead: number): DecodeSuccess<T> {
  return { ok: true, value, bytesRead }
}

/** Create a failed decode result. */
export function decodeFailure(
  code: DecodeErrorCode,
  message: string,
  offset?: number
): DecodeFailure {
  return { ok: false, error: { code, message, offset } }
}
