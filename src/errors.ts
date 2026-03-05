/**
 * Error hierarchy for the Kafka client.
 *
 * Errors are split into two axes:
 * - **Level**: protocol-level (Kafka error codes) vs connection-level (network/socket)
 * - **Recoverability**: retriable (transient) vs fatal (unrecoverable)
 *
 * All error classes include a static `isError()` method for type narrowing.
 *
 * @packageDocumentation
 */

/**
 * Base error for all Kafka client errors.
 */
export class KafkaError extends Error {
  override readonly name: string = "KafkaError"

  /** Whether this error is retriable (transient). */
  readonly retriable: boolean

  constructor(message: string, retriable: boolean, options?: ErrorOptions) {
    super(message, options)
    this.retriable = retriable
  }

  static isError(error: unknown): error is KafkaError {
    return error instanceof KafkaError
  }
}

// ---------------------------------------------------------------------------
// Protocol-level errors (Kafka error codes from the broker)
// ---------------------------------------------------------------------------

/**
 * Error returned by a Kafka broker in a protocol response.
 * Maps to the error codes defined in the Kafka protocol spec.
 */
export class KafkaProtocolError extends KafkaError {
  override readonly name = "KafkaProtocolError"

  /** Kafka protocol error code (e.g. 1 = OFFSET_OUT_OF_RANGE). */
  readonly errorCode: number

  constructor(message: string, errorCode: number, retriable: boolean, options?: ErrorOptions) {
    super(message, retriable, options)
    this.errorCode = errorCode
  }

  static override isError(error: unknown): error is KafkaProtocolError {
    return error instanceof KafkaProtocolError
  }
}

// ---------------------------------------------------------------------------
// Connection-level errors (network, socket, timeout)
// ---------------------------------------------------------------------------

/**
 * Error related to network connectivity or socket operations.
 */
export class KafkaConnectionError extends KafkaError {
  override readonly name: string = "KafkaConnectionError"

  /** Broker address that caused the error, if known. */
  readonly broker?: string

  constructor(
    message: string,
    options?: { retriable?: boolean; broker?: string; cause?: unknown }
  ) {
    super(
      message,
      options?.retriable ?? true,
      options?.cause !== undefined ? { cause: options.cause } : undefined
    )
    this.broker = options?.broker
  }

  static override isError(error: unknown): error is KafkaConnectionError {
    return error instanceof KafkaConnectionError
  }
}

/**
 * Error when a request to a broker times out.
 */
export class KafkaTimeoutError extends KafkaConnectionError {
  override readonly name = "KafkaTimeoutError"

  /** Timeout duration in milliseconds. */
  readonly timeoutMs: number

  constructor(message: string, timeoutMs: number, options?: { broker?: string; cause?: unknown }) {
    super(message, { retriable: true, broker: options?.broker, cause: options?.cause })
    this.timeoutMs = timeoutMs
  }

  static override isError(error: unknown): error is KafkaTimeoutError {
    return error instanceof KafkaTimeoutError
  }
}

// ---------------------------------------------------------------------------
// Configuration / usage errors (always fatal)
// ---------------------------------------------------------------------------

/**
 * Error caused by invalid configuration or incorrect API usage.
 * These are never retriable — the caller must fix the input.
 */
export class KafkaConfigError extends KafkaError {
  override readonly name = "KafkaConfigError"

  constructor(message: string, options?: ErrorOptions) {
    super(message, false, options)
  }

  static override isError(error: unknown): error is KafkaConfigError {
    return error instanceof KafkaConfigError
  }
}
