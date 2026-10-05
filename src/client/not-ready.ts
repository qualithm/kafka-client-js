import { KafkaProtocolError } from "../errors.js"

/**
 * Broker error codes that mean "not ready yet". A broker returns them while it
 * starts, loads a coordinator, or propagates a just-created topic, and they
 * clear within seconds.
 */
const NOT_READY_CODES: ReadonlySet<number> = new Set([
  3, // UNKNOWN_TOPIC_OR_PARTITION — metadata right after the topic is created
  5, // LEADER_NOT_AVAILABLE
  14, // COORDINATOR_LOAD_IN_PROGRESS
  15, // COORDINATOR_NOT_AVAILABLE
  16 // NOT_COORDINATOR
])

/** How long, and how often, to retry a not-ready broker error. */
export type NotReadyRetryPolicy = {
  /** Total time budget in milliseconds; once spent the last error is thrown. */
  readonly windowMs: number
  /** Delay before the first retry in milliseconds; doubles per attempt up to `maxDelayMs`. */
  readonly initialDelayMs: number
  /** Per-attempt delay cap in milliseconds. */
  readonly maxDelayMs: number
}

export const defaultNotReadyRetry: NotReadyRetryPolicy = {
  windowMs: 15_000,
  initialDelayMs: 100,
  maxDelayMs: 1_000
}

/**
 * Whether an error is a broker reporting it is not ready yet.
 *
 * @param error - the thrown value
 * @returns true for a `KafkaProtocolError` carrying a not-ready code
 */
export function isNotReadyError(error: unknown): error is KafkaProtocolError {
  return KafkaProtocolError.isError(error) && NOT_READY_CODES.has(error.errorCode)
}

/**
 * Run `fn`, retrying while the broker reports it is not ready, until the
 * policy's window is spent. Any other error is thrown at once.
 *
 * @param fn - the operation to run
 * @param onRetry - called before each retry, e.g. to drop a cached coordinator
 * @param policy - the retry window and backoff
 * @returns the operation's result
 * @throws the last error once the window is spent, or any non-not-ready error
 */
export async function retryWhileNotReady<T>(
  fn: () => Promise<T>,
  onRetry?: (error: KafkaProtocolError) => void,
  policy: NotReadyRetryPolicy = defaultNotReadyRetry
): Promise<T> {
  const deadline = Date.now() + policy.windowMs
  for (let attempt = 0; ; attempt++) {
    try {
      return await fn()
    } catch (error) {
      const delay = Math.min(policy.initialDelayMs * 2 ** attempt, policy.maxDelayMs)
      if (!isNotReadyError(error) || Date.now() + delay > deadline) {
        throw error
      }
      onRetry?.(error)
      await new Promise<void>((resolve) => setTimeout(resolve, delay))
    }
  }
}
