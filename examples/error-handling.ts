/**
 * Error handling example.
 *
 * Demonstrates the Kafka client error hierarchy and type narrowing.
 *
 * @example
 * ```bash
 * bun run examples/error-handling.ts
 * ```
 */

/* eslint-disable no-console */

import {
  KafkaConfigError,
  KafkaConnectionError,
  KafkaError,
  KafkaProtocolError,
  KafkaTimeoutError
} from "../src/index"

/**
 * Simulate handling different error types.
 */
function handleError(error: unknown): void {
  if (KafkaTimeoutError.isError(error)) {
    console.log(`  Timeout (${String(error.timeoutMs)}ms) at broker: ${error.broker ?? "unknown"}`)
    console.log(`  Retriable: ${String(error.retriable)}`)
  } else if (KafkaConnectionError.isError(error)) {
    console.log(`  Connection error at broker: ${error.broker ?? "unknown"}`)
    console.log(`  Retriable: ${String(error.retriable)}`)
  } else if (KafkaProtocolError.isError(error)) {
    console.log(`  Protocol error code ${String(error.errorCode)}: ${error.message}`)
    console.log(`  Retriable: ${String(error.retriable)}`)
  } else if (KafkaConfigError.isError(error)) {
    console.log(`  Configuration error: ${error.message}`)
  } else if (KafkaError.isError(error)) {
    console.log(`  Kafka error: ${error.message}`)
  } else {
    console.log(`  Unknown error: ${String(error)}`)
  }
}

function main(): void {
  console.log("=== Error Handling Examples ===\n")

  // Example 1: Timeout error
  console.log("--- Example 1: Timeout Error ---")
  handleError(new KafkaTimeoutError("request timed out", 30000, { broker: "kafka-1:9092" }))
  console.log()

  // Example 2: Connection error
  console.log("--- Example 2: Connection Error ---")
  handleError(
    new KafkaConnectionError("connection refused", {
      broker: "kafka-2:9092",
      cause: new Error("ECONNREFUSED")
    })
  )
  console.log()

  // Example 3: Protocol error (retriable)
  console.log("--- Example 3: Retriable Protocol Error ---")
  handleError(new KafkaProtocolError("not leader for partition", 6, true))
  console.log()

  // Example 4: Protocol error (fatal)
  console.log("--- Example 4: Fatal Protocol Error ---")
  handleError(new KafkaProtocolError("topic authorisation failed", 29, false))
  console.log()

  // Example 5: Config error
  console.log("--- Example 5: Config Error ---")
  handleError(new KafkaConfigError("invalid broker address format"))

  console.log("\nExamples complete.")
}

main()
