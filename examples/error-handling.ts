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
} from "@qualithm/kafka-client"

// Simulate handling different error types.
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
  console.log("=== Error Handling ===\n")

  // Timeout error
  console.log("--- Timeout Error ---")
  handleError(new KafkaTimeoutError("request timed out", 30000, { broker: "kafka-1:9092" }))
  console.log()

  // Connection error
  console.log("--- Connection Error ---")
  handleError(
    new KafkaConnectionError("connection refused", {
      broker: "kafka-2:9092",
      cause: new Error("ECONNREFUSED")
    })
  )
  console.log()

  // Protocol error (retriable)
  console.log("--- Retriable Protocol Error ---")
  handleError(new KafkaProtocolError("not leader for partition", 6, true))
  console.log()

  // Protocol error (fatal)
  console.log("--- Fatal Protocol Error ---")
  handleError(new KafkaProtocolError("topic authorisation failed", 29, false))
  console.log()

  // Config error
  console.log("--- Config Error ---")
  handleError(new KafkaConfigError("invalid broker address format"))

  console.log("\nDone.")
}

main()
