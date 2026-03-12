/**
 * Basic usage example.
 *
 * Demonstrates core type usage patterns for the Kafka client.
 *
 * @example
 * ```bash
 * bun run examples/basic-usage.ts
 * ```
 */

/* eslint-disable no-console */

import {
  ApiKey,
  decodeFailure,
  decodeSuccess,
  type KafkaConfig,
  type Message,
  negotiateVersion,
  parseBrokerAddress
} from "@qualithm/kafka-client"

function main(): void {
  console.log("=== Basic Usage Examples ===\n")

  // Example 1: Parse broker addresses
  console.log("--- Example 1: Broker Address Parsing ---")
  const broker = parseBrokerAddress("kafka-1:9092")
  console.log(`  Parsed: ${broker.host}:${String(broker.port)}`)
  console.log()

  // Example 2: Configuration
  console.log("--- Example 2: Client Configuration ---")
  const config: KafkaConfig = {
    brokers: ["kafka-1:9092", "kafka-2:9092"],
    clientId: "my-app",
    connectionTimeoutMs: 5000
  }
  console.log(`  Client ID: ${config.clientId ?? "(default)"}`)
  console.log(`  Brokers: ${(config.brokers as readonly string[]).join(", ")}`)
  console.log()

  // Example 3: Message types
  console.log("--- Example 3: Message Types ---")
  const encoder = new TextEncoder()
  const message: Message = {
    key: encoder.encode("user-123"),
    value: encoder.encode(JSON.stringify({ event: "login" })),
    headers: [{ key: "source", value: encoder.encode("auth-service") }]
  }
  console.log(`  Key length: ${String(message.key?.byteLength)} bytes`)
  console.log(`  Value length: ${String(message.value?.byteLength)} bytes`)
  console.log(`  Headers: ${String(message.headers?.length)}`)
  console.log()

  // Example 4: Decode results
  console.log("--- Example 4: Decode Results ---")
  const success = decodeSuccess(42, 4)
  console.log(`  Success: value=${String(success.value)}, bytesRead=${String(success.bytesRead)}`)
  const failure = decodeFailure("BUFFER_UNDERFLOW", "need 4 bytes, got 2")
  console.log(`  Failure: ${failure.error.code} - ${failure.error.message}`)
  console.log()

  // Example 5: Version negotiation
  console.log("--- Example 5: Version Negotiation ---")
  const version = negotiateVersion(ApiKey.Metadata, { minVersion: 0, maxVersion: 10 })
  console.log(`  Negotiated Metadata version: ${String(version)}`)

  console.log("\nExamples complete.")
}

main()
