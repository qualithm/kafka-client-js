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
  console.log("=== Basic Usage ===\n")

  // Parse broker addresses
  console.log("--- Broker Address Parsing ---")
  const broker = parseBrokerAddress("kafka-1:9092")
  console.log(`  Parsed: ${broker.host}:${String(broker.port)}`)

  // Configuration
  console.log("\n--- Client Configuration ---")
  const config: KafkaConfig = {
    brokers: ["kafka-1:9092", "kafka-2:9092"],
    clientId: "my-app",
    connectionTimeoutMs: 5000
  }
  console.log(`  Client ID: ${config.clientId ?? "(default)"}`)
  console.log(`  Brokers: ${(config.brokers as readonly string[]).join(", ")}`)

  // Message types
  console.log("\n--- Message Types ---")
  const encoder = new TextEncoder()
  const message: Message = {
    key: encoder.encode("user-123"),
    value: encoder.encode(JSON.stringify({ event: "login" })),
    headers: [{ key: "source", value: encoder.encode("auth-service") }]
  }
  console.log(`  Key length: ${String(message.key?.byteLength)} bytes`)
  console.log(`  Value length: ${String(message.value?.byteLength)} bytes`)
  console.log(`  Headers: ${String(message.headers?.length)}`)

  // Decode results
  console.log("\n--- Decode Results ---")
  const success = decodeSuccess(42, 4)
  console.log(`  Success: value=${String(success.value)}, bytesRead=${String(success.bytesRead)}`)
  const failure = decodeFailure("BUFFER_UNDERFLOW", "need 4 bytes, got 2")
  console.log(`  Failure: ${failure.error.code} - ${failure.error.message}`)

  // Version negotiation
  console.log("\n--- Version Negotiation ---")
  const version = negotiateVersion(ApiKey.Metadata, { minVersion: 0, maxVersion: 10 })
  console.log(`  Negotiated Metadata version: ${String(version)}`)

  console.log("\nDone.")
}

main()
