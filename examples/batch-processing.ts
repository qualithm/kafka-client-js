/**
 * Batch processing example.
 *
 * Demonstrates constructing batches of Kafka messages.
 *
 * @example
 * ```bash
 * bun run examples/batch-processing.ts
 * ```
 */

/* eslint-disable no-console */

import { type Message, parseBrokerAddress, type TopicPartition } from "../src/index"

const encoder = new TextEncoder()

/**
 * Create a batch of messages for a topic-partition.
 */
function createBatch(
  topicPartition: TopicPartition,
  records: { key: string; value: string }[]
): { topicPartition: TopicPartition; messages: Message[] } {
  const messages: Message[] = records.map((r) => ({
    key: encoder.encode(r.key),
    value: encoder.encode(r.value)
  }))
  return { topicPartition, messages }
}

function main(): void {
  console.log("=== Batch Processing Examples ===\n")

  // Parse broker addresses
  const brokers = ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]
  console.log("--- Broker Addresses ---")
  for (const addr of brokers) {
    const parsed = parseBrokerAddress(addr)
    console.log(`  ${addr} => ${parsed.host}:${String(parsed.port)}`)
  }
  console.log()

  // Create a batch of messages
  const tp: TopicPartition = { topic: "user-events", partition: 0 }
  const batch = createBatch(tp, [
    { key: "user-1", value: JSON.stringify({ event: "login" }) },
    { key: "user-2", value: JSON.stringify({ event: "purchase" }) },
    { key: "user-3", value: JSON.stringify({ event: "logout" }) }
  ])

  console.log("--- Message Batch ---")
  console.log(`  Topic: ${batch.topicPartition.topic}`)
  console.log(`  Partition: ${String(batch.topicPartition.partition)}`)
  console.log(`  Messages: ${String(batch.messages.length)}`)
  for (const msg of batch.messages) {
    console.log(
      `    Key: ${String(msg.key?.byteLength)} bytes, Value: ${String(msg.value?.byteLength)} bytes`
    )
  }

  console.log("\nExamples complete.")
}

main()
