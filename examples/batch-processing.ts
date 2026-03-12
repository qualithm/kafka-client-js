/**
 * Batch processing example.
 *
 * Demonstrates constructing message batches and encoding them into
 * Kafka RecordBatch format (the on-wire binary format).
 *
 * @example
 * ```bash
 * bun run examples/batch-processing.ts
 * ```
 */

/* eslint-disable no-console */

import {
  buildRecordBatch,
  createRecord,
  decodeRecordBatch,
  encodeRecordBatch,
  type Message,
  parseBrokerAddress,
  type TopicPartition
} from "@qualithm/kafka-client"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

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
  console.log()

  // Encode messages into a Kafka RecordBatch (on-wire binary format)
  console.log("--- RecordBatch Encoding ---")
  const records = batch.messages.map((msg, i) =>
    createRecord(msg.key ?? null, msg.value ?? null, [], i)
  )
  const recordBatch = buildRecordBatch(records)
  const encoded = encodeRecordBatch(recordBatch)
  console.log(`  Records: ${String(recordBatch.records.length)}`)
  console.log(`  Encoded size: ${String(encoded.byteLength)} bytes`)
  console.log()

  // Decode the RecordBatch back
  console.log("--- RecordBatch Decoding ---")
  const decoded = decodeRecordBatch(encoded)
  if (!decoded.ok) {
    console.error(`  Decode failed: ${decoded.error.message}`)
    return
  }
  console.log(`  Decoded records: ${String(decoded.value.records.length)}`)
  for (const rec of decoded.value.records) {
    const key = rec.key ? decoder.decode(rec.key) : "(null)"
    const value = rec.value ? decoder.decode(rec.value) : "(null)"
    console.log(`    Offset ${String(rec.offsetDelta)}: key=${key} value=${value}`)
  }

  console.log("\nExamples complete.")
}

main()
