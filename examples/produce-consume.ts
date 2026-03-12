/**
 * Produce and consume example.
 *
 * Demonstrates end-to-end message flow with a live Kafka broker.
 * Requires a running broker (see docker-compose.yml):
 *
 *   docker compose up -d
 *
 * @example
 * ```bash
 * bun run examples/produce-consume.ts
 * ```
 */

/* eslint-disable no-console */

import { createKafka, createNodeSocketFactory } from "@qualithm/kafka-client"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

async function main(): Promise<void> {
  console.log("=== Produce & Consume Example ===\n")

  const broker = process.env.KAFKA_BROKER ?? "localhost:9092"
  console.log(`Broker: ${broker}\n`)

  // Create client
  const kafka = createKafka({
    config: { brokers: [broker], clientId: "example-app" },
    socketFactory: createNodeSocketFactory()
  })
  await kafka.connect()

  const admin = kafka.admin()
  const topicName = `example-topic-${String(Date.now())}`

  try {
    // Create topic
    console.log(`Creating topic: ${topicName}`)
    await admin.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })
    console.log("Topic created.\n")

    // Produce messages
    console.log("--- Producing Messages ---")
    const producer = kafka.producer()
    const messages = [
      { key: "user-1", value: JSON.stringify({ event: "login", ts: Date.now() }) },
      { key: "user-2", value: JSON.stringify({ event: "purchase", ts: Date.now() }) },
      { key: "user-1", value: JSON.stringify({ event: "logout", ts: Date.now() }) }
    ]

    const result = await producer.send(
      topicName,
      messages.map((m) => ({
        key: encoder.encode(m.key),
        value: encoder.encode(m.value)
      }))
    )
    console.log(`Produced ${String(messages.length)} messages:`, result)
    await producer.close()
    console.log()

    // Consume messages
    console.log("--- Consuming Messages ---")
    const consumer = kafka.consumer({ groupId: `example-group-${String(Date.now())}` })
    consumer.subscribe([topicName])
    await consumer.connect()

    const maxAttempts = 20
    let received = 0
    for (let i = 0; i < maxAttempts && received < messages.length; i++) {
      const records = await consumer.poll()
      for (const record of records) {
        const key = record.message.key ? decoder.decode(record.message.key) : "(null)"
        const value = record.message.value ? decoder.decode(record.message.value) : "(null)"
        console.log(
          `  Partition ${String(record.partition)} Offset ${String(record.offset)}: key=${key} value=${value}`
        )
        received++
      }
    }

    console.log(`\nReceived ${String(received)} of ${String(messages.length)} messages.`)
    await consumer.close()

    // Clean up topic
    await admin.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
    console.log(`\nDeleted topic: ${topicName}`)
  } finally {
    await kafka.disconnect()
  }

  console.log("\nExample complete.")
}

main().catch(console.error)
