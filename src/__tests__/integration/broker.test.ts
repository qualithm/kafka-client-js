/**
 * Integration test: broker connectivity, admin operations, produce/consume.
 *
 * Requires a running Kafka broker (see docker-compose.yml).
 * Run: `docker compose up -d` then `bun run test:integration`
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest"

import { createKafka, type Kafka } from "../../kafka"
import type { ConsumerRecord } from "../../messages"
import { getBroker, getSocketFactory, isKafkaAvailable } from "./setup"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

describe("broker connectivity", () => {
  let kafka: Kafka | undefined
  let available: boolean

  beforeAll(async () => {
    available = await isKafkaAvailable()
    if (!available) {
      return
    }

    const broker = getBroker()
    kafka = createKafka({
      config: {
        brokers: [`${broker.host}:${String(broker.port)}`],
        clientId: "integration-test"
      },
      socketFactory: getSocketFactory()
    })
    await kafka.connect()
  })

  afterAll(async () => {
    if (kafka) {
      await kafka.disconnect()
    }
  })

  it("connects to the broker and discovers metadata", () => {
    if (!available) {
      return
    }

    expect(kafka!.state).toBe("connected")
    expect(kafka!.brokers.size).toBeGreaterThan(0)
  })

  it("discovers brokers with host and port", () => {
    if (!available) {
      return
    }

    for (const [, info] of kafka!.brokers) {
      expect(info.host).toBeTruthy()
      expect(info.port).toBeGreaterThan(0)
    }
  })
})

describe("admin operations", () => {
  let kafka: Kafka | undefined
  let available: boolean

  beforeAll(async () => {
    available = await isKafkaAvailable()
    if (!available) {
      return
    }

    const broker = getBroker()
    kafka = createKafka({
      config: {
        brokers: [`${broker.host}:${String(broker.port)}`],
        clientId: "integration-test-admin"
      },
      socketFactory: getSocketFactory()
    })
    await kafka.connect()
  })

  afterAll(async () => {
    if (kafka) {
      await kafka.disconnect()
    }
  })

  it("creates and deletes a topic", async () => {
    if (!available) {
      return
    }

    const admin = kafka!.admin()
    const topicName = `integration-test-${String(Date.now())}`

    // Create topic
    await admin.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    // Verify it exists
    const topics = await admin.listTopics()
    const topicNames = topics.map((t) => t.name)
    expect(topicNames).toContain(topicName)

    // Delete topic
    await admin.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  })
})

describe("produce and consume", () => {
  let kafka: Kafka | undefined
  let available: boolean

  beforeAll(async () => {
    available = await isKafkaAvailable()
    if (!available) {
      return
    }

    const broker = getBroker()
    kafka = createKafka({
      config: {
        brokers: [`${broker.host}:${String(broker.port)}`],
        clientId: "integration-test-produce-consume"
      },
      socketFactory: getSocketFactory()
    })
    await kafka.connect()
  })

  afterAll(async () => {
    if (kafka) {
      await kafka.disconnect()
    }
  })

  it("produces and consumes messages", async () => {
    if (!available) {
      return
    }

    const topicName = `integration-produce-consume-${String(Date.now())}`
    const admin = kafka!.admin()

    // Create topic
    await admin.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    // Produce messages
    const producer = kafka!.producer()
    const result = await producer.send(topicName, [
      { key: encoder.encode("key-1"), value: encoder.encode("value-1") },
      { key: encoder.encode("key-2"), value: encoder.encode("value-2") }
    ])
    expect(result).toBeDefined()
    await producer.close()

    // Consume messages
    const consumer = kafka!.consumer({
      groupId: `test-group-${String(Date.now())}`,
      offsetReset: "earliest"
    })
    consumer.subscribe([topicName])
    await consumer.connect()

    const records: ConsumerRecord[] = []
    const maxAttempts = 20
    for (let i = 0; i < maxAttempts && records.length < 2; i++) {
      const batch = await consumer.poll()
      records.push(...batch)
    }

    await consumer.close()

    expect(records.length).toBeGreaterThanOrEqual(2)
    expect(decoder.decode(records[0].message.value!)).toBe("value-1")
    expect(decoder.decode(records[1].message.value!)).toBe("value-2")

    // Clean up
    await admin.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 30000)
})
