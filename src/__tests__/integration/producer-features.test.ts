/**
 * Integration test: producer features — message headers, tombstones,
 * partitioning, and batching.
 *
 * Requires a running Kafka broker (see docker-compose.yml).
 * Run: `docker compose --profile kafka up -d --wait && bun run test:integration`
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest"

import type { KafkaAdmin } from "../../client/admin"
import { createKafka, type Kafka } from "../../client/kafka"
import type { ConsumerRecord } from "../../messages"
import { getBroker, getSocketFactory, isKafkaAvailable, uniqueTopic } from "./setup"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

describe("message headers", () => {
  let kafka: Kafka | undefined
  let admin: KafkaAdmin | undefined
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
        clientId: "integration-headers"
      },
      socketFactory: getSocketFactory()
    })
    await kafka.connect()
    admin = kafka.admin()
  })

  afterAll(async () => {
    if (kafka) {
      await kafka.disconnect()
    }
  })

  it("produces and consumes messages with headers", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("headers")
    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const producer = kafka!.producer()
    await producer.send(topicName, [
      {
        key: encoder.encode("h-key"),
        value: encoder.encode("h-value"),
        headers: [
          { key: "trace-id", value: encoder.encode("abc-123") },
          { key: "source", value: encoder.encode("integration-test") },
          { key: "nullable-header", value: null }
        ]
      }
    ])
    await producer.close()

    const consumer = kafka!.consumer({
      groupId: uniqueTopic("headers-group"),
      offsetReset: "earliest"
    })
    consumer.subscribe([topicName])
    await consumer.connect()

    const records: ConsumerRecord[] = []
    for (let i = 0; i < 20 && records.length < 1; i++) {
      const batch = await consumer.poll()
      records.push(...batch)
    }

    await consumer.close()

    expect(records.length).toBeGreaterThanOrEqual(1)
    const msg = records[0].message
    expect(msg.headers).toBeDefined()
    expect(msg.headers!.length).toBe(3)

    const traceHeader = msg.headers!.find((h) => h.key === "trace-id")
    expect(traceHeader).toBeDefined()
    expect(decoder.decode(traceHeader!.value!)).toBe("abc-123")

    const sourceHeader = msg.headers!.find((h) => h.key === "source")
    expect(sourceHeader).toBeDefined()
    expect(decoder.decode(sourceHeader!.value!)).toBe("integration-test")

    const nullHeader = msg.headers!.find((h) => h.key === "nullable-header")
    expect(nullHeader).toBeDefined()
    expect(nullHeader!.value).toBeNull()

    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 30000)
})

describe("tombstone messages", () => {
  let kafka: Kafka | undefined
  let admin: KafkaAdmin | undefined
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
        clientId: "integration-tombstones"
      },
      socketFactory: getSocketFactory()
    })
    await kafka.connect()
    admin = kafka.admin()
  })

  afterAll(async () => {
    if (kafka) {
      await kafka.disconnect()
    }
  })

  it("produces and consumes tombstone (null value) messages", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("tombstones")
    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const producer = kafka!.producer()
    await producer.send(topicName, [{ key: encoder.encode("delete-me"), value: null }])
    await producer.close()

    const consumer = kafka!.consumer({
      groupId: uniqueTopic("tombstone-group"),
      offsetReset: "earliest"
    })
    consumer.subscribe([topicName])
    await consumer.connect()

    const records: ConsumerRecord[] = []
    for (let i = 0; i < 20 && records.length < 1; i++) {
      const batch = await consumer.poll()
      records.push(...batch)
    }

    await consumer.close()

    expect(records.length).toBeGreaterThanOrEqual(1)
    expect(decoder.decode(records[0].message.key!)).toBe("delete-me")
    expect(records[0].message.value).toBeNull()

    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 30000)
})

describe("multi-partition produce and consume", () => {
  let kafka: Kafka | undefined
  let admin: KafkaAdmin | undefined
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
        clientId: "integration-multi-part"
      },
      socketFactory: getSocketFactory()
    })
    await kafka.connect()
    admin = kafka.admin()
  })

  afterAll(async () => {
    if (kafka) {
      await kafka.disconnect()
    }
  })

  it("produces to multiple partitions and consumes all", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("multi-part")
    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 3, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const producer = kafka!.producer({ retry: { maxRetries: 3 } })
    const messageCount = 30

    const messages = Array.from({ length: messageCount }, (_, i) => ({
      key: encoder.encode(`key-${String(i)}`),
      value: encoder.encode(`value-${String(i)}`)
    }))

    await producer.send(topicName, messages)
    await producer.close()

    const consumer = kafka!.consumer({
      groupId: uniqueTopic("multi-part-group"),
      offsetReset: "earliest"
    })
    consumer.subscribe([topicName])
    await consumer.connect()

    const records: ConsumerRecord[] = []
    for (let i = 0; i < 30 && records.length < messageCount; i++) {
      const batch = await consumer.poll()
      records.push(...batch)
    }

    await consumer.close()

    expect(records.length).toBeGreaterThanOrEqual(messageCount)

    // Verify messages arrived across multiple partitions
    const partitions = new Set(records.map((r) => r.partition))
    expect(partitions.size).toBeGreaterThan(1)

    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 30000)
})
