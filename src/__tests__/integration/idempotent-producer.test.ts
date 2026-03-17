/**
 * Integration test: idempotent producer.
 *
 * Tests that the idempotent producer allocates a producer ID,
 * assigns sequence numbers, and produces messages correctly.
 *
 * Requires a running Kafka broker (see docker-compose.yaml).
 * Run: `docker compose --profile kafka up -d --wait && bun run test:integration`
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest"

import type { KafkaAdmin } from "../../client/admin"
import { createKafka, type Kafka } from "../../client/kafka"
import type { ConsumerRecord } from "../../messages"
import { getBroker, getSocketFactory, isKafkaAvailable, uniqueTopic } from "./setup"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

describe("idempotent producer", () => {
  let kafka: Kafka | undefined
  let admin: KafkaAdmin | undefined
  let available: boolean
  let topicName: string

  beforeAll(async () => {
    available = await isKafkaAvailable()
    if (!available) {
      return
    }

    const broker = getBroker()
    kafka = createKafka({
      config: {
        brokers: [`${broker.host}:${String(broker.port)}`],
        clientId: "integration-idempotent"
      },
      socketFactory: getSocketFactory()
    })
    await kafka.connect()
    admin = kafka.admin()

    topicName = uniqueTopic("idempotent")
    await admin.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })
  })

  afterAll(async () => {
    if (admin && available) {
      try {
        await admin.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
      } catch {
        // best-effort cleanup
      }
    }
    if (kafka) {
      await kafka.disconnect()
    }
  })

  it("produces messages with idempotent mode enabled", async () => {
    if (!available) {
      return
    }

    const producer = kafka!.producer({ idempotent: true })
    const results = await producer.send(topicName, [
      { key: encoder.encode("idem-1"), value: encoder.encode("value-1") },
      { key: encoder.encode("idem-2"), value: encoder.encode("value-2") },
      { key: encoder.encode("idem-3"), value: encoder.encode("value-3") }
    ])

    expect(results).toBeDefined()
    expect(results.length).toBeGreaterThan(0)
    for (const r of results) {
      expect(r.baseOffset).toBeGreaterThanOrEqual(0n)
    }

    await producer.close()
  })

  it("produces sequential batches with increasing offsets", async () => {
    if (!available) {
      return
    }

    const producer = kafka!.producer({ idempotent: true })

    const result1 = await producer.send(topicName, [
      { key: encoder.encode("seq-1"), value: encoder.encode("batch-1-msg") }
    ])

    const result2 = await producer.send(topicName, [
      { key: encoder.encode("seq-2"), value: encoder.encode("batch-2-msg") }
    ])

    expect(result2[0].baseOffset).toBeGreaterThan(result1[0].baseOffset)

    await producer.close()
  })

  it("idempotent messages are consumable", async () => {
    if (!available) {
      return
    }

    const idemTopic = uniqueTopic("idem-consume")
    await admin!.createTopics({
      topics: [{ name: idemTopic, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const producer = kafka!.producer({ idempotent: true })
    await producer.send(idemTopic, [
      { key: encoder.encode("k1"), value: encoder.encode("v1") },
      { key: encoder.encode("k2"), value: encoder.encode("v2") }
    ])
    await producer.close()

    const consumer = kafka!.consumer({
      groupId: uniqueTopic("idem-group"),
      offsetReset: "earliest"
    })
    consumer.subscribe([idemTopic])
    await consumer.connect()

    const records: ConsumerRecord[] = []
    for (let i = 0; i < 20 && records.length < 2; i++) {
      const batch = await consumer.poll()
      records.push(...batch)
    }

    await consumer.close()

    expect(records.length).toBeGreaterThanOrEqual(2)
    expect(decoder.decode(records[0].message.value!)).toBe("v1")
    expect(decoder.decode(records[1].message.value!)).toBe("v2")

    await admin!.deleteTopics({ topicNames: [idemTopic], timeoutMs: 30000 })
  }, 30000)
})
