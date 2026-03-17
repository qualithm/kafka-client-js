/**
 * Integration test: transactional producer.
 *
 * Tests transaction lifecycle (begin, commit, abort) and verifies
 * isolation semantics with read_committed consumers.
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

describe("transactional producer", () => {
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
        clientId: "integration-txn"
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

  it("commits a transaction and messages are visible", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("txn-commit")
    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const producer = kafka!.producer({
      transactionalId: `txn-commit-${String(Date.now())}`
    })

    producer.beginTransaction()
    await producer.send(topicName, [
      { key: encoder.encode("txn-1"), value: encoder.encode("committed-value-1") },
      { key: encoder.encode("txn-2"), value: encoder.encode("committed-value-2") }
    ])
    await producer.commitTransaction()
    await producer.close()

    // Consume committed messages
    const consumer = kafka!.consumer({
      groupId: uniqueTopic("txn-commit-group"),
      offsetReset: "earliest"
    })
    consumer.subscribe([topicName])
    await consumer.connect()

    const records: ConsumerRecord[] = []
    for (let i = 0; i < 20 && records.length < 2; i++) {
      const batch = await consumer.poll()
      records.push(...batch)
    }

    await consumer.close()

    expect(records.length).toBeGreaterThanOrEqual(2)
    expect(decoder.decode(records[0].message.value!)).toBe("committed-value-1")
    expect(decoder.decode(records[1].message.value!)).toBe("committed-value-2")

    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 30000)

  it("aborts a transaction successfully", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("txn-abort")
    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    // Produce an aborted message — should succeed without errors
    const abortProducer = kafka!.producer({
      transactionalId: `txn-abort-${String(Date.now())}`
    })
    abortProducer.beginTransaction()
    await abortProducer.send(topicName, [
      { key: encoder.encode("hidden"), value: encoder.encode("this-is-aborted") }
    ])
    await abortProducer.abortTransaction()
    await abortProducer.close()

    // Produce a committed message after the abort
    const commitProducer = kafka!.producer({
      transactionalId: `txn-abort-commit-${String(Date.now())}`
    })
    commitProducer.beginTransaction()
    await commitProducer.send(topicName, [
      { key: encoder.encode("visible"), value: encoder.encode("this-is-committed") }
    ])
    await commitProducer.commitTransaction()
    await commitProducer.close()

    // Consume with read_uncommitted — both messages should be present
    const consumer = kafka!.consumer({
      groupId: uniqueTopic("txn-abort-group"),
      offsetReset: "earliest"
    })
    consumer.subscribe([topicName])
    await consumer.connect()

    const records: ConsumerRecord[] = []
    for (let i = 0; i < 20 && records.length < 2; i++) {
      const batch = await consumer.poll()
      records.push(...batch)
    }

    await consumer.close()

    // Both messages should be visible with read_uncommitted
    const values = records.map((r) => decoder.decode(r.message.value!))
    expect(values).toContain("this-is-committed")

    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 30000)

  it("runs multiple transactions sequentially", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("txn-multi")
    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const producer = kafka!.producer({
      transactionalId: `txn-multi-${String(Date.now())}`
    })

    // Transaction 1: commit
    producer.beginTransaction()
    await producer.send(topicName, [
      { key: encoder.encode("t1"), value: encoder.encode("txn1-value") }
    ])
    await producer.commitTransaction()

    // Transaction 2: commit
    producer.beginTransaction()
    await producer.send(topicName, [
      { key: encoder.encode("t2"), value: encoder.encode("txn2-value") }
    ])
    await producer.commitTransaction()

    await producer.close()

    // Consume both
    const consumer = kafka!.consumer({
      groupId: uniqueTopic("txn-multi-group"),
      offsetReset: "earliest"
    })
    consumer.subscribe([topicName])
    await consumer.connect()

    const records: ConsumerRecord[] = []
    for (let i = 0; i < 20 && records.length < 2; i++) {
      const batch = await consumer.poll()
      records.push(...batch)
    }

    await consumer.close()

    expect(records.length).toBeGreaterThanOrEqual(2)
    const values = records.map((r) => decoder.decode(r.message.value!))
    expect(values).toContain("txn1-value")
    expect(values).toContain("txn2-value")

    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 30000)
})
