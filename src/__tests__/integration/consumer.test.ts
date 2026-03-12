/**
 * Integration test: consumer group coordination and rebalance.
 *
 * Tests that consumers join groups, receive partition assignments,
 * and handle rebalance when members join or leave.
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

describe("consumer group rebalance", () => {
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
        clientId: "integration-rebalance"
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

  it("single consumer receives all partitions", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("rebalance-single")
    const groupId = uniqueTopic("rebalance-single-group")

    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 3, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const consumer = kafka!.consumer({
      groupId,
      offsetReset: "earliest",
      sessionTimeoutMs: 10000,
      heartbeatIntervalMs: 1000
    })
    consumer.subscribe([topicName])
    await consumer.connect()

    // After joining, the single consumer should own all 3 partitions
    expect(consumer.partitions.length).toBe(3)

    const assignedPartitions = consumer.partitions.map((p) => p.partition).sort()
    expect(assignedPartitions).toEqual([0, 1, 2])

    await consumer.close()
    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 30000)

  it("two consumers share partitions after rebalance", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("rebalance-two")
    const groupId = uniqueTopic("rebalance-two-group")

    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 4, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const broker = getBroker()

    // Each consumer needs its own Kafka instance (separate connection pool)
    // because the pool defaults to 1 connection per broker and JoinGroup
    // blocks on the broker side until all members rejoin.
    const kafka1 = createKafka({
      config: {
        brokers: [`${broker.host}:${String(broker.port)}`],
        clientId: "integration-rebalance-c1"
      },
      socketFactory: getSocketFactory()
    })
    await kafka1.connect()

    const kafka2 = createKafka({
      config: {
        brokers: [`${broker.host}:${String(broker.port)}`],
        clientId: "integration-rebalance-c2"
      },
      socketFactory: getSocketFactory()
    })
    await kafka2.connect()

    // Consumer 1 joins first
    const consumer1 = kafka1.consumer({
      groupId,
      offsetReset: "earliest",
      sessionTimeoutMs: 10000,
      heartbeatIntervalMs: 1000
    })
    consumer1.subscribe([topicName])
    await consumer1.connect()

    // Consumer 1 should initially own all partitions
    expect(consumer1.partitions.length).toBe(4)

    // Consumer 2 joins — triggers rebalance.
    const consumer2 = kafka2.consumer({
      groupId,
      offsetReset: "earliest",
      sessionTimeoutMs: 10000,
      heartbeatIntervalMs: 1000
    })
    consumer2.subscribe([topicName])

    // Poll consumer1 concurrently so it detects the rebalance via heartbeat
    // and rejoins the group (JoinGroup blocks until all members rejoin).
    const connectPromise = consumer2.connect()
    const pollUntilDone = async (): Promise<void> => {
      for (let i = 0; i < 40; i++) {
        await consumer1.poll()
        await new Promise((r) => setTimeout(r, 250))
      }
    }
    await Promise.all([connectPromise, pollUntilDone()])

    const total = consumer1.partitions.length + consumer2.partitions.length
    expect(total).toBe(4)
    // Each consumer should have at least 1 partition
    expect(consumer1.partitions.length).toBeGreaterThanOrEqual(1)
    expect(consumer2.partitions.length).toBeGreaterThanOrEqual(1)

    await consumer1.close()
    await consumer2.close()
    await kafka1.disconnect()
    await kafka2.disconnect()
    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 45000)

  it("consumer gets partitions back when another leaves", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("rebalance-leave")
    const groupId = uniqueTopic("rebalance-leave-group")

    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 2, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const broker = getBroker()

    // Separate Kafka instances to avoid connection pool deadlock
    const kafka1 = createKafka({
      config: {
        brokers: [`${broker.host}:${String(broker.port)}`],
        clientId: "integration-leave-c1"
      },
      socketFactory: getSocketFactory()
    })
    await kafka1.connect()

    const kafka2 = createKafka({
      config: {
        brokers: [`${broker.host}:${String(broker.port)}`],
        clientId: "integration-leave-c2"
      },
      socketFactory: getSocketFactory()
    })
    await kafka2.connect()

    const consumer1 = kafka1.consumer({
      groupId,
      offsetReset: "earliest",
      sessionTimeoutMs: 10000,
      heartbeatIntervalMs: 1000
    })
    consumer1.subscribe([topicName])
    await consumer1.connect()

    const consumer2 = kafka2.consumer({
      groupId,
      offsetReset: "earliest",
      sessionTimeoutMs: 10000,
      heartbeatIntervalMs: 1000
    })
    consumer2.subscribe([topicName])

    // Poll consumer1 concurrently during consumer2 join
    const connectPromise = consumer2.connect()
    const pollDuringJoin = async (): Promise<void> => {
      for (let i = 0; i < 40; i++) {
        await consumer1.poll()
        await new Promise((r) => setTimeout(r, 250))
      }
    }
    await Promise.all([connectPromise, pollDuringJoin()])

    // Each should have 1 partition
    expect(consumer1.partitions.length + consumer2.partitions.length).toBe(2)

    // Consumer 2 leaves
    await consumer2.close()
    await kafka2.disconnect()

    // Consumer 1 should eventually reclaim all partitions via rebalance
    for (let i = 0; i < 30; i++) {
      await consumer1.poll()
      if (consumer1.partitions.length === 2) {
        break
      }
      await new Promise((r) => setTimeout(r, 500))
    }

    expect(consumer1.partitions.length).toBe(2)

    await consumer1.close()
    await kafka1.disconnect()
    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 45000)
})

describe("consumer offset management", () => {
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
        clientId: "integration-offsets"
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

  it("resumes from committed offsets after reconnect", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("offset-resume")
    const groupId = uniqueTopic("offset-resume-group")

    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    // Produce 4 messages
    const producer = kafka!.producer()
    await producer.send(topicName, [
      { key: null, value: encoder.encode("msg-0") },
      { key: null, value: encoder.encode("msg-1") },
      { key: null, value: encoder.encode("msg-2") },
      { key: null, value: encoder.encode("msg-3") }
    ])
    await producer.close()

    // First consumer reads first 2 and commits
    const consumer1 = kafka!.consumer({
      groupId,
      offsetReset: "earliest",
      autoCommit: false
    })
    consumer1.subscribe([topicName])
    await consumer1.connect()

    const firstBatch: ConsumerRecord[] = []
    for (let i = 0; i < 20 && firstBatch.length < 2; i++) {
      const batch = await consumer1.poll()
      firstBatch.push(...batch)
    }

    expect(firstBatch.length).toBeGreaterThanOrEqual(2)
    await consumer1.commitOffsets()
    await consumer1.close()

    // Second consumer with same groupId should resume after committed offset
    const consumer2 = kafka!.consumer({
      groupId,
      offsetReset: "earliest",
      autoCommit: false
    })
    consumer2.subscribe([topicName])
    await consumer2.connect()

    const secondBatch: ConsumerRecord[] = []
    for (let i = 0; i < 20 && secondBatch.length < 2; i++) {
      const batch = await consumer2.poll()
      secondBatch.push(...batch)
    }

    await consumer2.close()

    // Should get messages starting after what was committed
    if (secondBatch.length > 0) {
      const firstOffset = secondBatch[0].offset
      const lastCommittedOffset = firstBatch[firstBatch.length - 1].offset
      expect(firstOffset).toBeGreaterThan(lastCommittedOffset)
    }

    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 30000)

  it("earliest offset reset reads from beginning", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("offset-earliest")

    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    // Produce before consumer starts
    const producer = kafka!.producer()
    await producer.send(topicName, [
      { key: null, value: encoder.encode("pre-existing-1") },
      { key: null, value: encoder.encode("pre-existing-2") }
    ])
    await producer.close()

    const consumer = kafka!.consumer({
      groupId: uniqueTopic("offset-earliest-group"),
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
    expect(decoder.decode(records[0].message.value!)).toBe("pre-existing-1")

    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 30000)
})
