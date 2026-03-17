/**
 * Integration test: SASL/PLAIN and SASL/SCRAM authentication.
 *
 * Requires a SASL-enabled Kafka broker (see docker-compose.yaml kafka-sasl profile).
 * Run: `docker compose --profile kafka-sasl up -d` then `KAFKA_SASL_PORT=9094 bun run test:integration:sasl`
 *
 * Tests verify that the client can connect, authenticate, and perform
 * basic produce/consume operations over SASL_PLAINTEXT listeners with
 * PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 mechanisms.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_SaslHandshake
 * @see https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest"

import { createKafka, type Kafka } from "../../client/kafka"
import type { SaslConfig } from "../../config"
import type { ConsumerRecord } from "../../messages"
import {
  getSaslBroker,
  getSaslPlainConfig,
  getSaslScram256Config,
  getSaslScram512Config,
  getSocketFactory,
  isSaslBrokerAvailable
} from "./setup"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

function createSaslKafka(sasl: SaslConfig, clientId: string): Kafka {
  const broker = getSaslBroker()
  return createKafka({
    config: {
      brokers: [`${broker.host}:${String(broker.port)}`],
      clientId,
      sasl
    },
    socketFactory: getSocketFactory()
  })
}

// ---------------------------------------------------------------------------
// SASL/PLAIN
// ---------------------------------------------------------------------------

describe("SASL/PLAIN authentication", () => {
  let kafka: Kafka | undefined
  let available: boolean

  beforeAll(async () => {
    available = await isSaslBrokerAvailable()
    if (!available) {
      return
    }

    kafka = createSaslKafka(getSaslPlainConfig(), "sasl-plain-test")
    await kafka.connect()
  })

  afterAll(async () => {
    if (kafka) {
      await kafka.disconnect()
    }
  })

  it("connects and discovers brokers", () => {
    if (!available) {
      return
    }

    expect(kafka!.state).toBe("connected")
    expect(kafka!.brokers.size).toBeGreaterThan(0)
  })

  it("produces and consumes messages", async () => {
    if (!available) {
      return
    }

    const topicName = `sasl-plain-test-${String(Date.now())}`
    const admin = kafka!.admin()

    await admin.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const producer = kafka!.producer()
    const result = await producer.send(topicName, [
      { key: encoder.encode("plain-key"), value: encoder.encode("plain-value") }
    ])
    expect(result).toBeDefined()
    await producer.close()

    const consumer = kafka!.consumer({
      groupId: `sasl-plain-group-${String(Date.now())}`,
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
    expect(decoder.decode(records[0].message.value!)).toBe("plain-value")

    await admin.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 45000)

  it("rejects invalid credentials", async () => {
    if (!available) {
      return
    }

    const badKafka = createSaslKafka(
      { mechanism: "PLAIN", username: "baduser", password: "badpassword" },
      "sasl-plain-bad-creds"
    )

    await expect(badKafka.connect()).rejects.toThrow()
  }, 15000)
})

// ---------------------------------------------------------------------------
// SASL/SCRAM-SHA-256
// ---------------------------------------------------------------------------

describe("SASL/SCRAM-SHA-256 authentication", () => {
  let kafka: Kafka | undefined
  let available: boolean

  beforeAll(async () => {
    available = await isSaslBrokerAvailable()
    if (!available) {
      return
    }

    kafka = createSaslKafka(getSaslScram256Config(), "sasl-scram256-test")
    await kafka.connect()
  })

  afterAll(async () => {
    if (kafka) {
      await kafka.disconnect()
    }
  })

  it("connects and discovers brokers", () => {
    if (!available) {
      return
    }

    expect(kafka!.state).toBe("connected")
    expect(kafka!.brokers.size).toBeGreaterThan(0)
  })

  it("produces and consumes messages", async () => {
    if (!available) {
      return
    }

    const topicName = `sasl-scram256-test-${String(Date.now())}`
    const admin = kafka!.admin()

    await admin.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const producer = kafka!.producer()
    const result = await producer.send(topicName, [
      { key: encoder.encode("scram256-key"), value: encoder.encode("scram256-value") }
    ])
    expect(result).toBeDefined()
    await producer.close()

    const consumer = kafka!.consumer({
      groupId: `sasl-scram256-group-${String(Date.now())}`,
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
    expect(decoder.decode(records[0].message.value!)).toBe("scram256-value")

    await admin.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 45000)

  it("rejects invalid credentials", async () => {
    if (!available) {
      return
    }

    const badKafka = createSaslKafka(
      { mechanism: "SCRAM-SHA-256", username: "baduser", password: "badpassword" },
      "sasl-scram256-bad-creds"
    )

    await expect(badKafka.connect()).rejects.toThrow()
  }, 15000)
})

// ---------------------------------------------------------------------------
// SASL/SCRAM-SHA-512
// ---------------------------------------------------------------------------

describe("SASL/SCRAM-SHA-512 authentication", () => {
  let kafka: Kafka | undefined
  let available: boolean

  beforeAll(async () => {
    available = await isSaslBrokerAvailable()
    if (!available) {
      return
    }

    kafka = createSaslKafka(getSaslScram512Config(), "sasl-scram512-test")
    await kafka.connect()
  })

  afterAll(async () => {
    if (kafka) {
      await kafka.disconnect()
    }
  })

  it("connects and discovers brokers", () => {
    if (!available) {
      return
    }

    expect(kafka!.state).toBe("connected")
    expect(kafka!.brokers.size).toBeGreaterThan(0)
  })

  it("produces and consumes messages", async () => {
    if (!available) {
      return
    }

    const topicName = `sasl-scram512-test-${String(Date.now())}`
    const admin = kafka!.admin()

    await admin.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const producer = kafka!.producer()
    const result = await producer.send(topicName, [
      { key: encoder.encode("scram512-key"), value: encoder.encode("scram512-value") }
    ])
    expect(result).toBeDefined()
    await producer.close()

    const consumer = kafka!.consumer({
      groupId: `sasl-scram512-group-${String(Date.now())}`,
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
    expect(decoder.decode(records[0].message.value!)).toBe("scram512-value")

    await admin.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  }, 45000)

  it("rejects invalid credentials", async () => {
    if (!available) {
      return
    }

    const badKafka = createSaslKafka(
      { mechanism: "SCRAM-SHA-512", username: "baduser", password: "badpassword" },
      "sasl-scram512-bad-creds"
    )

    await expect(badKafka.connect()).rejects.toThrow()
  }, 15000)
})
