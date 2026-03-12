/**
 * Integration test: admin client operations.
 *
 * Tests topic management, partition creation, and config describe/alter
 * against a real Kafka broker.
 *
 * Requires a running Kafka broker (see docker-compose.yml).
 * Run: `docker compose --profile kafka up -d --wait && bun run test:integration`
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest"

import type { KafkaAdmin } from "../../client/admin"
import { createKafka, type Kafka } from "../../client/kafka"
import { ConfigResourceType } from "../../protocol/describe-configs"
import { getBroker, getSocketFactory, isKafkaAvailable, uniqueTopic } from "./setup"

describe("admin: topic lifecycle", () => {
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
        clientId: "integration-admin-lifecycle"
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

  it("creates a topic with multiple partitions", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("admin-multi-part")
    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 3, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const topics = await admin!.listTopics()
    const found = topics.find((t) => t.name === topicName)
    expect(found).toBeDefined()
    expect(found!.partitionCount).toBe(3)

    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  })

  it("lists topics and finds a known topic", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("admin-list")
    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    const topics = await admin!.listTopics()
    const names = topics.map((t) => t.name)
    expect(names).toContain(topicName)

    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  })

  it("creates and then adds partitions to a topic", async () => {
    if (!available) {
      return
    }

    const topicName = uniqueTopic("admin-add-parts")
    await admin!.createTopics({
      topics: [{ name: topicName, numPartitions: 1, replicationFactor: 1 }],
      timeoutMs: 30000
    })

    // Add partitions to bring total to 3
    await admin!.createPartitions({
      topics: [{ name: topicName, count: 3, assignments: null }],
      timeoutMs: 30000
    })

    const topics = await admin!.listTopics()
    const found = topics.find((t) => t.name === topicName)
    expect(found).toBeDefined()
    expect(found!.partitionCount).toBe(3)

    await admin!.deleteTopics({ topicNames: [topicName], timeoutMs: 30000 })
  })

  it("deletes multiple topics at once", async () => {
    if (!available) {
      return
    }

    const topic1 = uniqueTopic("admin-del-1")
    const topic2 = uniqueTopic("admin-del-2")

    await admin!.createTopics({
      topics: [
        { name: topic1, numPartitions: 1, replicationFactor: 1 },
        { name: topic2, numPartitions: 1, replicationFactor: 1 }
      ],
      timeoutMs: 30000
    })

    await admin!.deleteTopics({ topicNames: [topic1, topic2], timeoutMs: 30000 })

    const topics = await admin!.listTopics()
    const names = topics.map((t) => t.name)
    expect(names).not.toContain(topic1)
    expect(names).not.toContain(topic2)
  })
})

describe("admin: config management", () => {
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
        clientId: "integration-admin-configs"
      },
      socketFactory: getSocketFactory()
    })
    await kafka.connect()
    admin = kafka.admin()

    topicName = uniqueTopic("admin-config")
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

  it("describes topic configuration", async () => {
    if (!available) {
      return
    }

    const resources = await admin!.describeConfigs({
      resources: [
        {
          resourceType: ConfigResourceType.Topic,
          resourceName: topicName,
          configNames: null
        }
      ]
    })

    expect(resources.length).toBe(1)
    expect(resources[0].errorCode).toBe(0)
    // Every topic has retention.ms config
    const retentionConfig = resources[0].configs.find((c) => c.name === "retention.ms")
    expect(retentionConfig).toBeDefined()
  })

  it("describes specific config keys only", async () => {
    if (!available) {
      return
    }

    const resources = await admin!.describeConfigs({
      resources: [
        {
          resourceType: ConfigResourceType.Topic,
          resourceName: topicName,
          configNames: ["retention.ms", "cleanup.policy"]
        }
      ]
    })

    expect(resources.length).toBe(1)
    expect(resources[0].configs.length).toBe(2)
    const names = resources[0].configs.map((c) => c.name)
    expect(names).toContain("retention.ms")
    expect(names).toContain("cleanup.policy")
  })

  it("alters and verifies topic configuration", async () => {
    if (!available) {
      return
    }

    // Set retention.ms to 86400000 (1 day)
    await admin!.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceType.Topic,
          resourceName: topicName,
          configs: [{ name: "retention.ms", value: "86400000" }]
        }
      ]
    })

    // Verify the change
    const resources = await admin!.describeConfigs({
      resources: [
        {
          resourceType: ConfigResourceType.Topic,
          resourceName: topicName,
          configNames: ["retention.ms"]
        }
      ]
    })

    expect(resources[0].configs[0].value).toBe("86400000")
  })

  it("describes broker configuration", async () => {
    if (!available) {
      return
    }

    const resources = await admin!.describeConfigs({
      resources: [
        {
          resourceType: ConfigResourceType.Broker,
          resourceName: "1",
          configNames: ["log.retention.hours"]
        }
      ]
    })

    expect(resources.length).toBe(1)
    expect(resources[0].errorCode).toBe(0)
  })
})
