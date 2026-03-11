import { describe, expect, it, vi } from "vitest"

import { createAdmin, KafkaAdmin } from "../../../client/admin"
import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import { KafkaConnectionError } from "../../../errors"
import type { ConnectionPool } from "../../../network/broker-pool"
import { ConfigResourceType } from "../../../protocol/describe-configs"

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function buildApiVersionsBody(
  apis: { apiKey: number; minVersion: number; maxVersion: number }[]
): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt16(0) // error_code
  w.writeInt32(apis.length)
  for (const api of apis) {
    w.writeInt16(api.apiKey)
    w.writeInt16(api.minVersion)
    w.writeInt16(api.maxVersion)
  }
  return w.finish()
}

const STANDARD_APIS = [
  { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
  { apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 1 },
  { apiKey: ApiKey.CreateTopics, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DeleteTopics, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.CreatePartitions, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeConfigs, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.AlterConfigs, minVersion: 0, maxVersion: 0 }
]

const TEST_BROKERS = [{ nodeId: 1, host: "localhost", port: 9092, rack: null }]

function createMockConnection(
  responses: Uint8Array[],
  brokerAddr = "localhost:9092"
): {
  send: ReturnType<typeof vi.fn>
  broker: string
  connected: boolean
} {
  let callIndex = 0
  return {
    // eslint-disable-next-line @typescript-eslint/require-await
    send: vi.fn(async () => {
      if (callIndex >= responses.length) {
        throw new Error("no more mock responses")
      }
      return new BinaryReader(responses[callIndex++])
    }),
    broker: brokerAddr,
    connected: true
  }
}

function createMockPool(overrides?: Partial<ConnectionPool>): ConnectionPool {
  return {
    brokers: new Map(),
    isClosed: false,
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    connect: async () => {},
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    refreshMetadata: async () => {},
    getConnection: () => {
      throw new Error("not implemented")
    },
    getConnectionByNodeId: () => {
      throw new Error("not implemented")
    },
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    releaseConnection: () => {},
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    close: async () => {},
    connectionCount: () => 0,
    ...overrides
  } as unknown as ConnectionPool
}

function poolWithConn(mockConn: ReturnType<typeof createMockConnection>): ConnectionPool {
  const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
  return createMockPool({
    brokers: brokerMap as ConnectionPool["brokers"],
    getConnectionByNodeId: vi.fn(
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => mockConn
    ) as unknown as ConnectionPool["getConnectionByNodeId"],
    releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
  })
}

// ---------------------------------------------------------------------------
// Response body builders
// ---------------------------------------------------------------------------

function buildCreateTopicsResponseBody(): Uint8Array {
  const w = new BinaryWriter()
  // v0: no throttle_time
  w.writeInt32(1) // topics
  w.writeString("new-topic")
  w.writeInt16(0) // error_code
  return w.finish()
}

function buildDeleteTopicsResponseBody(): Uint8Array {
  const w = new BinaryWriter()
  // v0: no throttle_time
  w.writeInt32(1) // topics
  w.writeString("old-topic")
  w.writeInt16(0) // error_code
  return w.finish()
}

function buildCreatePartitionsResponseBody(): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt32(0) // throttle_time
  w.writeInt32(1) // topics
  w.writeString("grow-topic")
  w.writeInt16(0) // error_code
  w.writeString(null) // error_message
  return w.finish()
}

function buildDescribeConfigsResponseBody(): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt32(0) // throttle_time
  w.writeInt32(1) // resources
  w.writeInt16(0) // error_code
  w.writeString(null) // error_message
  w.writeInt8(2) // Topic
  w.writeString("cfg-topic")
  w.writeInt32(1) // configs
  w.writeString("cleanup.policy")
  w.writeString("delete")
  w.writeBoolean(false) // read_only
  w.writeBoolean(true) // is_default
  w.writeBoolean(false) // is_sensitive
  return w.finish()
}

function buildAlterConfigsResponseBody(): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt32(0) // throttle_time
  w.writeInt32(1) // resources
  w.writeInt16(0) // error_code
  w.writeString(null) // error_message
  w.writeInt8(2) // Topic
  w.writeString("altered-topic")
  return w.finish()
}

function buildMetadataV1Body(
  brokers: { nodeId: number; host: string; port: number }[],
  topics: {
    errorCode: number
    name: string
    isInternal: boolean
    partitions: {
      errorCode: number
      partitionIndex: number
      leaderId: number
      replicaNodes: number[]
      isrNodes: number[]
    }[]
  }[]
): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt32(brokers.length)
  for (const b of brokers) {
    w.writeInt32(b.nodeId)
    w.writeString(b.host)
    w.writeInt32(b.port)
    w.writeString(null) // rack
  }
  w.writeInt32(0) // controller_id
  w.writeInt32(topics.length)
  for (const t of topics) {
    w.writeInt16(t.errorCode)
    w.writeString(t.name)
    w.writeBoolean(t.isInternal)
    w.writeInt32(t.partitions.length)
    for (const p of t.partitions) {
      w.writeInt16(p.errorCode)
      w.writeInt32(p.partitionIndex)
      w.writeInt32(p.leaderId)
      w.writeInt32(p.replicaNodes.length)
      for (const r of p.replicaNodes) {
        w.writeInt32(r)
      }
      w.writeInt32(p.isrNodes.length)
      for (const n of p.isrNodes) {
        w.writeInt32(n)
      }
    }
  }
  return w.finish()
}

// ---------------------------------------------------------------------------
// KafkaAdmin — constructor and createAdmin
// ---------------------------------------------------------------------------

describe("createAdmin", () => {
  it("returns a KafkaAdmin instance", () => {
    const pool = createMockPool()
    const admin = createAdmin({ connectionPool: pool })
    expect(admin).toBeInstanceOf(KafkaAdmin)
  })
})

describe("KafkaAdmin", () => {
  // -------------------------------------------------------------------------
  // createTopics
  // -------------------------------------------------------------------------

  describe("createTopics", () => {
    it("creates topics via the controller", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildCreateTopicsResponseBody()
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const topics = await admin.createTopics({
        topics: [{ name: "new-topic", numPartitions: 1, replicationFactor: 1 }],
        timeoutMs: 5000
      })

      expect(topics).toHaveLength(1)
      expect(topics[0].name).toBe("new-topic")
      expect(topics[0].errorCode).toBe(0)
      expect(conn.send).toHaveBeenCalledTimes(2) // ApiVersions + CreateTopics
    })
  })

  // -------------------------------------------------------------------------
  // deleteTopics
  // -------------------------------------------------------------------------

  describe("deleteTopics", () => {
    it("deletes topics via the controller", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildDeleteTopicsResponseBody()
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const topics = await admin.deleteTopics({
        topicNames: ["old-topic"],
        timeoutMs: 5000
      })

      expect(topics).toHaveLength(1)
      expect(topics[0].name).toBe("old-topic")
      expect(topics[0].errorCode).toBe(0)
    })
  })

  // -------------------------------------------------------------------------
  // createPartitions
  // -------------------------------------------------------------------------

  describe("createPartitions", () => {
    it("creates partitions via the controller", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildCreatePartitionsResponseBody()
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const topics = await admin.createPartitions({
        topics: [{ name: "grow-topic", count: 6, assignments: null }],
        timeoutMs: 5000
      })

      expect(topics).toHaveLength(1)
      expect(topics[0].name).toBe("grow-topic")
      expect(topics[0].errorCode).toBe(0)
    })
  })

  // -------------------------------------------------------------------------
  // describeConfigs
  // -------------------------------------------------------------------------

  describe("describeConfigs", () => {
    it("describes configs from any broker", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildDescribeConfigsResponseBody()
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const resources = await admin.describeConfigs({
        resources: [
          { resourceType: ConfigResourceType.Topic, resourceName: "cfg-topic", configNames: null }
        ]
      })

      expect(resources).toHaveLength(1)
      expect(resources[0].resourceName).toBe("cfg-topic")
      expect(resources[0].configs).toHaveLength(1)
      expect(resources[0].configs[0].name).toBe("cleanup.policy")
      expect(resources[0].configs[0].value).toBe("delete")
    })
  })

  // -------------------------------------------------------------------------
  // alterConfigs
  // -------------------------------------------------------------------------

  describe("alterConfigs", () => {
    it("alters configs via the controller", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildAlterConfigsResponseBody()
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const resources = await admin.alterConfigs({
        resources: [
          {
            resourceType: ConfigResourceType.Topic,
            resourceName: "altered-topic",
            configs: [{ name: "retention.ms", value: "86400000" }]
          }
        ]
      })

      expect(resources).toHaveLength(1)
      expect(resources[0].resourceName).toBe("altered-topic")
      expect(resources[0].errorCode).toBe(0)
    })
  })

  // -------------------------------------------------------------------------
  // listTopics
  // -------------------------------------------------------------------------

  describe("listTopics", () => {
    it("lists topics via metadata", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildMetadataV1Body(
          [{ nodeId: 1, host: "localhost", port: 9092 }],
          [
            {
              errorCode: 0,
              name: "topic-a",
              isInternal: false,
              partitions: [
                { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
              ]
            },
            {
              errorCode: 0,
              name: "__consumer_offsets",
              isInternal: true,
              partitions: [
                { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] },
                { errorCode: 0, partitionIndex: 1, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
              ]
            }
          ]
        )
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const topics = await admin.listTopics()

      expect(topics).toHaveLength(2)
      expect(topics[0].name).toBe("topic-a")
      expect(topics[0].isInternal).toBe(false)
      expect(topics[0].partitionCount).toBe(1)
      expect(topics[1].name).toBe("__consumer_offsets")
      expect(topics[1].isInternal).toBe(true)
      expect(topics[1].partitionCount).toBe(2)
    })
  })

  // -------------------------------------------------------------------------
  // describeTopics
  // -------------------------------------------------------------------------

  describe("describeTopics", () => {
    it("describes specific topics via metadata", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildMetadataV1Body(
          [{ nodeId: 1, host: "localhost", port: 9092 }],
          [
            {
              errorCode: 0,
              name: "my-topic",
              isInternal: false,
              partitions: [
                { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] },
                { errorCode: 0, partitionIndex: 1, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
              ]
            }
          ]
        )
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const topics = await admin.describeTopics(["my-topic"])

      expect(topics).toHaveLength(1)
      expect(topics[0].name).toBe("my-topic")
      expect(topics[0].partitions).toHaveLength(2)
    })
  })

  // -------------------------------------------------------------------------
  // close
  // -------------------------------------------------------------------------

  describe("close", () => {
    it("prevents further operations after close", async () => {
      const pool = poolWithConn(createMockConnection([]))
      const admin = new KafkaAdmin({ connectionPool: pool })

      admin.close()

      await expect(
        admin.createTopics({
          topics: [{ name: "x", numPartitions: 1, replicationFactor: 1 }],
          timeoutMs: 1000
        })
      ).rejects.toThrow("admin client is closed")
    })
  })

  // -------------------------------------------------------------------------
  // error / no brokers
  // -------------------------------------------------------------------------

  describe("no brokers available", () => {
    it("throws when pool has no brokers", async () => {
      const pool = createMockPool()
      const admin = new KafkaAdmin({ connectionPool: pool })

      await expect(admin.listTopics()).rejects.toThrow("no brokers available")
    })
  })

  // -------------------------------------------------------------------------
  // retry behaviour
  // -------------------------------------------------------------------------

  describe("retry", () => {
    it("retries on retriable errors", async () => {
      let callCount = 0
      const conn = createMockConnection([])
      // Override send to fail once then succeed
      // eslint-disable-next-line @typescript-eslint/require-await
      conn.send = vi.fn(async () => {
        callCount++
        if (callCount <= 2) {
          // First two calls: ApiVersions succeeds, CreateTopics fails
          if (callCount === 1) {
            return new BinaryReader(buildApiVersionsBody(STANDARD_APIS))
          }
          throw new KafkaConnectionError("connection reset", { retriable: true })
        }
        if (callCount === 3) {
          return new BinaryReader(buildApiVersionsBody(STANDARD_APIS))
        }
        // Fourth call: CreateTopics succeeds
        return new BinaryReader(buildCreateTopicsResponseBody())
      })

      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({
        connectionPool: pool,
        retry: { maxRetries: 3, initialRetryMs: 1, maxRetryMs: 10, multiplier: 1 }
      })

      const topics = await admin.createTopics({
        topics: [{ name: "new-topic", numPartitions: 1, replicationFactor: 1 }],
        timeoutMs: 5000
      })

      expect(topics).toHaveLength(1)
      expect(topics[0].name).toBe("new-topic")
      expect(callCount).toBe(4) // 2 calls per attempt × 2 attempts
    })

    it("does not retry non-retriable errors", async () => {
      const responses = [
        buildApiVersionsBody(STANDARD_APIS),
        buildApiVersionsBody(STANDARD_APIS) // never reached
      ]
      let responseIndex = 0
      let callCount = 0
      const conn = {
        // eslint-disable-next-line @typescript-eslint/require-await
        send: vi.fn(async () => {
          callCount++
          if (callCount === 2) {
            throw new KafkaConnectionError("not authorised", { retriable: false })
          }
          return new BinaryReader(responses[responseIndex++])
        }),
        broker: "localhost:9092",
        connected: true
      }

      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({
        connectionPool: pool,
        retry: { maxRetries: 3, initialRetryMs: 1 }
      })

      await expect(
        admin.createTopics({
          topics: [{ name: "x", numPartitions: 1, replicationFactor: 1 }],
          timeoutMs: 1000
        })
      ).rejects.toThrow("not authorised")

      expect(callCount).toBe(2) // only one attempt
    })
  })

  // -------------------------------------------------------------------------
  // Decode error paths — each admin op with truncated response
  // -------------------------------------------------------------------------

  describe("decode error paths", () => {
    it("throws on createTopics decode failure", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        new Uint8Array([0x00]) // truncated
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(
        admin.createTopics({
          topics: [{ name: "x", numPartitions: 1, replicationFactor: 1 }],
          timeoutMs: 1000
        })
      ).rejects.toThrow("failed to decode create topics response")
    })

    it("throws on deleteTopics decode failure", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        new Uint8Array([0x00]) // truncated
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(admin.deleteTopics({ topicNames: ["x"], timeoutMs: 1000 })).rejects.toThrow(
        "failed to decode delete topics response"
      )
    })

    it("throws on createPartitions decode failure", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        new Uint8Array([0x00]) // truncated
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(
        admin.createPartitions({
          topics: [{ name: "x", count: 3, assignments: null }],
          timeoutMs: 1000
        })
      ).rejects.toThrow("failed to decode create partitions response")
    })

    it("throws on describeConfigs decode failure", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        new Uint8Array([0x00]) // truncated
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(
        admin.describeConfigs({
          resources: [
            { resourceType: ConfigResourceType.Topic, resourceName: "x", configNames: null }
          ]
        })
      ).rejects.toThrow("failed to decode describe configs response")
    })

    it("throws on alterConfigs decode failure", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        new Uint8Array([0x00]) // truncated
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(
        admin.alterConfigs({
          resources: [
            {
              resourceType: ConfigResourceType.Topic,
              resourceName: "x",
              configs: [{ name: "a", value: "b" }]
            }
          ]
        })
      ).rejects.toThrow("failed to decode alter configs response")
    })

    it("throws on listTopics (metadata) decode failure", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        new Uint8Array([0x00]) // truncated
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(admin.listTopics()).rejects.toThrow("failed to decode metadata response")
    })

    it("throws on describeTopics (metadata) decode failure", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        new Uint8Array([0x00]) // truncated
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(admin.describeTopics(["x"])).rejects.toThrow(
        "failed to decode metadata response"
      )
    })
  })

  // -------------------------------------------------------------------------
  // negotiateApiVersion error paths
  // -------------------------------------------------------------------------

  describe("negotiateApiVersion errors", () => {
    it("throws on ApiVersions decode failure", async () => {
      const conn = createMockConnection([
        new Uint8Array([0x00]) // truncated ApiVersions response
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(admin.listTopics()).rejects.toThrow("failed to decode api versions response")
    })

    it("throws on ApiVersions non-zero error code", async () => {
      const w = new BinaryWriter()
      w.writeInt16(35) // UNSUPPORTED_VERSION error code
      w.writeInt32(0) // empty api keys array
      const errorBody = w.finish()

      const conn = createMockConnection([errorBody])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(admin.listTopics()).rejects.toThrow(
        "api versions request failed with error code"
      )
    })

    it("throws when broker does not support required API key", async () => {
      // Only advertise ApiVersions, not Metadata
      const limitedApis = [{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 }]
      const conn = createMockConnection([buildApiVersionsBody(limitedApis)])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(admin.listTopics()).rejects.toThrow("broker does not support api key")
    })

    it("throws when no compatible version exists for API key", async () => {
      // Advertise Metadata with version range that does not overlap our supported range
      const incompatibleApis = [
        { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
        { apiKey: ApiKey.Metadata, minVersion: 99, maxVersion: 100 }
      ]
      const conn = createMockConnection([buildApiVersionsBody(incompatibleApis)])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(admin.listTopics()).rejects.toThrow("no compatible api version")
    })
  })

  // -------------------------------------------------------------------------
  // No brokers available - separate controller and any-broker paths
  // -------------------------------------------------------------------------

  describe("no brokers available", () => {
    it("throws when pool has no brokers (controller path)", async () => {
      const pool = createMockPool()
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(
        admin.createTopics({
          topics: [{ name: "x", numPartitions: 1, replicationFactor: 1 }],
          timeoutMs: 1000
        })
      ).rejects.toThrow("no brokers available")
    })

    it("throws when pool has no brokers (any-broker path)", async () => {
      const pool = createMockPool()
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(
        admin.describeConfigs({
          resources: [
            { resourceType: ConfigResourceType.Topic, resourceName: "x", configNames: null }
          ]
        })
      ).rejects.toThrow("no brokers available")
    })
  })

  // -------------------------------------------------------------------------
  // releaseConnection is always called
  // -------------------------------------------------------------------------

  describe("connection release", () => {
    it("releases connection after success", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildCreateTopicsResponseBody()
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      await admin.createTopics({
        topics: [{ name: "new-topic", numPartitions: 1, replicationFactor: 1 }],
        timeoutMs: 5000
      })

      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(pool.releaseConnection).toHaveBeenCalledWith(conn)
    })

    it("releases connection after decode failure", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        new Uint8Array(0) // empty response causes decode failure
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(
        admin.createTopics({
          topics: [{ name: "x", numPartitions: 1, replicationFactor: 1 }],
          timeoutMs: 1000
        })
      ).rejects.toThrow()

      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(pool.releaseConnection).toHaveBeenCalledWith(conn)
    })
  })
})
