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
  { apiKey: ApiKey.AlterConfigs, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeClientQuotas, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.AlterClientQuotas, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeGroups, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.ListGroups, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DeleteGroups, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DeleteRecords, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.ElectLeaders, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.IncrementalAlterConfigs, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.AlterPartitionReassignments, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.ListPartitionReassignments, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeAcls, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.CreateAcls, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DeleteAcls, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.CreateDelegationToken, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.RenewDelegationToken, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.ExpireDelegationToken, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeDelegationToken, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeUserScramCredentials, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.AlterUserScramCredentials, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeLogDirs, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.AlterReplicaLogDirs, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeCluster, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeProducers, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeTransactions, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.ListTransactions, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeTopicPartitions, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.UpdateFeatures, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.DescribeQuorum, minVersion: 0, maxVersion: 0 }
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
    send: vi.fn(async () => {
      if (callIndex >= responses.length) {
        throw new Error("no more mock responses")
      }
      return Promise.resolve(new BinaryReader(responses[callIndex++]))
    }),
    broker: brokerAddr,
    connected: true
  }
}

function createMockPool(overrides?: Partial<ConnectionPool>): ConnectionPool {
  return {
    brokers: new Map(),
    isClosed: false,
    connect: async () => Promise.resolve(),
    refreshMetadata: async () => Promise.resolve(),
    getConnection: () => {
      throw new Error("not implemented")
    },
    getConnectionByNodeId: () => {
      throw new Error("not implemented")
    },
    releaseConnection: () => {
      /* noop */
    },
    close: async () => Promise.resolve(),
    connectionCount: () => 0,
    ...overrides
  } as unknown as ConnectionPool
}

function poolWithConn(mockConn: ReturnType<typeof createMockConnection>): ConnectionPool {
  const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
  return createMockPool({
    brokers: brokerMap as ConnectionPool["brokers"],
    getConnectionByNodeId: vi.fn(async () =>
      Promise.resolve(mockConn)
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
  // describeGroups
  // -------------------------------------------------------------------------

  describe("describeGroups", () => {
    it("describes consumer groups", async () => {
      const w = new BinaryWriter()
      // v0: groups array (no throttle)
      w.writeInt32(1) // groups count
      w.writeInt16(0) // error_code
      w.writeString("my-group") // group_id
      w.writeString("Stable") // group_state
      w.writeString("consumer") // protocol_type
      w.writeString("range") // protocol_data
      w.writeInt32(0) // members count

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeGroups({ groups: ["my-group"] })
      expect(result).toHaveLength(1)
      expect(result[0].groupId).toBe("my-group")
      expect(result[0].groupState).toBe("Stable")
    })
  })

  // -------------------------------------------------------------------------
  // listGroups
  // -------------------------------------------------------------------------

  describe("listGroups", () => {
    it("lists consumer groups", async () => {
      const w = new BinaryWriter()
      // v0: error_code, groups
      w.writeInt16(0) // error_code
      w.writeInt32(1) // groups count
      w.writeString("group-1") // group_id
      w.writeString("consumer") // protocol_type

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.listGroups()
      expect(result).toHaveLength(1)
      expect(result[0].groupId).toBe("group-1")
    })

    it("throws on non-zero error code", async () => {
      const w = new BinaryWriter()
      w.writeInt16(29) // CLUSTER_AUTHORIZATION_FAILED
      w.writeInt32(0) // 0 groups

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(admin.listGroups()).rejects.toThrow("list groups request failed with error code")
    })
  })

  // -------------------------------------------------------------------------
  // deleteGroups
  // -------------------------------------------------------------------------

  describe("deleteGroups", () => {
    it("deletes consumer groups", async () => {
      const w = new BinaryWriter()
      // v0: results
      w.writeInt32(1) // results count
      w.writeString("dead-group") // group_id
      w.writeInt16(0) // error_code

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.deleteGroups({ groupsNames: ["dead-group"] })
      expect(result).toHaveLength(1)
      expect(result[0].groupId).toBe("dead-group")
      expect(result[0].errorCode).toBe(0)
    })
  })

  // -------------------------------------------------------------------------
  // deleteRecords
  // -------------------------------------------------------------------------

  describe("deleteRecords", () => {
    it("deletes records from partitions", async () => {
      const w = new BinaryWriter()
      // v0: throttle, topics
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(1) // topics count
      w.writeString("test-topic") // name
      w.writeInt32(1) // partitions count
      w.writeInt32(0) // partition_index
      w.writeInt64(100n) // low_watermark
      w.writeInt16(0) // error_code

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.deleteRecords({
        topics: [{ name: "test-topic", partitions: [{ partitionIndex: 0, offset: 100n }] }],
        timeoutMs: 5000
      })
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe("test-topic")
    })
  })

  // -------------------------------------------------------------------------
  // electLeaders
  // -------------------------------------------------------------------------

  describe("electLeaders", () => {
    it("elects leaders for partitions", async () => {
      const w = new BinaryWriter()
      // v0 standard: throttle_time_ms, replica_election_results (no top-level error_code in v0)
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(1) // results count
      w.writeString("test-topic") // topic
      w.writeInt32(1) // partitions count
      w.writeInt32(0) // partition_id
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.electLeaders({
        electionType: 0,
        topicPartitions: [{ topic: "test-topic", partitions: [{ partitionId: 0 }] }],
        timeoutMs: 30000
      })
      expect(result).toHaveLength(1)
      expect(result[0].topic).toBe("test-topic")
    })
  })

  // -------------------------------------------------------------------------
  // incrementalAlterConfigs
  // -------------------------------------------------------------------------

  describe("incrementalAlterConfigs", () => {
    it("incrementally alters configs", async () => {
      const w = new BinaryWriter()
      // v0: throttle, responses
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(1) // responses count
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message
      w.writeInt8(2) // resource_type (Topic)
      w.writeString("inc-topic") // resource_name

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.incrementalAlterConfigs({
        resources: [
          {
            resourceType: 2,
            resourceName: "inc-topic",
            configs: [{ name: "retention.ms", configOperation: 0, value: "86400000" }]
          }
        ]
      })
      expect(result).toHaveLength(1)
      expect(result[0].resourceName).toBe("inc-topic")
    })
  })

  // -------------------------------------------------------------------------
  // alterPartitionReassignments
  // -------------------------------------------------------------------------

  describe("alterPartitionReassignments", () => {
    it("alters partition reassignments", async () => {
      const w = new BinaryWriter()
      // v0 flexible: throttle, error_code, error_message, responses, tagged
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeUnsignedVarInt(2) // 1 response + 1
      w.writeCompactString("reass-topic")
      w.writeUnsignedVarInt(2) // 1 partition + 1
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeTaggedFields([])
      w.writeTaggedFields([])
      w.writeTaggedFields([])

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.alterPartitionReassignments({
        topics: [{ name: "reass-topic", partitions: [{ partitionIndex: 0, replicas: [1, 2, 3] }] }],
        timeoutMs: 30000
      })
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe("reass-topic")
    })
  })

  // -------------------------------------------------------------------------
  // listPartitionReassignments
  // -------------------------------------------------------------------------

  describe("listPartitionReassignments", () => {
    it("lists partition reassignments", async () => {
      const w = new BinaryWriter()
      // v0 flexible: throttle, error_code, error_message, topics, tagged
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeUnsignedVarInt(2) // 1 topic + 1
      w.writeCompactString("reass-topic")
      w.writeUnsignedVarInt(2) // 1 partition + 1
      w.writeInt32(0) // partition_index
      w.writeUnsignedVarInt(4) // 3 replicas + 1
      w.writeInt32(1)
      w.writeInt32(2)
      w.writeInt32(3)
      w.writeUnsignedVarInt(2) // 1 adding + 1
      w.writeInt32(3)
      w.writeUnsignedVarInt(2) // 1 removing + 1
      w.writeInt32(2)
      w.writeTaggedFields([])
      w.writeTaggedFields([])
      w.writeTaggedFields([])

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.listPartitionReassignments({
        topics: [{ name: "reass-topic", partitionIndexes: [0] }],
        timeoutMs: 30000
      })
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe("reass-topic")
    })
  })

  // -------------------------------------------------------------------------
  // describeAcls
  // -------------------------------------------------------------------------

  describe("describeAcls", () => {
    it("describes ACLs", async () => {
      const w = new BinaryWriter()
      // v0: throttle, error_code, error_message, resources
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeString(null)
      w.writeInt32(1) // resources
      w.writeInt8(2) // TOPIC
      w.writeString("acl-topic")
      w.writeInt32(1) // acls count
      w.writeString("User:alice")
      w.writeString("*")
      w.writeInt8(2) // ALLOW
      w.writeInt8(3) // WRITE

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeAcls({
        resourceTypeFilter: 2,
        resourceNameFilter: "acl-topic",
        patternTypeFilter: 3,
        principalFilter: null,
        hostFilter: null,
        operation: 0,
        permissionType: 0
      })
      expect(result).toHaveLength(1)
    })
  })

  // -------------------------------------------------------------------------
  // createAcls
  // -------------------------------------------------------------------------

  describe("createAcls", () => {
    it("creates ACLs", async () => {
      const w = new BinaryWriter()
      // v0: throttle, results
      w.writeInt32(0)
      w.writeInt32(1) // results count
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.createAcls({
        creations: [
          {
            resourceType: 2,
            resourceName: "t",
            resourcePatternType: 3,
            principal: "User:x",
            host: "*",
            operation: 3,
            permissionType: 2
          }
        ]
      })
      expect(result).toHaveLength(1)
      expect(result[0].errorCode).toBe(0)
    })
  })

  // -------------------------------------------------------------------------
  // deleteAcls
  // -------------------------------------------------------------------------

  describe("deleteAcls", () => {
    it("deletes ACLs", async () => {
      const w = new BinaryWriter()
      // v0: throttle, filter_results
      w.writeInt32(0)
      w.writeInt32(1) // filter_results count
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message
      w.writeInt32(0) // matching_acls count (0 = no matches)

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.deleteAcls([
        {
          resourceTypeFilter: 2,
          resourceNameFilter: "t",
          patternTypeFilter: 3,
          principalFilter: "User:x",
          hostFilter: "*",
          operation: 3,
          permissionType: 2
        }
      ])
      expect(result).toHaveLength(1)
      expect(result[0].errorCode).toBe(0)
    })
  })

  // -------------------------------------------------------------------------
  // createDelegationToken
  // -------------------------------------------------------------------------

  describe("createDelegationToken", () => {
    it("creates a delegation token", async () => {
      const w = new BinaryWriter()
      // v0: error_code, principal_type, principal_name, issue_timestamp, expiry_timestamp, max_timestamp, token_id, hmac
      w.writeInt16(0)
      w.writeString("User")
      w.writeString("admin")
      w.writeInt64(1000n) // issue
      w.writeInt64(2000n) // expiry
      w.writeInt64(3000n) // max
      w.writeString("token-1")
      w.writeBytes(new Uint8Array([0x01, 0x02, 0x03]))
      w.writeInt32(0) // throttle_time_ms

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.createDelegationToken({
        renewers: [],
        maxLifetimeMs: 86400000n
      })
      expect(result.errorCode).toBe(0)
      expect(result.tokenId).toBe("token-1")
    })
  })

  // -------------------------------------------------------------------------
  // renewDelegationToken
  // -------------------------------------------------------------------------

  describe("renewDelegationToken", () => {
    it("renews a delegation token", async () => {
      const w = new BinaryWriter()
      // v0: error_code, expiry_timestamp, throttle
      w.writeInt16(0)
      w.writeInt64(5000n)
      w.writeInt32(0)

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.renewDelegationToken({
        hmac: new Uint8Array([0x01]),
        renewPeriodMs: 3600000n
      })
      expect(result.errorCode).toBe(0)
    })
  })

  // -------------------------------------------------------------------------
  // expireDelegationToken
  // -------------------------------------------------------------------------

  describe("expireDelegationToken", () => {
    it("expires a delegation token", async () => {
      const w = new BinaryWriter()
      // v0: error_code, expiry_timestamp, throttle
      w.writeInt16(0)
      w.writeInt64(0n)
      w.writeInt32(0)

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.expireDelegationToken({
        hmac: new Uint8Array([0x01]),
        expiryTimePeriodMs: -1n
      })
      expect(result.errorCode).toBe(0)
    })
  })

  // -------------------------------------------------------------------------
  // describeDelegationTokens
  // -------------------------------------------------------------------------

  describe("describeDelegationTokens", () => {
    it("describes delegation tokens", async () => {
      const w = new BinaryWriter()
      // v0: error_code, tokens, throttle
      w.writeInt16(0)
      w.writeInt32(0) // 0 tokens
      w.writeInt32(0) // throttle

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeDelegationTokens({ owners: null })
      expect(result).toHaveLength(0)
    })
  })

  // -------------------------------------------------------------------------
  // describeUserScramCredentials
  // -------------------------------------------------------------------------

  describe("describeUserScramCredentials", () => {
    it("describes SCRAM credentials", async () => {
      const w = new BinaryWriter()
      // v0 flexible: throttle, error_code, error_message, results, tagged
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeUnsignedVarInt(2) // 1 result + 1
      w.writeCompactString("alice")
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeUnsignedVarInt(2) // 1 credential + 1
      w.writeInt8(1) // SCRAM-SHA-256
      w.writeInt32(4096)
      w.writeTaggedFields([])
      w.writeTaggedFields([])
      w.writeTaggedFields([])

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeUserScramCredentials({ users: [{ name: "alice" }] })
      expect(result).toHaveLength(1)
      expect(result[0].user).toBe("alice")
    })
  })

  // -------------------------------------------------------------------------
  // alterUserScramCredentials
  // -------------------------------------------------------------------------

  describe("alterUserScramCredentials", () => {
    it("alters SCRAM credentials", async () => {
      const w = new BinaryWriter()
      // v0 flexible: throttle, results, tagged
      w.writeInt32(0)
      w.writeUnsignedVarInt(2) // 1 result + 1
      w.writeCompactString("bob")
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeTaggedFields([])
      w.writeTaggedFields([])

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.alterUserScramCredentials({
        upsertions: [
          {
            name: "bob",
            mechanism: 1,
            iterations: 4096,
            salt: new Uint8Array([1]),
            saltedPassword: new Uint8Array([2])
          }
        ],
        deletions: []
      })
      expect(result).toHaveLength(1)
      expect(result[0].user).toBe("bob")
    })
  })

  // -------------------------------------------------------------------------
  // describeLogDirs
  // -------------------------------------------------------------------------

  describe("describeLogDirs", () => {
    it("describes log directories", async () => {
      const w = new BinaryWriter()
      // v0: throttle, results
      w.writeInt32(0)
      w.writeInt32(1) // results
      w.writeInt16(0) // error_code
      w.writeString("/var/kafka-logs") // log_dir
      w.writeInt32(0) // topics count

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeLogDirs({ topics: null })
      expect(result).toHaveLength(1)
      expect(result[0].logDir).toBe("/var/kafka-logs")
    })
  })

  // -------------------------------------------------------------------------
  // alterReplicaLogDirs
  // -------------------------------------------------------------------------

  describe("alterReplicaLogDirs", () => {
    it("alters replica log dirs", async () => {
      const w = new BinaryWriter()
      // v0: throttle, results
      w.writeInt32(0)
      w.writeInt32(1) // results
      w.writeString("moved-topic") // topic_name
      w.writeInt32(1) // partitions
      w.writeInt32(0) // partition_index
      w.writeInt16(0) // error_code

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.alterReplicaLogDirs({
        dirs: [{ logDir: "/var/kafka-logs-2", topics: [{ topic: "moved-topic", partitions: [0] }] }]
      })
      expect(result).toHaveLength(1)
      expect(result[0].topicName).toBe("moved-topic")
    })
  })

  // -------------------------------------------------------------------------
  // describeCluster
  // -------------------------------------------------------------------------

  describe("describeCluster", () => {
    it("describes the cluster", async () => {
      const w = new BinaryWriter()
      // v0 flexible: throttle, error_code, error_message, cluster_id, controller_id, brokers, auth_ops, tagged
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeCompactString("cluster-1")
      w.writeInt32(1)
      w.writeUnsignedVarInt(1) // 0 brokers
      w.writeInt32(-2147483648)
      w.writeTaggedFields([])

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeCluster({
        includeClusterAuthorizedOperations: false
      })
      expect(result.clusterId).toBe("cluster-1")
      expect(result.controllerId).toBe(1)
    })
  })

  // -------------------------------------------------------------------------
  // describeProducers
  // -------------------------------------------------------------------------

  describe("describeProducers", () => {
    it("describes producers", async () => {
      const w = new BinaryWriter()
      // v0 flexible: throttle, topics, tagged
      w.writeInt32(0)
      w.writeUnsignedVarInt(2) // 1 topic + 1
      w.writeCompactString("test-topic")
      w.writeUnsignedVarInt(2) // 1 partition + 1
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeUnsignedVarInt(1) // 0 producers
      w.writeTaggedFields([])
      w.writeTaggedFields([])
      w.writeTaggedFields([])

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeProducers({
        topics: [{ name: "test-topic", partitionIndexes: [0] }]
      })
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe("test-topic")
    })
  })

  // -------------------------------------------------------------------------
  // describeTransactions
  // -------------------------------------------------------------------------

  describe("describeTransactions", () => {
    it("describes transactions", async () => {
      const w = new BinaryWriter()
      // v0 flexible: throttle, states, tagged
      w.writeInt32(0)
      w.writeUnsignedVarInt(2) // 1 state + 1
      w.writeInt16(0)
      w.writeCompactString("txn-1")
      w.writeCompactString("Ongoing")
      w.writeInt32(60000)
      w.writeInt64(1000n)
      w.writeInt64(500n)
      w.writeInt16(1)
      w.writeUnsignedVarInt(1) // 0 topics
      w.writeTaggedFields([])
      w.writeTaggedFields([])

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeTransactions({
        transactionalIds: ["txn-1"]
      })
      expect(result).toHaveLength(1)
      expect(result[0].transactionalId).toBe("txn-1")
    })
  })

  // -------------------------------------------------------------------------
  // listTransactions
  // -------------------------------------------------------------------------

  describe("listTransactions", () => {
    it("lists transactions", async () => {
      const w = new BinaryWriter()
      // v0 flexible: throttle, error_code, unknown_state_filters, states, tagged
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeUnsignedVarInt(1) // 0 unknown filters
      w.writeUnsignedVarInt(1) // 0 states
      w.writeTaggedFields([])

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.listTransactions()
      expect(result.errorCode).toBe(0)
      expect(result.transactionStates).toHaveLength(0)
    })
  })

  // -------------------------------------------------------------------------
  // describeTopicPartitions
  // -------------------------------------------------------------------------

  describe("describeTopicPartitions", () => {
    it("describes topic partitions", async () => {
      const w = new BinaryWriter()
      // v0 flexible: throttle, topics, cursor, tagged
      w.writeInt32(0)
      w.writeUnsignedVarInt(1) // 0 topics
      w.writeInt8(-1) // null cursor
      w.writeTaggedFields([])

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeTopicPartitions({
        topics: [{ name: "test" }],
        responsePartitionLimit: 100
      })
      expect(result.topics).toHaveLength(0)
      expect(result.nextCursor).toBeNull()
    })
  })

  // -------------------------------------------------------------------------
  // updateFeatures
  // -------------------------------------------------------------------------

  describe("updateFeatures", () => {
    it("updates features", async () => {
      const w = new BinaryWriter()
      // v0 flexible: throttle, error_code, error_message, results, tagged
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeUnsignedVarInt(1) // 0 results
      w.writeTaggedFields([])

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.updateFeatures({
        timeoutMs: 30000,
        featureUpdates: []
      })
      expect(result.errorCode).toBe(0)
    })
  })

  // -------------------------------------------------------------------------
  // describeQuorum
  // -------------------------------------------------------------------------

  describe("describeQuorum", () => {
    it("describes the quorum", async () => {
      const w = new BinaryWriter()
      // v0 flexible: error_code, error_message, topics, tagged
      w.writeInt16(0)
      w.writeCompactString(null)
      w.writeUnsignedVarInt(1) // 0 topics
      w.writeTaggedFields([])

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeQuorum({
        topics: []
      })
      expect(result.errorCode).toBe(0)
      expect(result.topics).toHaveLength(0)
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
      conn.send = vi.fn(async () => {
        callCount++
        if (callCount <= 2) {
          // First two calls: ApiVersions succeeds, CreateTopics fails
          if (callCount === 1) {
            return Promise.resolve(new BinaryReader(buildApiVersionsBody(STANDARD_APIS)))
          }
          return Promise.reject(new KafkaConnectionError("connection reset", { retriable: true }))
        }
        if (callCount === 3) {
          return Promise.resolve(new BinaryReader(buildApiVersionsBody(STANDARD_APIS)))
        }
        // Fourth call: CreateTopics succeeds
        return Promise.resolve(new BinaryReader(buildCreateTopicsResponseBody()))
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
        send: vi.fn(async () => {
          callCount++
          if (callCount === 2) {
            return Promise.reject(new KafkaConnectionError("not authorised", { retriable: false }))
          }
          return Promise.resolve(new BinaryReader(responses[responseIndex++]))
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

    it("throws on describeClientQuotas decode failure", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        new Uint8Array([0x00]) // truncated
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(admin.describeClientQuotas({ components: [], strict: false })).rejects.toThrow(
        "failed to decode describe client quotas response"
      )
    })

    it("throws on alterClientQuotas decode failure", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        new Uint8Array([0x00]) // truncated
      ])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(admin.alterClientQuotas({ entries: [], validateOnly: false })).rejects.toThrow(
        "failed to decode alter client quotas response"
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
  // describeClientQuotas
  // -------------------------------------------------------------------------

  describe("describeClientQuotas", () => {
    it("describes client quotas from any broker", async () => {
      const w = new BinaryWriter()
      // v0 response: throttle, error_code, error_message, entries
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message
      w.writeInt32(1) // entries array
      // entry: entity array
      w.writeInt32(1)
      w.writeString("user")
      w.writeString("alice")
      // entry: values array
      w.writeInt32(1)
      w.writeString("producer_byte_rate")
      w.writeFloat64(1048576.0)

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeClientQuotas({
        components: [{ entityType: "user", matchType: 0, match: "alice" }],
        strict: false
      })

      expect(result).toHaveLength(1)
      expect(result[0].entity[0].entityType).toBe("user")
      expect(result[0].entity[0].entityName).toBe("alice")
      expect(result[0].values[0].key).toBe("producer_byte_rate")
      expect(result[0].values[0].value).toBe(1048576.0)
    })

    it("returns empty array when entries are null", async () => {
      const w = new BinaryWriter()
      w.writeInt32(0)
      w.writeInt16(0)
      w.writeString(null)
      w.writeInt32(-1) // null entries

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.describeClientQuotas({
        components: [],
        strict: false
      })

      expect(result).toEqual([])
    })
  })

  // -------------------------------------------------------------------------
  // alterClientQuotas
  // -------------------------------------------------------------------------

  describe("alterClientQuotas", () => {
    it("alters client quotas via the controller", async () => {
      const w = new BinaryWriter()
      // v0 response: throttle, entries
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(1) // entries array
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message
      w.writeInt32(1) // entity array
      w.writeString("user")
      w.writeString("alice")

      const conn = createMockConnection([buildApiVersionsBody(STANDARD_APIS), w.finish()])
      const pool = poolWithConn(conn)
      const admin = new KafkaAdmin({ connectionPool: pool })

      const result = await admin.alterClientQuotas({
        entries: [
          {
            entity: [{ entityType: "user", entityName: "alice" }],
            ops: [{ key: "producer_byte_rate", value: 1048576.0, remove: false }]
          }
        ],
        validateOnly: false
      })

      expect(result).toHaveLength(1)
      expect(result[0].errorCode).toBe(0)
      expect(result[0].entity[0].entityType).toBe("user")
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
      const releaseSpy = vi.fn()
      const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
      const pool = createMockPool({
        brokers: brokerMap as ConnectionPool["brokers"],
        getConnectionByNodeId: vi.fn(async () =>
          Promise.resolve(conn)
        ) as unknown as ConnectionPool["getConnectionByNodeId"],
        releaseConnection: releaseSpy as ConnectionPool["releaseConnection"]
      })
      const admin = new KafkaAdmin({ connectionPool: pool })

      await admin.createTopics({
        topics: [{ name: "new-topic", numPartitions: 1, replicationFactor: 1 }],
        timeoutMs: 5000
      })

      expect(releaseSpy).toHaveBeenCalledWith(conn)
    })

    it("releases connection after decode failure", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        new Uint8Array(0) // empty response causes decode failure
      ])
      const releaseSpy = vi.fn()
      const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
      const pool = createMockPool({
        brokers: brokerMap as ConnectionPool["brokers"],
        getConnectionByNodeId: vi.fn(async () =>
          Promise.resolve(conn)
        ) as unknown as ConnectionPool["getConnectionByNodeId"],
        releaseConnection: releaseSpy as ConnectionPool["releaseConnection"]
      })
      const admin = new KafkaAdmin({ connectionPool: pool, retry: { maxRetries: 0 } })

      await expect(
        admin.createTopics({
          topics: [{ name: "x", numPartitions: 1, replicationFactor: 1 }],
          timeoutMs: 1000
        })
      ).rejects.toThrow()

      expect(releaseSpy).toHaveBeenCalledWith(conn)
    })
  })
})
