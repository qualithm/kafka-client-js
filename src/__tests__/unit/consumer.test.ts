import { afterEach, describe, expect, it, vi } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import type { ConnectionPool } from "../../broker-pool"
import {
  type ConsumerOptions,
  createConsumer,
  KafkaConsumer,
  OffsetResetStrategy
} from "../../consumer"
import { buildRecordBatch, createRecord, encodeRecordBatch } from "../../record-batch"

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

function buildFindCoordinatorV0Body(
  errorCode: number,
  nodeId: number,
  host: string,
  port: number
): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt16(errorCode)
  w.writeInt32(nodeId)
  w.writeString(host)
  w.writeInt32(port)
  return w.finish()
}

function buildJoinGroupV0Body(
  errorCode: number,
  generationId: number,
  protocolName: string,
  leader: string,
  memberId: string,
  members: { memberId: string; metadata: Uint8Array }[]
): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt16(errorCode)
  w.writeInt32(generationId)
  w.writeString(protocolName)
  w.writeString(leader)
  w.writeString(memberId)
  w.writeInt32(members.length)
  for (const m of members) {
    w.writeString(m.memberId)
    w.writeBytes(m.metadata)
  }
  return w.finish()
}

function buildSyncGroupV0Body(errorCode: number, assignment: Uint8Array): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt16(errorCode)
  w.writeBytes(assignment)
  return w.finish()
}

function buildOffsetFetchV0Body(
  topics: {
    name: string
    partitions: {
      partitionIndex: number
      committedOffset: bigint
      metadata: string | null
      errorCode: number
    }[]
  }[]
): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt32(topics.length)
  for (const topic of topics) {
    w.writeString(topic.name)
    w.writeInt32(topic.partitions.length)
    for (const p of topic.partitions) {
      w.writeInt32(p.partitionIndex)
      w.writeInt64(p.committedOffset)
      w.writeString(p.metadata)
      w.writeInt16(p.errorCode)
    }
  }
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

function buildListOffsetsV1Body(
  topics: {
    name: string
    partitions: { partitionIndex: number; errorCode: number; timestamp: bigint; offset: bigint }[]
  }[]
): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt32(topics.length)
  for (const t of topics) {
    w.writeString(t.name)
    w.writeInt32(t.partitions.length)
    for (const p of t.partitions) {
      w.writeInt32(p.partitionIndex)
      w.writeInt16(p.errorCode)
      w.writeInt64(p.timestamp)
      w.writeInt64(p.offset)
    }
  }
  return w.finish()
}

function buildLeaveGroupV0Body(errorCode: number): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt16(errorCode)
  return w.finish()
}

function buildConsumerProtocolMetadata(topics: string[]): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt16(0) // version
  w.writeInt32(topics.length)
  for (const t of topics) {
    w.writeString(t)
  }
  w.writeBytes(null) // user_data
  return w.finish()
}

function buildConsumerProtocolAssignment(
  assignments: { topic: string; partitions: number[] }[]
): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt16(0) // version
  w.writeInt32(assignments.length)
  for (const a of assignments) {
    w.writeString(a.topic)
    w.writeInt32(a.partitions.length)
    for (const p of a.partitions) {
      w.writeInt32(p)
    }
  }
  w.writeBytes(null) // user_data
  return w.finish()
}

const STANDARD_APIS = [
  { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
  { apiKey: ApiKey.FindCoordinator, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.JoinGroup, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.SyncGroup, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.Heartbeat, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.LeaveGroup, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.OffsetFetch, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.OffsetCommit, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 1 },
  { apiKey: ApiKey.ListOffsets, minVersion: 0, maxVersion: 1 },
  { apiKey: ApiKey.Fetch, minVersion: 0, maxVersion: 4 }
]

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
    connect: async () => Promise.resolve(),
    refreshMetadata: async () => Promise.resolve(),
    getConnection: () => {
      throw new Error("not implemented")
    },
    getConnectionByNodeId: () => {
      throw new Error("not implemented")
    },
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    releaseConnection: () => {},
    close: async () => Promise.resolve(),
    connectionCount: () => 0,
    ...overrides
  } as unknown as ConnectionPool
}

function defaultConsumerOptions(overrides?: Partial<ConsumerOptions>): ConsumerOptions {
  return {
    connectionPool: createMockPool(),
    groupId: "test-group",
    ...overrides
  }
}

/**
 * Create a full consumer join-group flow mock pool.
 * Response sequence for connect():
 *   1. ApiVersions (for FindCoordinator)
 *   2. FindCoordinator response
 *   3. ApiVersions (for JoinGroup)
 *   4. JoinGroup response
 *   5. ApiVersions (for SyncGroup)
 *   6. SyncGroup response
 *   7. ApiVersions (for OffsetFetch)
 *   8. OffsetFetch response
 *   9. ApiVersions (for Metadata - topic metadata refresh)
 *  10. Metadata response
 *  11. ApiVersions (for ListOffsets)
 *  12. ListOffsets response
 */
function createJoinFlowMock(opts?: {
  memberId?: string
  isLeader?: boolean
  assignment?: { topic: string; partitions: number[] }[]
  committedOffset?: bigint
  topics?: string[]
  extraResponses?: Uint8Array[]
}): { pool: ConnectionPool; conn: ReturnType<typeof createMockConnection> } {
  const memberId = opts?.memberId ?? "member-1"
  const topics = opts?.topics ?? ["test-topic"]
  const assignment = opts?.assignment ?? topics.map((t) => ({ topic: t, partitions: [0] }))
  const committedOffset = opts?.committedOffset ?? -1n
  const isLeader = opts?.isLeader ?? false

  const memberMetadata = buildConsumerProtocolMetadata(topics)
  const assignmentBytes = buildConsumerProtocolAssignment(assignment)

  const members = isLeader ? [{ memberId, metadata: memberMetadata }] : []

  const coordinatorResponses = [
    // FindCoordinator flow
    buildApiVersionsBody(STANDARD_APIS),
    buildFindCoordinatorV0Body(0, 1, "localhost", 9092),
    // JoinGroup flow
    buildApiVersionsBody(STANDARD_APIS),
    buildJoinGroupV0Body(0, 1, "range", isLeader ? memberId : "leader-1", memberId, members),
    // SyncGroup flow
    buildApiVersionsBody(STANDARD_APIS),
    buildSyncGroupV0Body(0, assignmentBytes),
    // OffsetFetch flow
    buildApiVersionsBody(STANDARD_APIS),
    buildOffsetFetchV0Body(
      assignment.map((a) => ({
        name: a.topic,
        partitions: a.partitions.map((p) => ({
          partitionIndex: p,
          committedOffset,
          metadata: null,
          errorCode: 0
        }))
      }))
    )
  ]

  // Metadata for topic refresh (used in fetchInitialOffsets)
  const metadataResponses = [
    buildApiVersionsBody(STANDARD_APIS),
    buildMetadataV1Body(
      [{ nodeId: 1, host: "localhost", port: 9092 }],
      assignment.map((a) => ({
        errorCode: 0,
        name: a.topic,
        isInternal: false,
        partitions: a.partitions.map((p) => ({
          errorCode: 0,
          partitionIndex: p,
          leaderId: 1,
          replicaNodes: [1],
          isrNodes: [1]
        }))
      }))
    )
  ]

  // ListOffsets for offset reset
  const listOffsetsResponses = [
    buildApiVersionsBody(STANDARD_APIS),
    buildListOffsetsV1Body(
      assignment.map((a) => ({
        name: a.topic,
        partitions: a.partitions.map((p) => ({
          partitionIndex: p,
          errorCode: 0,
          timestamp: -1n,
          offset: 0n
        }))
      }))
    )
  ]

  // Call order: findCoordinator → joinAndSync → fetchInitialOffsets
  // fetchInitialOffsets calls refreshTopicMetadata FIRST, then OffsetFetch
  // So the coordinator responses (including OffsetFetch) need to interleave correctly.
  // Since all go to the same nodeId 1 connection, order matters.
  //
  // Actual sequence:
  //   1. ApiVersions + FindCoordinator (findCoordinator)
  //   2. ApiVersions + JoinGroup + ApiVersions + SyncGroup (joinAndSync)
  //   3. ApiVersions + Metadata (refreshTopicMetadata in fetchInitialOffsets)
  //   4. ApiVersions + OffsetFetch (fetchInitialOffsets)
  //   5. ApiVersions + ListOffsets (resetUncommittedOffsets, if needed)
  const joinSyncResponses = coordinatorResponses.slice(0, 6) // FindCoordinator + JoinGroup + SyncGroup
  const offsetFetchResponses = coordinatorResponses.slice(6) // OffsetFetch

  const allResponses =
    committedOffset >= 0n
      ? [
          ...joinSyncResponses,
          ...metadataResponses,
          ...offsetFetchResponses,
          ...(opts?.extraResponses ?? [])
        ]
      : [
          ...joinSyncResponses,
          ...metadataResponses,
          ...offsetFetchResponses,
          ...listOffsetsResponses,
          ...(opts?.extraResponses ?? [])
        ]

  const conn = createMockConnection(allResponses)
  const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])

  const pool = createMockPool({
    brokers: brokerMap as ConnectionPool["brokers"],

    getConnectionByNodeId: vi.fn(async () =>
      Promise.resolve(conn)
    ) as unknown as ConnectionPool["getConnectionByNodeId"],
    releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
  })

  return { pool, conn }
}

// ---------------------------------------------------------------------------
// createConsumer factory
// ---------------------------------------------------------------------------

describe("createConsumer", () => {
  it("returns a KafkaConsumer instance", () => {
    const consumer = createConsumer(defaultConsumerOptions())
    expect(consumer).toBeInstanceOf(KafkaConsumer)
  })
})

// ---------------------------------------------------------------------------
// KafkaConsumer constructor & defaults
// ---------------------------------------------------------------------------

describe("KafkaConsumer", () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe("constructor", () => {
    it("creates instance with default options", () => {
      const consumer = new KafkaConsumer(defaultConsumerOptions())
      expect(consumer).toBeInstanceOf(KafkaConsumer)
      expect(consumer.partitions).toEqual([])
    })

    it("accepts custom options", () => {
      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          sessionTimeoutMs: 10_000,
          rebalanceTimeoutMs: 20_000,
          heartbeatIntervalMs: 1_000,
          maxPartitionBytes: 512_000,
          maxBytes: 10_000_000,
          minBytes: 100,
          maxWaitMs: 1_000,
          offsetReset: OffsetResetStrategy.Earliest,
          autoCommit: false,
          autoCommitIntervalMs: 10_000,
          groupInstanceId: "static-1",
          retry: {
            maxRetries: 3,
            initialRetryMs: 50,
            maxRetryMs: 5_000,
            multiplier: 3
          }
        })
      )
      expect(consumer).toBeInstanceOf(KafkaConsumer)
    })
  })

  // -------------------------------------------------------------------------
  // subscribe
  // -------------------------------------------------------------------------

  describe("subscribe", () => {
    it("stores subscribed topics", () => {
      const consumer = new KafkaConsumer(defaultConsumerOptions())
      consumer.subscribe(["topic-a", "topic-b"])
      // No public accessor for subscribedTopics, but connect will use them
      expect(consumer).toBeInstanceOf(KafkaConsumer)
    })
  })

  // -------------------------------------------------------------------------
  // connect
  // -------------------------------------------------------------------------

  describe("connect", () => {
    it("throws when closed", async () => {
      const consumer = new KafkaConsumer(defaultConsumerOptions())
      consumer.subscribe(["test-topic"])
      await consumer.close()
      await expect(consumer.connect()).rejects.toThrow("consumer is closed")
    })

    it("throws when no topics subscribed", async () => {
      const consumer = new KafkaConsumer(defaultConsumerOptions())
      await expect(consumer.connect()).rejects.toThrow("no topics subscribed")
    })

    it("throws when no brokers available", async () => {
      const pool = createMockPool({
        brokers: new Map() as ConnectionPool["brokers"]
      })
      const consumer = new KafkaConsumer(defaultConsumerOptions({ connectionPool: pool }))
      consumer.subscribe(["test-topic"])
      await expect(consumer.connect()).rejects.toThrow("no brokers available")
    })

    it("completes the join flow successfully", async () => {
      const { pool } = createJoinFlowMock({ committedOffset: 10n })
      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          autoCommit: false,
          heartbeatIntervalMs: 100_000 // avoid timer fires
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      expect(consumer.partitions).toEqual([{ topic: "test-topic", partition: 0 }])
      await consumer.close()
    })

    it("completes join flow with offset reset (earliest)", async () => {
      const { pool } = createJoinFlowMock({
        committedOffset: -1n,
        topics: ["test-topic"]
      })
      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          autoCommit: false,
          offsetReset: OffsetResetStrategy.Earliest,
          heartbeatIntervalMs: 100_000
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      expect(consumer.partitions).toEqual([{ topic: "test-topic", partition: 0 }])
      await consumer.close()
    })

    it("invokes rebalance listener on assignment", async () => {
      const onAssigned = vi.fn()
      const { pool } = createJoinFlowMock({ committedOffset: 10n })
      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          autoCommit: false,
          heartbeatIntervalMs: 100_000,
          rebalanceListener: { onPartitionsAssigned: onAssigned }
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      expect(onAssigned).toHaveBeenCalledWith([{ topic: "test-topic", partition: 0 }])
      await consumer.close()
    })

    it("retries on retriable errors", async () => {
      // First attempt: FindCoordinator returns COORDINATOR_NOT_AVAILABLE (15)
      const failConn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildFindCoordinatorV0Body(15, 0, "", 0)
      ])

      // Second attempt: success
      const { conn: successConn } = createJoinFlowMock({ committedOffset: 10n })

      let callCount = 0
      const pool = createMockPool({
        brokers: new Map([
          [1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]
        ]) as ConnectionPool["brokers"],
        // eslint-disable-next-line @typescript-eslint/require-await
        getConnectionByNodeId: vi.fn(async () => {
          callCount++
          // First getConnectionByNodeId call is the failing attempt
          if (callCount <= 1) {
            return failConn
          }
          return successConn
        }) as unknown as ConnectionPool["getConnectionByNodeId"],
        releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          autoCommit: false,
          heartbeatIntervalMs: 100_000,
          retry: { maxRetries: 3, initialRetryMs: 1, maxRetryMs: 10, multiplier: 1 }
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      expect(consumer.partitions).toHaveLength(1)
      await consumer.close()
    })

    it("handles MEMBER_ID_REQUIRED during join", async () => {
      const memberId = "assigned-member-id"
      const assignmentBytes = buildConsumerProtocolAssignment([
        { topic: "test-topic", partitions: [0] }
      ])

      // First JoinGroup returns MEMBER_ID_REQUIRED (79) with assigned memberId
      // Second JoinGroup with that memberId succeeds
      const conn = createMockConnection([
        // First attempt: FindCoordinator
        buildApiVersionsBody(STANDARD_APIS),
        buildFindCoordinatorV0Body(0, 1, "localhost", 9092),
        // First JoinGroup: MEMBER_ID_REQUIRED
        buildApiVersionsBody(STANDARD_APIS),
        buildJoinGroupV0Body(79, -1, "", "", memberId, []),
        // Recursive joinAndSync: second JoinGroup succeeds
        buildApiVersionsBody(STANDARD_APIS),
        buildJoinGroupV0Body(0, 1, "range", "leader-1", memberId, []),
        // SyncGroup
        buildApiVersionsBody(STANDARD_APIS),
        buildSyncGroupV0Body(0, assignmentBytes),
        // Metadata refresh (comes before OffsetFetch in fetchInitialOffsets)
        buildApiVersionsBody(STANDARD_APIS),
        buildMetadataV1Body(
          [{ nodeId: 1, host: "localhost", port: 9092 }],
          [
            {
              errorCode: 0,
              name: "test-topic",
              isInternal: false,
              partitions: [
                {
                  errorCode: 0,
                  partitionIndex: 0,
                  leaderId: 1,
                  replicaNodes: [1],
                  isrNodes: [1]
                }
              ]
            }
          ]
        ),
        // OffsetFetch
        buildApiVersionsBody(STANDARD_APIS),
        buildOffsetFetchV0Body([
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, committedOffset: 5n, metadata: null, errorCode: 0 }]
          }
        ])
      ])

      const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])

      const pool = createMockPool({
        brokers: brokerMap as ConnectionPool["brokers"],

        getConnectionByNodeId: vi.fn(async () =>
          Promise.resolve(conn)
        ) as unknown as ConnectionPool["getConnectionByNodeId"],
        releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          autoCommit: false,
          heartbeatIntervalMs: 100_000
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      expect(consumer.partitions).toEqual([{ topic: "test-topic", partition: 0 }])
      await consumer.close()
    })

    it("throws when offset reset strategy is none and no committed offset", async () => {
      // Build a flow where committedOffset = -1 and offsetReset = None
      const assignmentBytes = buildConsumerProtocolAssignment([
        { topic: "test-topic", partitions: [0] }
      ])

      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildFindCoordinatorV0Body(0, 1, "localhost", 9092),
        buildApiVersionsBody(STANDARD_APIS),
        buildJoinGroupV0Body(0, 1, "range", "leader-1", "member-1", []),
        buildApiVersionsBody(STANDARD_APIS),
        buildSyncGroupV0Body(0, assignmentBytes),
        // Metadata refresh (comes before OffsetFetch in fetchInitialOffsets)
        buildApiVersionsBody(STANDARD_APIS),
        buildMetadataV1Body(
          [{ nodeId: 1, host: "localhost", port: 9092 }],
          [
            {
              errorCode: 0,
              name: "test-topic",
              isInternal: false,
              partitions: [
                {
                  errorCode: 0,
                  partitionIndex: 0,
                  leaderId: 1,
                  replicaNodes: [1],
                  isrNodes: [1]
                }
              ]
            }
          ]
        ),
        // OffsetFetch
        buildApiVersionsBody(STANDARD_APIS),
        buildOffsetFetchV0Body([
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, committedOffset: -1n, metadata: null, errorCode: 0 }]
          }
        ])
      ])

      const pool = createMockPool({
        brokers: new Map([
          [1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]
        ]) as ConnectionPool["brokers"],

        getConnectionByNodeId: vi.fn(async () =>
          Promise.resolve(conn)
        ) as unknown as ConnectionPool["getConnectionByNodeId"],
        releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          autoCommit: false,
          heartbeatIntervalMs: 100_000,
          offsetReset: OffsetResetStrategy.None,
          retry: { maxRetries: 0 }
        })
      )
      consumer.subscribe(["test-topic"])
      await expect(consumer.connect()).rejects.toThrow(
        "no committed offset found and offset reset strategy is 'none'"
      )
    })

    it("handles leader assignment (isLeader=true)", async () => {
      const { pool } = createJoinFlowMock({
        committedOffset: 10n,
        isLeader: true,
        memberId: "leader-member"
      })
      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          autoCommit: false,
          heartbeatIntervalMs: 100_000
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      expect(consumer.partitions).toHaveLength(1)
      await consumer.close()
    })
  })

  // -------------------------------------------------------------------------
  // poll
  // -------------------------------------------------------------------------

  describe("poll", () => {
    it("returns empty when closed", async () => {
      const consumer = new KafkaConsumer(defaultConsumerOptions())
      await consumer.close()
      const records = await consumer.poll()
      expect(records).toEqual([])
    })

    it("triggers join when not yet joined", async () => {
      const { pool } = createJoinFlowMock({ committedOffset: 10n })
      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          autoCommit: false,
          heartbeatIntervalMs: 100_000
        })
      )
      consumer.subscribe(["test-topic"])
      // poll should trigger join, then return empty (no Fetch response mocked for after join)
      // Actually, poll will try to fetch after joining, so we need fetch response too
      // For simplicity, close before poll triggers fetch
      await consumer.connect()
      await consumer.close()
    })
  })

  // -------------------------------------------------------------------------
  // commitOffsets
  // -------------------------------------------------------------------------

  describe("commitOffsets", () => {
    it("returns immediately when closed", async () => {
      const consumer = new KafkaConsumer(defaultConsumerOptions())
      await consumer.close()
      await consumer.commitOffsets() // should not throw
    })

    it("returns immediately when not joined", async () => {
      const consumer = new KafkaConsumer(defaultConsumerOptions())
      await consumer.commitOffsets() // should not throw
    })
  })

  // -------------------------------------------------------------------------
  // close
  // -------------------------------------------------------------------------

  describe("close", () => {
    it("is idempotent", async () => {
      const consumer = new KafkaConsumer(defaultConsumerOptions())
      await consumer.close()
      await consumer.close() // should not throw
    })

    it("commits offsets and leaves group on close", async () => {
      const assignmentBytes = buildConsumerProtocolAssignment([
        { topic: "test-topic", partitions: [0] }
      ])

      // Full join flow + leave
      const conn = createMockConnection([
        // Join flow
        buildApiVersionsBody(STANDARD_APIS),
        buildFindCoordinatorV0Body(0, 1, "localhost", 9092),
        buildApiVersionsBody(STANDARD_APIS),
        buildJoinGroupV0Body(0, 1, "range", "leader-1", "member-1", []),
        buildApiVersionsBody(STANDARD_APIS),
        buildSyncGroupV0Body(0, assignmentBytes),
        // Metadata refresh (comes before OffsetFetch)
        buildApiVersionsBody(STANDARD_APIS),
        buildMetadataV1Body(
          [{ nodeId: 1, host: "localhost", port: 9092 }],
          [
            {
              errorCode: 0,
              name: "test-topic",
              isInternal: false,
              partitions: [
                {
                  errorCode: 0,
                  partitionIndex: 0,
                  leaderId: 1,
                  replicaNodes: [1],
                  isrNodes: [1]
                }
              ]
            }
          ]
        ),
        // OffsetFetch
        buildApiVersionsBody(STANDARD_APIS),
        buildOffsetFetchV0Body([
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, committedOffset: 5n, metadata: null, errorCode: 0 }]
          }
        ]),
        // LeaveGroup on close
        buildApiVersionsBody(STANDARD_APIS),
        buildLeaveGroupV0Body(0)
      ])

      const pool = createMockPool({
        brokers: new Map([
          [1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]
        ]) as ConnectionPool["brokers"],

        getConnectionByNodeId: vi.fn(async () =>
          Promise.resolve(conn)
        ) as unknown as ConnectionPool["getConnectionByNodeId"],
        releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          autoCommit: false, // no commit on close since we never fetched
          heartbeatIntervalMs: 100_000
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()
      await consumer.close()

      expect(consumer.partitions).toEqual([])
    })

    it("handles errors during leave gracefully", async () => {
      const { pool, conn } = createJoinFlowMock({ committedOffset: 10n })

      // Override conn to throw on the leave-group send
      const originalSend = conn.send as (...args: unknown[]) => Promise<BinaryReader>
      let sendCallCount = 0
      conn.send = vi.fn(async (...args: unknown[]) => {
        sendCallCount++
        // Fail on the leave group call (after the join flow responses)
        if (sendCallCount > conn.send.mock.calls.length) {
          throw new Error("connection failed")
        }
        const result = await originalSend(...args)
        return result
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          autoCommit: false,
          heartbeatIntervalMs: 100_000
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()
      // Close should not throw despite leave errors
      await consumer.close()
    })
  })

  // -------------------------------------------------------------------------
  // partitions getter
  // -------------------------------------------------------------------------

  describe("partitions", () => {
    it("returns empty array before connect", () => {
      const consumer = new KafkaConsumer(defaultConsumerOptions())
      expect(consumer.partitions).toEqual([])
    })

    it("returns multiple assigned partitions", async () => {
      const { pool } = createJoinFlowMock({
        committedOffset: 10n,
        assignment: [{ topic: "test-topic", partitions: [0, 1, 2] }]
      })
      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          autoCommit: false,
          heartbeatIntervalMs: 100_000
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      expect(consumer.partitions).toEqual([
        { topic: "test-topic", partition: 0 },
        { topic: "test-topic", partition: 1 },
        { topic: "test-topic", partition: 2 }
      ])
      await consumer.close()
    })
  })

  // -------------------------------------------------------------------------
  // OffsetResetStrategy
  // -------------------------------------------------------------------------

  describe("OffsetResetStrategy", () => {
    it("has the expected values", () => {
      expect(OffsetResetStrategy.Earliest).toBe("earliest")
      expect(OffsetResetStrategy.Latest).toBe("latest")
      expect(OffsetResetStrategy.None).toBe("none")
    })
  })

  // -------------------------------------------------------------------------
  // Error scenarios
  // -------------------------------------------------------------------------

  describe("error handling", () => {
    it("throws on JoinGroup protocol error", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildFindCoordinatorV0Body(0, 1, "localhost", 9092),
        buildApiVersionsBody(STANDARD_APIS),
        // INCONSISTENT_GROUP_PROTOCOL (23) - not retriable
        buildJoinGroupV0Body(23, -1, "", "", "", [])
      ])

      const pool = createMockPool({
        brokers: new Map([
          [1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]
        ]) as ConnectionPool["brokers"],

        getConnectionByNodeId: vi.fn(async () =>
          Promise.resolve(conn)
        ) as unknown as ConnectionPool["getConnectionByNodeId"],
        releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          retry: { maxRetries: 0 }
        })
      )
      consumer.subscribe(["test-topic"])
      await expect(consumer.connect()).rejects.toThrow("join group failed")
    })

    it("throws on SyncGroup protocol error", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildFindCoordinatorV0Body(0, 1, "localhost", 9092),
        buildApiVersionsBody(STANDARD_APIS),
        buildJoinGroupV0Body(0, 1, "range", "leader-1", "member-1", []),
        buildApiVersionsBody(STANDARD_APIS),
        // UNKNOWN_MEMBER_ID (25) - not retriable for sync
        buildSyncGroupV0Body(25, new Uint8Array(0))
      ])

      const pool = createMockPool({
        brokers: new Map([
          [1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]
        ]) as ConnectionPool["brokers"],

        getConnectionByNodeId: vi.fn(async () =>
          Promise.resolve(conn)
        ) as unknown as ConnectionPool["getConnectionByNodeId"],
        releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          retry: { maxRetries: 0 }
        })
      )
      consumer.subscribe(["test-topic"])
      await expect(consumer.connect()).rejects.toThrow("sync group failed")
    })

    it("throws on FindCoordinator error", async () => {
      const conn = createMockConnection([
        buildApiVersionsBody(STANDARD_APIS),
        buildFindCoordinatorV0Body(23, 0, "", 0) // INCONSISTENT_GROUP_PROTOCOL
      ])

      const pool = createMockPool({
        brokers: new Map([
          [1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]
        ]) as ConnectionPool["brokers"],

        getConnectionByNodeId: vi.fn(async () =>
          Promise.resolve(conn)
        ) as unknown as ConnectionPool["getConnectionByNodeId"],
        releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          retry: { maxRetries: 0 }
        })
      )
      consumer.subscribe(["test-topic"])
      await expect(consumer.connect()).rejects.toThrow("find coordinator failed")
    })
  })

  // -------------------------------------------------------------------------
  // poll() with records
  // -------------------------------------------------------------------------

  describe("poll with fetch", () => {
    function buildFetchV4Body(
      topics: {
        name: string
        partitions: {
          partitionIndex: number
          errorCode: number
          highWatermark: bigint
          records: Uint8Array | null
        }[]
      }[]
    ): Uint8Array {
      const w = new BinaryWriter()
      // throttle_time_ms (v1+)
      w.writeInt32(0)
      // topics array
      w.writeInt32(topics.length)
      for (const t of topics) {
        w.writeString(t.name)
        w.writeInt32(t.partitions.length)
        for (const p of t.partitions) {
          w.writeInt32(p.partitionIndex)
          w.writeInt16(p.errorCode)
          w.writeInt64(p.highWatermark)
          w.writeInt64(-1n) // last_stable_offset (v4+)
          w.writeInt32(-1) // aborted_transactions: null array
          w.writeBytes(p.records) // records
        }
      }
      return w.finish()
    }

    function buildOffsetCommitV0Body(
      topics: { name: string; partitions: { partitionIndex: number; errorCode: number }[] }[]
    ): Uint8Array {
      const w = new BinaryWriter()
      w.writeInt32(topics.length)
      for (const t of topics) {
        w.writeString(t.name)
        w.writeInt32(t.partitions.length)
        for (const p of t.partitions) {
          w.writeInt32(p.partitionIndex)
          w.writeInt16(p.errorCode)
        }
      }
      return w.finish()
    }

    it("returns records from poll", async () => {
      const textEncoder = new TextEncoder()

      // Build a record batch with one record
      const record = createRecord(textEncoder.encode("key1"), textEncoder.encode("value1"))
      const batch = buildRecordBatch([record], {
        baseOffset: 0n,
        baseTimestamp: 1000n
      })
      const recordBytes = encodeRecordBatch(batch)

      const fetchResponses = [
        buildApiVersionsBody(STANDARD_APIS),
        buildFetchV4Body([
          {
            name: "test-topic",
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 0,
                highWatermark: 1n,
                records: recordBytes
              }
            ]
          }
        ])
      ]

      const { pool } = createJoinFlowMock({
        committedOffset: 0n,
        extraResponses: fetchResponses
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          heartbeatIntervalMs: 60_000 // prevent heartbeat during test
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      const records = await consumer.poll()

      expect(records.length).toBe(1)
      expect(records[0].topic).toBe("test-topic")
      expect(records[0].partition).toBe(0)
      expect(records[0].offset).toBe(0n)
      expect(records[0].message.key).toEqual(textEncoder.encode("key1"))
      expect(records[0].message.value).toEqual(textEncoder.encode("value1"))

      await consumer.close()
    })

    it("handles empty fetch response", async () => {
      const fetchResponses = [
        buildApiVersionsBody(STANDARD_APIS),
        buildFetchV4Body([
          {
            name: "test-topic",
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 0,
                highWatermark: 0n,
                records: null
              }
            ]
          }
        ])
      ]

      const { pool } = createJoinFlowMock({
        committedOffset: 0n,
        extraResponses: fetchResponses
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          heartbeatIntervalMs: 60_000
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      const records = await consumer.poll()
      expect(records.length).toBe(0)

      await consumer.close()
    })

    it("handles OFFSET_OUT_OF_RANGE in fetch response", async () => {
      const fetchResponses = [
        buildApiVersionsBody(STANDARD_APIS),
        buildFetchV4Body([
          {
            name: "test-topic",
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 1, // OFFSET_OUT_OF_RANGE
                highWatermark: 0n,
                records: null
              }
            ]
          }
        ])
      ]

      const { pool } = createJoinFlowMock({
        committedOffset: 0n,
        extraResponses: fetchResponses
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          heartbeatIntervalMs: 60_000
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      // poll() should not throw; it handles the error by resetting the offset
      const records = await consumer.poll()
      expect(records.length).toBe(0)

      await consumer.close()
    })

    it("commits offsets after polling records", async () => {
      const textEncoder = new TextEncoder()

      const record = createRecord(textEncoder.encode("key1"), textEncoder.encode("value1"))
      const batch = buildRecordBatch([record], {
        baseOffset: 5n,
        baseTimestamp: 1000n
      })
      const recordBytes = encodeRecordBatch(batch)

      const fetchResponses = [
        buildApiVersionsBody(STANDARD_APIS),
        buildFetchV4Body([
          {
            name: "test-topic",
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 0,
                highWatermark: 10n,
                records: recordBytes
              }
            ]
          }
        ])
      ]

      const commitResponses = [
        buildApiVersionsBody(STANDARD_APIS),
        buildOffsetCommitV0Body([
          {
            name: "test-topic",
            partitions: [{ partitionIndex: 0, errorCode: 0 }]
          }
        ])
      ]

      const { pool, conn } = createJoinFlowMock({
        committedOffset: 5n,
        extraResponses: [...fetchResponses, ...commitResponses]
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          heartbeatIntervalMs: 60_000,
          autoCommit: false
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      const records = await consumer.poll()
      expect(records.length).toBe(1)

      // Commit should succeed
      await consumer.commitOffsets()

      // Verify send was called for commit (join flow + fetch + commit)
      expect(conn.send.mock.calls.length).toBeGreaterThan(10)

      await consumer.close()
    })

    it("returns multiple records from a batch", async () => {
      const textEncoder = new TextEncoder()

      const batchRecords = [
        createRecord(textEncoder.encode("k0"), textEncoder.encode("v0"), [], 0),
        createRecord(textEncoder.encode("k1"), textEncoder.encode("v1"), [], 1),
        createRecord(textEncoder.encode("k2"), textEncoder.encode("v2"), [], 2)
      ]
      const batch = buildRecordBatch(batchRecords, {
        baseOffset: 10n,
        baseTimestamp: 2000n
      })
      const recordBytes = encodeRecordBatch(batch)

      const fetchResponses = [
        buildApiVersionsBody(STANDARD_APIS),
        buildFetchV4Body([
          {
            name: "test-topic",
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 0,
                highWatermark: 13n,
                records: recordBytes
              }
            ]
          }
        ])
      ]

      const { pool } = createJoinFlowMock({
        committedOffset: 10n,
        extraResponses: fetchResponses
      })

      const consumer = new KafkaConsumer(
        defaultConsumerOptions({
          connectionPool: pool,
          heartbeatIntervalMs: 60_000
        })
      )
      consumer.subscribe(["test-topic"])
      await consumer.connect()

      const fetched = await consumer.poll()
      expect(fetched.length).toBe(3)
      expect(fetched[0].offset).toBe(10n)
      expect(fetched[1].offset).toBe(11n)
      expect(fetched[2].offset).toBe(12n)
      expect(fetched[0].message.value).toEqual(textEncoder.encode("v0"))

      await consumer.close()
    })
  })
})
