/**
 * Kafka consumer with group coordination and offset management.
 *
 * The consumer joins a consumer group, receives partition assignments,
 * fetches records from assigned partitions, and commits offsets. It handles
 * the full group coordination protocol: JoinGroup, SyncGroup, Heartbeat,
 * and LeaveGroup.
 *
 * @packageDocumentation
 */

/* eslint-disable @typescript-eslint/no-unnecessary-type-conversion */

import { ApiKey, negotiateVersion } from "../codec/api-keys.js"
import { BinaryReader } from "../codec/binary-reader.js"
import { BinaryWriter } from "../codec/binary-writer.js"
import { decodeRecordBatch } from "../codec/record-batch.js"
import { KafkaConnectionError, KafkaError, KafkaProtocolError } from "../errors.js"
import type { ConsumerRecord } from "../messages.js"
import type { ConnectionPool } from "../network/broker-pool.js"
import type { KafkaConnection } from "../network/connection.js"
import { apiVersionsToMap, decodeApiVersionsResponse } from "../protocol/api-versions.js"
import {
  type ConsumerGroupHeartbeatRequest,
  type ConsumerGroupHeartbeatRequestTopicPartition,
  decodeConsumerGroupHeartbeatResponse,
  encodeConsumerGroupHeartbeatRequest
} from "../protocol/consumer-group-heartbeat.js"
import {
  decodeFetchResponse,
  encodeFetchRequest,
  FetchIsolationLevel,
  type FetchRequest,
  type FetchTopicRequest
} from "../protocol/fetch.js"
import {
  CoordinatorType,
  decodeFindCoordinatorResponse,
  encodeFindCoordinatorRequest,
  type FindCoordinatorRequest
} from "../protocol/find-coordinator.js"
import {
  decodeHeartbeatResponse,
  encodeHeartbeatRequest,
  type HeartbeatRequest
} from "../protocol/heartbeat.js"
import {
  decodeJoinGroupResponse,
  encodeJoinGroupRequest,
  type JoinGroupRequest,
  type JoinGroupResponse
} from "../protocol/join-group.js"
import {
  decodeLeaveGroupResponse,
  encodeLeaveGroupRequest,
  type LeaveGroupRequest
} from "../protocol/leave-group.js"
import {
  decodeListOffsetsResponse,
  encodeListOffsetsRequest,
  type ListOffsetsRequest,
  OffsetTimestamp
} from "../protocol/list-offsets.js"
import {
  decodeMetadataResponse,
  encodeMetadataRequest,
  type MetadataPartition
} from "../protocol/metadata.js"
import {
  decodeOffsetCommitResponse,
  encodeOffsetCommitRequest,
  type OffsetCommitRequest,
  type OffsetCommitTopicRequest
} from "../protocol/offset-commit.js"
import {
  decodeOffsetFetchResponse,
  encodeOffsetFetchRequest,
  type OffsetFetchRequest
} from "../protocol/offset-fetch.js"
import {
  decodeSyncGroupResponse,
  encodeSyncGroupRequest,
  type SyncGroupAssignment,
  type SyncGroupRequest
} from "../protocol/sync-group.js"
import { type PartitionAssignor, rangeAssignor } from "./assignors.js"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Offset reset strategy when no committed offset is found.
 */
export const OffsetResetStrategy = {
  /** Start from the earliest available offset. */
  Earliest: "earliest",
  /** Start from the latest offset (only new messages). */
  Latest: "latest",
  /** Throw an error if no committed offset exists. */
  None: "none"
} as const

export type OffsetResetStrategy = (typeof OffsetResetStrategy)[keyof typeof OffsetResetStrategy]

/**
 * Consumer group protocol selection.
 *
 * - `"classic"` — JoinGroup/SyncGroup/Heartbeat/LeaveGroup (default)
 * - `"consumer"` — KIP-848 ConsumerGroupHeartbeat with server-side assignment
 */
export const GroupProtocol = {
  /** Classic consumer group protocol (JoinGroup/SyncGroup/Heartbeat/LeaveGroup). */
  Classic: "classic",
  /** KIP-848 consumer group protocol (ConsumerGroupHeartbeat with server-side assignment). */
  Consumer: "consumer"
} as const

export type GroupProtocol = (typeof GroupProtocol)[keyof typeof GroupProtocol]

/**
 * Callback invoked when a rebalance occurs.
 */
export type RebalanceListener = {
  /** Called when partitions are about to be revoked. */
  readonly onPartitionsRevoked?: (partitions: readonly AssignedPartition[]) => void | Promise<void>
  /** Called when new partitions are assigned. */
  readonly onPartitionsAssigned?: (partitions: readonly AssignedPartition[]) => void | Promise<void>
}

/**
 * A partition assigned to this consumer.
 */
export type AssignedPartition = {
  readonly topic: string
  readonly partition: number
}

/**
 * Consumer configuration options.
 */
export type ConsumerOptions = {
  /** The connection pool to use. */
  readonly connectionPool: ConnectionPool
  /** The consumer group ID. */
  readonly groupId: string
  /** Session timeout in milliseconds. @default 30000 */
  readonly sessionTimeoutMs?: number
  /** Rebalance timeout in milliseconds. @default 60000 */
  readonly rebalanceTimeoutMs?: number
  /** Heartbeat interval in milliseconds. @default 3000 */
  readonly heartbeatIntervalMs?: number
  /** Maximum bytes to fetch per partition. @default 1048576 (1 MiB) */
  readonly maxPartitionBytes?: number
  /** Maximum bytes to fetch per request. @default 52428800 (50 MiB) */
  readonly maxBytes?: number
  /** Minimum bytes for the broker to return. @default 1 */
  readonly minBytes?: number
  /** Maximum wait time in milliseconds for the broker to accumulate data. @default 500 */
  readonly maxWaitMs?: number
  /** Offset reset strategy. @default "latest" */
  readonly offsetReset?: OffsetResetStrategy
  /** Whether to auto-commit offsets. @default true */
  readonly autoCommit?: boolean
  /** Auto-commit interval in milliseconds. @default 5000 */
  readonly autoCommitIntervalMs?: number
  /** Fetch isolation level. @default ReadUncommitted */
  readonly isolationLevel?: (typeof FetchIsolationLevel)[keyof typeof FetchIsolationLevel]
  /** Rebalance listener callbacks. */
  readonly rebalanceListener?: RebalanceListener
  /** Group instance ID for static membership (KIP-345). Null for dynamic. */
  readonly groupInstanceId?: string | null
  /** Partition assignor strategy (classic protocol only). @default rangeAssignor */
  readonly assignor?: PartitionAssignor
  /** Retry configuration. */
  readonly retry?: ConsumerRetryConfig
  /**
   * Consumer group protocol.
   * - `"classic"` \u2014 JoinGroup/SyncGroup/Heartbeat/LeaveGroup (default)
   * - `"consumer"` \u2014 KIP-848 ConsumerGroupHeartbeat with server-side assignment
   * @default "classic"
   */
  readonly groupProtocol?: GroupProtocol
  /**
   * Server-side assignor name for KIP-848 protocol (e.g. "range", "uniform").
   * Only used when `groupProtocol` is `"consumer"`. Null uses the broker default.
   */
  readonly serverAssignor?: string | null
}

/**
 * Consumer retry configuration.
 */
export type ConsumerRetryConfig = {
  /** Maximum retries for retriable errors. @default 5 */
  readonly maxRetries?: number
  /** Initial retry delay in milliseconds. @default 100 */
  readonly initialRetryMs?: number
  /** Maximum retry delay in milliseconds. @default 30000 */
  readonly maxRetryMs?: number
  /** Retry delay multiplier. @default 2 */
  readonly multiplier?: number
}

/**
 * Internal state for a topic-partition assignment.
 */
type PartitionState = {
  fetchOffset: bigint
  committed: boolean
}

// ---------------------------------------------------------------------------
// Consumer group assignment protocol
// ---------------------------------------------------------------------------

/**
 * Encode consumer protocol metadata.
 *
 * Format: version (INT16) + topics array (INT32 count, then string per topic) + user_data (bytes, nullable)
 */
function encodeConsumerProtocolMetadata(topics: readonly string[]): Uint8Array {
  const writer = new BinaryWriter()
  writer.writeInt16(0) // version
  writer.writeInt32(topics.length)
  for (const topic of topics) {
    writer.writeString(topic)
  }
  writer.writeBytes(null) // user_data
  return writer.finish()
}

/**
 * Decode a consumer protocol assignment from SyncGroup response bytes.
 *
 * Format: version (INT16) + topic-partition array + user_data (bytes, nullable)
 */
function decodeConsumerProtocolAssignment(
  data: Uint8Array
): { topic: string; partitions: number[] }[] {
  if (data.byteLength === 0) {
    return []
  }

  const reader = new BinaryReader(data)

  // version (INT16) — ignore
  const vResult = reader.readInt16()
  if (!vResult.ok) {
    return []
  }

  // topics array
  const countResult = reader.readInt32()
  if (!countResult.ok) {
    return []
  }

  const assignments: { topic: string; partitions: number[] }[] = []
  for (let i = 0; i < countResult.value; i++) {
    const topicResult = reader.readString()
    if (!topicResult.ok) {
      return []
    }

    const partCountResult = reader.readInt32()
    if (!partCountResult.ok) {
      return []
    }

    const partitions: number[] = []
    for (let j = 0; j < partCountResult.value; j++) {
      const pResult = reader.readInt32()
      if (!pResult.ok) {
        return []
      }
      partitions.push(pResult.value)
    }

    assignments.push({ topic: topicResult.value ?? "", partitions })
  }

  return assignments
}

/**
 * Encode a consumer protocol assignment for a group member.
 */
function encodeConsumerProtocolAssignment(
  assignments: { topic: string; partitions: number[] }[]
): Uint8Array {
  const writer = new BinaryWriter()
  writer.writeInt16(0) // version

  writer.writeInt32(assignments.length)
  for (const a of assignments) {
    writer.writeString(a.topic)
    writer.writeInt32(a.partitions.length)
    for (const p of a.partitions) {
      writer.writeInt32(p)
    }
  }

  writer.writeBytes(null) // user_data
  return writer.finish()
}

// ---------------------------------------------------------------------------
// Error codes
// ---------------------------------------------------------------------------

/** Kafka protocol error codes relevant to consumer group coordination. */
const ErrorCode = {
  NONE: 0,
  OFFSET_OUT_OF_RANGE: 1,
  UNKNOWN_TOPIC_OR_PARTITION: 3,
  COORDINATOR_NOT_AVAILABLE: 15,
  NOT_COORDINATOR: 16,
  ILLEGAL_GENERATION: 22,
  INCONSISTENT_GROUP_PROTOCOL: 23,
  UNKNOWN_MEMBER_ID: 25,
  INVALID_SESSION_TIMEOUT: 26,
  REBALANCE_IN_PROGRESS: 27,
  MEMBER_ID_REQUIRED: 79,
  FENCED_INSTANCE_ID: 82
} as const

function isRetriableConsumerError(errorCode: number): boolean {
  return (
    errorCode === ErrorCode.NOT_COORDINATOR ||
    errorCode === ErrorCode.COORDINATOR_NOT_AVAILABLE ||
    errorCode === ErrorCode.REBALANCE_IN_PROGRESS
  )
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// ---------------------------------------------------------------------------
// KafkaConsumer
// ---------------------------------------------------------------------------

/**
 * Kafka consumer with consumer group support.
 *
 * Handles the full consumer group lifecycle: joining, syncing, heartbeating,
 * fetching records, committing offsets, and graceful shutdown.
 *
 * @example
 * ```ts
 * const consumer = new KafkaConsumer({
 *   connectionPool: kafka.connectionPool,
 *   groupId: "my-group",
 * })
 *
 * consumer.subscribe(["my-topic"])
 * await consumer.connect()
 *
 * while (running) {
 *   const records = await consumer.poll(1000)
 *   for (const record of records) {
 *     // process record
 *   }
 * }
 *
 * await consumer.close()
 * ```
 */
export class KafkaConsumer {
  private readonly pool: ConnectionPool
  private readonly groupId: string
  private readonly sessionTimeoutMs: number
  private readonly rebalanceTimeoutMs: number
  private readonly heartbeatIntervalMs: number
  private readonly maxPartitionBytes: number
  private readonly maxBytes: number
  private readonly minBytes: number
  private readonly maxWaitMs: number
  private readonly offsetReset: OffsetResetStrategy
  private readonly autoCommit: boolean
  private readonly autoCommitIntervalMs: number
  private readonly isolationLevel: FetchIsolationLevel
  private readonly rebalanceListener: RebalanceListener
  private readonly groupInstanceId: string | null
  private readonly assignor: PartitionAssignor
  private readonly groupProtocol: GroupProtocol
  private readonly serverAssignor: string | null
  private maxRetries!: number
  private initialRetryMs!: number
  private maxRetryMs!: number
  private retryMultiplier!: number

  // Subscriptions
  private subscribedTopics: string[] = []

  // Group coordination state (classic protocol)
  private memberId = ""
  private generationId = -1
  private coordinatorNodeId = -1
  private isLeader = false
  private joined = false
  private closed = false

  // KIP-848 state
  private memberEpoch = 0
  private readonly topicIdToName = new Map<string, string>()
  private readonly topicNameToId = new Map<string, Uint8Array>()

  // Partition assignments
  private readonly assignments = new Map<string, Map<number, PartitionState>>()

  // Timers
  private heartbeatTimer: ReturnType<typeof setTimeout> | null = null
  private autoCommitTimer: ReturnType<typeof setTimeout> | null = null

  constructor(options: ConsumerOptions) {
    this.pool = options.connectionPool
    this.groupId = options.groupId
    this.sessionTimeoutMs = options.sessionTimeoutMs ?? 30_000
    this.rebalanceTimeoutMs = options.rebalanceTimeoutMs ?? 60_000
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? 3_000
    this.maxPartitionBytes = options.maxPartitionBytes ?? 1_048_576
    this.maxBytes = options.maxBytes ?? 52_428_800
    this.minBytes = options.minBytes ?? 1
    this.maxWaitMs = options.maxWaitMs ?? 500
    this.offsetReset = options.offsetReset ?? OffsetResetStrategy.Latest
    this.autoCommit = options.autoCommit ?? true
    this.autoCommitIntervalMs = options.autoCommitIntervalMs ?? 5_000
    this.isolationLevel = options.isolationLevel ?? FetchIsolationLevel.ReadUncommitted
    this.rebalanceListener = options.rebalanceListener ?? {}
    this.groupInstanceId = options.groupInstanceId ?? null
    this.assignor = options.assignor ?? rangeAssignor
    this.groupProtocol = options.groupProtocol ?? GroupProtocol.Classic
    this.serverAssignor = options.serverAssignor ?? null
    this.initRetryOptions(options.retry)
  }

  private initRetryOptions(retry?: {
    maxRetries?: number
    initialRetryMs?: number
    maxRetryMs?: number
    multiplier?: number
  }): void {
    this.maxRetries = retry?.maxRetries ?? 5
    this.initialRetryMs = retry?.initialRetryMs ?? 100
    this.maxRetryMs = retry?.maxRetryMs ?? 30_000
    this.retryMultiplier = retry?.multiplier ?? 2
  }

  /**
   * Set the topics to subscribe to.
   *
   * Must be called before {@link connect}. Calling again after connecting
   * will trigger a rejoin on the next {@link poll}.
   *
   * @param topics - Topic names to subscribe to.
   */
  subscribe(topics: readonly string[]): void {
    this.subscribedTopics = [...topics]
  }

  /**
   * Connect to the group coordinator and join the consumer group.
   *
   * Discovers the coordinator, joins the group, receives partition
   * assignments, and starts heartbeating.
   */
  async connect(): Promise<void> {
    if (this.closed) {
      throw new KafkaConnectionError("consumer is closed", { retriable: false })
    }

    if (this.subscribedTopics.length === 0) {
      throw new KafkaConnectionError("no topics subscribed", { retriable: false })
    }

    await this.joinGroupWithRetry()
  }

  /**
   * Poll for new records from assigned partitions.
   *
   * Sends Fetch requests to the brokers holding the assigned partitions and
   * returns any records received. If no records are available within the
   * timeout, returns an empty array.
   *
   * Also handles rebalance signals from heartbeat responses and triggers
   * rejoin when necessary.
   *
   * @param _timeoutMs - Maximum time to wait for records (reserved for future use).
   * @returns Consumer records from the assigned partitions.
   */
  async poll(_timeoutMs?: number): Promise<readonly ConsumerRecord[]> {
    if (this.closed) {
      return []
    }

    if (!this.joined) {
      await this.joinGroupWithRetry()
    }

    // timeoutMs reserved for future use
    const records = await this.fetchRecords()
    return records
  }

  /**
   * Commit offsets for all consumed partitions.
   *
   * Commits the current fetch position for each assigned partition.
   */
  async commitOffsets(): Promise<void> {
    if (this.closed || !this.joined) {
      return
    }

    const topics = this.buildCommitTopics()
    if (topics.length === 0) {
      return
    }

    await this.sendOffsetCommit(topics)
  }

  /**
   * Get the current partition assignments.
   */
  get partitions(): readonly AssignedPartition[] {
    const result: AssignedPartition[] = []
    for (const [topic, partitions] of this.assignments) {
      for (const [partition] of partitions) {
        result.push({ topic, partition })
      }
    }
    return result
  }

  /**
   * Gracefully close the consumer.
   *
   * Commits offsets (if auto-commit is enabled), leaves the consumer group,
   * and stops all timers.
   */
  async close(): Promise<void> {
    if (this.closed) {
      return
    }
    this.closed = true

    this.stopHeartbeat()
    this.stopAutoCommit()

    try {
      if (this.autoCommit && this.joined) {
        await this.commitOffsets()
      }
    } catch {
      // best-effort commit on close
    }

    try {
      if (this.joined) {
        if (this.groupProtocol === GroupProtocol.Consumer) {
          await this.kip848Leave()
        } else {
          await this.leaveGroup()
        }
      }
    } catch {
      // best-effort leave
    }

    this.joined = false
    this.assignments.clear()
    this.memberId = ""
    this.generationId = -1
    this.memberEpoch = 0
  }

  // -------------------------------------------------------------------------
  // Internal — Group Coordination
  // -------------------------------------------------------------------------

  /**
   * Join the consumer group with retry for retriable errors.
   */
  private async joinGroupWithRetry(): Promise<void> {
    for (let attempt = 0; ; attempt++) {
      try {
        await this.findCoordinator()
        if (this.groupProtocol === GroupProtocol.Consumer) {
          await this.kip848Join()
        } else {
          await this.joinAndSync()
        }
        await this.fetchInitialOffsets()
        this.startHeartbeat()
        if (this.autoCommit) {
          this.startAutoCommit()
        }
        return
      } catch (error) {
        const isRetriable = error instanceof KafkaError && error.retriable
        if (!isRetriable || attempt >= this.maxRetries) {
          throw error
        }
        const delay = Math.min(
          this.initialRetryMs * this.retryMultiplier ** attempt,
          this.maxRetryMs
        )
        await sleep(delay)
      }
    }
  }

  /**
   * Discover the group coordinator broker.
   */
  private async findCoordinator(): Promise<void> {
    const { brokers } = this.pool
    const firstBroker = brokers.values().next().value
    if (!firstBroker) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const conn = await this.pool.getConnectionByNodeId(firstBroker.nodeId)
    try {
      const findCoordVersion = await this.negotiateApiVersion(conn, ApiKey.FindCoordinator)
      const request: FindCoordinatorRequest = {
        key: this.groupId,
        keyType: CoordinatorType.Group
      }

      const responseReader = await conn.send(ApiKey.FindCoordinator, findCoordVersion, (writer) => {
        encodeFindCoordinatorRequest(writer, request, findCoordVersion)
      })

      const result = decodeFindCoordinatorResponse(responseReader, findCoordVersion)
      if (!result.ok) {
        throw new KafkaConnectionError(
          `failed to decode find coordinator response: ${result.error.message}`,
          { broker: conn.broker }
        )
      }

      if (result.value.errorCode !== 0) {
        throw new KafkaProtocolError(
          `find coordinator failed with error code ${String(result.value.errorCode)}`,
          result.value.errorCode,
          isRetriableConsumerError(result.value.errorCode)
        )
      }

      this.coordinatorNodeId = result.value.nodeId
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Join the group and sync to receive partition assignments.
   */
  private async joinAndSync(): Promise<void> {
    const conn = await this.pool.getConnectionByNodeId(this.coordinatorNodeId)
    try {
      const joinGroupVersion = await this.negotiateApiVersion(conn, ApiKey.JoinGroup)
      const metadata = encodeConsumerProtocolMetadata(this.subscribedTopics)

      const joinRequest: JoinGroupRequest = {
        groupId: this.groupId,
        sessionTimeoutMs: this.sessionTimeoutMs,
        rebalanceTimeoutMs: this.rebalanceTimeoutMs,
        memberId: this.memberId,
        groupInstanceId: this.groupInstanceId,
        protocolType: "consumer",
        protocols: [{ name: this.assignor.name, metadata }]
      }

      const joinReader = await conn.send(ApiKey.JoinGroup, joinGroupVersion, (writer) => {
        encodeJoinGroupRequest(writer, joinRequest, joinGroupVersion)
      })

      const joinResult = decodeJoinGroupResponse(joinReader, joinGroupVersion)
      if (!joinResult.ok) {
        throw new KafkaConnectionError(
          `failed to decode join group response: ${joinResult.error.message}`,
          { broker: conn.broker }
        )
      }

      // Handle MEMBER_ID_REQUIRED (v4+): retry with assigned member ID
      if (joinResult.value.errorCode === ErrorCode.MEMBER_ID_REQUIRED) {
        this.memberId = joinResult.value.memberId
        this.pool.releaseConnection(conn)
        await this.joinAndSync()
        return
      }

      if (joinResult.value.errorCode !== 0) {
        throw new KafkaProtocolError(
          `join group failed with error code ${String(joinResult.value.errorCode)}`,
          joinResult.value.errorCode,
          isRetriableConsumerError(joinResult.value.errorCode)
        )
      }

      this.memberId = joinResult.value.memberId
      this.generationId = joinResult.value.generationId
      this.isLeader = joinResult.value.leader === this.memberId

      // Compute assignments if leader
      let assignments: SyncGroupAssignment[] = []
      if (this.isLeader) {
        await this.fetchTopicMetadataOn(conn)
        assignments = this.assignPartitions(joinResult.value)
      }

      // SyncGroup
      const syncGroupVersion = await this.negotiateApiVersion(conn, ApiKey.SyncGroup)
      const syncRequest: SyncGroupRequest = {
        groupId: this.groupId,
        generationId: this.generationId,
        memberId: this.memberId,
        groupInstanceId: this.groupInstanceId,
        protocolType: joinResult.value.protocolType,
        protocolName: joinResult.value.protocolName,
        assignments
      }

      const syncReader = await conn.send(ApiKey.SyncGroup, syncGroupVersion, (writer) => {
        encodeSyncGroupRequest(writer, syncRequest, syncGroupVersion)
      })

      const syncResult = decodeSyncGroupResponse(syncReader, syncGroupVersion)
      if (!syncResult.ok) {
        throw new KafkaConnectionError(
          `failed to decode sync group response: ${syncResult.error.message}`,
          { broker: conn.broker }
        )
      }

      if (syncResult.value.errorCode !== 0) {
        throw new KafkaProtocolError(
          `sync group failed with error code ${String(syncResult.value.errorCode)}`,
          syncResult.value.errorCode,
          isRetriableConsumerError(syncResult.value.errorCode)
        )
      }

      // Parse assignment
      const oldAssignments = [...this.partitions]
      const newAssignedTopicPartitions = decodeConsumerProtocolAssignment(
        syncResult.value.assignment
      )

      // Notify revoked
      if (oldAssignments.length > 0 && this.rebalanceListener.onPartitionsRevoked) {
        await this.rebalanceListener.onPartitionsRevoked(oldAssignments)
      }

      // Update assignments
      this.assignments.clear()
      const newAssignments: AssignedPartition[] = []
      for (const { topic, partitions } of newAssignedTopicPartitions) {
        const partMap = new Map<number, PartitionState>()
        for (const p of partitions) {
          partMap.set(p, { fetchOffset: -1n, committed: false })
          newAssignments.push({ topic, partition: p })
        }
        this.assignments.set(topic, partMap)
      }

      // Notify assigned
      if (newAssignments.length > 0 && this.rebalanceListener.onPartitionsAssigned) {
        await this.rebalanceListener.onPartitionsAssigned(newAssignments)
      }

      this.joined = true
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Assign partitions as group leader using the configured assignor strategy.
   */
  private assignPartitions(joinResponse: JoinGroupResponse): SyncGroupAssignment[] {
    // Build member subscriptions from JoinGroup metadata
    const members: { memberId: string; topics: string[] }[] = []
    for (const member of joinResponse.members) {
      const memberMeta = decodeConsumerProtocolMemberMetadata(member.metadata)
      members.push({ memberId: member.memberId, topics: memberMeta.topics })
    }

    // Get partition counts from cached topic metadata
    const partitionCounts = new Map<string, number>()
    for (const member of members) {
      for (const topic of member.topics) {
        if (!partitionCounts.has(topic)) {
          const meta = this.topicMetadataCache.get(topic)
          partitionCounts.set(topic, meta?.length ?? 0)
        }
      }
    }

    // Run the assignor
    const assignorResult = this.assignor.assign(members, partitionCounts)

    // Encode assignments for SyncGroup
    return assignorResult.map((a) => ({
      memberId: a.memberId,
      assignment: encodeConsumerProtocolAssignment(
        a.topicPartitions.map((tp) => ({ topic: tp.topic, partitions: [...tp.partitions] }))
      )
    }))
  }

  /**
   * Fetch initial offsets for assigned partitions.
   *
   * Retrieves committed offsets from the coordinator. For partitions without
   * committed offsets, applies the offset reset strategy.
   */
  private async fetchInitialOffsets(): Promise<void> {
    if (this.assignments.size === 0) {
      return
    }

    // First, get topic metadata to know partition leaders
    await this.refreshTopicMetadata()

    // Fetch committed offsets
    const conn = await this.pool.getConnectionByNodeId(this.coordinatorNodeId)
    try {
      const offsetFetchVersion = await this.negotiateApiVersion(conn, ApiKey.OffsetFetch)

      const topics = [...this.assignments.entries()].map(([topic, partitions]) => ({
        name: topic,
        partitionIndexes: [...partitions.keys()]
      }))

      const request: OffsetFetchRequest = {
        groupId: this.groupId,
        topics
      }

      const responseReader = await conn.send(ApiKey.OffsetFetch, offsetFetchVersion, (writer) => {
        encodeOffsetFetchRequest(writer, request, offsetFetchVersion)
      })

      const result = decodeOffsetFetchResponse(responseReader, offsetFetchVersion)
      if (!result.ok) {
        throw new KafkaConnectionError(
          `failed to decode offset fetch response: ${result.error.message}`,
          { broker: conn.broker }
        )
      }

      if (result.value.errorCode !== 0) {
        throw new KafkaProtocolError(
          `offset fetch failed with error code ${String(result.value.errorCode)}`,
          result.value.errorCode,
          isRetriableConsumerError(result.value.errorCode)
        )
      }

      // Apply committed offsets
      for (const topicResponse of result.value.topics) {
        const topicAssignments = this.assignments.get(topicResponse.name)
        if (!topicAssignments) {
          continue
        }

        for (const partResponse of topicResponse.partitions) {
          const state = topicAssignments.get(partResponse.partitionIndex)
          if (!state) {
            continue
          }

          if (partResponse.errorCode !== 0) {
            continue
          }

          if (partResponse.committedOffset >= 0n) {
            state.fetchOffset = partResponse.committedOffset
            state.committed = true
          }
        }
      }
    } finally {
      this.pool.releaseConnection(conn)
    }

    // For partitions without committed offsets, apply reset strategy
    await this.resetUncommittedOffsets()
  }

  /**
   * Reset offsets for partitions that have no committed offset.
   */
  private async resetUncommittedOffsets(): Promise<void> {
    const partitionsToReset = this.findPartitionsToReset()
    if (partitionsToReset.size === 0) {
      return
    }

    this.validateOffsetResetStrategy()
    const timestamp = this.getOffsetResetTimestamp()
    const topicMetadata = await this.getTopicMetadata()
    const byLeader = this.groupPartitionsByLeader(partitionsToReset, topicMetadata, timestamp)

    await this.fetchOffsetsFromLeaders(byLeader)
  }

  private findPartitionsToReset(): Map<string, number[]> {
    const partitionsToReset = new Map<string, number[]>()

    for (const [topic, partitions] of this.assignments) {
      for (const [partition, state] of partitions) {
        if (state.fetchOffset < 0n) {
          let topicPartitions = partitionsToReset.get(topic)
          if (!topicPartitions) {
            topicPartitions = []
            partitionsToReset.set(topic, topicPartitions)
          }
          topicPartitions.push(partition)
        }
      }
    }

    return partitionsToReset
  }

  private validateOffsetResetStrategy(): void {
    if (this.offsetReset === OffsetResetStrategy.None) {
      throw new KafkaConnectionError(
        "no committed offset found and offset reset strategy is 'none'",
        { retriable: false }
      )
    }
  }

  private getOffsetResetTimestamp(): bigint {
    return this.offsetReset === OffsetResetStrategy.Earliest
      ? OffsetTimestamp.Earliest
      : OffsetTimestamp.Latest
  }

  private groupPartitionsByLeader(
    partitionsToReset: Map<string, number[]>,
    topicMetadata: Map<string, readonly MetadataPartition[]>,
    timestamp: bigint
  ): Map<number, { topic: string; partition: number; timestamp: bigint }[]> {
    const byLeader = new Map<number, { topic: string; partition: number; timestamp: bigint }[]>()

    for (const [topic, partitions] of partitionsToReset) {
      const meta = topicMetadata.get(topic)
      if (!meta) {
        continue
      }

      for (const partition of partitions) {
        const partMeta = meta.find((p) => p.partitionIndex === partition)
        if (!partMeta || partMeta.leaderId < 0) {
          continue
        }

        let leaderPartitions = byLeader.get(partMeta.leaderId)
        if (!leaderPartitions) {
          leaderPartitions = []
          byLeader.set(partMeta.leaderId, leaderPartitions)
        }
        leaderPartitions.push({ topic, partition, timestamp })
      }
    }

    return byLeader
  }

  private async fetchOffsetsFromLeaders(
    byLeader: Map<number, { topic: string; partition: number; timestamp: bigint }[]>
  ): Promise<void> {
    for (const [leaderId, partitions] of byLeader) {
      await this.fetchOffsetsFromLeader(leaderId, partitions)
    }
  }

  private async fetchOffsetsFromLeader(
    leaderId: number,
    partitions: { topic: string; partition: number; timestamp: bigint }[]
  ): Promise<void> {
    const conn = await this.pool.getConnectionByNodeId(leaderId)
    try {
      const listOffsetsVersion = await this.negotiateApiVersion(conn, ApiKey.ListOffsets)
      const request = this.buildListOffsetsRequest(partitions, listOffsetsVersion)

      const responseReader = await conn.send(ApiKey.ListOffsets, listOffsetsVersion, (writer) => {
        encodeListOffsetsRequest(writer, request, listOffsetsVersion)
      })

      const result = decodeListOffsetsResponse(responseReader, listOffsetsVersion)
      if (!result.ok) {
        throw new KafkaConnectionError(
          `failed to decode list offsets response: ${result.error.message}`,
          { broker: conn.broker }
        )
      }

      this.applyOffsetResetResults(result.value.topics)
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  private buildListOffsetsRequest(
    partitions: { topic: string; partition: number; timestamp: bigint }[],
    version: number
  ): ListOffsetsRequest {
    const topicMap = new Map<string, { partition: number; timestamp: bigint }[]>()
    for (const p of partitions) {
      let arr = topicMap.get(p.topic)
      if (!arr) {
        arr = []
        topicMap.set(p.topic, arr)
      }
      arr.push({ partition: p.partition, timestamp: p.timestamp })
    }

    const request: ListOffsetsRequest = {
      replicaId: -1,
      topics: [...topicMap.entries()].map(([topic, parts]) => ({
        name: topic,
        partitions: parts.map((p) => ({
          partitionIndex: p.partition,
          timestamp: p.timestamp,
          currentLeaderEpoch: -1
        }))
      }))
    }

    if (version >= 2) {
      ;(request as { isolationLevel: number }).isolationLevel = this.isolationLevel
    }

    return request
  }

  private applyOffsetResetResults(
    topics: readonly {
      name: string
      partitions: readonly { partitionIndex: number; errorCode: number; offset: bigint }[]
    }[]
  ): void {
    for (const topicResult of topics) {
      const topicAssignments = this.assignments.get(topicResult.name)
      if (!topicAssignments) {
        continue
      }

      for (const partResult of topicResult.partitions) {
        if (partResult.errorCode !== 0) {
          continue
        }
        const state = topicAssignments.get(partResult.partitionIndex)
        if (state && state.fetchOffset < 0n) {
          state.fetchOffset = partResult.offset
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // Internal — Fetching
  // -------------------------------------------------------------------------

  /**
   * Fetch records from all assigned partitions.
   */
  private async fetchRecords(): Promise<ConsumerRecord[]> {
    const topicMetadata = await this.getTopicMetadata()
    const records: ConsumerRecord[] = []

    // Group partitions by leader
    const byLeader = new Map<number, { topic: string; partition: number; fetchOffset: bigint }[]>()

    for (const [topic, partitions] of this.assignments) {
      const meta = topicMetadata.get(topic)
      if (!meta) {
        continue
      }

      for (const [partition, state] of partitions) {
        if (state.fetchOffset < 0n) {
          continue
        }

        const partMeta = meta.find((p) => p.partitionIndex === partition)
        if (!partMeta || partMeta.leaderId < 0) {
          continue
        }

        let leaderPartitions = byLeader.get(partMeta.leaderId)
        if (!leaderPartitions) {
          leaderPartitions = []
          byLeader.set(partMeta.leaderId, leaderPartitions)
        }
        leaderPartitions.push({
          topic,
          partition,
          fetchOffset: state.fetchOffset
        })
      }
    }

    // Fetch from each leader
    const fetchPromises: Promise<ConsumerRecord[]>[] = []
    for (const [leaderId, partitions] of byLeader) {
      fetchPromises.push(this.fetchFromLeader(leaderId, partitions))
    }

    const results = await Promise.all(fetchPromises)
    for (const batch of results) {
      records.push(...batch)
    }

    return records
  }

  /**
   * Send a Fetch request to a specific leader and return parsed records.
   */
  private async fetchFromLeader(
    leaderId: number,
    partitions: { topic: string; partition: number; fetchOffset: bigint }[]
  ): Promise<ConsumerRecord[]> {
    const conn = await this.pool.getConnectionByNodeId(leaderId)
    try {
      const fetchVersion = await this.negotiateApiVersion(conn, ApiKey.Fetch)

      // Build per-topic partition requests
      const topicMap = new Map<string, { partition: number; fetchOffset: bigint }[]>()
      for (const p of partitions) {
        let arr = topicMap.get(p.topic)
        if (!arr) {
          arr = []
          topicMap.set(p.topic, arr)
        }
        arr.push({ partition: p.partition, fetchOffset: p.fetchOffset })
      }

      const topics: FetchTopicRequest[] = [...topicMap.entries()].map(([topic, parts]) => ({
        name: topic,
        partitions: parts.map((p) => ({
          partitionIndex: p.partition,
          fetchOffset: p.fetchOffset,
          partitionMaxBytes: this.maxPartitionBytes,
          currentLeaderEpoch: -1,
          logStartOffset: -1n
        }))
      }))

      const request: FetchRequest = {
        replicaId: -1,
        maxWaitMs: this.maxWaitMs,
        minBytes: this.minBytes,
        maxBytes: this.maxBytes,
        isolationLevel: this.isolationLevel,
        topics
      }

      const responseReader = await conn.send(ApiKey.Fetch, fetchVersion, (writer) => {
        encodeFetchRequest(writer, request, fetchVersion)
      })

      const result = decodeFetchResponse(responseReader, fetchVersion)
      if (!result.ok) {
        throw new KafkaConnectionError(`failed to decode fetch response: ${result.error.message}`, {
          broker: conn.broker
        })
      }

      // Parse records from response
      const records: ConsumerRecord[] = []

      for (const topicResponse of result.value.topics) {
        for (const partResponse of topicResponse.partitions) {
          this.processFetchPartitionResponse(records, topicResponse.name, partResponse)
        }
      }

      return records
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  private processFetchPartitionResponse(
    records: ConsumerRecord[],
    topicName: string,
    partResponse: { partitionIndex: number; errorCode: number; records?: Uint8Array | null }
  ): void {
    if (partResponse.errorCode === ErrorCode.OFFSET_OUT_OF_RANGE) {
      this.handleOffsetOutOfRange(topicName, partResponse.partitionIndex)
      return
    }

    if (partResponse.errorCode !== 0) {
      return
    }

    if (!partResponse.records || partResponse.records.byteLength === 0) {
      return
    }

    const batchRecords = this.decodePartitionRecords(
      topicName,
      partResponse.partitionIndex,
      partResponse.records
    )
    records.push(...batchRecords)

    if (batchRecords.length > 0) {
      this.advanceFetchOffset(topicName, partResponse.partitionIndex, batchRecords)
    }
  }

  private handleOffsetOutOfRange(topic: string, partition: number): void {
    const topicAssignments = this.assignments.get(topic)
    if (!topicAssignments) {
      return
    }

    const state = topicAssignments.get(partition)
    if (state) {
      state.fetchOffset = -1n
    }
  }

  private advanceFetchOffset(topic: string, partition: number, records: ConsumerRecord[]): void {
    const lastRecord = records[records.length - 1]
    const topicAssignments = this.assignments.get(topic)
    if (!topicAssignments) {
      return
    }

    const state = topicAssignments.get(partition)
    if (state) {
      state.fetchOffset = lastRecord.offset + 1n
      state.committed = false
    }
  }

  /**
   * Decode record batches from raw partition record bytes.
   */
  private decodePartitionRecords(
    topic: string,
    partition: number,
    recordBytes: Uint8Array
  ): ConsumerRecord[] {
    const records: ConsumerRecord[] = []
    let offset = 0

    while (offset < recordBytes.byteLength) {
      const remainingBytes = recordBytes.subarray(offset)
      const batchResult = decodeRecordBatch(remainingBytes)
      if (!batchResult.ok) {
        break
      }

      const batch = batchResult.value
      for (const record of batch.records) {
        records.push({
          topic,
          partition,
          offset: batch.baseOffset + BigInt(record.offsetDelta),
          message: {
            key: record.key,
            value: record.value,
            headers: record.headers.map((h) => ({
              key: h.key,
              value: h.value
            })),
            timestamp: batch.baseTimestamp + BigInt(record.timestampDelta)
          },
          leaderEpoch: batch.partitionLeaderEpoch
        })
      }

      offset += batchResult.bytesRead
    }

    return records
  }

  // -------------------------------------------------------------------------
  // Internal — Offset Commit
  // -------------------------------------------------------------------------

  /**
   * Build the topics array for an offset commit request.
   */
  private buildCommitTopics(): OffsetCommitTopicRequest[] {
    const topics: OffsetCommitTopicRequest[] = []

    for (const [topic, partitions] of this.assignments) {
      const partitionRequests = []
      for (const [partition, state] of partitions) {
        if (state.fetchOffset >= 0n && !state.committed) {
          partitionRequests.push({
            partitionIndex: partition,
            committedOffset: state.fetchOffset,
            committedMetadata: null
          })
        }
      }
      if (partitionRequests.length > 0) {
        topics.push({ name: topic, partitions: partitionRequests })
      }
    }

    return topics
  }

  /**
   * Send an OffsetCommit request to the coordinator.
   */
  private async sendOffsetCommit(topics: OffsetCommitTopicRequest[]): Promise<void> {
    const conn = await this.pool.getConnectionByNodeId(this.coordinatorNodeId)
    try {
      const commitVersion = await this.negotiateApiVersion(conn, ApiKey.OffsetCommit)

      const request: OffsetCommitRequest = {
        groupId: this.groupId,
        generationIdOrMemberEpoch:
          this.groupProtocol === GroupProtocol.Consumer ? this.memberEpoch : this.generationId,
        memberId: this.memberId,
        groupInstanceId: this.groupInstanceId,
        topics
      }

      const responseReader = await conn.send(ApiKey.OffsetCommit, commitVersion, (writer) => {
        encodeOffsetCommitRequest(writer, request, commitVersion)
      })

      const result = decodeOffsetCommitResponse(responseReader, commitVersion)
      if (!result.ok) {
        throw new KafkaConnectionError(
          `failed to decode offset commit response: ${result.error.message}`,
          { broker: conn.broker }
        )
      }

      // Mark committed partitions
      this.markPartitionsCommitted(result.value.topics)
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  private markPartitionsCommitted(
    topics: readonly {
      name: string
      partitions: readonly { partitionIndex: number; errorCode: number }[]
    }[]
  ): void {
    for (const topicResult of topics) {
      const topicAssignments = this.assignments.get(topicResult.name)
      if (!topicAssignments) {
        continue
      }

      for (const partResult of topicResult.partitions) {
        if (partResult.errorCode === 0) {
          const state = topicAssignments.get(partResult.partitionIndex)
          if (state) {
            state.committed = true
          }
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // Internal — Heartbeat
  // -------------------------------------------------------------------------

  private startHeartbeat(): void {
    this.stopHeartbeat()
    this.heartbeatTimer = setInterval(() => {
      if (this.groupProtocol === GroupProtocol.Consumer) {
        void this.kip848Heartbeat()
      } else {
        void this.sendHeartbeat()
      }
    }, this.heartbeatIntervalMs)
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
  }

  private async sendHeartbeat(): Promise<void> {
    if (this.closed || !this.joined) {
      return
    }

    try {
      const conn = await this.pool.getConnectionByNodeId(this.coordinatorNodeId)
      try {
        const heartbeatVersion = await this.negotiateApiVersion(conn, ApiKey.Heartbeat)

        const request: HeartbeatRequest = {
          groupId: this.groupId,
          generationId: this.generationId,
          memberId: this.memberId,
          groupInstanceId: this.groupInstanceId
        }

        const responseReader = await conn.send(ApiKey.Heartbeat, heartbeatVersion, (writer) => {
          encodeHeartbeatRequest(writer, request, heartbeatVersion)
        })

        const result = decodeHeartbeatResponse(responseReader, heartbeatVersion)
        if (!result.ok) {
          return
        }

        if (result.value.errorCode === ErrorCode.REBALANCE_IN_PROGRESS) {
          // Need to rejoin
          this.joined = false
          this.stopHeartbeat()
          this.stopAutoCommit()
        } else if (
          result.value.errorCode === ErrorCode.ILLEGAL_GENERATION ||
          result.value.errorCode === ErrorCode.UNKNOWN_MEMBER_ID ||
          result.value.errorCode === ErrorCode.FENCED_INSTANCE_ID
        ) {
          this.joined = false
          this.memberId = ""
          this.generationId = -1
          this.stopHeartbeat()
          this.stopAutoCommit()
        }
      } finally {
        this.pool.releaseConnection(conn)
      }
    } catch {
      // heartbeat failures are handled on next poll
    }
  }

  // -------------------------------------------------------------------------
  // Internal — Auto-commit
  // -------------------------------------------------------------------------

  private startAutoCommit(): void {
    this.stopAutoCommit()
    this.autoCommitTimer = setInterval(() => {
      void this.commitOffsets()
    }, this.autoCommitIntervalMs)
  }

  private stopAutoCommit(): void {
    if (this.autoCommitTimer !== null) {
      clearInterval(this.autoCommitTimer)
      this.autoCommitTimer = null
    }
  }

  // -------------------------------------------------------------------------
  // Internal — Leave Group
  // -------------------------------------------------------------------------

  private async leaveGroup(): Promise<void> {
    const conn = await this.pool.getConnectionByNodeId(this.coordinatorNodeId)
    try {
      const leaveVersion = await this.negotiateApiVersion(conn, ApiKey.LeaveGroup)

      const request: LeaveGroupRequest =
        leaveVersion >= 3
          ? {
              groupId: this.groupId,
              members: [
                {
                  memberId: this.memberId,
                  groupInstanceId: this.groupInstanceId
                }
              ]
            }
          : {
              groupId: this.groupId,
              memberId: this.memberId
            }

      const responseReader = await conn.send(ApiKey.LeaveGroup, leaveVersion, (writer) => {
        encodeLeaveGroupRequest(writer, request, leaveVersion)
      })

      const result = decodeLeaveGroupResponse(responseReader, leaveVersion)
      if (!result.ok) {
        return
      }

      // Ignore errors on leave — best effort
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  // -------------------------------------------------------------------------
  // Internal — KIP-848 Consumer Group Protocol
  // -------------------------------------------------------------------------

  /** KIP-848 error codes. */
  private static readonly kip848UnknownMemberId = 25
  private static readonly kip848FencedMemberEpoch = 110
  private static readonly kip848UnreleasedInstanceId = 112

  /**
   * Join the group using the KIP-848 ConsumerGroupHeartbeat API.
   *
   * Sends a heartbeat with memberEpoch=0 to join. The coordinator responds
   * with a memberId, memberEpoch, and partition assignment.
   */
  private async kip848Join(): Promise<void> {
    // Fetch metadata first to build topic ID mappings
    await this.refreshTopicMetadata()
    this.buildTopicIdMappings()

    const conn = await this.pool.getConnectionByNodeId(this.coordinatorNodeId)
    try {
      const version = await this.negotiateApiVersion(conn, ApiKey.ConsumerGroupHeartbeat)

      const request: ConsumerGroupHeartbeatRequest = {
        groupId: this.groupId,
        memberId: this.memberId,
        memberEpoch: 0,
        instanceId: this.groupInstanceId ?? null,
        rackId: null,
        rebalanceTimeoutMs: this.rebalanceTimeoutMs,
        subscribedTopicNames: this.subscribedTopics,
        serverAssignor: this.serverAssignor,
        topicPartitions: null
      }

      const responseReader = await conn.send(ApiKey.ConsumerGroupHeartbeat, version, (writer) => {
        encodeConsumerGroupHeartbeatRequest(writer, request, version)
      })

      const result = decodeConsumerGroupHeartbeatResponse(responseReader, version)
      if (!result.ok) {
        throw new KafkaConnectionError(
          `failed to decode consumer group heartbeat response: ${result.error.message}`,
          { broker: conn.broker }
        )
      }

      if (result.value.errorCode !== 0) {
        throw new KafkaProtocolError(
          `consumer group heartbeat failed with error code ${String(result.value.errorCode)}`,
          result.value.errorCode,
          isRetriableConsumerError(result.value.errorCode)
        )
      }

      this.memberId = result.value.memberId ?? this.memberId
      this.memberEpoch = result.value.memberEpoch

      if (result.value.assignment) {
        await this.kip848ApplyAssignment(result.value.assignment.topicPartitions)
      }

      this.joined = true
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Periodic KIP-848 heartbeat — keeps the member alive and receives
   * assignment updates from the coordinator.
   */
  private async kip848Heartbeat(): Promise<void> {
    if (this.closed || !this.joined) {
      return
    }

    try {
      const conn = await this.pool.getConnectionByNodeId(this.coordinatorNodeId)
      try {
        const version = await this.negotiateApiVersion(conn, ApiKey.ConsumerGroupHeartbeat)

        const request: ConsumerGroupHeartbeatRequest = {
          groupId: this.groupId,
          memberId: this.memberId,
          memberEpoch: this.memberEpoch,
          instanceId: this.groupInstanceId ?? null,
          rackId: null,
          rebalanceTimeoutMs: this.rebalanceTimeoutMs,
          subscribedTopicNames: null, // unchanged
          serverAssignor: null,
          topicPartitions: this.buildKip848TopicPartitions()
        }

        const responseReader = await conn.send(ApiKey.ConsumerGroupHeartbeat, version, (writer) => {
          encodeConsumerGroupHeartbeatRequest(writer, request, version)
        })

        const result = decodeConsumerGroupHeartbeatResponse(responseReader, version)
        if (!result.ok) {
          return
        }

        if (
          result.value.errorCode === KafkaConsumer.kip848UnknownMemberId ||
          result.value.errorCode === KafkaConsumer.kip848FencedMemberEpoch ||
          result.value.errorCode === KafkaConsumer.kip848UnreleasedInstanceId
        ) {
          this.joined = false
          this.memberId = ""
          this.memberEpoch = 0
          this.stopHeartbeat()
          this.stopAutoCommit()
          return
        }

        if (result.value.errorCode !== 0) {
          return
        }

        this.memberEpoch = result.value.memberEpoch

        if (result.value.assignment) {
          await this.kip848ApplyAssignment(result.value.assignment.topicPartitions)
        }
      } finally {
        this.pool.releaseConnection(conn)
      }
    } catch {
      // heartbeat failures are handled on next poll
    }
  }

  /**
   * Leave the group using KIP-848: send heartbeat with memberEpoch=-1.
   */
  private async kip848Leave(): Promise<void> {
    const conn = await this.pool.getConnectionByNodeId(this.coordinatorNodeId)
    try {
      const version = await this.negotiateApiVersion(conn, ApiKey.ConsumerGroupHeartbeat)

      const request: ConsumerGroupHeartbeatRequest = {
        groupId: this.groupId,
        memberId: this.memberId,
        memberEpoch: -1,
        instanceId: this.groupInstanceId ?? null,
        rackId: null,
        rebalanceTimeoutMs: this.rebalanceTimeoutMs,
        subscribedTopicNames: null,
        serverAssignor: null,
        topicPartitions: null
      }

      const responseReader = await conn.send(ApiKey.ConsumerGroupHeartbeat, version, (writer) => {
        encodeConsumerGroupHeartbeatRequest(writer, request, version)
      })

      const result = decodeConsumerGroupHeartbeatResponse(responseReader, version)
      if (!result.ok) {
        return
      }

      // Ignore errors on leave — best effort
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Apply a new partition assignment from a KIP-848 heartbeat response.
   */
  private async kip848ApplyAssignment(
    topicPartitions: readonly {
      readonly topicId: Uint8Array
      readonly partitions: readonly number[]
    }[]
  ): Promise<void> {
    const oldAssignments = [...this.partitions]

    // Notify revoked
    if (oldAssignments.length > 0 && this.rebalanceListener.onPartitionsRevoked) {
      await this.rebalanceListener.onPartitionsRevoked(oldAssignments)
    }

    // Update assignments
    this.assignments.clear()
    const newAssignments: AssignedPartition[] = []

    for (const tp of topicPartitions) {
      const topicName = this.topicIdToName.get(uuidToHex(tp.topicId))
      if (topicName === undefined || topicName === "") {
        continue
      }

      const partMap = new Map<number, PartitionState>()
      for (const p of tp.partitions) {
        partMap.set(p, { fetchOffset: -1n, committed: false })
        newAssignments.push({ topic: topicName, partition: p })
      }
      this.assignments.set(topicName, partMap)
    }

    // Notify assigned
    if (newAssignments.length > 0 && this.rebalanceListener.onPartitionsAssigned) {
      await this.rebalanceListener.onPartitionsAssigned(newAssignments)
    }
  }

  /**
   * Build topic-partitions for KIP-848 heartbeat (current assignment state).
   */
  private buildKip848TopicPartitions(): ConsumerGroupHeartbeatRequestTopicPartition[] {
    const result: ConsumerGroupHeartbeatRequestTopicPartition[] = []

    for (const [topic, partitions] of this.assignments) {
      const topicId = this.topicNameToId.get(topic)
      if (!topicId) {
        continue
      }

      result.push({
        topicId,
        partitions: [...partitions.keys()]
      })
    }

    return result
  }

  /**
   * Build topic ID ↔ name mappings from cached metadata.
   */
  private buildTopicIdMappings(): void {
    this.topicIdToName.clear()
    this.topicNameToId.clear()

    for (const [topicName, partitions] of this.topicMetadataCache) {
      // We need to get the topic ID from the metadata — it's on the MetadataTopic,
      // not MetadataPartition. Store it from metadata fetch instead.
      void partitions
      const topicId = this.topicIdCache.get(topicName)
      if (topicId) {
        const hex = uuidToHex(topicId)
        this.topicIdToName.set(hex, topicName)
        this.topicNameToId.set(topicName, topicId)
      }
    }
  }

  /** Cache of topic name → topic ID from Metadata responses. */
  private readonly topicIdCache = new Map<string, Uint8Array>()

  // -------------------------------------------------------------------------
  // Internal — Metadata & Version Negotiation
  // -------------------------------------------------------------------------

  /**
   * Negotiate the API version for a given API key against a broker.
   */
  private async negotiateApiVersion(conn: KafkaConnection, apiKey: ApiKey): Promise<number> {
    const apiVersionsReader = await conn.send(ApiKey.ApiVersions, 0, () => {
      // v0 has empty body
    })
    const apiVersionsResult = decodeApiVersionsResponse(apiVersionsReader, 0)
    if (!apiVersionsResult.ok) {
      throw new KafkaConnectionError(
        `failed to decode api versions response: ${apiVersionsResult.error.message}`,
        { broker: conn.broker }
      )
    }

    const versionMap = apiVersionsToMap(apiVersionsResult.value)
    const range = versionMap.get(apiKey)
    if (!range) {
      throw new KafkaConnectionError(`broker does not support api key ${String(apiKey)}`, {
        broker: conn.broker,
        retriable: false
      })
    }

    const version = negotiateVersion(apiKey, range)
    if (version === null) {
      throw new KafkaConnectionError(`no compatible version for api key ${String(apiKey)}`, {
        broker: conn.broker,
        retriable: false
      })
    }

    return version
  }

  /**
   * Fetch topic metadata using an existing connection (avoids pool deadlock).
   */
  private async fetchTopicMetadataOn(conn: KafkaConnection): Promise<void> {
    const metadataVersion = await this.negotiateApiVersion(conn, ApiKey.Metadata)

    const responseReader = await conn.send(ApiKey.Metadata, metadataVersion, (writer) => {
      encodeMetadataRequest(
        writer,
        {
          topics: this.subscribedTopics.map((t) => ({ name: t })),
          allowAutoTopicCreation: false
        },
        metadataVersion
      )
    })

    const result = decodeMetadataResponse(responseReader, metadataVersion)
    if (!result.ok) {
      throw new KafkaConnectionError(
        `failed to decode metadata response: ${result.error.message}`,
        { broker: conn.broker }
      )
    }

    this.topicMetadataCache.clear()
    for (const topic of result.value.topics) {
      if (topic.errorCode === 0) {
        this.topicMetadataCache.set(topic.name ?? "", topic.partitions)
        if (topic.topicId) {
          this.topicIdCache.set(topic.name ?? "", topic.topicId)
        }
      }
    }
  }

  /**
   * Refresh topic metadata and return partition info.
   */
  private async refreshTopicMetadata(): Promise<void> {
    const { brokers } = this.pool
    const firstBroker = brokers.values().next().value
    if (!firstBroker) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const conn = await this.pool.getConnectionByNodeId(firstBroker.nodeId)
    try {
      const metadataVersion = await this.negotiateApiVersion(conn, ApiKey.Metadata)

      const responseReader = await conn.send(ApiKey.Metadata, metadataVersion, (writer) => {
        encodeMetadataRequest(
          writer,
          {
            topics: this.subscribedTopics.map((t) => ({ name: t })),
            allowAutoTopicCreation: false
          },
          metadataVersion
        )
      })

      const result = decodeMetadataResponse(responseReader, metadataVersion)
      if (!result.ok) {
        throw new KafkaConnectionError(
          `failed to decode metadata response: ${result.error.message}`,
          { broker: conn.broker }
        )
      }

      // Cache metadata
      this.topicMetadataCache.clear()
      for (const topic of result.value.topics) {
        if (topic.errorCode === 0) {
          this.topicMetadataCache.set(topic.name ?? "", topic.partitions)
          if (topic.topicId) {
            this.topicIdCache.set(topic.name ?? "", topic.topicId)
          }
        }
      }
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  private readonly topicMetadataCache = new Map<string, readonly MetadataPartition[]>()

  /**
   * Get cached topic metadata, refreshing if necessary.
   */
  private async getTopicMetadata(): Promise<Map<string, readonly MetadataPartition[]>> {
    if (this.topicMetadataCache.size === 0) {
      await this.refreshTopicMetadata()
    }
    return this.topicMetadataCache
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Decode consumer protocol member metadata (from JoinGroup response).
 */
function decodeConsumerProtocolMemberMetadata(data: Uint8Array): { topics: string[] } {
  if (data.byteLength === 0) {
    return { topics: [] }
  }

  const reader = new BinaryReader(data)
  const vResult = reader.readInt16()
  if (!vResult.ok) {
    return { topics: [] }
  }

  const countResult = reader.readInt32()
  if (!countResult.ok) {
    return { topics: [] }
  }

  const topics: string[] = []
  for (let i = 0; i < countResult.value; i++) {
    const topicResult = reader.readString()
    if (!topicResult.ok) {
      return { topics: [] }
    }
    if (topicResult.value !== null) {
      topics.push(topicResult.value)
    }
  }

  return { topics }
}

/**
 * Convert a 16-byte UUID to a hex string for use as a Map key.
 */
function uuidToHex(uuid: Uint8Array): string {
  let hex = ""
  for (let i = 0; i < uuid.byteLength; i++) {
    hex += uuid[i].toString(16).padStart(2, "0")
  }
  return hex
}

/**
 * Create a new consumer.
 *
 * Convenience factory for {@link KafkaConsumer}.
 */
export function createConsumer(options: ConsumerOptions): KafkaConsumer {
  return new KafkaConsumer(options)
}
