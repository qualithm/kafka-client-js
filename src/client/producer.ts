/**
 * Kafka producer with batching, partitioning, and retry support.
 *
 * The producer sends record batches to topic partitions via the Produce API.
 * It manages partition assignment, record batch encoding, and connection
 * routing through the {@link ConnectionPool}.
 *
 * @packageDocumentation
 */

import { ApiKey, negotiateVersion } from "../codec/api-keys.js"
import {
  buildRecordBatch,
  CompressionCodec,
  type CompressionCodec as CompressionCodecType,
  createRecord,
  encodeRecordBatch
} from "../codec/record-batch.js"
import { KafkaConnectionError, KafkaError, KafkaProtocolError } from "../errors.js"
import type { Message, ProduceResult, TopicPartition } from "../messages.js"
import type { ConnectionPool } from "../network/broker-pool.js"
import type { KafkaConnection } from "../network/connection.js"
import {
  decodeAddOffsetsToTxnResponse,
  encodeAddOffsetsToTxnRequest
} from "../protocol/add-offsets-to-txn.js"
import {
  decodeAddPartitionsToTxnResponse,
  encodeAddPartitionsToTxnRequest
} from "../protocol/add-partitions-to-txn.js"
import { apiVersionsToMap, decodeApiVersionsResponse } from "../protocol/api-versions.js"
import { decodeEndTxnResponse, encodeEndTxnRequest } from "../protocol/end-txn.js"
import {
  CoordinatorType,
  decodeFindCoordinatorResponse,
  encodeFindCoordinatorRequest
} from "../protocol/find-coordinator.js"
import {
  decodeInitProducerIdResponse,
  encodeInitProducerIdRequest,
  type InitProducerIdRequest
} from "../protocol/init-producer-id.js"
import {
  decodeMetadataResponse,
  encodeMetadataRequest,
  type MetadataPartition,
  type MetadataTopic
} from "../protocol/metadata.js"
import {
  Acks,
  decodeProduceResponse,
  encodeProduceRequest,
  type ProduceRequest,
  type ProduceTopicData
} from "../protocol/produce.js"
import {
  decodeTxnOffsetCommitResponse,
  encodeTxnOffsetCommitRequest
} from "../protocol/txn-offset-commit.js"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Partitioner function.
 *
 * Determines which partition a message should be sent to.
 *
 * @param topic - The topic name.
 * @param key - The message key, or null.
 * @param partitionCount - Total number of partitions for the topic.
 * @returns The partition index to send to.
 */
export type Partitioner = (topic: string, key: Uint8Array | null, partitionCount: number) => number

/**
 * Batching configuration for the producer.
 *
 * When `lingerMs` is greater than 0, messages are accumulated and sent in
 * batches to reduce the number of requests to the broker.
 */
export type BatchConfig = {
  /**
   * Maximum time in milliseconds to wait before flushing a batch.
   * Set to 0 for no lingering (immediate send).
   * @default 0
   */
  readonly lingerMs?: number
  /**
   * Maximum batch size in bytes per partition before triggering a flush.
   * @default 16384
   */
  readonly batchBytes?: number
}

/**
 * Retry configuration for the producer.
 */
export type RetryConfig = {
  /**
   * Maximum number of retry attempts for retriable errors.
   * @default 0
   */
  readonly maxRetries?: number
  /**
   * Initial delay in milliseconds before the first retry.
   * @default 100
   */
  readonly initialRetryMs?: number
  /**
   * Maximum delay in milliseconds between retries.
   * @default 30000
   */
  readonly maxRetryMs?: number
  /**
   * Multiplier for exponential backoff between retries.
   * @default 2
   */
  readonly multiplier?: number
}

/**
 * Offset with metadata for a topic-partition, used in transactional offset commits.
 */
export type TopicPartitionOffset = {
  /** Topic name. */
  readonly topic: string
  /** Partition index. */
  readonly partition: number
  /** The offset to commit. */
  readonly offset: bigint
  /** Optional metadata for the offset. */
  readonly metadata?: string | null
}

/**
 * Options for creating a {@link KafkaProducer}.
 */
export type ProducerOptions = {
  /** The connection pool to use for sending messages. */
  readonly connectionPool: ConnectionPool
  /**
   * Acknowledgement mode.
   * @default Acks.All (-1)
   */
  readonly acks?: Acks
  /**
   * Timeout in milliseconds for the broker to acknowledge the produce request.
   * @default 30000
   */
  readonly timeoutMs?: number
  /**
   * Custom partitioner function.
   * @default defaultPartitioner (murmur2-based key hash, round-robin for null keys)
   */
  readonly partitioner?: Partitioner
  /**
   * Compression codec for record batches.
   * @default CompressionCodec.NONE
   */
  readonly compression?: CompressionCodecType
  /**
   * Whether this is an idempotent producer.
   * When true, the producer sets acks to All and uses producer IDs.
   * @default false
   */
  readonly idempotent?: boolean
  /**
   * Transactional ID for transactional producers.
   * Implies idempotent = true.
   */
  readonly transactionalId?: string
  /**
   * Batching configuration.
   * When `lingerMs > 0`, messages are accumulated and flushed in batches.
   */
  readonly batch?: BatchConfig
  /**
   * Retry configuration for retriable errors.
   */
  readonly retry?: RetryConfig
}

/**
 * Cached topic metadata for partition routing.
 */
type TopicMetadata = {
  readonly partitions: readonly MetadataPartition[]
  readonly partitionLeaders: ReadonlyMap<number, number>
}

/**
 * Deferred promise callbacks for a single message in a batch.
 */
type DeferredResult = {
  readonly resolve: (result: ProduceResult) => void
  readonly reject: (error: Error) => void
}

/**
 * Accumulated messages for a single partition, pending flush.
 */
type PartitionAccumulator = {
  readonly messages: Message[]
  readonly deferreds: DeferredResult[]
  sizeBytes: number
}

// ---------------------------------------------------------------------------
// Partitioners
// ---------------------------------------------------------------------------

/**
 * Default partitioner using murmur2 hash for keys and round-robin for null keys.
 *
 * This matches the default Java producer partitioner behaviour.
 */
export function defaultPartitioner(): Partitioner {
  let counter = 0
  return (_topic: string, key: Uint8Array | null, partitionCount: number): number => {
    if (partitionCount <= 0) {
      return 0
    }
    if (key === null) {
      return counter++ % partitionCount
    }
    return Math.abs(murmur2(key)) % partitionCount
  }
}

/**
 * Round-robin partitioner. Ignores the message key.
 */
export function roundRobinPartitioner(): Partitioner {
  let counter = 0
  return (_topic: string, _key: Uint8Array | null, partitionCount: number): number => {
    if (partitionCount <= 0) {
      return 0
    }
    return counter++ % partitionCount
  }
}

// ---------------------------------------------------------------------------
// Murmur2 hash (matching Java Kafka client)
// ---------------------------------------------------------------------------

/**
 * Murmur2 hash compatible with the Java Kafka client's default partitioner.
 *
 * @see https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/utils/Utils.java
 */
function murmur2(data: Uint8Array): number {
  const seed = 0x9747b28c
  const m = 0x5bd1e995
  const r = 24

  let length = data.byteLength
  let h = seed ^ length
  let i = 0

  while (length >= 4) {
    let k =
      (data[i] & 0xff) |
      ((data[i + 1] & 0xff) << 8) |
      ((data[i + 2] & 0xff) << 16) |
      ((data[i + 3] & 0xff) << 24)

    k = Math.imul(k, m)
    k ^= k >>> r
    k = Math.imul(k, m)
    h = Math.imul(h, m)
    h ^= k

    i += 4
    length -= 4
  }

  switch (length) {
    case 3:
      h ^= (data[i + 2] & 0xff) << 16
      h ^= (data[i + 1] & 0xff) << 8
      h ^= data[i] & 0xff
      h = Math.imul(h, m)
      break
    case 2:
      h ^= (data[i + 1] & 0xff) << 8
      h ^= data[i] & 0xff
      h = Math.imul(h, m)
      break
    case 1:
      h ^= data[i] & 0xff
      h = Math.imul(h, m)
      break
    default:
      break
  }

  h ^= h >>> 13
  h = Math.imul(h, m)
  h ^= h >>> 15

  return h | 0
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Estimate the wire size of a message in bytes.
 */
function estimateMessageSize(message: Message): number {
  let size = 64 // record metadata overhead
  if (message.key) {
    size += message.key.byteLength
  }
  if (message.value) {
    size += message.value.byteLength
  }
  if (message.headers) {
    for (const header of message.headers) {
      size += header.key.length + 4
      if (header.value) {
        size += header.value.byteLength
      }
    }
  }
  return size
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Resolve the acks setting, forcing All for idempotent/transactional producers.
 */
function resolveAcks(options: ProducerOptions): Acks {
  const acks = options.acks ?? Acks.All
  if (
    (options.idempotent === true || typeof options.transactionalId === "string") &&
    acks !== Acks.All
  ) {
    return Acks.All
  }
  return acks
}

// ---------------------------------------------------------------------------
// KafkaProducer
// ---------------------------------------------------------------------------

/**
 * Kafka producer.
 *
 * Sends messages to topic partitions via the Produce API. Handles partition
 * assignment, record batch encoding, and routing to the correct broker.
 *
 * @example
 * ```ts
 * const producer = new KafkaProducer({
 *   connectionPool: kafka.connectionPool,
 * })
 * const results = await producer.send("my-topic", [
 *   { key: null, value: new TextEncoder().encode("hello") },
 * ])
 * await producer.close()
 * ```
 */
export class KafkaProducer {
  private readonly pool: ConnectionPool
  private readonly acks: Acks
  private readonly timeoutMs: number
  private readonly partitioner: Partitioner
  private readonly compression: CompressionCodecType
  private readonly transactionalId: string | null
  private readonly lingerMs: number
  private readonly batchBytes: number
  private readonly maxRetries: number
  private readonly initialRetryMs: number
  private readonly maxRetryMs: number
  private readonly retryMultiplier: number
  private readonly idempotent: boolean
  private readonly topicMetadataCache = new Map<string, TopicMetadata>()
  private readonly accumulator = new Map<string, Map<number, PartitionAccumulator>>()
  private readonly pendingEnqueues = new Set<Promise<void>>()
  private lingerTimer: ReturnType<typeof setTimeout> | null = null
  private closed = false

  // Idempotent producer state
  private producerId = -1n
  private producerEpoch = -1
  private readonly sequenceNumbers = new Map<string, number>()
  private producerIdPromise: Promise<void> | null = null

  // Transaction state
  private inTransaction = false
  private readonly txnPartitions = new Set<string>()
  private coordinatorHost: string | null = null
  private coordinatorPort: number | null = null

  constructor(options: ProducerOptions) {
    this.pool = options.connectionPool
    this.acks = resolveAcks(options)
    this.timeoutMs = options.timeoutMs ?? 30_000
    this.partitioner = options.partitioner ?? defaultPartitioner()
    this.compression = options.compression ?? CompressionCodec.NONE
    this.idempotent = options.idempotent === true || typeof options.transactionalId === "string"
    this.transactionalId = options.transactionalId ?? null
    this.lingerMs = options.batch?.lingerMs ?? 0
    this.batchBytes = options.batch?.batchBytes ?? 16_384
    this.maxRetries = options.retry?.maxRetries ?? (this.idempotent ? 5 : 0)
    this.initialRetryMs = options.retry?.initialRetryMs ?? 100
    this.maxRetryMs = options.retry?.maxRetryMs ?? 30_000
    this.retryMultiplier = options.retry?.multiplier ?? 2
  }

  /**
   * Send messages to a topic.
   *
   * Partitions messages using the configured partitioner, encodes them as
   * record batches, and sends produce requests to the appropriate brokers.
   *
   * @param topic - The topic to send messages to.
   * @param messages - The messages to send.
   * @returns Produce results per partition.
   * @throws {KafkaError} If the producer is closed or sending fails.
   */
  async send(topic: string, messages: readonly Message[]): Promise<readonly ProduceResult[]> {
    if (this.closed) {
      throw new KafkaConnectionError("producer is closed", { retriable: false })
    }

    if (this.transactionalId !== null && !this.inTransaction) {
      throw new KafkaError("transactional producer requires beginTransaction before send", false)
    }

    if (messages.length === 0) {
      return []
    }

    if (this.lingerMs <= 0) {
      await this.ensureProducerId()
      return this.withRetry(topic, async () => this.sendImmediate(topic, messages))
    }

    return this.enqueueMessages(topic, messages)
  }

  /**
   * Send messages targeting a specific partition directly.
   *
   * Bypasses the partitioner. The caller is responsible for choosing the
   * correct partition index.
   *
   * @param topicPartition - The topic and partition to send to.
   * @param messages - The messages to send.
   * @returns Produce result for the partition.
   */
  async sendToPartition(
    topicPartition: TopicPartition,
    messages: readonly Message[]
  ): Promise<ProduceResult> {
    if (this.closed) {
      throw new KafkaConnectionError("producer is closed", { retriable: false })
    }

    if (this.transactionalId !== null && !this.inTransaction) {
      throw new KafkaError("transactional producer requires beginTransaction before send", false)
    }

    if (messages.length === 0) {
      return { topicPartition, baseOffset: -1n }
    }

    return this.withRetry(topicPartition.topic, async () => {
      await this.ensureProducerId()
      if (this.inTransaction && this.transactionalId !== null) {
        await this.addNewPartitionsToTxn(topicPartition.topic, [topicPartition.partition])
      }
      const metadata = await this.getTopicMetadata(topicPartition.topic)
      const leaderId = metadata.partitionLeaders.get(topicPartition.partition)
      if (leaderId === undefined || leaderId < 0) {
        throw new KafkaConnectionError(
          `no leader for ${topicPartition.topic}-${String(topicPartition.partition)}`,
          { retriable: true }
        )
      }

      const partitions = new Map<number, Message[]>()
      partitions.set(topicPartition.partition, [...messages])

      const results = await this.sendToLeader(topicPartition.topic, leaderId, partitions)
      return results[0]
    })
  }

  /**
   * Close the producer.
   *
   * Flushes any pending batches before closing. After closing, no more
   * messages can be sent.
   */
  async close(): Promise<void> {
    if (this.closed) {
      return
    }
    try {
      if (this.inTransaction && this.transactionalId !== null) {
        this.discardBufferedMessages()
        try {
          await this.endTransaction(false)
        } catch {
          // Best-effort abort on close
        }
        this.inTransaction = false
        this.txnPartitions.clear()
      } else {
        await this.flush()
      }
    } finally {
      this.clearLingerTimer()
      this.closed = true
      this.topicMetadataCache.clear()
      this.sequenceNumbers.clear()
      this.producerId = -1n
      this.producerEpoch = -1
      this.coordinatorHost = null
      this.coordinatorPort = null
    }
  }

  /**
   * Flush all accumulated messages immediately.
   *
   * Forces all pending batches to be sent. Resolves when all batched
   * messages have been acknowledged (or rejected on failure).
   */
  async flush(): Promise<void> {
    // Wait for any in-flight accumulations to finish before draining
    if (this.pendingEnqueues.size > 0) {
      await Promise.all(this.pendingEnqueues)
    }
    this.clearLingerTimer()
    const snapshot = this.drainAccumulator()
    if (snapshot.size === 0) {
      return
    }

    const promises: Promise<void>[] = []
    for (const [topic, partitions] of snapshot) {
      promises.push(this.flushTopicBatches(topic, partitions))
    }
    await Promise.all(promises)
  }

  // -------------------------------------------------------------------------
  // Transaction API
  // -------------------------------------------------------------------------

  /**
   * Begin a new transaction.
   *
   * Requires the producer to be configured with a transactional ID.
   * Must be called before producing messages within a transaction.
   *
   * @throws {KafkaError} If not a transactional producer or already in a transaction.
   */
  beginTransaction(): void {
    if (this.transactionalId === null) {
      throw new KafkaError("beginTransaction requires a transactional producer", false)
    }
    if (this.inTransaction) {
      throw new KafkaError("transaction already in progress", false)
    }
    this.inTransaction = true
    this.txnPartitions.clear()
  }

  /**
   * Commit the current transaction.
   *
   * Flushes any pending messages and sends an EndTxn(committed=true) request
   * to the transaction coordinator.
   *
   * @throws {KafkaError} If not in a transaction or the commit fails.
   */
  async commitTransaction(): Promise<void> {
    this.requireInTransaction("commitTransaction")
    await this.flush()
    await this.endTransaction(true)
    this.inTransaction = false
    this.txnPartitions.clear()
  }

  /**
   * Abort the current transaction.
   *
   * Discards any buffered messages and sends an EndTxn(committed=false)
   * request to the transaction coordinator.
   *
   * @throws {KafkaError} If not in a transaction or the abort fails.
   */
  async abortTransaction(): Promise<void> {
    this.requireInTransaction("abortTransaction")
    this.discardBufferedMessages()
    await this.endTransaction(false)
    this.inTransaction = false
    this.txnPartitions.clear()
  }

  /**
   * Send consumer offsets to the transaction.
   *
   * Atomically commits consumer offsets as part of the current transaction,
   * enabling exactly-once consume-transform-produce patterns.
   *
   * @param offsets - The offsets to commit.
   * @param groupId - The consumer group ID.
   * @throws {KafkaError} If not in a transaction or the operation fails.
   */
  async sendOffsetsToTransaction(
    offsets: readonly TopicPartitionOffset[],
    groupId: string
  ): Promise<void> {
    this.requireInTransaction("sendOffsetsToTransaction")
    await this.ensureProducerId()
    // Step 1: Tell coordinator this txn includes offsets for this group
    await this.sendAddOffsetsToTxn(groupId)
    // Step 2: Find the group coordinator and commit offsets
    const groupConn = await this.findGroupCoordinator(groupId)
    try {
      await this.sendTxnOffsetCommit(groupConn, offsets, groupId)
    } finally {
      this.pool.releaseConnection(groupConn)
    }
  }

  // -------------------------------------------------------------------------
  // Internal — Idempotent producer
  // -------------------------------------------------------------------------

  /**
   * Ensure a producer ID has been allocated.
   *
   * For idempotent/transactional producers, sends an InitProducerId request
   * to the broker on the first call. Subsequent calls return immediately.
   * Concurrent callers share the same in-flight request.
   */
  private async ensureProducerId(): Promise<void> {
    if (!this.idempotent) {
      return
    }
    if (this.producerId >= 0n) {
      return
    }
    if (this.producerIdPromise) {
      return this.producerIdPromise
    }
    this.producerIdPromise = this.initProducerId()
    try {
      await this.producerIdPromise
    } finally {
      this.producerIdPromise = null
    }
  }

  /**
   * Send an InitProducerId request to the broker.
   */
  private async initProducerId(): Promise<void> {
    let conn: KafkaConnection
    if (this.transactionalId !== null) {
      conn = await this.getCoordinatorConnection()
    } else {
      const { brokers } = this.pool
      if (brokers.size === 0) {
        throw new KafkaConnectionError("no brokers available", { retriable: true })
      }
      const firstBroker = brokers.values().next().value
      if (!firstBroker) {
        throw new KafkaConnectionError("no brokers available", { retriable: true })
      }
      conn = await this.pool.getConnectionByNodeId(firstBroker.nodeId)
    }
    try {
      // Negotiate InitProducerId version
      const apiVersionsReader = await conn.send(ApiKey.ApiVersions, 0, () => {
        // v0 has an empty body
      })
      const apiVersionsResult = decodeApiVersionsResponse(apiVersionsReader, 0)
      if (!apiVersionsResult.ok) {
        throw new KafkaConnectionError(
          `failed to decode api versions response: ${apiVersionsResult.error.message}`,
          { broker: conn.broker }
        )
      }

      const versionMap = apiVersionsToMap(apiVersionsResult.value)
      const initPidRange = versionMap.get(ApiKey.InitProducerId)
      if (!initPidRange) {
        throw new KafkaConnectionError("broker does not support init producer id api", {
          broker: conn.broker,
          retriable: false
        })
      }

      const initPidVersion = negotiateVersion(ApiKey.InitProducerId, initPidRange)
      if (initPidVersion === null) {
        throw new KafkaConnectionError("no compatible init producer id api version", {
          broker: conn.broker,
          retriable: false
        })
      }

      const request: InitProducerIdRequest = {
        transactionalId: this.transactionalId,
        transactionTimeoutMs: this.transactionalId !== null ? this.timeoutMs : -1,
        producerId: initPidVersion >= 3 ? this.producerId : undefined,
        producerEpoch: initPidVersion >= 3 ? this.producerEpoch : undefined
      }

      const responseReader = await conn.send(ApiKey.InitProducerId, initPidVersion, (writer) => {
        encodeInitProducerIdRequest(writer, request, initPidVersion)
      })

      const responseResult = decodeInitProducerIdResponse(responseReader, initPidVersion)
      if (!responseResult.ok) {
        throw new KafkaConnectionError(
          `failed to decode init producer id response: ${responseResult.error.message}`,
          { broker: conn.broker }
        )
      }

      if (responseResult.value.errorCode !== 0) {
        throw new KafkaProtocolError(
          `init producer id failed with error code ${String(responseResult.value.errorCode)}`,
          responseResult.value.errorCode,
          isRetriableInitProducerIdError(responseResult.value.errorCode)
        )
      }

      this.producerId = responseResult.value.producerId
      this.producerEpoch = responseResult.value.producerEpoch
      this.sequenceNumbers.clear()
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Get the next sequence number for a topic-partition and advance it by count.
   *
   * Returns the base sequence for the batch. Sequence numbers wrap at 2^31.
   */
  private nextSequence(topic: string, partition: number, count: number): number {
    const key = `${topic}-${String(partition)}`
    const current = this.sequenceNumbers.get(key) ?? 0
    // Kafka sequence numbers are INT32, wrapping at 2^31
    const next = (current + count) & 0x7fffffff
    this.sequenceNumbers.set(key, next)
    return current
  }

  // -------------------------------------------------------------------------
  // Internal — Transaction support
  // -------------------------------------------------------------------------

  /**
   * Validate that a transaction is in progress.
   */
  private requireInTransaction(method: string): void {
    if (this.transactionalId === null) {
      throw new KafkaError(`${method} requires a transactional producer`, false)
    }
    if (!this.inTransaction) {
      throw new KafkaError("no transaction in progress", false)
    }
  }

  /**
   * Return the transactional ID, throwing if not configured.
   */
  private requireTransactionalId(): string {
    if (this.transactionalId === null) {
      throw new KafkaError("operation requires a transactional producer", false)
    }
    return this.transactionalId
  }

  /**
   * Discard all buffered (unsent) messages, rejecting their promises.
   */
  private discardBufferedMessages(): void {
    this.clearLingerTimer()
    const drained = this.drainAccumulator()
    for (const [, partitions] of drained) {
      for (const [, batch] of partitions) {
        for (const d of batch.deferreds) {
          d.reject(new KafkaError("transaction aborted", false))
        }
      }
    }
  }

  /**
   * Negotiate an API version on a connection.
   */
  private async negotiateApiVersion(conn: KafkaConnection, apiKey: ApiKey): Promise<number> {
    const apiVersionsReader = await conn.send(ApiKey.ApiVersions, 0, () => {
      // v0 has an empty body
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
   * Discover and cache the transaction coordinator for this producer's transactional ID.
   */
  private async findTransactionCoordinator(): Promise<void> {
    if (this.coordinatorHost !== null) {
      return
    }

    const { brokers } = this.pool
    if (brokers.size === 0) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }
    const firstBroker = brokers.values().next().value
    if (!firstBroker) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const conn = await this.pool.getConnectionByNodeId(firstBroker.nodeId)
    try {
      const version = await this.negotiateApiVersion(conn, ApiKey.FindCoordinator)

      const responseReader = await conn.send(ApiKey.FindCoordinator, version, (writer) => {
        encodeFindCoordinatorRequest(
          writer,
          { key: this.requireTransactionalId(), keyType: CoordinatorType.Transaction },
          version
        )
      })

      const result = decodeFindCoordinatorResponse(responseReader, version)
      if (!result.ok) {
        throw new KafkaConnectionError(
          `failed to decode find coordinator response: ${result.error.message}`,
          { broker: conn.broker }
        )
      }

      if (result.value.errorCode !== 0) {
        throw new KafkaProtocolError(
          `find transaction coordinator failed with error code ${String(result.value.errorCode)}`,
          result.value.errorCode,
          isRetriableCoordinatorError(result.value.errorCode)
        )
      }

      this.coordinatorHost = result.value.host
      this.coordinatorPort = result.value.port
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Get a connection to the transaction coordinator.
   */
  private async getCoordinatorConnection(): Promise<KafkaConnection> {
    await this.findTransactionCoordinator()
    if (this.coordinatorHost === null || this.coordinatorPort === null) {
      throw new KafkaConnectionError("transaction coordinator not found", { retriable: true })
    }
    return this.pool.getConnection(this.coordinatorHost, this.coordinatorPort)
  }

  /**
   * Clear the cached coordinator on coordinator-related errors.
   */
  private invalidateCoordinatorOnError(errorCode: number): void {
    // COORDINATOR_NOT_AVAILABLE (14) or NOT_COORDINATOR (15)
    if (errorCode === 14 || errorCode === 15) {
      this.coordinatorHost = null
      this.coordinatorPort = null
    }
  }

  /**
   * Add new partitions to the current transaction via AddPartitionsToTxn.
   */
  private async addNewPartitionsToTxn(topic: string, partitions: Iterable<number>): Promise<void> {
    const newPartitions: number[] = []
    for (const partition of partitions) {
      const key = `${topic}:${String(partition)}`
      if (!this.txnPartitions.has(key)) {
        newPartitions.push(partition)
      }
    }

    if (newPartitions.length === 0) {
      return
    }

    const conn = await this.getCoordinatorConnection()
    try {
      const version = await this.negotiateApiVersion(conn, ApiKey.AddPartitionsToTxn)

      const responseReader = await conn.send(ApiKey.AddPartitionsToTxn, version, (writer) => {
        encodeAddPartitionsToTxnRequest(
          writer,
          {
            transactionalId: this.requireTransactionalId(),
            producerId: this.producerId,
            producerEpoch: this.producerEpoch,
            topics: [{ name: topic, partitions: newPartitions }]
          },
          version
        )
      })

      const result = decodeAddPartitionsToTxnResponse(responseReader, version)
      if (!result.ok) {
        throw new KafkaConnectionError(
          `failed to decode add partitions to txn response: ${result.error.message}`,
          { broker: conn.broker }
        )
      }

      for (const topicResult of result.value.topics) {
        for (const partitionResult of topicResult.partitions) {
          if (partitionResult.partitionErrorCode !== 0) {
            this.invalidateCoordinatorOnError(partitionResult.partitionErrorCode)
            throw new KafkaProtocolError(
              `add partitions to txn failed for ${topic}-${String(partitionResult.partitionIndex)} with error code ${String(partitionResult.partitionErrorCode)}`,
              partitionResult.partitionErrorCode,
              isRetriableTransactionError(partitionResult.partitionErrorCode)
            )
          }
        }
      }

      for (const partition of newPartitions) {
        this.txnPartitions.add(`${topic}:${String(partition)}`)
      }
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Send EndTxn (commit or abort) to the transaction coordinator.
   */
  private async endTransaction(committed: boolean): Promise<void> {
    await this.ensureProducerId()
    const conn = await this.getCoordinatorConnection()
    try {
      const version = await this.negotiateApiVersion(conn, ApiKey.EndTxn)

      const responseReader = await conn.send(ApiKey.EndTxn, version, (writer) => {
        encodeEndTxnRequest(
          writer,
          {
            transactionalId: this.requireTransactionalId(),
            producerId: this.producerId,
            producerEpoch: this.producerEpoch,
            committed
          },
          version
        )
      })

      const result = decodeEndTxnResponse(responseReader, version)
      if (!result.ok) {
        throw new KafkaConnectionError(
          `failed to decode end txn response: ${result.error.message}`,
          { broker: conn.broker }
        )
      }

      if (result.value.errorCode !== 0) {
        this.invalidateCoordinatorOnError(result.value.errorCode)
        throw new KafkaProtocolError(
          `end transaction failed with error code ${String(result.value.errorCode)}`,
          result.value.errorCode,
          isRetriableTransactionError(result.value.errorCode)
        )
      }
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Send AddOffsetsToTxn to the transaction coordinator.
   */
  private async sendAddOffsetsToTxn(groupId: string): Promise<void> {
    const conn = await this.getCoordinatorConnection()
    try {
      const version = await this.negotiateApiVersion(conn, ApiKey.AddOffsetsToTxn)

      const responseReader = await conn.send(ApiKey.AddOffsetsToTxn, version, (writer) => {
        encodeAddOffsetsToTxnRequest(
          writer,
          {
            transactionalId: this.requireTransactionalId(),
            producerId: this.producerId,
            producerEpoch: this.producerEpoch,
            groupId
          },
          version
        )
      })

      const result = decodeAddOffsetsToTxnResponse(responseReader, version)
      if (!result.ok) {
        throw new KafkaConnectionError(
          `failed to decode add offsets to txn response: ${result.error.message}`,
          { broker: conn.broker }
        )
      }

      if (result.value.errorCode !== 0) {
        this.invalidateCoordinatorOnError(result.value.errorCode)
        throw new KafkaProtocolError(
          `add offsets to txn failed with error code ${String(result.value.errorCode)}`,
          result.value.errorCode,
          isRetriableTransactionError(result.value.errorCode)
        )
      }
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Find a consumer group coordinator and return a connection to it.
   */
  private async findGroupCoordinator(groupId: string): Promise<KafkaConnection> {
    const { brokers } = this.pool
    if (brokers.size === 0) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }
    const firstBroker = brokers.values().next().value
    if (!firstBroker) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const conn = await this.pool.getConnectionByNodeId(firstBroker.nodeId)
    try {
      const version = await this.negotiateApiVersion(conn, ApiKey.FindCoordinator)

      const responseReader = await conn.send(ApiKey.FindCoordinator, version, (writer) => {
        encodeFindCoordinatorRequest(
          writer,
          { key: groupId, keyType: CoordinatorType.Group },
          version
        )
      })

      const result = decodeFindCoordinatorResponse(responseReader, version)
      if (!result.ok) {
        throw new KafkaConnectionError(
          `failed to decode find coordinator response: ${result.error.message}`,
          { broker: conn.broker }
        )
      }

      if (result.value.errorCode !== 0) {
        throw new KafkaProtocolError(
          `find group coordinator failed with error code ${String(result.value.errorCode)}`,
          result.value.errorCode,
          isRetriableCoordinatorError(result.value.errorCode)
        )
      }

      return await this.pool.getConnection(result.value.host, result.value.port)
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Send TxnOffsetCommit to a group coordinator.
   */
  private async sendTxnOffsetCommit(
    conn: KafkaConnection,
    offsets: readonly TopicPartitionOffset[],
    groupId: string
  ): Promise<void> {
    const version = await this.negotiateApiVersion(conn, ApiKey.TxnOffsetCommit)

    // Group offsets by topic
    const byTopic = new Map<
      string,
      { partitionIndex: number; committedOffset: bigint; committedMetadata: string | null }[]
    >()
    for (const offset of offsets) {
      let parts = byTopic.get(offset.topic)
      if (!parts) {
        parts = []
        byTopic.set(offset.topic, parts)
      }
      parts.push({
        partitionIndex: offset.partition,
        committedOffset: offset.offset,
        committedMetadata: offset.metadata ?? null
      })
    }

    const topics = [...byTopic.entries()].map(([name, partitions]) => ({ name, partitions }))

    const responseReader = await conn.send(ApiKey.TxnOffsetCommit, version, (writer) => {
      encodeTxnOffsetCommitRequest(
        writer,
        {
          transactionalId: this.requireTransactionalId(),
          groupId,
          producerId: this.producerId,
          producerEpoch: this.producerEpoch,
          topics
        },
        version
      )
    })

    const result = decodeTxnOffsetCommitResponse(responseReader, version)
    if (!result.ok) {
      throw new KafkaConnectionError(
        `failed to decode txn offset commit response: ${result.error.message}`,
        { broker: conn.broker }
      )
    }

    for (const topicResult of result.value.topics) {
      for (const partitionResult of topicResult.partitions) {
        if (partitionResult.errorCode !== 0) {
          throw new KafkaProtocolError(
            `txn offset commit failed for ${topicResult.name}-${String(partitionResult.partitionIndex)} with error code ${String(partitionResult.errorCode)}`,
            partitionResult.errorCode,
            isRetriableTransactionError(partitionResult.errorCode)
          )
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

  /**
   * Send messages immediately without batching.
   */
  private async sendImmediate(
    topic: string,
    messages: readonly Message[]
  ): Promise<readonly ProduceResult[]> {
    const metadata = await this.getTopicMetadata(topic)
    const partitionCount = metadata.partitions.length
    const byPartition = new Map<number, Message[]>()

    for (const message of messages) {
      const partition = this.partitioner(topic, message.key, partitionCount)
      let group = byPartition.get(partition)
      if (!group) {
        group = []
        byPartition.set(partition, group)
      }
      group.push(message)
    }

    // For transactional producers, register new partitions with the coordinator
    if (this.inTransaction && this.transactionalId !== null) {
      await this.addNewPartitionsToTxn(topic, byPartition.keys())
    }

    const byLeader = new Map<number, Map<number, Message[]>>()

    for (const [partition, msgs] of byPartition) {
      const leaderId = metadata.partitionLeaders.get(partition)
      if (leaderId === undefined || leaderId < 0) {
        throw new KafkaConnectionError(`no leader for ${topic}-${String(partition)}`, {
          retriable: true
        })
      }

      let leaderPartitions = byLeader.get(leaderId)
      if (!leaderPartitions) {
        leaderPartitions = new Map()
        byLeader.set(leaderId, leaderPartitions)
      }
      leaderPartitions.set(partition, msgs)
    }

    const sendPromises: Promise<ProduceResult[]>[] = []
    for (const [leaderId, partitions] of byLeader) {
      sendPromises.push(this.sendToLeader(topic, leaderId, partitions))
    }

    const results = await Promise.all(sendPromises)
    return results.flat()
  }

  /**
   * Retry a function on retriable errors with exponential backoff.
   */
  private async withRetry<T>(topic: string, fn: () => Promise<T>): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      try {
        return await fn()
      } catch (error) {
        const isRetriable = error instanceof KafkaError && error.retriable
        if (!isRetriable || attempt >= this.maxRetries) {
          throw error
        }
        this.topicMetadataCache.delete(topic)
        const delay = Math.min(
          this.initialRetryMs * this.retryMultiplier ** attempt,
          this.maxRetryMs
        )
        await sleep(delay)
      }
    }
  }

  /**
   * Enqueue messages into the accumulator for batched sending.
   */
  private async enqueueMessages(
    topic: string,
    messages: readonly Message[]
  ): Promise<readonly ProduceResult[]> {
    let resolveAccumulated!: () => void
    const accumulatedPromise = new Promise<void>((resolve) => {
      resolveAccumulated = resolve
    })
    this.pendingEnqueues.add(accumulatedPromise)

    let promises: Promise<ProduceResult>[]
    try {
      await this.ensureProducerId()
      const metadata = await this.getTopicMetadata(topic)
      const partitionCount = metadata.partitions.length

      promises = []
      let batchReady = false

      for (const message of messages) {
        const partition = this.partitioner(topic, message.key, partitionCount)

        const promise = new Promise<ProduceResult>((resolve, reject) => {
          this.addToAccumulator(topic, partition, message, { resolve, reject })
        })
        promises.push(promise)

        const batch = this.getPartitionBatch(topic, partition)
        if (batch && batch.sizeBytes >= this.batchBytes) {
          batchReady = true
        }
      }

      // Signal accumulation complete before triggering flush
      resolveAccumulated()
      this.pendingEnqueues.delete(accumulatedPromise)

      if (batchReady) {
        void this.flush()
      } else {
        this.ensureLingerTimer()
      }
    } catch (error) {
      resolveAccumulated()
      this.pendingEnqueues.delete(accumulatedPromise)
      throw error
    }

    return Promise.all(promises)
  }

  private addToAccumulator(
    topic: string,
    partition: number,
    message: Message,
    deferred: DeferredResult
  ): void {
    let topicMap = this.accumulator.get(topic)
    if (!topicMap) {
      topicMap = new Map()
      this.accumulator.set(topic, topicMap)
    }

    let batch = topicMap.get(partition)
    if (!batch) {
      batch = { messages: [], deferreds: [], sizeBytes: 0 }
      topicMap.set(partition, batch)
    }

    batch.messages.push(message)
    batch.deferreds.push(deferred)
    batch.sizeBytes += estimateMessageSize(message)
  }

  private getPartitionBatch(topic: string, partition: number): PartitionAccumulator | undefined {
    return this.accumulator.get(topic)?.get(partition)
  }

  private drainAccumulator(): Map<string, Map<number, PartitionAccumulator>> {
    const snapshot = new Map(this.accumulator)
    this.accumulator.clear()
    return snapshot
  }

  /**
   * Flush accumulated batches for a single topic.
   */
  private async flushTopicBatches(
    topic: string,
    partitions: Map<number, PartitionAccumulator>
  ): Promise<void> {
    try {
      const results = await this.withRetry(topic, async () => {
        if (this.inTransaction && this.transactionalId !== null) {
          await this.addNewPartitionsToTxn(topic, partitions.keys())
        }
        const metadata = await this.getTopicMetadata(topic)

        const byLeader = new Map<number, Map<number, Message[]>>()
        for (const [partition, batch] of partitions) {
          const leaderId = metadata.partitionLeaders.get(partition)
          if (leaderId === undefined || leaderId < 0) {
            throw new KafkaConnectionError(`no leader for ${topic}-${String(partition)}`, {
              retriable: true
            })
          }
          let lp = byLeader.get(leaderId)
          if (!lp) {
            lp = new Map()
            byLeader.set(leaderId, lp)
          }
          lp.set(partition, batch.messages)
        }

        const sendPromises: Promise<ProduceResult[]>[] = []
        for (const [leaderId, leaderPartitions] of byLeader) {
          sendPromises.push(this.sendToLeader(topic, leaderId, leaderPartitions))
        }

        const leaderResults = await Promise.all(sendPromises)
        return leaderResults.flat()
      })

      const resultsByPartition = new Map<number, ProduceResult>()
      for (const result of results) {
        resultsByPartition.set(result.topicPartition.partition, result)
      }

      for (const [partition, batch] of partitions) {
        const result = resultsByPartition.get(partition)
        if (result) {
          for (const d of batch.deferreds) {
            d.resolve(result)
          }
        }
      }
    } catch (error) {
      for (const [, batch] of partitions) {
        for (const d of batch.deferreds) {
          d.reject(error instanceof Error ? error : new Error(String(error)))
        }
      }
    }
  }

  private ensureLingerTimer(): void {
    if (this.lingerTimer !== null) {
      return
    }
    this.lingerTimer = setTimeout(() => {
      this.lingerTimer = null
      void this.flush()
    }, this.lingerMs)
  }

  private clearLingerTimer(): void {
    if (this.lingerTimer !== null) {
      clearTimeout(this.lingerTimer)
      this.lingerTimer = null
    }
  }

  /**
   * Send partitioned messages to a specific leader broker.
   */
  private async sendToLeader(
    topic: string,
    leaderId: number,
    partitions: Map<number, Message[]>
  ): Promise<ProduceResult[]> {
    const conn = await this.pool.getConnectionByNodeId(leaderId)
    try {
      // Negotiate Produce API version
      const apiVersionsReader = await conn.send(ApiKey.ApiVersions, 0, () => {
        // v0 has an empty body
      })
      const apiVersionsResult = decodeApiVersionsResponse(apiVersionsReader, 0)
      if (!apiVersionsResult.ok) {
        throw new KafkaConnectionError(
          `failed to decode api versions response: ${apiVersionsResult.error.message}`,
          { broker: conn.broker }
        )
      }

      const versionMap = apiVersionsToMap(apiVersionsResult.value)
      const produceRange = versionMap.get(ApiKey.Produce)
      if (!produceRange) {
        throw new KafkaConnectionError("broker does not support produce api", {
          broker: conn.broker,
          retriable: false
        })
      }

      const produceVersion = negotiateVersion(ApiKey.Produce, produceRange)
      if (produceVersion === null) {
        throw new KafkaConnectionError("no compatible produce api version", {
          broker: conn.broker,
          retriable: false
        })
      }

      // Idempotent producers require produce API v3+ (transactional_id field)
      if (this.idempotent && produceVersion < 3) {
        throw new KafkaConnectionError(
          `idempotent producer requires produce api v3+, but broker only supports up to v${String(produceRange.maxVersion)}`,
          { broker: conn.broker, retriable: false }
        )
      }

      // Build topic data with encoded record batches
      const topicData = this.buildTopicData(topic, partitions)

      // Build produce request
      const request: ProduceRequest = {
        transactionalId: produceVersion >= 3 ? this.transactionalId : undefined,
        acks: this.acks,
        timeoutMs: this.timeoutMs,
        topics: [topicData]
      }

      // For acks = 0 (fire-and-forget), send without waiting for response
      if (this.acks === Acks.None) {
        await conn.send(ApiKey.Produce, produceVersion, (writer) => {
          encodeProduceRequest(writer, request, produceVersion)
        })
        // Return synthetic results for acks=0
        const results: ProduceResult[] = []
        for (const [partition] of partitions) {
          results.push({
            topicPartition: { topic, partition },
            baseOffset: -1n
          })
        }
        return results
      }

      // Send with response
      const responseReader = await conn.send(ApiKey.Produce, produceVersion, (writer) => {
        encodeProduceRequest(writer, request, produceVersion)
      })

      const responseResult = decodeProduceResponse(responseReader, produceVersion)
      if (!responseResult.ok) {
        throw new KafkaConnectionError(
          `failed to decode produce response: ${responseResult.error.message}`,
          { broker: conn.broker }
        )
      }

      // Convert response to ProduceResult array
      const results: ProduceResult[] = []
      for (const topicResponse of responseResult.value.topics) {
        for (const partitionResponse of topicResponse.partitions) {
          if (partitionResponse.errorCode !== 0) {
            throw new KafkaProtocolError(
              partitionResponse.errorMessage ??
                `produce failed for ${topic}-${String(partitionResponse.partitionIndex)} with error code ${String(partitionResponse.errorCode)}`,
              partitionResponse.errorCode,
              isRetriableProduceError(partitionResponse.errorCode)
            )
          }
          results.push({
            topicPartition: {
              topic: topicResponse.name,
              partition: partitionResponse.partitionIndex
            },
            baseOffset: partitionResponse.baseOffset,
            timestamp:
              partitionResponse.logAppendTimeMs >= 0n
                ? partitionResponse.logAppendTimeMs
                : undefined
          })
        }
      }

      return results
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Build topic data with encoded record batches for each partition.
   */
  private buildTopicData(topic: string, partitions: Map<number, Message[]>): ProduceTopicData {
    const partitionData = []

    for (const [partitionIndex, messages] of partitions) {
      const records = messages.map((msg, i) =>
        createRecord(msg.key, msg.value, msg.headers, i, msg.timestamp ?? 0n)
      )

      const baseSequence = this.idempotent
        ? this.nextSequence(topic, partitionIndex, records.length)
        : undefined

      const batch = buildRecordBatch(records, {
        compression: this.compression,
        baseTimestamp: messages[0].timestamp ?? BigInt(Date.now()),
        producerId: this.idempotent ? this.producerId : undefined,
        producerEpoch: this.idempotent ? this.producerEpoch : undefined,
        baseSequence,
        isTransactional: this.transactionalId !== null
      })

      const encoded = encodeRecordBatch(batch)

      partitionData.push({
        partitionIndex,
        records: encoded
      })
    }

    return {
      name: topic,
      partitions: partitionData
    }
  }

  /**
   * Get or refresh topic metadata.
   */
  private async getTopicMetadata(topic: string): Promise<TopicMetadata> {
    const cached = this.topicMetadataCache.get(topic)
    if (cached) {
      return cached
    }

    return this.refreshTopicMetadata(topic)
  }

  /**
   * Fetch topic metadata from the cluster.
   */
  private async refreshTopicMetadata(topic: string): Promise<TopicMetadata> {
    // Get any connection from the pool for metadata
    const { brokers } = this.pool
    if (brokers.size === 0) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const firstBroker = brokers.values().next().value
    if (!firstBroker) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const conn = await this.pool.getConnectionByNodeId(firstBroker.nodeId)
    try {
      // Negotiate Metadata version
      const apiVersionsReader = await conn.send(ApiKey.ApiVersions, 0, () => {
        // v0 has an empty body
      })
      const apiVersionsResult = decodeApiVersionsResponse(apiVersionsReader, 0)
      if (!apiVersionsResult.ok) {
        throw new KafkaConnectionError(
          `failed to decode api versions response: ${apiVersionsResult.error.message}`,
          { broker: conn.broker }
        )
      }

      const versionMap = apiVersionsToMap(apiVersionsResult.value)
      const metadataRange = versionMap.get(ApiKey.Metadata)
      if (!metadataRange) {
        throw new KafkaConnectionError("broker does not support metadata api", {
          broker: conn.broker,
          retriable: false
        })
      }

      const metadataVersion = negotiateVersion(ApiKey.Metadata, metadataRange)
      if (metadataVersion === null) {
        throw new KafkaConnectionError("no compatible metadata api version", {
          broker: conn.broker,
          retriable: false
        })
      }

      // Fetch metadata for the specific topic
      const metadataReader = await conn.send(ApiKey.Metadata, metadataVersion, (writer) => {
        encodeMetadataRequest(writer, { topics: [{ name: topic }] }, metadataVersion)
      })

      const metadataResult = decodeMetadataResponse(metadataReader, metadataVersion)
      if (!metadataResult.ok) {
        throw new KafkaConnectionError(
          `failed to decode metadata response: ${metadataResult.error.message}`,
          { broker: conn.broker }
        )
      }

      const topicMeta = metadataResult.value.topics.find((t: MetadataTopic) => t.name === topic)
      if (!topicMeta) {
        throw new KafkaError(`topic not found: ${topic}`, false)
      }

      if (topicMeta.errorCode !== 0) {
        throw new KafkaProtocolError(
          `metadata for topic ${topic} returned error code ${String(topicMeta.errorCode)}`,
          topicMeta.errorCode,
          isRetriableMetadataError(topicMeta.errorCode)
        )
      }

      const partitionLeaders = new Map<number, number>()
      for (const partition of topicMeta.partitions) {
        partitionLeaders.set(partition.partitionIndex, partition.leaderId)
      }

      const metadata: TopicMetadata = {
        partitions: topicMeta.partitions,
        partitionLeaders
      }
      this.topicMetadataCache.set(topic, metadata)
      return metadata
    } finally {
      this.pool.releaseConnection(conn)
    }
  }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/**
 * Create a new Kafka producer.
 *
 * @param options - Producer options.
 * @returns A new producer instance.
 */
export function createProducer(options: ProducerOptions): KafkaProducer {
  return new KafkaProducer(options)
}

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------

/**
 * Retriable Kafka error codes for produce operations.
 *
 * @see https://kafka.apache.org/protocol.html#protocol_error_codes
 */
function isRetriableProduceError(errorCode: number): boolean {
  switch (errorCode) {
    case 0: // NONE
      return false
    case 1: // OFFSET_OUT_OF_RANGE
    case 3: // UNKNOWN_TOPIC_OR_PARTITION
    case 6: // NOT_LEADER_OR_FOLLOWER
    case 7: // REQUEST_TIMED_OUT
    case 19: // REBALANCE_IN_PROGRESS
      return true
    case 45: // DUPLICATE_SEQUENCE_NUMBER — idempotent duplicate (safe to treat as success-like)
      return false
    case 46: // OUT_OF_ORDER_SEQUENCE_NUMBER
      return true
    default:
      return false
  }
}

/**
 * Retriable Kafka error codes for InitProducerId.
 */
function isRetriableInitProducerIdError(errorCode: number): boolean {
  switch (errorCode) {
    case 14: // COORDINATOR_NOT_AVAILABLE
    case 15: // NOT_COORDINATOR
      return true
    default:
      return false
  }
}

/**
 * Retriable Kafka error codes for metadata requests.
 */
function isRetriableMetadataError(errorCode: number): boolean {
  switch (errorCode) {
    case 5: // LEADER_NOT_AVAILABLE
    case 6: // NOT_LEADER_OR_FOLLOWER
      return true
    default:
      return false
  }
}

/**
 * Retriable Kafka error codes for transaction operations.
 */
function isRetriableTransactionError(errorCode: number): boolean {
  switch (errorCode) {
    case 14: // COORDINATOR_NOT_AVAILABLE
    case 15: // NOT_COORDINATOR
    case 51: // CONCURRENT_TRANSACTIONS
      return true
    default:
      return false
  }
}

/**
 * Retriable Kafka error codes for FindCoordinator.
 */
function isRetriableCoordinatorError(errorCode: number): boolean {
  switch (errorCode) {
    case 14: // COORDINATOR_NOT_AVAILABLE
    case 15: // NOT_COORDINATOR
      return true
    default:
      return false
  }
}
