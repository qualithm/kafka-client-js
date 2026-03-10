/**
 * Top-level Kafka client with explicit resource lifecycle.
 *
 * Wraps {@link ConnectionPool} and provides connect/disconnect semantics.
 * Future producer, consumer, and admin clients will be created from this
 * instance.
 *
 * @packageDocumentation
 */

import { type AdminOptions, type AdminRetryConfig, KafkaAdmin } from "./admin.js"
import { type BrokerInfo, ConnectionPool, type ConnectionPoolOptions } from "./broker-pool.js"
import { type BrokerAddress, type KafkaConfig, parseBrokerAddress } from "./config.js"
import {
  type ConsumerOptions,
  type ConsumerRetryConfig,
  KafkaConsumer,
  type OffsetResetStrategy,
  type RebalanceListener
} from "./consumer.js"
import { KafkaConfigError, KafkaConnectionError } from "./errors.js"
import type { FetchIsolationLevel } from "./fetch.js"
import type { Acks } from "./produce.js"
import {
  type BatchConfig,
  KafkaProducer,
  type Partitioner,
  type ProducerOptions,
  type RetryConfig
} from "./producer.js"
import type { CompressionCodec } from "./record-batch.js"
import type { SocketFactory } from "./socket.js"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Client lifecycle state.
 *
 * - `disconnected` — initial state or after {@link Kafka.disconnect}
 * - `connecting` — {@link Kafka.connect} is in progress
 * - `connected` — ready for use
 * - `disconnecting` — {@link Kafka.disconnect} is in progress
 */
export type KafkaState = "disconnected" | "connecting" | "connected" | "disconnecting"

/**
 * Options for creating a consumer from a {@link Kafka} client.
 */
export type KafkaConsumerOptions = {
  /**
   * The consumer group ID.
   */
  readonly groupId: string
  /**
   * Session timeout in milliseconds.
   * @default 30000
   */
  readonly sessionTimeoutMs?: number
  /**
   * Rebalance timeout in milliseconds.
   * @default 60000
   */
  readonly rebalanceTimeoutMs?: number
  /**
   * Heartbeat interval in milliseconds.
   * @default 3000
   */
  readonly heartbeatIntervalMs?: number
  /**
   * Maximum bytes per partition.
   * @default 1048576
   */
  readonly maxPartitionBytes?: number
  /**
   * Maximum bytes per fetch request.
   * @default 52428800
   */
  readonly maxBytes?: number
  /**
   * Minimum bytes for the broker to return.
   * @default 1
   */
  readonly minBytes?: number
  /**
   * Maximum wait time for the broker to accumulate data.
   * @default 500
   */
  readonly maxWaitMs?: number
  /**
   * Offset reset strategy.
   * @default "latest"
   */
  readonly offsetReset?: OffsetResetStrategy
  /**
   * Whether to auto-commit offsets.
   * @default true
   */
  readonly autoCommit?: boolean
  /**
   * Auto-commit interval in milliseconds.
   * @default 5000
   */
  readonly autoCommitIntervalMs?: number
  /**
   * Fetch isolation level.
   */
  readonly isolationLevel?: (typeof FetchIsolationLevel)[keyof typeof FetchIsolationLevel]
  /**
   * Rebalance listener callbacks.
   */
  readonly rebalanceListener?: RebalanceListener
  /**
   * Group instance ID for static membership.
   */
  readonly groupInstanceId?: string | null
  /**
   * Retry configuration.
   */
  readonly retry?: ConsumerRetryConfig
}

/**
 * Options for creating an admin client from a {@link Kafka} client.
 */
export type KafkaAdminOptions = {
  /**
   * Retry configuration for retriable errors.
   */
  readonly retry?: AdminRetryConfig
}

/**
 * Options for creating a {@link Kafka} client.
 */
export type KafkaOptions = {
  /** Kafka client configuration. */
  readonly config: KafkaConfig
  /** Factory function for creating runtime-specific sockets. */
  readonly socketFactory: SocketFactory
  /** Maximum connections per broker (default: 1). */
  readonly maxConnectionsPerBroker?: number
}

/**
 * Options for creating a producer from a {@link Kafka} client.
 */
export type KafkaProducerOptions = {
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
   * @default defaultPartitioner
   */
  readonly partitioner?: Partitioner
  /**
   * Compression codec for record batches.
   * @default CompressionCodec.NONE
   */
  readonly compression?: CompressionCodec
  /**
   * Whether this is an idempotent producer.
   * @default false
   */
  readonly idempotent?: boolean
  /**
   * Transactional ID for transactional producers.
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

// ---------------------------------------------------------------------------
// Kafka client
// ---------------------------------------------------------------------------

/**
 * Top-level Kafka client.
 *
 * Manages the connection pool and provides the entry point for producing,
 * consuming, and administering Kafka clusters.
 *
 * @example
 * ```ts
 * const kafka = createKafka({
 *   config: { brokers: ["localhost:9092"] },
 *   socketFactory: createNodeSocketFactory(),
 * })
 * await kafka.connect()
 * // ... use kafka ...
 * await kafka.disconnect()
 * ```
 */
export class Kafka {
  private currentState: KafkaState = "disconnected"
  private pool: ConnectionPool | null = null
  private readonly poolOptions: ConnectionPoolOptions

  constructor(options: KafkaOptions) {
    const brokers = resolveBrokerAddresses(options.config.brokers)

    if (brokers.length === 0) {
      throw new KafkaConfigError("at least one broker address is required")
    }

    this.poolOptions = {
      bootstrapBrokers: brokers,
      socketFactory: options.socketFactory,
      clientId: options.config.clientId,
      tls: options.config.tls,
      connectTimeoutMs: options.config.connectionTimeoutMs,
      requestTimeoutMs: options.config.requestTimeoutMs,
      maxConnectionsPerBroker: options.maxConnectionsPerBroker
    }
  }

  /** Current lifecycle state. */
  get state(): KafkaState {
    return this.currentState
  }

  /** All known brokers discovered from cluster metadata. */
  get brokers(): ReadonlyMap<number, BrokerInfo> {
    if (!this.pool) {
      return new Map()
    }
    return this.pool.brokers
  }

  /**
   * Connect to the Kafka cluster.
   *
   * Discovers brokers via the Metadata API and establishes the connection
   * pool. Must be called before using producer, consumer, or admin clients.
   *
   * Calling `connect()` on an already-connected client is a no-op.
   *
   * @throws {KafkaConnectionError} If connecting fails or the client is disconnecting.
   */
  async connect(): Promise<void> {
    if (this.currentState === "connected") {
      return
    }

    if (this.currentState === "disconnecting") {
      throw new KafkaConnectionError("client is disconnecting", { retriable: false })
    }

    if (this.currentState === "connecting") {
      throw new KafkaConnectionError("client is already connecting", { retriable: false })
    }

    this.currentState = "connecting"
    try {
      this.pool = new ConnectionPool(this.poolOptions)
      await this.pool.connect()
      this.currentState = "connected"
    } catch (error) {
      this.pool = null
      this.currentState = "disconnected"
      throw error
    }
  }

  /**
   * Disconnect from the Kafka cluster.
   *
   * Closes all connections and shuts down the connection pool.
   * Calling `disconnect()` on an already-disconnected client is a no-op.
   *
   * @throws {KafkaConnectionError} If the client is currently connecting.
   */
  async disconnect(): Promise<void> {
    if (this.currentState === "disconnected") {
      return
    }

    if (this.currentState === "connecting") {
      throw new KafkaConnectionError("client is currently connecting", { retriable: false })
    }

    if (this.currentState === "disconnecting") {
      throw new KafkaConnectionError("client is already disconnecting", { retriable: false })
    }

    this.currentState = "disconnecting"
    try {
      if (this.pool) {
        await this.pool.close()
      }
    } finally {
      this.pool = null
      this.currentState = "disconnected"
    }
  }

  /**
   * Refresh broker metadata.
   *
   * Re-discovers brokers from the cluster to pick up topology changes.
   *
   * @throws {KafkaConnectionError} If the client is not connected.
   */
  async refreshMetadata(): Promise<void> {
    const pool = this.getPool()
    await pool.refreshMetadata()
  }

  /**
   * Create a producer bound to this client.
   *
   * The client must be connected before creating a producer.
   *
   * @param options - Producer-specific options.
   * @returns A new {@link KafkaProducer} instance.
   * @throws {KafkaConnectionError} If the client is not connected.
   */
  producer(options?: KafkaProducerOptions): KafkaProducer {
    const pool = this.getPool()
    const producerOpts: ProducerOptions = {
      connectionPool: pool,
      ...options
    }
    return new KafkaProducer(producerOpts)
  }

  /**
   * Create a consumer bound to this client.
   *
   * The client must be connected before creating a consumer.
   *
   * @param options - Consumer-specific options.
   * @returns A new {@link KafkaConsumer} instance.
   * @throws {KafkaConnectionError} If the client is not connected.
   */
  consumer(options: KafkaConsumerOptions): KafkaConsumer {
    const pool = this.getPool()
    const consumerOpts: ConsumerOptions = {
      connectionPool: pool,
      ...options
    }
    return new KafkaConsumer(consumerOpts)
  }

  /**
   * Create an admin client bound to this client.
   *
   * The client must be connected before creating an admin client.
   *
   * @param options - Admin-specific options.
   * @returns A new {@link KafkaAdmin} instance.
   * @throws {KafkaConnectionError} If the client is not connected.
   */
  admin(options?: KafkaAdminOptions): KafkaAdmin {
    const pool = this.getPool()
    const adminOpts: AdminOptions = {
      connectionPool: pool,
      ...options
    }
    return new KafkaAdmin(adminOpts)
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

  /**
   * Access the underlying connection pool.
   *
   * @internal — intended for use by producer/consumer/admin implementations.
   * @throws {KafkaConnectionError} If the client is not connected.
   */
  get connectionPool(): ConnectionPool {
    return this.getPool()
  }

  private getPool(): ConnectionPool {
    if (this.currentState !== "connected" || !this.pool) {
      throw new KafkaConnectionError("client is not connected", { retriable: false })
    }
    return this.pool
  }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/**
 * Create a new Kafka client.
 *
 * Convenience factory that constructs a {@link Kafka} instance.
 *
 * @example
 * ```ts
 * const kafka = createKafka({
 *   config: { brokers: ["localhost:9092"] },
 *   socketFactory: createNodeSocketFactory(),
 * })
 * ```
 */
export function createKafka(options: KafkaOptions): Kafka {
  return new Kafka(options)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Resolve broker address strings or objects into a uniform array.
 */
function resolveBrokerAddresses(
  brokers: readonly BrokerAddress[] | readonly string[]
): BrokerAddress[] {
  return brokers.map((broker) => {
    if (typeof broker === "string") {
      return parseBrokerAddress(broker)
    }
    return broker
  })
}
