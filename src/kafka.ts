/**
 * Top-level Kafka client with explicit resource lifecycle.
 *
 * Wraps {@link ConnectionPool} and provides connect/disconnect semantics.
 * Future producer, consumer, and admin clients will be created from this
 * instance.
 *
 * @packageDocumentation
 */

import { type BrokerInfo, ConnectionPool, type ConnectionPoolOptions } from "./broker-pool.js"
import { type BrokerAddress, type KafkaConfig, parseBrokerAddress } from "./config.js"
import { KafkaConfigError, KafkaConnectionError } from "./errors.js"
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
