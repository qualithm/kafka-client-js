/**
 * Broker discovery and connection pooling.
 *
 * Provides two capabilities:
 *
 * 1. **Broker discovery** — uses the Metadata API to learn about all brokers in
 *    the cluster from a set of bootstrap addresses.
 *
 * 2. **Connection pool** — manages a pool of {@link KafkaConnection} instances
 *    with configurable maximum connections per broker and lifecycle management.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_Metadata
 *
 * @packageDocumentation
 */

import { ApiKey, negotiateVersion } from "./api-keys.js"
import { apiVersionsToMap, decodeApiVersionsResponse } from "./api-versions.js"
import type { BrokerAddress, TlsConfig } from "./config.js"
import { KafkaConnection } from "./connection.js"
import { KafkaConnectionError } from "./errors.js"
import { decodeMetadataResponse, encodeMetadataRequest, type MetadataBroker } from "./metadata.js"
import type { SocketFactory } from "./socket.js"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Information about a discovered broker.
 */
export type BrokerInfo = {
  /** Broker node ID assigned by the cluster. */
  readonly nodeId: number
  /** Broker hostname. */
  readonly host: string
  /** Broker port. */
  readonly port: number
  /** Rack ID, if configured (null otherwise). */
  readonly rack: string | null
}

/**
 * Opt-in auto-reconnection strategy.
 *
 * When enabled, the connection pool will attempt to replace dead connections
 * in the background using exponential backoff. Disabled by default per the
 * explicit resource lifecycle principle.
 */
export type ReconnectStrategy = {
  /** Maximum number of reconnection attempts per dead connection (default: 5). */
  readonly maxRetries?: number
  /** Initial delay in milliseconds before the first reconnect attempt (default: 250). */
  readonly initialDelayMs?: number
  /** Maximum delay in milliseconds between reconnect attempts (default: 30000). */
  readonly maxDelayMs?: number
  /** Backoff multiplier applied after each failed attempt (default: 2). */
  readonly multiplier?: number
}

/**
 * Options for the {@link ConnectionPool}.
 */
export type ConnectionPoolOptions = {
  /** Bootstrap broker addresses for initial cluster discovery. */
  readonly bootstrapBrokers: readonly BrokerAddress[]
  /** Factory function for creating the underlying socket. */
  readonly socketFactory: SocketFactory
  /** Client ID sent in request headers (default: "@qualithm/kafka-client"). */
  readonly clientId?: string
  /** TLS configuration. */
  readonly tls?: TlsConfig
  /** Connection timeout in milliseconds (default: 30000). */
  readonly connectTimeoutMs?: number
  /** Per-request timeout in milliseconds (default: 30000). */
  readonly requestTimeoutMs?: number
  /** Maximum number of connections per broker (default: 1). */
  readonly maxConnectionsPerBroker?: number
  /**
   * Opt-in reconnection strategy. When set, the pool replaces dead connections
   * in the background with exponential backoff. Disabled by default.
   */
  readonly reconnect?: ReconnectStrategy
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

const DEFAULT_MAX_CONNECTIONS_PER_BROKER = 1
const DEFAULT_RECONNECT_MAX_RETRIES = 5
const DEFAULT_RECONNECT_INITIAL_DELAY_MS = 250
const DEFAULT_RECONNECT_MAX_DELAY_MS = 30_000
const DEFAULT_RECONNECT_MULTIPLIER = 2

/** @internal */
type ResolvedReconnectStrategy = {
  readonly maxRetries: number
  readonly initialDelayMs: number
  readonly maxDelayMs: number
  readonly multiplier: number
}

// ---------------------------------------------------------------------------
// Broker discovery
// ---------------------------------------------------------------------------

/**
 * Discover all brokers in a Kafka cluster using the Metadata API.
 *
 * Connects to one of the bootstrap brokers, sends an ApiVersions request
 * to negotiate the Metadata API version, then sends a Metadata request
 * with an empty topic list (broker discovery only).
 *
 * @param bootstrap - Bootstrap broker addresses to try.
 * @param socketFactory - Factory for creating sockets.
 * @param options - Optional connection settings.
 * @returns Array of discovered brokers.
 * @throws {KafkaConnectionError} If no bootstrap broker is reachable.
 */
export async function discoverBrokers(
  bootstrap: readonly BrokerAddress[],
  socketFactory: SocketFactory,
  options?: {
    readonly clientId?: string
    readonly tls?: TlsConfig
    readonly connectTimeoutMs?: number
    readonly requestTimeoutMs?: number
  }
): Promise<readonly BrokerInfo[]> {
  if (bootstrap.length === 0) {
    throw new KafkaConnectionError("no bootstrap brokers provided", { retriable: false })
  }

  let lastError: Error | undefined

  for (const broker of bootstrap) {
    const conn = new KafkaConnection({
      host: broker.host,
      port: broker.port,
      socketFactory,
      clientId: options?.clientId,
      tls: options?.tls,
      connectTimeoutMs: options?.connectTimeoutMs,
      requestTimeoutMs: options?.requestTimeoutMs
    })

    try {
      await conn.connect()

      const brokers = await fetchBrokerMetadata(conn)
      return brokers
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error))
    } finally {
      await conn.close()
    }
  }

  throw new KafkaConnectionError(
    "failed to discover brokers: none of the bootstrap brokers are reachable",
    { retriable: true, cause: lastError }
  )
}

/**
 * Fetch broker metadata from an established connection.
 *
 * Performs ApiVersions negotiation, then sends a Metadata request with an
 * empty topic list to discover all brokers in the cluster.
 *
 * @internal
 */
async function fetchBrokerMetadata(conn: KafkaConnection): Promise<readonly BrokerInfo[]> {
  // Step 1: ApiVersions to negotiate Metadata version
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

  if (apiVersionsResult.value.errorCode !== 0) {
    throw new KafkaConnectionError(
      `api versions request failed with error code ${String(apiVersionsResult.value.errorCode)}`,
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

  // Step 2: Metadata request with empty topic list (broker discovery)
  const metadataReader = await conn.send(ApiKey.Metadata, metadataVersion, (writer) => {
    encodeMetadataRequest(writer, { topics: [] }, metadataVersion)
  })

  const metadataResult = decodeMetadataResponse(metadataReader, metadataVersion)
  if (!metadataResult.ok) {
    throw new KafkaConnectionError(
      `failed to decode metadata response: ${metadataResult.error.message}`,
      { broker: conn.broker }
    )
  }

  return metadataResult.value.brokers.map(toBrokerInfo)
}

function toBrokerInfo(broker: MetadataBroker): BrokerInfo {
  return {
    nodeId: broker.nodeId,
    host: broker.host,
    port: broker.port,
    rack: broker.rack
  }
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

/**
 * A pool of connections to Kafka brokers.
 *
 * Manages connections with configurable maximum connections per broker.
 * Connections are created lazily on first request and reused for subsequent
 * requests to the same broker. Supports broker discovery from metadata to
 * learn about all brokers in the cluster.
 *
 * @example
 * ```ts
 * const pool = new ConnectionPool({
 *   bootstrapBrokers: [{ host: "localhost", port: 9092 }],
 *   socketFactory: createNodeSocketFactory(),
 * })
 * await pool.connect()
 * const conn = await pool.getConnection("localhost", 9092)
 * // use conn.send(...)
 * pool.releaseConnection(conn)
 * await pool.close()
 * ```
 */
export class ConnectionPool {
  private readonly pools = new Map<string, PoolEntry>()
  private readonly knownBrokers = new Map<number, BrokerInfo>()
  private closed = false
  private readonly reconnecting = new Set<string>() // brokerKey tracking in-flight reconnects

  private readonly bootstrapBrokers: readonly BrokerAddress[]
  private readonly socketFactory: SocketFactory
  private readonly clientId?: string
  private readonly tls?: TlsConfig
  private readonly connectTimeoutMs?: number
  private readonly requestTimeoutMs?: number
  private readonly maxConnectionsPerBroker: number
  private readonly reconnect?: ResolvedReconnectStrategy

  constructor(options: ConnectionPoolOptions) {
    this.bootstrapBrokers = options.bootstrapBrokers
    this.socketFactory = options.socketFactory
    this.clientId = options.clientId
    this.tls = options.tls
    this.connectTimeoutMs = options.connectTimeoutMs
    this.requestTimeoutMs = options.requestTimeoutMs
    this.maxConnectionsPerBroker =
      options.maxConnectionsPerBroker ?? DEFAULT_MAX_CONNECTIONS_PER_BROKER
    if (options.reconnect) {
      this.reconnect = {
        maxRetries: options.reconnect.maxRetries ?? DEFAULT_RECONNECT_MAX_RETRIES,
        initialDelayMs: options.reconnect.initialDelayMs ?? DEFAULT_RECONNECT_INITIAL_DELAY_MS,
        maxDelayMs: options.reconnect.maxDelayMs ?? DEFAULT_RECONNECT_MAX_DELAY_MS,
        multiplier: options.reconnect.multiplier ?? DEFAULT_RECONNECT_MULTIPLIER
      }
    }
  }

  /** Whether the pool has been closed. */
  get isClosed(): boolean {
    return this.closed
  }

  /** All known brokers discovered from metadata. */
  get brokers(): ReadonlyMap<number, BrokerInfo> {
    return this.knownBrokers
  }

  /**
   * Connect to the cluster by discovering brokers from metadata.
   *
   * Contacts one of the bootstrap brokers to learn about all brokers in the
   * cluster. Must be called before {@link getConnection}.
   *
   * @throws {KafkaConnectionError} If the pool is already closed or no brokers are reachable.
   */
  async connect(): Promise<void> {
    if (this.closed) {
      throw new KafkaConnectionError("connection pool has been closed", { retriable: false })
    }

    const discovered = await discoverBrokers(this.bootstrapBrokers, this.socketFactory, {
      clientId: this.clientId,
      tls: this.tls,
      connectTimeoutMs: this.connectTimeoutMs,
      requestTimeoutMs: this.requestTimeoutMs
    })

    for (const broker of discovered) {
      this.knownBrokers.set(broker.nodeId, broker)
    }
  }

  /**
   * Refresh broker metadata by re-running discovery.
   *
   * Useful when the cluster topology changes (e.g. brokers added/removed).
   * Uses discovered brokers first, falling back to bootstrap addresses.
   *
   * @throws {KafkaConnectionError} If the pool is closed or no brokers are reachable.
   */
  async refreshMetadata(): Promise<void> {
    if (this.closed) {
      throw new KafkaConnectionError("connection pool has been closed", { retriable: false })
    }

    // Try discovered brokers first, fall back to bootstrap
    const addresses: BrokerAddress[] = []
    for (const broker of this.knownBrokers.values()) {
      addresses.push({ host: broker.host, port: broker.port })
    }
    for (const broker of this.bootstrapBrokers) {
      const key = brokerKey(broker.host, broker.port)
      if (!addresses.some((a) => brokerKey(a.host, a.port) === key)) {
        addresses.push(broker)
      }
    }

    const discovered = await discoverBrokers(addresses, this.socketFactory, {
      clientId: this.clientId,
      tls: this.tls,
      connectTimeoutMs: this.connectTimeoutMs,
      requestTimeoutMs: this.requestTimeoutMs
    })

    this.knownBrokers.clear()
    for (const broker of discovered) {
      this.knownBrokers.set(broker.nodeId, broker)
    }
  }

  /**
   * Get a connection to a specific broker.
   *
   * Returns an existing idle connection if available, or creates a new one
   * up to {@link ConnectionPoolOptions.maxConnectionsPerBroker}. If all
   * connections are in use and the limit is reached, waits for one to become
   * available.
   *
   * @param host - Broker hostname.
   * @param port - Broker port.
   * @returns A connected {@link KafkaConnection}.
   * @throws {KafkaConnectionError} If the pool is closed or connection fails.
   */
  async getConnection(host: string, port: number): Promise<KafkaConnection> {
    if (this.closed) {
      throw new KafkaConnectionError("connection pool has been closed", { retriable: false })
    }

    const key = brokerKey(host, port)
    let entry = this.pools.get(key)

    if (!entry) {
      entry = { idle: [], active: new Set(), waiters: [] }
      this.pools.set(key, entry)
    }

    // Try to reuse an idle connection
    while (entry.idle.length > 0) {
      const conn = entry.idle.pop()
      if (conn === undefined) {
        continue
      }
      if (conn.connected) {
        entry.active.add(conn)
        return conn
      }
      // Connection was closed externally; discard it
    }

    // Create a new connection if under the limit
    const totalConnections = entry.idle.length + entry.active.size
    if (totalConnections < this.maxConnectionsPerBroker) {
      const conn = this.createConnection(host, port)
      await conn.connect()
      entry.active.add(conn)
      return conn
    }

    // At capacity — wait for a connection to be released
    return new Promise<KafkaConnection>((resolve, reject) => {
      if (this.closed) {
        reject(new KafkaConnectionError("connection pool has been closed", { retriable: false }))
        return
      }
      entry.waiters.push({ resolve, reject })
    })
  }

  /**
   * Get a connection to a broker by its node ID.
   *
   * Looks up the broker address from the metadata cache and delegates to
   * {@link getConnection}.
   *
   * @param nodeId - Broker node ID.
   * @returns A connected {@link KafkaConnection}.
   * @throws {KafkaConnectionError} If the node ID is unknown or connection fails.
   */
  async getConnectionByNodeId(nodeId: number): Promise<KafkaConnection> {
    const broker = this.knownBrokers.get(nodeId)
    if (!broker) {
      throw new KafkaConnectionError(`unknown broker node id ${String(nodeId)}`, {
        retriable: false
      })
    }
    return this.getConnection(broker.host, broker.port)
  }

  /**
   * Release a connection back to the pool.
   *
   * Makes the connection available for reuse. If waiters are pending, the
   * connection is given to the first waiter instead of returning to the
   * idle list.
   *
   * @param conn - The connection to release.
   */
  releaseConnection(conn: KafkaConnection): void {
    const key = conn.broker
    const entry = this.pools.get(key)
    if (!entry) {
      return
    }

    entry.active.delete(conn)

    if (!conn.connected) {
      // Connection is dead — wake a waiter so it can create a new one
      this.wakeWaiter(entry, conn.host, conn.port)
      // Optionally start background reconnection to replenish the pool
      if (this.reconnect && !this.closed) {
        this.backgroundReconnect(entry, conn.host, conn.port)
      }
      return
    }

    // If someone is waiting, give them this connection immediately
    if (entry.waiters.length > 0) {
      const waiter = entry.waiters.shift()
      if (waiter) {
        entry.active.add(conn)
        waiter.resolve(conn)
      }
      return
    }

    entry.idle.push(conn)
  }

  /**
   * Close all connections and shut down the pool.
   *
   * Rejects any pending waiters and closes all idle and active connections.
   */
  async close(): Promise<void> {
    if (this.closed) {
      return
    }
    this.closed = true

    const closePromises: Promise<void>[] = []

    for (const [, entry] of this.pools) {
      // Reject all waiters
      for (const waiter of entry.waiters) {
        waiter.reject(new KafkaConnectionError("connection pool closed", { retriable: false }))
      }
      entry.waiters.length = 0

      // Close all connections
      for (const conn of entry.idle) {
        closePromises.push(conn.close())
      }
      for (const conn of entry.active) {
        closePromises.push(conn.close())
      }
    }

    await Promise.allSettled(closePromises)
    this.pools.clear()
    this.knownBrokers.clear()
  }

  /**
   * Get the number of connections (idle + active) for a specific broker.
   */
  connectionCount(host: string, port: number): number {
    const entry = this.pools.get(brokerKey(host, port))
    if (!entry) {
      return 0
    }
    return entry.idle.length + entry.active.size
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

  private createConnection(host: string, port: number): KafkaConnection {
    return new KafkaConnection({
      host,
      port,
      socketFactory: this.socketFactory,
      clientId: this.clientId,
      tls: this.tls,
      connectTimeoutMs: this.connectTimeoutMs,
      requestTimeoutMs: this.requestTimeoutMs
    })
  }

  private wakeWaiter(entry: PoolEntry, host: string, port: number): void {
    if (entry.waiters.length === 0) {
      return
    }

    const waiter = entry.waiters.shift()
    if (!waiter) {
      return
    }
    const conn = this.createConnection(host, port)
    conn.connect().then(
      () => {
        entry.active.add(conn)
        waiter.resolve(conn)
      },
      (error: unknown) => {
        waiter.reject(
          error instanceof Error
            ? error
            : new KafkaConnectionError(String(error), { broker: brokerKey(host, port) })
        )
      }
    )
  }

  /**
   * Attempt to create a fresh connection in the background with exponential backoff.
   * The new connection is added to the idle list on success. If all retries fail
   * the attempt is silently abandoned — the next getConnection() call will create
   * a fresh connection as usual.
   */
  private backgroundReconnect(entry: PoolEntry, host: string, port: number): void {
    const key = brokerKey(host, port)

    // Avoid duplicate reconnect loops for the same broker
    if (this.reconnecting.has(key)) {
      return
    }
    this.reconnecting.add(key)

    const strategy = this.reconnect
    if (!strategy) {
      return
    }
    let attempt = 0

    const tryReconnect = (): void => {
      if (this.closed) {
        this.reconnecting.delete(key)
        return
      }

      attempt++
      const conn = this.createConnection(host, port)
      conn.connect().then(
        () => {
          this.reconnecting.delete(key)
          if (this.closed) {
            // Pool closed while we were reconnecting
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            conn.close().catch(() => {})
            return
          }

          // Give to a waiter if one is pending, otherwise add to idle
          if (entry.waiters.length > 0) {
            const waiter = entry.waiters.shift()
            if (waiter) {
              entry.active.add(conn)
              waiter.resolve(conn)
              return
            }
          }
          entry.idle.push(conn)
        },
        () => {
          if (this.closed || attempt >= strategy.maxRetries) {
            this.reconnecting.delete(key)
            return
          }

          const delay = Math.min(
            strategy.initialDelayMs * strategy.multiplier ** (attempt - 1),
            strategy.maxDelayMs
          )
          setTimeout(tryReconnect, delay)
        }
      )
    }

    // Start first attempt after initialDelayMs
    setTimeout(tryReconnect, strategy.initialDelayMs)
  }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

type PoolEntry = {
  idle: KafkaConnection[]
  active: Set<KafkaConnection>
  waiters: ConnectionWaiter[]
}

type ConnectionWaiter = {
  resolve: (conn: KafkaConnection) => void
  reject: (error: Error) => void
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function brokerKey(host: string, port: number): string {
  return `${host}:${String(port)}`
}
