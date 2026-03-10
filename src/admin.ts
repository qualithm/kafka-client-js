/**
 * Kafka admin client for topic and cluster management.
 *
 * Provides administrative operations: creating/deleting topics, adding
 * partitions, describing/altering configuration, and listing topics.
 *
 * @packageDocumentation
 */

import {
  type AlterConfigsRequest,
  type AlterConfigsResourceResponse,
  decodeAlterConfigsResponse,
  encodeAlterConfigsRequest
} from "./alter-configs.js"
import { ApiKey, negotiateVersion } from "./api-keys.js"
import { apiVersionsToMap, decodeApiVersionsResponse } from "./api-versions.js"
import type { ConnectionPool } from "./broker-pool.js"
import type { KafkaConnection } from "./connection.js"
import {
  type CreatePartitionsRequest,
  type CreatePartitionsTopicResponse,
  decodeCreatePartitionsResponse,
  encodeCreatePartitionsRequest
} from "./create-partitions.js"
import {
  type CreateTopicsRequest,
  type CreateTopicsTopicResponse,
  decodeCreateTopicsResponse,
  encodeCreateTopicsRequest
} from "./create-topics.js"
import {
  decodeDeleteTopicsResponse,
  type DeleteTopicsRequest,
  type DeleteTopicsTopicResponse,
  encodeDeleteTopicsRequest
} from "./delete-topics.js"
import {
  decodeDescribeConfigsResponse,
  type DescribeConfigsRequest,
  type DescribeConfigsResourceResponse,
  encodeDescribeConfigsRequest
} from "./describe-configs.js"
import { KafkaConnectionError, KafkaError } from "./errors.js"
import {
  decodeMetadataResponse,
  encodeMetadataRequest,
  type MetadataRequest,
  type MetadataTopic
} from "./metadata.js"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Retry configuration for the admin client.
 */
export type AdminRetryConfig = {
  /**
   * Maximum number of retry attempts for retriable errors.
   * @default 3
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
 * Options for creating a {@link KafkaAdmin}.
 */
export type AdminOptions = {
  /** The connection pool to use. */
  readonly connectionPool: ConnectionPool
  /** Retry configuration for retriable errors. */
  readonly retry?: AdminRetryConfig
}

/**
 * Topic info returned by {@link KafkaAdmin.listTopics}.
 */
export type TopicInfo = {
  /** Topic name. */
  readonly name: string
  /** Whether this is an internal topic. */
  readonly isInternal: boolean
  /** Number of partitions. */
  readonly partitionCount: number
}

// ---------------------------------------------------------------------------
// KafkaAdmin
// ---------------------------------------------------------------------------

/**
 * Kafka admin client.
 *
 * Performs administrative operations on topics and cluster configuration.
 * Operates through the {@link ConnectionPool} and negotiates API versions
 * automatically.
 *
 * @example
 * ```ts
 * const admin = new KafkaAdmin({ connectionPool: kafka.connectionPool })
 * await admin.createTopics({
 *   topics: [{ name: "my-topic", numPartitions: 3, replicationFactor: 1 }],
 *   timeoutMs: 30000,
 * })
 * const topics = await admin.listTopics()
 * await admin.close()
 * ```
 */
export class KafkaAdmin {
  private readonly pool: ConnectionPool
  private readonly maxRetries: number
  private readonly initialRetryMs: number
  private readonly maxRetryMs: number
  private readonly retryMultiplier: number
  private closed = false

  constructor(options: AdminOptions) {
    this.pool = options.connectionPool
    this.maxRetries = options.retry?.maxRetries ?? 3
    this.initialRetryMs = options.retry?.initialRetryMs ?? 100
    this.maxRetryMs = options.retry?.maxRetryMs ?? 30_000
    this.retryMultiplier = options.retry?.multiplier ?? 2
  }

  /**
   * Create one or more topics.
   *
   * @param request - The CreateTopics request payload.
   * @returns Per-topic results.
   * @throws {KafkaProtocolError} If the broker returns a non-zero error code.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async createTopics(request: CreateTopicsRequest): Promise<readonly CreateTopicsTopicResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.CreateTopics)
      try {
        const responseReader = await conn.send(ApiKey.CreateTopics, apiVersion, (writer) => {
          encodeCreateTopicsRequest(writer, request, apiVersion)
        })

        const result = decodeCreateTopicsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode create topics response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Delete one or more topics.
   *
   * @param request - The DeleteTopics request payload.
   * @returns Per-topic results.
   * @throws {KafkaProtocolError} If the broker returns a non-zero error code.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async deleteTopics(request: DeleteTopicsRequest): Promise<readonly DeleteTopicsTopicResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.DeleteTopics)
      try {
        const responseReader = await conn.send(ApiKey.DeleteTopics, apiVersion, (writer) => {
          encodeDeleteTopicsRequest(writer, request, apiVersion)
        })

        const result = decodeDeleteTopicsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode delete topics response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Create additional partitions for existing topics.
   *
   * @param request - The CreatePartitions request payload.
   * @returns Per-topic results.
   * @throws {KafkaProtocolError} If the broker returns a non-zero error code.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async createPartitions(
    request: CreatePartitionsRequest
  ): Promise<readonly CreatePartitionsTopicResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.CreatePartitions)
      try {
        const responseReader = await conn.send(ApiKey.CreatePartitions, apiVersion, (writer) => {
          encodeCreatePartitionsRequest(writer, request, apiVersion)
        })

        const result = decodeCreatePartitionsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode create partitions response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Describe configuration for specified resources.
   *
   * @param request - The DescribeConfigs request payload.
   * @returns Per-resource config results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async describeConfigs(
    request: DescribeConfigsRequest
  ): Promise<readonly DescribeConfigsResourceResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.DescribeConfigs)
      try {
        const responseReader = await conn.send(ApiKey.DescribeConfigs, apiVersion, (writer) => {
          encodeDescribeConfigsRequest(writer, request, apiVersion)
        })

        const result = decodeDescribeConfigsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode describe configs response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.resources
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Alter configuration for specified resources.
   *
   * This is a non-incremental set: all config keys must be provided.
   * Omitted keys revert to their defaults.
   *
   * @param request - The AlterConfigs request payload.
   * @returns Per-resource results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async alterConfigs(
    request: AlterConfigsRequest
  ): Promise<readonly AlterConfigsResourceResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.AlterConfigs)
      try {
        const responseReader = await conn.send(ApiKey.AlterConfigs, apiVersion, (writer) => {
          encodeAlterConfigsRequest(writer, request, apiVersion)
        })

        const result = decodeAlterConfigsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode alter configs response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.resources
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * List all topics in the cluster.
   *
   * Uses the Metadata API with a null topics list to discover all topics.
   * Internal topics (e.g. `__consumer_offsets`) are included.
   *
   * @returns Array of topic info objects.
   * @throws {KafkaConnectionError} If connection or decoding fails.
   */
  async listTopics(): Promise<readonly TopicInfo[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.Metadata)
      try {
        const metadataRequest: MetadataRequest = { topics: null }
        const responseReader = await conn.send(ApiKey.Metadata, apiVersion, (writer) => {
          encodeMetadataRequest(writer, metadataRequest, apiVersion)
        })

        const result = decodeMetadataResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode metadata response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics.map(toTopicInfo)
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Describe specific topics using the Metadata API.
   *
   * @param topicNames - Topic names to describe.
   * @returns Topic metadata including partitions and leaders.
   * @throws {KafkaConnectionError} If connection or decoding fails.
   */
  async describeTopics(topicNames: readonly string[]): Promise<readonly MetadataTopic[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.Metadata)
      try {
        const metadataRequest: MetadataRequest = {
          topics: topicNames.map((name) => ({ name }))
        }
        const responseReader = await conn.send(ApiKey.Metadata, apiVersion, (writer) => {
          encodeMetadataRequest(writer, metadataRequest, apiVersion)
        })

        const result = decodeMetadataResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode metadata response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Close the admin client.
   *
   * After closing, no more operations can be performed.
   */
  close(): void {
    this.closed = true
  }

  // -------------------------------------------------------------------------
  // Internal — connection management
  // -------------------------------------------------------------------------

  /**
   * Negotiate the API version for a given API key on a connection.
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

    if (apiVersionsResult.value.errorCode !== 0) {
      throw new KafkaConnectionError(
        `api versions request failed with error code ${String(apiVersionsResult.value.errorCode)}`,
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
      throw new KafkaConnectionError(`no compatible api version for key ${String(apiKey)}`, {
        broker: conn.broker,
        retriable: false
      })
    }

    return version
  }

  /**
   * Get a connection to a controller-eligible broker with negotiated API version.
   *
   * Admin write operations (CreateTopics, DeleteTopics, etc.) must be sent to
   * the cluster controller. We use any known broker as the entry point.
   */
  private async getControllerConnection(
    apiKey: ApiKey
  ): Promise<{ conn: KafkaConnection; apiVersion: number }> {
    this.ensureOpen()

    const { brokers } = this.pool
    if (brokers.size === 0) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const firstBroker = brokers.values().next().value
    if (!firstBroker) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const conn = await this.pool.getConnectionByNodeId(firstBroker.nodeId)
    const apiVersion = await this.negotiateApiVersion(conn, apiKey)
    return { conn, apiVersion }
  }

  /**
   * Get a connection to any broker with negotiated API version.
   *
   * Read-only operations (DescribeConfigs, Metadata) can go to any broker.
   */
  private async getAnyBrokerConnection(
    apiKey: ApiKey
  ): Promise<{ conn: KafkaConnection; apiVersion: number }> {
    this.ensureOpen()

    const { brokers } = this.pool
    if (brokers.size === 0) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const firstBroker = brokers.values().next().value
    if (!firstBroker) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const conn = await this.pool.getConnectionByNodeId(firstBroker.nodeId)
    const apiVersion = await this.negotiateApiVersion(conn, apiKey)
    return { conn, apiVersion }
  }

  // -------------------------------------------------------------------------
  // Internal — retry & lifecycle
  // -------------------------------------------------------------------------

  private async withRetry<T>(fn: () => Promise<T>): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      try {
        return await fn()
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

  private ensureOpen(): void {
    if (this.closed) {
      throw new KafkaConnectionError("admin client is closed", { retriable: false })
    }
  }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/**
 * Create a new Kafka admin client.
 *
 * @param options - Admin client options.
 * @returns A new {@link KafkaAdmin} instance.
 */
export function createAdmin(options: AdminOptions): KafkaAdmin {
  return new KafkaAdmin(options)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function toTopicInfo(topic: MetadataTopic): TopicInfo {
  return {
    name: topic.name ?? "",
    isInternal: topic.isInternal,
    partitionCount: topic.partitions.length
  }
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
