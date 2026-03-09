/**
 * Kafka producer with batching, partitioning, and retry support.
 *
 * The producer sends record batches to topic partitions via the Produce API.
 * It manages partition assignment, record batch encoding, and connection
 * routing through the {@link ConnectionPool}.
 *
 * @packageDocumentation
 */

import { ApiKey, negotiateVersion } from "./api-keys.js"
import { apiVersionsToMap, decodeApiVersionsResponse } from "./api-versions.js"
import type { ConnectionPool } from "./broker-pool.js"
import { KafkaConnectionError, KafkaError, KafkaProtocolError } from "./errors.js"
import type { Message, ProduceResult, TopicPartition } from "./messages.js"
import {
  decodeMetadataResponse,
  encodeMetadataRequest,
  type MetadataPartition,
  type MetadataTopic
} from "./metadata.js"
import {
  Acks,
  decodeProduceResponse,
  encodeProduceRequest,
  type ProduceRequest,
  type ProduceTopicData
} from "./produce.js"
import {
  buildRecordBatch,
  CompressionCodec,
  type CompressionCodec as CompressionCodecType,
  createRecord,
  encodeRecordBatch
} from "./record-batch.js"

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
}

/**
 * Cached topic metadata for partition routing.
 */
type TopicMetadata = {
  readonly partitions: readonly MetadataPartition[]
  readonly partitionLeaders: ReadonlyMap<number, number>
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
  private readonly topicMetadataCache = new Map<string, TopicMetadata>()
  private closed = false

  constructor(options: ProducerOptions) {
    this.pool = options.connectionPool
    this.acks = options.acks ?? Acks.All
    this.timeoutMs = options.timeoutMs ?? 30_000
    this.partitioner = options.partitioner ?? defaultPartitioner()
    this.compression = options.compression ?? CompressionCodec.NONE
    this.transactionalId = options.transactionalId ?? null

    // Idempotent or transactional producers must use acks = All
    if (
      (options.idempotent === true || typeof options.transactionalId === "string") &&
      this.acks !== Acks.All
    ) {
      this.acks = Acks.All
    }
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

    if (messages.length === 0) {
      return []
    }

    // Ensure we have metadata for this topic
    const metadata = await this.getTopicMetadata(topic)

    // Group messages by partition
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

    // Group partitions by leader broker
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

    // Send to each leader in parallel
    const sendPromises: Promise<ProduceResult[]>[] = []
    for (const [leaderId, partitions] of byLeader) {
      sendPromises.push(this.sendToLeader(topic, leaderId, partitions))
    }

    const results = await Promise.all(sendPromises)
    return results.flat()
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

    if (messages.length === 0) {
      return { topicPartition, baseOffset: -1n }
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
  }

  /**
   * Close the producer.
   *
   * After closing, no more messages can be sent.
   */
  async close(): Promise<void> {
    this.closed = true
    this.topicMetadataCache.clear()
    await Promise.resolve()
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

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

      const batch = buildRecordBatch(records, {
        compression: this.compression,
        baseTimestamp: messages[0].timestamp ?? BigInt(Date.now())
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
