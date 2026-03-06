/**
 * Core message and partition types for the Kafka client.
 *
 * Uses `Uint8Array` exclusively per locked decision #2.
 *
 * @packageDocumentation
 */

/**
 * A topic-partition pair identifying a specific partition within a topic.
 */
export type TopicPartition = {
  /** Topic name. */
  readonly topic: string
  /** Partition index (zero-based). */
  readonly partition: number
}

/**
 * Offset within a partition.
 */
export type Offset = {
  /** Topic-partition this offset belongs to. */
  readonly topicPartition: TopicPartition
  /** Numeric offset value. Use bigint to support the full int64 range. */
  readonly offset: bigint
  /** Optional metadata string (stored with committed offsets). */
  readonly metadata?: string
}

/**
 * A single Kafka message header (key-value pair).
 */
export type MessageHeader = {
  /** Header key. */
  readonly key: string
  /** Header value as raw bytes. `null` represents an absent value. */
  readonly value: Uint8Array | null
}

/**
 * A Kafka message to be produced or that has been consumed.
 *
 * Key and value use `Uint8Array` per locked decision #2.
 */
export type Message = {
  /** Optional message key for partitioning. `null` means no key. */
  readonly key: Uint8Array | null
  /** Message value (payload). `null` represents a tombstone. */
  readonly value: Uint8Array | null
  /** Optional message headers. */
  readonly headers?: readonly MessageHeader[]
  /** Timestamp in milliseconds since epoch. Set by producer or broker. */
  readonly timestamp?: bigint
}

/**
 * A consumed record includes the message plus its position in the log.
 */
export type ConsumerRecord = {
  /** The topic this record was consumed from. */
  readonly topic: string
  /** The partition this record was consumed from. */
  readonly partition: number
  /** The offset of this record in the partition. */
  readonly offset: bigint
  /** The message content. */
  readonly message: Message
  /** Leader epoch at the time of fetch, if available. */
  readonly leaderEpoch?: number
}

/**
 * Result of a produce operation for a single partition.
 */
export type ProduceResult = {
  /** The topic-partition the message was written to. */
  readonly topicPartition: TopicPartition
  /** The offset assigned to the first message in the batch. */
  readonly baseOffset: bigint
  /** Timestamp assigned by the broker (log append time), if applicable. */
  readonly timestamp?: bigint
}
