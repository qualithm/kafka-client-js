/**
 * Native Apache Kafka client for JavaScript and TypeScript runtimes.
 *
 * @packageDocumentation
 */

// Result types
export type {
  DecodeError,
  DecodeErrorCode,
  DecodeFailure,
  DecodeResult,
  DecodeSuccess
} from "./result.js"
export { decodeFailure, decodeSuccess } from "./result.js"

// Errors
export {
  KafkaConfigError,
  KafkaConnectionError,
  KafkaError,
  KafkaProtocolError,
  KafkaTimeoutError
} from "./errors.js"

// Configuration
export type { BrokerAddress, KafkaConfig, SaslConfig, SaslMechanism, TlsConfig } from "./config.js"
export { parseBrokerAddress } from "./config.js"

// Messages
export type {
  ConsumerRecord,
  Message,
  MessageHeader,
  Offset,
  ProduceResult,
  TopicPartition
} from "./messages.js"

// Protocol
export type { ApiVersionRange, FlexibleVersionThreshold } from "./api-keys.js"
export {
  ApiKey,
  CLIENT_API_VERSIONS,
  FLEXIBLE_VERSION_THRESHOLDS,
  isFlexibleVersion,
  negotiateVersion
} from "./api-keys.js"

// Binary codec
export type { TaggedField } from "./binary-reader.js"
export { BinaryReader } from "./binary-reader.js"
export { BinaryWriter } from "./binary-writer.js"

// Protocol framing
export type {
  RequestHeader,
  RequestHeaderVersion,
  ResponseHeader,
  ResponseHeaderVersion
} from "./protocol-framing.js"
export {
  decodeResponseHeader,
  encodeRequestHeader,
  frameRequest,
  readResponseFrame,
  requestHeaderVersion,
  responseHeaderVersion
} from "./protocol-framing.js"

// API messages
export type { ApiVersionEntry, ApiVersionsRequest, ApiVersionsResponse } from "./api-versions.js"
export {
  apiVersionsToMap,
  buildApiVersionsRequest,
  decodeApiVersionsResponse,
  encodeApiVersionsRequest
} from "./api-versions.js"
export type {
  Coordinator,
  FindCoordinatorRequest,
  FindCoordinatorResponse
} from "./find-coordinator.js"
export {
  buildFindCoordinatorRequest,
  CoordinatorType,
  decodeFindCoordinatorResponse,
  encodeFindCoordinatorRequest
} from "./find-coordinator.js"
export type {
  ListOffsetsPartitionRequest,
  ListOffsetsPartitionResponse,
  ListOffsetsRequest,
  ListOffsetsResponse,
  ListOffsetsTopicRequest,
  ListOffsetsTopicResponse
} from "./list-offsets.js"
export {
  buildListOffsetsRequest,
  decodeListOffsetsResponse,
  encodeListOffsetsRequest,
  IsolationLevel,
  OffsetTimestamp
} from "./list-offsets.js"
export type {
  MetadataBroker,
  MetadataPartition,
  MetadataRequest,
  MetadataRequestTopic,
  MetadataResponse,
  MetadataTopic
} from "./metadata.js"
export { buildMetadataRequest, decodeMetadataResponse, encodeMetadataRequest } from "./metadata.js"

// Fetch API
export type {
  FetchAbortedTransaction,
  FetchPartitionRequest,
  FetchPartitionResponse,
  FetchRequest,
  FetchResponse,
  FetchTopicRequest,
  FetchTopicResponse,
  ForgottenTopic
} from "./fetch.js"
export {
  buildFetchRequest,
  decodeFetchResponse,
  encodeFetchRequest,
  FetchIsolationLevel
} from "./fetch.js"

// Produce API
export type {
  ProducePartitionData,
  ProducePartitionResponse,
  ProduceRecordError,
  ProduceRequest,
  ProduceResponse,
  ProduceTopicData,
  ProduceTopicResponse
} from "./produce.js"
export {
  Acks,
  buildProduceRequest,
  decodeProduceResponse,
  encodeProduceRequest
} from "./produce.js"

// InitProducerId API
export type { InitProducerIdRequest, InitProducerIdResponse } from "./init-producer-id.js"
export {
  buildInitProducerIdRequest,
  decodeInitProducerIdResponse,
  encodeInitProducerIdRequest
} from "./init-producer-id.js"

// Record batches
export type {
  CompressionProvider,
  Record,
  RecordBatch,
  RecordBatchAttributes,
  RecordBatchOptions
} from "./record-batch.js"
export {
  buildRecordBatch,
  CompressionCodec,
  crc32c,
  createRecord,
  decodeAttributes,
  decodeRecordBatch,
  encodeAttributes,
  encodeRecordBatch,
  hasCompressionProvider,
  RECORD_BATCH_HEADER_OVERHEAD,
  RECORD_BATCH_MAGIC,
  RECORD_BATCH_METADATA_SIZE,
  registerCompressionProvider,
  TimestampType
} from "./record-batch.js"

// Compression providers
export {
  createLz4Provider,
  createSnappyProvider,
  createZstdProvider,
  deflateProvider,
  gzipProvider,
  type Lz4Codec,
  type SnappyCodec,
  type ZstdCodec
} from "./compression.js"

// Socket adapter
export type { KafkaSocket, SocketConnectOptions, SocketFactory } from "./socket.js"

// Bun socket adapter
export { createBunSocketFactory } from "./bun-socket.js"

// Node.js socket adapter
export { createNodeSocketFactory } from "./node-socket.js"

// Deno socket adapter
export { createDenoSocketFactory } from "./deno-socket.js"

// Connection
export type { ConnectionOptions } from "./connection.js"
export { KafkaConnection } from "./connection.js"

// Broker pool & discovery
export type { BrokerInfo, ConnectionPoolOptions } from "./broker-pool.js"
export { ConnectionPool, discoverBrokers } from "./broker-pool.js"

// Kafka client
export type {
  KafkaConsumerOptions,
  KafkaOptions,
  KafkaProducerOptions,
  KafkaState
} from "./kafka.js"
export { createKafka, Kafka } from "./kafka.js"

// Producer
export type { BatchConfig, Partitioner, ProducerOptions, RetryConfig } from "./producer.js"
export {
  createProducer,
  defaultPartitioner,
  KafkaProducer,
  roundRobinPartitioner
} from "./producer.js"

// Consumer
export type {
  AssignedPartition,
  ConsumerOptions,
  ConsumerRetryConfig,
  RebalanceListener
} from "./consumer.js"
export { createConsumer, KafkaConsumer, OffsetResetStrategy } from "./consumer.js"

// OffsetCommit API
export type {
  OffsetCommitPartitionRequest,
  OffsetCommitPartitionResponse,
  OffsetCommitRequest,
  OffsetCommitResponse,
  OffsetCommitTopicRequest,
  OffsetCommitTopicResponse
} from "./offset-commit.js"
export {
  buildOffsetCommitRequest,
  decodeOffsetCommitResponse,
  encodeOffsetCommitRequest
} from "./offset-commit.js"

// OffsetFetch API
export type {
  OffsetFetchPartitionResponse,
  OffsetFetchRequest,
  OffsetFetchResponse,
  OffsetFetchTopicRequest,
  OffsetFetchTopicResponse
} from "./offset-fetch.js"
export {
  buildOffsetFetchRequest,
  decodeOffsetFetchResponse,
  encodeOffsetFetchRequest
} from "./offset-fetch.js"

// JoinGroup API
export type {
  JoinGroupMember,
  JoinGroupProtocol,
  JoinGroupRequest,
  JoinGroupResponse
} from "./join-group.js"
export {
  buildJoinGroupRequest,
  decodeJoinGroupResponse,
  encodeJoinGroupRequest
} from "./join-group.js"

// SyncGroup API
export type { SyncGroupAssignment, SyncGroupRequest, SyncGroupResponse } from "./sync-group.js"
export {
  buildSyncGroupRequest,
  decodeSyncGroupResponse,
  encodeSyncGroupRequest
} from "./sync-group.js"

// Heartbeat API
export type { HeartbeatRequest, HeartbeatResponse } from "./heartbeat.js"
export {
  buildHeartbeatRequest,
  decodeHeartbeatResponse,
  encodeHeartbeatRequest
} from "./heartbeat.js"

// LeaveGroup API
export type {
  LeaveGroupMemberRequest,
  LeaveGroupMemberResponse,
  LeaveGroupRequest,
  LeaveGroupResponse
} from "./leave-group.js"
export {
  buildLeaveGroupRequest,
  decodeLeaveGroupResponse,
  encodeLeaveGroupRequest
} from "./leave-group.js"
