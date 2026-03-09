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

// Connection
export type { ConnectionOptions } from "./connection.js"
export { KafkaConnection } from "./connection.js"
