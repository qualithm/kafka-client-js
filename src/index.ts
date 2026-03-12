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
export type { ApiVersionRange, FlexibleVersionThreshold } from "./codec/api-keys.js"
export {
  ApiKey,
  CLIENT_API_VERSIONS,
  FLEXIBLE_VERSION_THRESHOLDS,
  isFlexibleVersion,
  negotiateVersion
} from "./codec/api-keys.js"

// Binary codec
export type { TaggedField } from "./codec/binary-reader.js"
export { BinaryReader } from "./codec/binary-reader.js"
export { BinaryWriter } from "./codec/binary-writer.js"

// Protocol framing
export type {
  RequestHeader,
  RequestHeaderVersion,
  ResponseHeader,
  ResponseHeaderVersion
} from "./codec/protocol-framing.js"
export {
  decodeResponseHeader,
  encodeRequestHeader,
  frameRequest,
  readResponseFrame,
  requestHeaderVersion,
  responseHeaderVersion
} from "./codec/protocol-framing.js"

// API messages
export type {
  ApiVersionEntry,
  ApiVersionsRequest,
  ApiVersionsResponse
} from "./protocol/api-versions.js"
export {
  apiVersionsToMap,
  buildApiVersionsRequest,
  decodeApiVersionsResponse,
  encodeApiVersionsRequest
} from "./protocol/api-versions.js"
export type {
  Coordinator,
  FindCoordinatorRequest,
  FindCoordinatorResponse
} from "./protocol/find-coordinator.js"
export {
  buildFindCoordinatorRequest,
  CoordinatorType,
  decodeFindCoordinatorResponse,
  encodeFindCoordinatorRequest
} from "./protocol/find-coordinator.js"
export type {
  ListOffsetsPartitionRequest,
  ListOffsetsPartitionResponse,
  ListOffsetsRequest,
  ListOffsetsResponse,
  ListOffsetsTopicRequest,
  ListOffsetsTopicResponse
} from "./protocol/list-offsets.js"
export {
  buildListOffsetsRequest,
  decodeListOffsetsResponse,
  encodeListOffsetsRequest,
  IsolationLevel,
  OffsetTimestamp
} from "./protocol/list-offsets.js"
export type {
  MetadataBroker,
  MetadataPartition,
  MetadataRequest,
  MetadataRequestTopic,
  MetadataResponse,
  MetadataTopic
} from "./protocol/metadata.js"
export {
  buildMetadataRequest,
  decodeMetadataResponse,
  encodeMetadataRequest
} from "./protocol/metadata.js"

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
} from "./protocol/fetch.js"
export {
  buildFetchRequest,
  decodeFetchResponse,
  encodeFetchRequest,
  FetchIsolationLevel
} from "./protocol/fetch.js"

// Produce API
export type {
  ProducePartitionData,
  ProducePartitionResponse,
  ProduceRecordError,
  ProduceRequest,
  ProduceResponse,
  ProduceTopicData,
  ProduceTopicResponse
} from "./protocol/produce.js"
export {
  Acks,
  buildProduceRequest,
  decodeProduceResponse,
  encodeProduceRequest
} from "./protocol/produce.js"

// InitProducerId API
export type { InitProducerIdRequest, InitProducerIdResponse } from "./protocol/init-producer-id.js"
export {
  buildInitProducerIdRequest,
  decodeInitProducerIdResponse,
  encodeInitProducerIdRequest
} from "./protocol/init-producer-id.js"

// Record batches
export type {
  CompressionProvider,
  Record,
  RecordBatch,
  RecordBatchAttributes,
  RecordBatchOptions
} from "./codec/record-batch.js"
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
} from "./codec/record-batch.js"

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
} from "./codec/compression.js"

// Socket adapter
export type { KafkaSocket, SocketConnectOptions, SocketFactory } from "./network/socket.js"

// Bun socket adapter
export { createBunSocketFactory } from "./network/bun-socket.js"

// Node.js socket adapter
export { createNodeSocketFactory } from "./network/node-socket.js"

// Deno socket adapter
export { createDenoSocketFactory } from "./network/deno-socket.js"

// Connection
export type { ConnectionOptions } from "./network/connection.js"
export { KafkaConnection } from "./network/connection.js"

// Broker pool & discovery
export type { BrokerInfo, ConnectionPoolOptions, ReconnectStrategy } from "./network/broker-pool.js"
export { ConnectionPool, discoverBrokers } from "./network/broker-pool.js"

// Kafka client
export type {
  KafkaAdminOptions,
  KafkaConsumerOptions,
  KafkaOptions,
  KafkaProducerOptions,
  KafkaState
} from "./client/kafka.js"
export { createKafka, Kafka } from "./client/kafka.js"

// Producer
export type {
  BatchConfig,
  Partitioner,
  ProducerOptions,
  RetryConfig,
  TopicPartitionOffset
} from "./client/producer.js"
export {
  createProducer,
  defaultPartitioner,
  KafkaProducer,
  roundRobinPartitioner
} from "./client/producer.js"

// Partition assignors
export type { MemberAssignment, MemberSubscription, PartitionAssignor } from "./client/assignors.js"
export {
  createCooperativeStickyAssignor,
  rangeAssignor,
  roundRobinAssignor
} from "./client/assignors.js"

// Consumer
export type {
  AssignedPartition,
  ConsumerOptions,
  ConsumerRetryConfig,
  RebalanceListener
} from "./client/consumer.js"
export { createConsumer, KafkaConsumer, OffsetResetStrategy } from "./client/consumer.js"

// OffsetCommit API
export type {
  OffsetCommitPartitionRequest,
  OffsetCommitPartitionResponse,
  OffsetCommitRequest,
  OffsetCommitResponse,
  OffsetCommitTopicRequest,
  OffsetCommitTopicResponse
} from "./protocol/offset-commit.js"
export {
  buildOffsetCommitRequest,
  decodeOffsetCommitResponse,
  encodeOffsetCommitRequest
} from "./protocol/offset-commit.js"

// OffsetFetch API
export type {
  OffsetFetchPartitionResponse,
  OffsetFetchRequest,
  OffsetFetchResponse,
  OffsetFetchTopicRequest,
  OffsetFetchTopicResponse
} from "./protocol/offset-fetch.js"
export {
  buildOffsetFetchRequest,
  decodeOffsetFetchResponse,
  encodeOffsetFetchRequest
} from "./protocol/offset-fetch.js"

// JoinGroup API
export type {
  JoinGroupMember,
  JoinGroupProtocol,
  JoinGroupRequest,
  JoinGroupResponse
} from "./protocol/join-group.js"
export {
  buildJoinGroupRequest,
  decodeJoinGroupResponse,
  encodeJoinGroupRequest
} from "./protocol/join-group.js"

// SyncGroup API
export type {
  SyncGroupAssignment,
  SyncGroupRequest,
  SyncGroupResponse
} from "./protocol/sync-group.js"
export {
  buildSyncGroupRequest,
  decodeSyncGroupResponse,
  encodeSyncGroupRequest
} from "./protocol/sync-group.js"

// Heartbeat API
export type { HeartbeatRequest, HeartbeatResponse } from "./protocol/heartbeat.js"
export {
  buildHeartbeatRequest,
  decodeHeartbeatResponse,
  encodeHeartbeatRequest
} from "./protocol/heartbeat.js"

// LeaveGroup API
export type {
  LeaveGroupMemberRequest,
  LeaveGroupMemberResponse,
  LeaveGroupRequest,
  LeaveGroupResponse
} from "./protocol/leave-group.js"
export {
  buildLeaveGroupRequest,
  decodeLeaveGroupResponse,
  encodeLeaveGroupRequest
} from "./protocol/leave-group.js"

// SaslHandshake API
export type { SaslHandshakeRequest, SaslHandshakeResponse } from "./protocol/sasl-handshake.js"
export {
  buildSaslHandshakeRequest,
  decodeSaslHandshakeResponse,
  encodeSaslHandshakeRequest
} from "./protocol/sasl-handshake.js"

// SaslAuthenticate API
export type {
  SaslAuthenticateRequest,
  SaslAuthenticateResponse
} from "./protocol/sasl-authenticate.js"
export {
  buildSaslAuthenticateRequest,
  decodeSaslAuthenticateResponse,
  encodeSaslAuthenticateRequest
} from "./protocol/sasl-authenticate.js"

// SASL mechanisms
export type { SaslAuthenticator, ScramAlgorithm } from "./network/sasl.js"
export {
  createPlainAuthenticator,
  createSaslAuthenticator,
  createScramAuthenticator
} from "./network/sasl.js"

// Admin client
export type { AdminOptions, AdminRetryConfig, TopicInfo } from "./client/admin.js"
export { createAdmin, KafkaAdmin } from "./client/admin.js"

// CreateTopics API
export type {
  CreateTopicRequest,
  CreateTopicsConfigEntry,
  CreateTopicsReplicaAssignment,
  CreateTopicsRequest,
  CreateTopicsResponse,
  CreateTopicsResponseConfigEntry,
  CreateTopicsTopicResponse
} from "./protocol/create-topics.js"
export {
  buildCreateTopicsRequest,
  decodeCreateTopicsResponse,
  encodeCreateTopicsRequest
} from "./protocol/create-topics.js"

// DeleteTopics API
export type {
  DeleteTopicsRequest,
  DeleteTopicsResponse,
  DeleteTopicState,
  DeleteTopicsTopicResponse
} from "./protocol/delete-topics.js"
export {
  buildDeleteTopicsRequest,
  decodeDeleteTopicsResponse,
  encodeDeleteTopicsRequest
} from "./protocol/delete-topics.js"

// CreatePartitions API
export type {
  CreatePartitionsAssignment,
  CreatePartitionsRequest,
  CreatePartitionsResponse,
  CreatePartitionsTopicRequest,
  CreatePartitionsTopicResponse
} from "./protocol/create-partitions.js"
export {
  buildCreatePartitionsRequest,
  decodeCreatePartitionsResponse,
  encodeCreatePartitionsRequest
} from "./protocol/create-partitions.js"

// DescribeConfigs API
export type {
  ConfigSynonym,
  DescribeConfigsEntry,
  DescribeConfigsRequest,
  DescribeConfigsResource,
  DescribeConfigsResourceResponse,
  DescribeConfigsResponse
} from "./protocol/describe-configs.js"
export {
  buildDescribeConfigsRequest,
  ConfigResourceType,
  decodeDescribeConfigsResponse,
  encodeDescribeConfigsRequest
} from "./protocol/describe-configs.js"

// AlterConfigs API
export type {
  AlterConfigsEntry,
  AlterConfigsRequest,
  AlterConfigsResource,
  AlterConfigsResourceResponse,
  AlterConfigsResponse
} from "./protocol/alter-configs.js"
export {
  buildAlterConfigsRequest,
  decodeAlterConfigsResponse,
  encodeAlterConfigsRequest
} from "./protocol/alter-configs.js"

// Serialization
export type {
  AsyncDeserializer,
  AsyncSerde,
  AsyncSerializer,
  Deserializer,
  Serde,
  Serializer
} from "./serialization/serialization.js"
export { jsonSerializer, stringSerializer } from "./serialization/serialization.js"

// Schema Registry
export type {
  RegisteredSchema,
  SchemaRegistryConfig,
  SchemaType,
  SubjectNameStrategy
} from "./serialization/schema-registry.js"
export {
  decodeWireFormatHeader,
  encodeWireFormatHeader,
  recordNameStrategy,
  SchemaRegistry,
  SchemaRegistryError,
  topicNameStrategy,
  topicRecordNameStrategy,
  WIRE_FORMAT_HEADER_SIZE,
  WIRE_FORMAT_MAGIC
} from "./serialization/schema-registry.js"

// Avro serializer
export type { AvroCodec, AvroSchema, AvroSerdeOptions } from "./serialization/avro-serializer.js"
export { createAvroSerde } from "./serialization/avro-serializer.js"

// Protobuf serializer
export type {
  ProtobufCodec,
  ProtobufSchema,
  ProtobufSerdeOptions
} from "./serialization/protobuf-serializer.js"
export { createProtobufSerde } from "./serialization/protobuf-serializer.js"

// AddPartitionsToTxn API
export type {
  AddPartitionsToTxnPartitionResult,
  AddPartitionsToTxnRequest,
  AddPartitionsToTxnResponse,
  AddPartitionsToTxnTopic,
  AddPartitionsToTxnTopicResult
} from "./protocol/add-partitions-to-txn.js"
export {
  buildAddPartitionsToTxnRequest,
  decodeAddPartitionsToTxnResponse,
  encodeAddPartitionsToTxnRequest
} from "./protocol/add-partitions-to-txn.js"

// AddOffsetsToTxn API
export type {
  AddOffsetsToTxnRequest,
  AddOffsetsToTxnResponse
} from "./protocol/add-offsets-to-txn.js"
export {
  buildAddOffsetsToTxnRequest,
  decodeAddOffsetsToTxnResponse,
  encodeAddOffsetsToTxnRequest
} from "./protocol/add-offsets-to-txn.js"

// EndTxn API
export type { EndTxnRequest, EndTxnResponse } from "./protocol/end-txn.js"
export {
  buildEndTxnRequest,
  decodeEndTxnResponse,
  encodeEndTxnRequest
} from "./protocol/end-txn.js"

// TxnOffsetCommit API
export type {
  TxnOffsetCommitPartitionRequest,
  TxnOffsetCommitPartitionResponse,
  TxnOffsetCommitRequest,
  TxnOffsetCommitResponse,
  TxnOffsetCommitTopicRequest,
  TxnOffsetCommitTopicResponse
} from "./protocol/txn-offset-commit.js"
export {
  buildTxnOffsetCommitRequest,
  decodeTxnOffsetCommitResponse,
  encodeTxnOffsetCommitRequest
} from "./protocol/txn-offset-commit.js"

// DescribeGroups API
export type {
  DescribeGroupsGroup,
  DescribeGroupsMember,
  DescribeGroupsRequest,
  DescribeGroupsResponse
} from "./protocol/describe-groups.js"
export {
  buildDescribeGroupsRequest,
  decodeDescribeGroupsResponse,
  encodeDescribeGroupsRequest
} from "./protocol/describe-groups.js"

// ListGroups API
export type {
  ListGroupsGroup,
  ListGroupsRequest,
  ListGroupsResponse
} from "./protocol/list-groups.js"
export {
  buildListGroupsRequest,
  decodeListGroupsResponse,
  encodeListGroupsRequest
} from "./protocol/list-groups.js"

// DeleteGroups API
export type {
  DeleteGroupsRequest,
  DeleteGroupsResponse,
  DeleteGroupsResult
} from "./protocol/delete-groups.js"
export {
  buildDeleteGroupsRequest,
  decodeDeleteGroupsResponse,
  encodeDeleteGroupsRequest
} from "./protocol/delete-groups.js"
