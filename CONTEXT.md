# CONTEXT.md

> **This is the single source of truth for this repository.**
>
> When CONTEXT.md conflicts with any other document (README, code comments), CONTEXT.md is correct.
> Update other documents to match, not the reverse.

---

## System Intent

Native Apache Kafka client for JavaScript and TypeScript runtimes. Implements the Kafka binary
protocol directly for producing, consuming, and administering Kafka clusters.

**Key capabilities:**

- Producer with batching, partitioning strategies, and retries
- Consumer with group coordination and offset management
- Admin client for topic and partition management
- SASL/SSL authentication support
- Pluggable serialization (JSON, Avro, Protobuf)
- Bun, Node.js, and Deno runtime support

**Scope:** Client library only; excludes broker implementation, Kafka Streams equivalent, Schema
Registry, and Connect framework.

---

## Current Reality

### Architecture

| Component | Technology             |
| --------- | ---------------------- |
| Language  | TypeScript (ESM-only)  |
| Runtime   | Bun, Node.js 20+, Deno |
| Build     | TypeScript compiler    |
| Test      | Vitest                 |
| Lint      | ESLint, Prettier       |
| Docs      | TypeDoc                |

### Modules

| Module                                      | Purpose                                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `index.ts`                                  | Main entry point, barrel exports                                                                                                                                                                                                                                                                                                                                                                  |
| `result.ts`                                 | `DecodeResult<T>` discriminated union and factory helpers                                                                                                                                                                                                                                                                                                                                         |
| `errors.ts`                                 | Error hierarchy: `KafkaError` base, protocol/connection/timeout/config subclasses                                                                                                                                                                                                                                                                                                                 |
| `config.ts`                                 | `KafkaConfig`, `BrokerAddress`, SASL/TLS types, `parseBrokerAddress`                                                                                                                                                                                                                                                                                                                              |
| `messages.ts`                               | `Message`, `TopicPartition`, `Offset`, `ConsumerRecord`, `ProduceResult`                                                                                                                                                                                                                                                                                                                          |
| `codec/api-keys.ts`                         | API key enum, version ranges, flexible version thresholds, `negotiateVersion`                                                                                                                                                                                                                                                                                                                     |
| `codec/binary-reader.ts`                    | `BinaryReader` bounds-checked cursor over `Uint8Array`, varint/string/bytes/array/tagged field decoding                                                                                                                                                                                                                                                                                           |
| `codec/binary-writer.ts`                    | `BinaryWriter` auto-growing buffer builder, varint/string/bytes/array/tagged field encoding                                                                                                                                                                                                                                                                                                       |
| `codec/protocol-framing.ts`                 | Request header v0–v2 encoding, response header v0–v1 decoding, size-prefixed framing, header version selection                                                                                                                                                                                                                                                                                    |
| `codec/record-batch.ts`                     | RecordBatch v2 (magic=2) encoding/decoding, Record codec, CRC-32C, compression provider registry                                                                                                                                                                                                                                                                                                  |
| `codec/compression.ts`                      | Compression providers for record batches: gzip, deflate, snappy, lz4, zstd                                                                                                                                                                                                                                                                                                                        |
| `protocol/api-versions.ts`                  | ApiVersions request/response codec (API key 18, v0–v3), `buildApiVersionsRequest`, `decodeApiVersionsResponse`                                                                                                                                                                                                                                                                                    |
| `protocol/metadata.ts`                      | Metadata request/response codec (API key 3, v0–v12), `buildMetadataRequest`, `decodeMetadataResponse`                                                                                                                                                                                                                                                                                             |
| `protocol/find-coordinator.ts`              | FindCoordinator request/response codec (API key 10, v0–v4), `buildFindCoordinatorRequest`, `decodeFindCoordinatorResponse`                                                                                                                                                                                                                                                                        |
| `protocol/list-offsets.ts`                  | ListOffsets request/response codec (API key 2, v0–v7), `buildListOffsetsRequest`, `decodeListOffsetsResponse`                                                                                                                                                                                                                                                                                     |
| `protocol/fetch.ts`                         | Fetch request/response codec (API key 1, v0–v13), `buildFetchRequest`, `decodeFetchResponse`                                                                                                                                                                                                                                                                                                      |
| `protocol/produce.ts`                       | Produce request/response codec (API key 0, v0–v9), `buildProduceRequest`, `decodeProduceResponse`                                                                                                                                                                                                                                                                                                 |
| `protocol/init-producer-id.ts`              | InitProducerId request/response codec (API key 22, v0–v4), `buildInitProducerIdRequest`, `decodeInitProducerIdResponse`                                                                                                                                                                                                                                                                           |
| `protocol/create-topics.ts`                 | CreateTopics request/response codec (API key 19, v0–v7), `buildCreateTopicsRequest`, `decodeCreateTopicsResponse`                                                                                                                                                                                                                                                                                 |
| `protocol/delete-topics.ts`                 | DeleteTopics request/response codec (API key 20, v0–v6), `buildDeleteTopicsRequest`, `decodeDeleteTopicsResponse`                                                                                                                                                                                                                                                                                 |
| `protocol/create-partitions.ts`             | CreatePartitions request/response codec (API key 37, v0–v3), `buildCreatePartitionsRequest`, `decodeCreatePartitionsResponse`                                                                                                                                                                                                                                                                     |
| `protocol/describe-configs.ts`              | DescribeConfigs request/response codec (API key 32, v0–v4), `buildDescribeConfigsRequest`, `decodeDescribeConfigsResponse`, `ConfigResourceType`                                                                                                                                                                                                                                                  |
| `protocol/alter-configs.ts`                 | AlterConfigs request/response codec (API key 33, v0–v2), `buildAlterConfigsRequest`, `decodeAlterConfigsResponse`, non-incremental config set                                                                                                                                                                                                                                                     |
| `protocol/incremental-alter-configs.ts`     | IncrementalAlterConfigs request/response codec (API key 44, v0–v1), `buildIncrementalAlterConfigsRequest`, `decodeIncrementalAlterConfigsResponse`, `AlterConfigOp`                                                                                                                                                                                                                               |
| `protocol/elect-leaders.ts`                 | ElectLeaders request/response codec (API key 43, v0–v2), `buildElectLeadersRequest`, `decodeElectLeadersResponse`, `ElectionType`                                                                                                                                                                                                                                                                 |
| `protocol/alter-partition-reassignments.ts` | AlterPartitionReassignments request/response codec (API key 45, v0), `buildAlterPartitionReassignmentsRequest`, `decodeAlterPartitionReassignmentsResponse`                                                                                                                                                                                                                                       |
| `protocol/list-partition-reassignments.ts`  | ListPartitionReassignments request/response codec (API key 46, v0), `buildListPartitionReassignmentsRequest`, `decodeListPartitionReassignmentsResponse`                                                                                                                                                                                                                                          |
| `protocol/describe-acls.ts`                 | DescribeAcls request/response codec (API key 29, v0–v3), `buildDescribeAclsRequest`, `decodeDescribeAclsResponse`, `AclResourceType`, `AclResourcePatternType`, `AclOperation`, `AclPermissionType`                                                                                                                                                                                               |
| `protocol/create-acls.ts`                   | CreateAcls request/response codec (API key 30, v0–v3), `buildCreateAclsRequest`, `decodeCreateAclsResponse`                                                                                                                                                                                                                                                                                       |
| `protocol/delete-acls.ts`                   | DeleteAcls request/response codec (API key 31, v0–v3), `buildDeleteAclsRequest`, `decodeDeleteAclsResponse`                                                                                                                                                                                                                                                                                       |
| `protocol/create-delegation-token.ts`       | CreateDelegationToken request/response codec (API key 38, v0–v3), `buildCreateDelegationTokenRequest`, `decodeCreateDelegationTokenResponse`                                                                                                                                                                                                                                                      |
| `protocol/renew-delegation-token.ts`        | RenewDelegationToken request/response codec (API key 39, v0–v2), `buildRenewDelegationTokenRequest`, `decodeRenewDelegationTokenResponse`                                                                                                                                                                                                                                                         |
| `protocol/expire-delegation-token.ts`       | ExpireDelegationToken request/response codec (API key 40, v0–v2), `buildExpireDelegationTokenRequest`, `decodeExpireDelegationTokenResponse`                                                                                                                                                                                                                                                      |
| `protocol/describe-delegation-token.ts`     | DescribeDelegationToken request/response codec (API key 41, v0–v3), `buildDescribeDelegationTokenRequest`, `decodeDescribeDelegationTokenResponse`                                                                                                                                                                                                                                                |
| `protocol/describe-groups.ts`               | DescribeGroups request/response codec (API key 15, v0–v5), `buildDescribeGroupsRequest`, `decodeDescribeGroupsResponse`                                                                                                                                                                                                                                                                           |
| `protocol/list-groups.ts`                   | ListGroups request/response codec (API key 16, v0–v4), `buildListGroupsRequest`, `decodeListGroupsResponse`                                                                                                                                                                                                                                                                                       |
| `protocol/delete-groups.ts`                 | DeleteGroups request/response codec (API key 42, v0–v2), `buildDeleteGroupsRequest`, `decodeDeleteGroupsResponse`                                                                                                                                                                                                                                                                                 |
| `protocol/offset-commit.ts`                 | OffsetCommit request/response codec (API key 8, v0–v8), group offset management, flexible versioning (v8+)                                                                                                                                                                                                                                                                                        |
| `protocol/offset-fetch.ts`                  | OffsetFetch request/response codec (API key 9, v0–v8), fetch committed offsets, nullable topics (v7+), leader epoch (v5+)                                                                                                                                                                                                                                                                         |
| `protocol/join-group.ts`                    | JoinGroup request/response codec (API key 11, v0–v9), consumer group coordination protocol, rebalance timeout, static membership (v5+)                                                                                                                                                                                                                                                            |
| `protocol/sync-group.ts`                    | SyncGroup request/response codec (API key 14, v0–v5), partition assignment distribution, protocol type negotiation (v5+)                                                                                                                                                                                                                                                                          |
| `protocol/heartbeat.ts`                     | Heartbeat request/response codec (API key 12, v0–v4), group session keepalive, rebalance detection                                                                                                                                                                                                                                                                                                |
| `protocol/leave-group.ts`                   | LeaveGroup request/response codec (API key 13, v0–v5), individual member leave, batch member leave (v3+), leave reason (v5+)                                                                                                                                                                                                                                                                      |
| `protocol/sasl-handshake.ts`                | SaslHandshake request/response codec (API key 17, v0–v1), `buildSaslHandshakeRequest`, `decodeSaslHandshakeResponse`                                                                                                                                                                                                                                                                              |
| `protocol/sasl-authenticate.ts`             | SaslAuthenticate request/response codec (API key 36, v0–v2), `buildSaslAuthenticateRequest`, `decodeSaslAuthenticateResponse`                                                                                                                                                                                                                                                                     |
| `protocol/add-partitions-to-txn.ts`         | AddPartitionsToTxn request/response codec (API key 24, v0–v3), `buildAddPartitionsToTxnRequest`, `decodeAddPartitionsToTxnResponse`                                                                                                                                                                                                                                                               |
| `protocol/add-offsets-to-txn.ts`            | AddOffsetsToTxn request/response codec (API key 25, v0–v3), `buildAddOffsetsToTxnRequest`, `decodeAddOffsetsToTxnResponse`                                                                                                                                                                                                                                                                        |
| `protocol/end-txn.ts`                       | EndTxn request/response codec (API key 26, v0–v3), `buildEndTxnRequest`, `decodeEndTxnResponse`                                                                                                                                                                                                                                                                                                   |
| `protocol/delete-records.ts`                | DeleteRecords request/response codec (API key 21, v0–v2), `buildDeleteRecordsRequest`, `decodeDeleteRecordsResponse`                                                                                                                                                                                                                                                                              |
| `protocol/offset-for-leader-epoch.ts`       | OffsetForLeaderEpoch request/response codec (API key 23, v0–v4), `buildOffsetForLeaderEpochRequest`, `decodeOffsetForLeaderEpochResponse`                                                                                                                                                                                                                                                         |
| `protocol/txn-offset-commit.ts`             | TxnOffsetCommit request/response codec (API key 28, v0–v3), `buildTxnOffsetCommitRequest`, `decodeTxnOffsetCommitResponse`                                                                                                                                                                                                                                                                        |
| `client/kafka.ts`                           | `Kafka` top-level client class, `createKafka` factory, lifecycle state machine (connect/disconnect), producer/consumer/admin factory methods                                                                                                                                                                                                                                                      |
| `client/producer.ts`                        | `KafkaProducer` class with send, batching (linger/size), retry with exponential backoff, partitioning (murmur2/round-robin), record batch encoding, broker routing, idempotent producer (PID/epoch/sequence numbers), transactional producer (begin/commit/abort, coordinator discovery, AddPartitionsToTxn, sendOffsetsToTransaction)                                                            |
| `client/consumer.ts`                        | KafkaConsumer class with group coordination, partition assignment, offset management, rebalance listener, auto-commit, offset reset strategies (earliest/latest/none), pluggable partition assignors                                                                                                                                                                                              |
| `client/admin.ts`                           | `KafkaAdmin` class with topic/partition management, config describe/alter/incremental-alter, topic listing via Metadata, group management (describe/list/delete), record deletion, elect leaders, partition reassignment (alter/list), ACL management (describe/create/delete), delegation token management (create/renew/expire/describe), retry with exponential backoff, `createAdmin` factory |
| `client/assignors.ts`                       | Partition assignor strategies: `rangeAssignor`, `roundRobinAssignor`, `createCooperativeStickyAssignor()`, `PartitionAssignor` interface                                                                                                                                                                                                                                                          |
| `network/socket.ts`                         | Socket adapter types: `KafkaSocket`, `SocketConnectOptions`, `SocketFactory` for runtime-agnostic TCP/TLS                                                                                                                                                                                                                                                                                         |
| `network/connection.ts`                     | `KafkaConnection` with request/response correlation, receive buffer reassembly, timeout management, SASL authentication                                                                                                                                                                                                                                                                           |
| `network/broker-pool.ts`                    | `ConnectionPool` with per-broker pooling, `discoverBrokers` for cluster discovery via Metadata API, opt-in auto-reconnection with `ReconnectStrategy`                                                                                                                                                                                                                                             |
| `network/bun-socket.ts`                     | Bun runtime socket adapter via `Bun.connect()`, TCP and TLS support, backpressure handling                                                                                                                                                                                                                                                                                                        |
| `network/node-socket.ts`                    | Node.js runtime socket adapter via `net`/`tls`, TCP and TLS support, backpressure handling                                                                                                                                                                                                                                                                                                        |
| `network/deno-socket.ts`                    | Deno runtime socket adapter via `Deno.connect()`, TCP and TLS support                                                                                                                                                                                                                                                                                                                             |
| `network/sasl.ts`                           | SASL mechanism implementations: PLAIN (RFC 4616), SCRAM-SHA-256/512 (RFC 5802), `SaslAuthenticator` type, `createSaslAuthenticator` factory                                                                                                                                                                                                                                                       |
| `serialization/serialization.ts`            | `Serializer<T>`, `Deserializer<T>`, `Serde<T>` sync types; `AsyncSerializer<T>`, `AsyncDeserializer<T>`, `AsyncSerde<T>` async types for Schema Registry; built-in `stringSerializer` (UTF-8) and `jsonSerializer<T>()` factory                                                                                                                                                                   |
| `serialization/schema-registry.ts`          | Confluent Schema Registry HTTP client, schema caching by ID and subject, Confluent wire format encode/decode, `SchemaRegistryError`, subject naming strategies (`topicNameStrategy`, `recordNameStrategy`, `topicRecordNameStrategy`)                                                                                                                                                             |
| `serialization/avro-serializer.ts`          | Avro serializer/deserializer with Schema Registry integration, pluggable `AvroCodec` interface, Confluent wire format, `createAvroSerde<T>()` factory                                                                                                                                                                                                                                             |
| `serialization/protobuf-serializer.ts`      | Protobuf serializer/deserializer with Schema Registry integration, pluggable `ProtobufCodec` interface, Confluent wire format with message index encoding, `createProtobufSerde<T>()` factory                                                                                                                                                                                                     |
| `testing/index.ts`                          | Testing utilities subpath (`@qualithm/kafka-client/testing`), re-exports codec primitives and protocol framing for building test fixtures                                                                                                                                                                                                                                                         |

### Features

| Feature          | Status   | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ---------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Core Types       | Complete | DecodeResult, errors, config, messages, API keys                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Binary Codec     | Complete | BinaryReader, BinaryWriter, varints, strings, bytes, arrays, tagged fields                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Protocol Framing | Complete | Request header v0–v2, response header v0–v1, size-prefixed framing, header version selection                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Producer         | Complete | Produce codec, KafkaProducer class with send/partitioning/record batch encoding/batching/retries/idempotent (PID, epoch, sequence numbers)                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Consumer         | Complete | OffsetCommit, OffsetFetch, JoinGroup, SyncGroup, Heartbeat, LeaveGroup codecs; KafkaConsumer class with group coordination, offset management, rebalance listener, auto-commit, offset reset strategies                                                                                                                                                                                                                                                                                                                                                                                                  |
| Consumer Groups  | Complete | Consumer group protocol (JoinGroup then SyncGroup then Heartbeat then Fetch then OffsetCommit then LeaveGroup), pluggable partition assignors (range, round-robin, cooperative sticky), rebalance listener pattern                                                                                                                                                                                                                                                                                                                                                                                       |
| Protocol Layer   | Complete | ApiVersions, Metadata, Fetch, Produce, InitProducerId, OffsetCommit, OffsetFetch, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, FindCoordinator, ListOffsets, SaslHandshake, SaslAuthenticate, CreateTopics, DeleteTopics, CreatePartitions, DescribeConfigs, AlterConfigs, DescribeGroups, ListGroups, DeleteGroups, DeleteRecords, OffsetForLeaderEpoch, ElectLeaders, IncrementalAlterConfigs, AlterPartitionReassignments, ListPartitionReassignments, DescribeAcls, CreateAcls, DeleteAcls, CreateDelegationToken, RenewDelegationToken, ExpireDelegationToken, DescribeDelegationToken all complete |
| Admin Client     | Complete | CreateTopics, DeleteTopics, CreatePartitions, DescribeConfigs, AlterConfigs, IncrementalAlterConfigs, DescribeGroups, ListGroups, DeleteGroups, DeleteRecords, OffsetForLeaderEpoch, ElectLeaders, AlterPartitionReassignments, ListPartitionReassignments, DescribeAcls, CreateAcls, DeleteAcls, CreateDelegationToken, RenewDelegationToken, ExpireDelegationToken, DescribeDelegationToken codecs; KafkaAdmin class with retry, listTopics/describeTopics via Metadata, group management, record deletion, elect leaders, partition reassignment, ACL management, delegation token management         |
| Record Batches   | Complete | RecordBatch v2, Record codec, CRC-32C, all compression types                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Connection       | Complete | Socket adapter interface, single-broker connection with correlation and timeouts, Bun/Node.js/Deno runtime adapters, broker discovery from metadata, connection pool with per-broker limits                                                                                                                                                                                                                                                                                                                                                                                                              |
| Connection Pool  | Complete | Per-broker pooling, idle/active tracking, waiter queue, metadata refresh, opt-in auto-reconnection with exponential backoff                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| API Design       | Complete | `Kafka` class, `createKafka()` factory, connect/disconnect lifecycle, `producer()`, `consumer()`, and `admin()` factory methods                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| SASL Auth        | Complete | SaslHandshake/SaslAuthenticate codecs, PLAIN/SCRAM-SHA-256/SCRAM-SHA-512 mechanisms, connection-level `authenticate()` method                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| SSL/TLS          | Complete | TlsConfig type, Bun/Node.js/Deno adapters handle TLS natively, mTLS support, rejectUnauthorised option                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Serialization    | Complete | Serializer/Deserializer/Serde sync and async types, built-in string and JSON serializers, Schema Registry client, Avro serializer (pluggable codec), Protobuf serializer (pluggable codec), Confluent wire format                                                                                                                                                                                                                                                                                                                                                                                        |
| Compression      | Complete | gzip, snappy (Xerial), lz4 (frame), zstd                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Transactions     | Complete | AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit codecs; transactional producer with begin/commit/abort, coordinator discovery/caching, partition registration, sendOffsetsToTransaction                                                                                                                                                                                                                                                                                                                                                                                                     |
| Assignors        | Complete | Range, round-robin, and cooperative sticky partition assignors; pluggable `PartitionAssignor` interface in consumer                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Auto-Reconnect   | Complete | Opt-in `ReconnectStrategy` with exponential backoff in `ConnectionPool`; disabled by default per Locked Decision #9                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Testing          | Complete | Property-based tests (fast-check) for protocol framing and chunk reassembly, spec reference annotations (`@see` Kafka protocol spec), integration test harness (docker-compose with KRaft broker), global coverage thresholds enforced (85% stmts, 80% branches, 85% funcs, 85% lines)                                                                                                                                                                                                                                                                                                                   |

### File Structure

| Directory            | Purpose                                                           |
| -------------------- | ----------------------------------------------------------------- |
| `bench/`             | Benchmarks with stats                                             |
| `examples/`          | Runnable usage examples                                           |
| `scripts/`           | Development utilities (bundle analysis, runtime validation)       |
| `src/`               | Source code root (shared types: result, errors, config, messages) |
| `src/codec/`         | Binary encoding/decoding, protocol framing, record batches        |
| `src/protocol/`      | Kafka API request/response codecs (35 APIs)                       |
| `src/client/`        | High-level client classes (Kafka, Producer, Consumer, Admin)      |
| `src/network/`       | Socket adapters, connection management, broker pool, SASL         |
| `src/serialization/` | Serialization framework, Schema Registry, Avro/Protobuf codecs    |
| `src/testing/`       | Testing utilities subpath (`@qualithm/kafka-client/testing`)      |

---

## Locked Decisions

1. **Native Kafka protocol** — Implement binary protocol directly; no librdkafka or other native
   dependencies
2. **Uint8Array only** — No `Buffer` in public API; Node callers can pass Buffer (subclass), but
   return types use `Uint8Array` exclusively
3. **Result types for decoding** — Decode functions return `DecodeResult<T>`, no exceptions in hot
   paths
4. **Spec-linked tests** — Tests reference Kafka protocol spec sections where applicable
5. **Runtime-agnostic** — No built-in TCP; runtime provides socket via adapter pattern
6. **Flexible API versioning** — Negotiate API versions per broker; support version ranges for
   backward compatibility
7. **Factory functions** — Provide `createKafka()` alongside class constructor
8. **Static error helpers** — Error classes include static `isError()` methods for type narrowing
9. **Explicit resource lifecycle** — User controls producer/consumer connect/disconnect; no implicit
   reconnection without opt-in
10. **Property-based testing** — Use fast-check for protocol edge cases and chunk reassembly
11. **Schema Registry built-in** — Ship in this package with pluggable providers (like compression);
    users only pay for what they import

---

## Open Decisions & Risks

### Open Decisions

_None._

### Risks

| ID  | Risk                        | Impact | Mitigation                                      |
| --- | --------------------------- | ------ | ----------------------------------------------- |
| R-1 | Kafka protocol complexity   | High   | Incremental implementation, start with basics   |
| R-2 | Binary encoding performance | Medium | Benchmark early, consider WebAssembly if needed |

---

## Work In Flight

> Claim work before starting. Include start timestamp. Remove within 24 hours of completion.

| ID  | Agent | Started | Task | Files |
| --- | ----- | ------- | ---- | ----- |

---

## Work Queue

### Core Types

- [x] Define `DecodeResult<T>` — shape: `{ ok: true; value: T; bytesRead: number }` |
      `{ ok: false; error: DecodeError }`
- [x] Define error hierarchy: retriable vs fatal, protocol-level vs connection-level
- [x] Error classes with static `isError()` helpers for type narrowing
- [x] Define `KafkaConfig`, `BrokerAddress` types
- [x] Define `Message`, `TopicPartition`, `Offset` types
- [x] Define API key enum and version ranges

Acceptance: All types compile, unit tests verify `isError()` narrows correctly, `DecodeResult`
round-trips through codec signatures.

### Binary Codec

- [x] `BinaryReader` — bounds-checked cursor over `Uint8Array`
- [x] `BinaryWriter` — auto-growing buffer builder (doubling strategy)
- [x] Kafka signed varint (zigzag encoding)
- [x] Kafka unsigned varint
- [x] Nullable/compact string encoding
- [x] Nullable/compact bytes encoding
- [x] Nullable/compact array encoding
- [x] Tagged fields (KIP-482 flexible versions)

Acceptance: Property-based tests prove round-trip for all primitive types, tagged fields
encode/decode correctly, benchmark shows no regression vs baseline.

### Protocol Framing

- [x] Request header v0–v1 encoding (non-flexible: API key, version, correlation ID, client ID)
- [x] Request header v2 encoding (flexible: adds tagged fields)
- [x] Response header v0 decoding (non-flexible)
- [x] Response header v1 decoding (flexible: adds tagged fields)
- [x] Size-prefixed message framing
- [x] Header version selection based on API key and API version

Acceptance: ApiVersions v0–v3 can be framed correctly using appropriate header versions.

### Record Batches

- [x] RecordBatch v2 format (magic=2) encoding and decoding
- [x] Record encoding (attributes, timestamp delta, offset delta, key, value, headers)
- [x] CRC-32C validation
- [x] Compression codec abstraction (compress/decompress interface)
- [x] gzip compression
- [x] snappy compression (Xerial framing, via `createSnappyProvider`)
- [x] lz4 compression (frame format, via `createLz4Provider`)
- [x] zstd compression (via `createZstdProvider`)

Acceptance: Can produce and parse records matching the Kafka on-wire format; compressed batches
round-trip correctly; CRC validation catches corruption.

### Initial API Messages

- [x] ApiVersions request/response (v0–v3, bootstrap with v0 non-flexible header)
- [x] Metadata request/response (broker/topic discovery)
- [x] FindCoordinator request/response (v0–v4, needed by consumer groups)
- [x] ListOffsets request/response (v0–v7, needed by consumer offset reset)
- [x] Produce request/response (v0–v9, Acks, transactional ID, record errors)
- [x] InitProducerId request/response (v0–v4, idempotent/transactional producers)

### Connection

- [x] Socket adapter interface (runtime provides TCP/TLS)
- [x] Bun runtime adapter (`Bun.connect`)
- [x] Node.js runtime adapter (`net`/`tls`)
- [x] Deno runtime adapter (`Deno.connect`)
- [x] Request/response correlation (correlation ID mapping)
- [x] Broker discovery from metadata
- [x] Connection pool — max connections per broker, lifecycle management

Acceptance: Socket adapter interface is runtime-agnostic; at least one adapter passes integration
tests against a real broker; connection pool manages connect/disconnect cleanly.

### API Design

- [x] Factory function `createKafka()` alongside class constructor
- [x] Resource lifecycle interface (connect/disconnect patterns)
- [x] Producer factory method
- [x] Consumer factory method
- [x] Admin client factory method

### Testing Infrastructure

- [x] Property-based testing setup with fast-check
- [x] Spec section reference pattern for tests
- [x] Integration test harness (docker-compose for broker)

Acceptance: `bun test` runs unit + property tests; integration tests can spin up a Kafka broker and
run basic produce/consume.

### Producer

- [x] Produce request/response encoding
- [x] Basic send functionality
- [x] Partitioning strategies (round-robin, key-hash, custom)
- [x] Batching with configurable linger and batch size
- [x] Retry logic with configurable attempts and backoff
- [x] Idempotent producer (InitProducerId, sequence numbers)

Acceptance: Can produce messages to a topic, messages land in expected partitions, retries recover
from transient failures, idempotent mode prevents duplicates.

### Consumer

- [x] Fetch request/response decoding
- [x] Subscribe/poll loop
- [x] Offset commit (OffsetCommit request/response)
- [x] Offset reset strategies (earliest, latest, none)
- [x] JoinGroup request/response
- [x] SyncGroup request/response
- [x] Heartbeat request/response
- [x] LeaveGroup request/response
- [x] Rebalance listener callback support

Acceptance: Consumer can subscribe, poll, commit offsets; group rebalancing works with multiple
consumers; offset reset behaves correctly per strategy.

### Authentication

- [x] SaslHandshake request/response
- [x] SaslAuthenticate request/response
- [x] SASL/PLAIN mechanism
- [x] SASL/SCRAM-SHA-256 and SCRAM-SHA-512 mechanisms
- [x] SSL/TLS via socket adapter

### Admin Client

- [x] CreateTopics request/response
- [x] DeleteTopics request/response
- [x] CreatePartitions request/response
- [x] DescribeTopics / DescribeCluster request/response (via Metadata)
- [x] DescribeConfigs / AlterConfigs request/response
- [x] ListTopics (via Metadata)

### Serialization

- [x] Serializer/Deserializer interface
- [x] Built-in JSON serializer
- [x] Built-in string serializer
- [x] Schema Registry client (HTTP, caching, subject naming strategies)
- [x] Avro serializer (pluggable AvroCodec, Confluent wire format)
- [x] Protobuf serializer (pluggable ProtobufCodec, Confluent wire format, message indexes)

### Template Cleanup

- [x] Remove `src/greet.ts` and `src/__tests__/unit/greet.test.ts`
- [x] Remove `greet.ts` entry from CONTEXT.md Modules table
- [x] Rewrite `bench/index.ts` to benchmark Kafka operations (binary codec, record batches)
- [x] Fix TypeDoc `navigationLinks` GitHub URL (points to `npm-example`)
- [x] Fix `examples/README.md` descriptions to match actual example content
- [x] Remove `coverage/` directory (gitignored build artefact checked in)

Acceptance: No references to `greet` in source code, benchmarks exercise real Kafka codecs, TypeDoc
links point to correct repository.

### Documentation

- [x] Expand README with feature overview, API usage examples, and architecture summary
- [x] Add produce/consume end-to-end example in `examples/`
- [x] Generate TypeDoc and verify output

Acceptance: README has quick-start code snippets for producer, consumer, and admin. Examples are
runnable.

### Package Publishing Preparation

- [x] Verify `package.json` exports, files, and metadata
- [x] Verify build produces correct `dist/` output
- [x] Verify `prepublishOnly` hook works end-to-end

Acceptance: `bun run build` succeeds, `dist/` contains `.js` and `.d.ts` files, package metadata is
correct.

### Transactions

- [x] AddPartitionsToTxn request/response codec (API key 24, v0–v4)
- [x] AddOffsetsToTxn request/response codec (API key 25, v0–v3)
- [x] EndTxn request/response codec (API key 26, v0–v3)
- [x] TxnOffsetCommit request/response codec (API key 28, v0–v3)
- [x] Transactional producer support in `KafkaProducer`

Acceptance: Transaction codecs round-trip correctly, transactional producer can begin/commit/abort
transactions.

### Cooperative Sticky Assignor

- [x] Implement cooperative sticky partition assignment strategy
- [x] Support incremental rebalancing (COOPERATIVE protocol)
- [x] Add assignor to consumer options

Acceptance: Consumer supports `cooperativeSticky` assignor, incremental rebalances avoid
stop-the-world.

### Opt-in Auto-Reconnection

- [x] Reconnection strategy interface (backoff, max retries)
- [x] Connection-level reconnect on disconnect
- [x] Pool-level reconnection coordination
- [x] Consumer/producer transparent retry on connection loss

Acceptance: Clients can opt in to automatic reconnection with configurable backoff; disabled by
default per Locked Decision #9.

### Integration Tests

- [x] End-to-end produce/consume test against KRaft broker (docker-compose)
- [x] SASL/PLAIN and SASL/SCRAM authentication integration tests
- [x] Consumer group rebalance integration test (multiple consumers)
- [x] Transactional produce with commit/abort integration test
- [x] Admin client operations integration test (create/delete topics, describe/alter configs)
- [x] Idempotent producer duplicate suppression integration test

Acceptance: Integration tests exercise real protocol codecs (writer callbacks), pass against
docker-compose KRaft broker, cover producer/consumer/admin/auth paths. Function coverage exceeds
90%.

### Group Management APIs

- [x] DescribeGroups request/response codec (API key 15, v0–v5)
- [x] ListGroups request/response codec (API key 16, v0–v4)
- [x] DeleteGroups request/response codec (API key 42, v0–v2)
- [x] Add `describeGroups()`, `listGroups()`, `deleteGroups()` to `KafkaAdmin`

Acceptance: Codecs round-trip correctly, admin client can list/describe/delete consumer groups.

### Record & Offset Management APIs

- [x] DeleteRecords request/response codec (API key 21, v0–v2)
- [x] OffsetForLeaderEpoch request/response codec (API key 23, v0–v4)
- [x] Add `deleteRecords()` to `KafkaAdmin`

Acceptance: Codecs round-trip correctly, admin client can truncate topic data.

### Admin Operations APIs

- [x] ElectLeaders request/response codec (API key 43, v0–v2)
- [x] IncrementalAlterConfigs request/response codec (API key 44, v0–v1)
- [x] AlterPartitionReassignments request/response codec (API key 45, v0–v0)
- [x] ListPartitionReassignments request/response codec (API key 46, v0–v0)
- [x] Add `electLeaders()`, `incrementalAlterConfigs()`, `alterPartitionReassignments()`,
      `listPartitionReassignments()` to `KafkaAdmin`

Acceptance: Codecs round-trip correctly, admin client supports preferred leader election,
incremental config updates, and partition reassignment.

### ACL Management APIs

- [x] DescribeAcls request/response codec (API key 29, v0–v3)
- [x] CreateAcls request/response codec (API key 30, v0–v3)
- [x] DeleteAcls request/response codec (API key 31, v0–v3)
- [x] Add `describeAcls()`, `createAcls()`, `deleteAcls()` to `KafkaAdmin`

Acceptance: Codecs round-trip correctly, admin client can manage ACL rules.

### Delegation Token APIs

- [x] CreateDelegationToken request/response codec (API key 38, v0–v3)
- [x] RenewDelegationToken request/response codec (API key 39, v0–v2)
- [x] ExpireDelegationToken request/response codec (API key 40, v0–v2)
- [x] DescribeDelegationToken request/response codec (API key 41, v0–v3)
- [x] Add delegation token methods to `KafkaAdmin`

Acceptance: Codecs round-trip correctly, admin client supports token lifecycle management.

### SCRAM Credential APIs

- [ ] DescribeUserScramCredentials request/response codec (API key 50, v0–v0)
- [ ] AlterUserScramCredentials request/response codec (API key 51, v0–v0)
- [ ] Add SCRAM credential methods to `KafkaAdmin`

Acceptance: Codecs round-trip correctly, admin client can manage SCRAM users.

### Client Quota APIs

- [ ] DescribeClientQuotas request/response codec (API key 48, v0–v1)
- [ ] AlterClientQuotas request/response codec (API key 49, v0–v1)
- [ ] Add quota methods to `KafkaAdmin`

Acceptance: Codecs round-trip correctly, admin client can inspect and set client quotas.

### Log Dir APIs

- [ ] DescribeLogDirs request/response codec (API key 35, v0–v4)
- [ ] AlterReplicaLogDirs request/response codec (API key 34, v0–v2)
- [ ] Add log dir methods to `KafkaAdmin`

Acceptance: Codecs round-trip correctly, admin client can query and manage broker log directories.

### Kafka 4.x Discovery & Introspection APIs

- [ ] DescribeCluster request/response codec (API key 60, v0–v1)
- [ ] DescribeProducers request/response codec (API key 61, v0–v0)
- [ ] DescribeTransactions request/response codec (API key 65, v0–v0)
- [ ] ListTransactions request/response codec (API key 66, v0–v0)
- [ ] DescribeTopicPartitions request/response codec (API key 75, v0–v0)
- [ ] UpdateFeatures request/response codec (API key 57, v0–v1)
- [ ] DescribeQuorum request/response codec (API key 55, v0–v1)
- [ ] Add discovery and introspection methods to `KafkaAdmin`

Acceptance: Codecs round-trip correctly, admin client supports modern cluster discovery and
transaction/producer introspection compatible with Kafka 4.2.

### KIP-848 Consumer Group Protocol

- [ ] ConsumerGroupHeartbeat request/response codec (API key 68, v0–v0)
- [ ] ConsumerGroupDescribe request/response codec (API key 69, v0–v0)
- [ ] Server-side assignor support in consumer (new group protocol)
- [ ] Backward compatibility with classic group protocol

Acceptance: Consumer can join groups using the new KIP-848 protocol against Kafka 4.2 brokers, falls
back to classic protocol for older brokers.

### Client Telemetry APIs

- [ ] GetTelemetrySubscriptions request/response codec (API key 71, v0–v0)
- [ ] PushTelemetry request/response codec (API key 72, v0–v0)
- [ ] Opt-in telemetry reporting from producer/consumer

Acceptance: Codecs round-trip correctly, clients can opt in to push metrics to brokers supporting
KIP-714.

---

## Learnings

> Append-only. Never edit or delete existing entries.

| Date       | Learning                                                                                                                                                                                                                                                                                |
| ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2026-03-06 | TypeScript `override readonly name = "SubclassName"` on Error subclasses causes TS2416 when the base uses a string literal type; declare `name` as `string` in the base class to allow overriding with narrower literals                                                                |
| 2026-03-06 | BinaryWriter/BinaryReader treat all strings as nullable — `writeString(string \| null)` and `readString(): DecodeResult<string \| null>` — no separate nullable variants needed                                                                                                         |
| 2026-03-10 | Producer batching with async metadata fetch requires tracking in-flight accumulations (`pendingEnqueues` set) so `flush()` waits for messages to be accumulated before draining; otherwise `flush()` races with `send()` and finds an empty accumulator                                 |
| 2026-03-10 | Web Crypto API in TypeScript ES2023 lib rejects `Uint8Array<ArrayBufferLike>` as `BufferSource` — use `.buffer.slice(offset, end) as ArrayBuffer` helper to extract a plain `ArrayBuffer` before passing to `crypto.subtle.importKey` / `deriveBits` / `sign` / `digest`                |
| 2026-03-10 | Schema Registry `register()` caches the schema by ID, so subsequent `getSchema()` calls for the same ID (e.g. during deserialization) are served from cache without an HTTP round-trip; test expectations should account for this                                                       |
| 2026-03-11 | fast-check v4 removed `fc.char()`; use `fc.string({ minLength, maxLength })` instead of `fc.stringOf(fc.char(), ...)` for arbitrary string generation                                                                                                                                   |
| 2026-03-11 | Kafka transaction protocol flow: FindCoordinator(TRANSACTION) → InitProducerId → beginTransaction (client-only) → AddPartitionsToTxn (per new topic-partition) → Produce → EndTxn (commit/abort); for consumer offset commit within transaction, also AddOffsetsToTxn → TxnOffsetCommit |
| 2026-03-11 | Kafka `FLEXIBLE_VERSION_THRESHOLDS` control header encoding, but individual response decoders may switch body format at a different version (e.g. FindCoordinator headers are flexible at v3 but batched response format starts at v4); tests must match the body format version        |
| 2026-03-12 | Mock-based unit tests for producer/consumer/admin achieve ~80% function coverage because `conn.send(apiKey, version, (writer) => { ... })` writer callbacks are never invoked by the mock; these callbacks are only exercisable via integration tests with a real broker                |
| 2026-03-12 | Single-broker Kafka docker-compose requires `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1` and `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1`; defaults of 3/2 cause `__transaction_state` topic creation to fail with `INVALID_REPLICATION_FACTOR`                                        |
| 2026-03-12 | DescribeConfigs v3+ added a `config_type` (INT8) field between `synonyms` and `documentation` in each config entry; omitting it shifts the decoder and corrupts all subsequent fields                                                                                                   |
| 2026-03-12 | `ConnectionPool` with `maxConnectionsPerBroker=1` (default) deadlocks when two consumers from the same `Kafka` instance join the same group: consumer2's JoinGroup holds the connection, consumer1 can't send its JoinGroup, broker blocks until all members rejoin                     |
