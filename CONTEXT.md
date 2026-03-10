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

| Module                 | Purpose                                                                                                                                                                                                              |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `index.ts`             | Main entry point, barrel exports                                                                                                                                                                                     |
| `greet.ts`             | Greeting utility (template placeholder)                                                                                                                                                                              |
| `result.ts`            | `DecodeResult<T>` discriminated union and factory helpers                                                                                                                                                            |
| `errors.ts`            | Error hierarchy: `KafkaError` base, protocol/connection/timeout/config subclasses                                                                                                                                    |
| `config.ts`            | `KafkaConfig`, `BrokerAddress`, SASL/TLS types, `parseBrokerAddress`                                                                                                                                                 |
| `messages.ts`          | `Message`, `TopicPartition`, `Offset`, `ConsumerRecord`, `ProduceResult`                                                                                                                                             |
| `api-keys.ts`          | API key enum, version ranges, flexible version thresholds, `negotiateVersion`                                                                                                                                        |
| `binary-reader.ts`     | `BinaryReader` bounds-checked cursor over `Uint8Array`, varint/string/bytes/array/tagged field decoding                                                                                                              |
| `binary-writer.ts`     | `BinaryWriter` auto-growing buffer builder, varint/string/bytes/array/tagged field encoding                                                                                                                          |
| `protocol-framing.ts`  | Request header v0–v2 encoding, response header v0–v1 decoding, size-prefixed framing, header version selection                                                                                                       |
| `api-versions.ts`      | ApiVersions request/response codec (API key 18, v0–v3), `buildApiVersionsRequest`, `decodeApiVersionsResponse`                                                                                                       |
| `metadata.ts`          | Metadata request/response codec (API key 3, v0–v12), `buildMetadataRequest`, `decodeMetadataResponse`                                                                                                                |
| `find-coordinator.ts`  | FindCoordinator request/response codec (API key 10, v0–v4), `buildFindCoordinatorRequest`, `decodeFindCoordinatorResponse`                                                                                           |
| `list-offsets.ts`      | ListOffsets request/response codec (API key 2, v0–v7), `buildListOffsetsRequest`, `decodeListOffsetsResponse`                                                                                                        |
| `record-batch.ts`      | RecordBatch v2 (magic=2) encoding/decoding, Record codec, CRC-32C, compression provider registry                                                                                                                     |
| `compression.ts`       | Compression providers for record batches: gzip, deflate, snappy, lz4, zstd                                                                                                                                           |
| `socket.ts`            | Socket adapter types: `KafkaSocket`, `SocketConnectOptions`, `SocketFactory` for runtime-agnostic TCP/TLS                                                                                                            |
| `connection.ts`        | `KafkaConnection` with request/response correlation, receive buffer reassembly, timeout management, SASL authentication                                                                                              |
| `broker-pool.ts`       | `ConnectionPool` with per-broker pooling, `discoverBrokers` for cluster discovery via Metadata API                                                                                                                   |
| `kafka.ts`             | `Kafka` top-level client class, `createKafka` factory, lifecycle state machine (connect/disconnect), producer/consumer/admin factory methods                                                                         |
| `admin.ts`             | `KafkaAdmin` class with topic/partition management, config describe/alter, topic listing via Metadata, retry with exponential backoff, `createAdmin` factory                                                         |
| `create-topics.ts`     | CreateTopics request/response codec (API key 19, v0–v7), `buildCreateTopicsRequest`, `decodeCreateTopicsResponse`                                                                                                    |
| `delete-topics.ts`     | DeleteTopics request/response codec (API key 20, v0–v6), `buildDeleteTopicsRequest`, `decodeDeleteTopicsResponse`                                                                                                    |
| `create-partitions.ts` | CreatePartitions request/response codec (API key 37, v0–v3), `buildCreatePartitionsRequest`, `decodeCreatePartitionsResponse`                                                                                        |
| `describe-configs.ts`  | DescribeConfigs request/response codec (API key 32, v0–v4), `buildDescribeConfigsRequest`, `decodeDescribeConfigsResponse`, `ConfigResourceType`                                                                     |
| `alter-configs.ts`     | AlterConfigs request/response codec (API key 33, v0–v2), `buildAlterConfigsRequest`, `decodeAlterConfigsResponse`, non-incremental config set                                                                        |
| `fetch.ts`             | Fetch request/response codec (API key 1, v0–v13), `buildFetchRequest`, `decodeFetchResponse`                                                                                                                         |
| `produce.ts`           | Produce request/response codec (API key 0, v0–v9), `buildProduceRequest`, `decodeProduceResponse`                                                                                                                    |
| `init-producer-id.ts`  | InitProducerId request/response codec (API key 22, v0–v4), `buildInitProducerIdRequest`, `decodeInitProducerIdResponse`                                                                                              |
| `producer.ts`          | `KafkaProducer` class with send, batching (linger/size), retry with exponential backoff, partitioning (murmur2/round-robin), record batch encoding, broker routing, idempotent producer (PID/epoch/sequence numbers) |
| `offset-commit.ts`     | OffsetCommit request/response codec (API key 8, v0–v8), group offset management, flexible versioning (v8+)                                                                                                           |
| `offset-fetch.ts`      | OffsetFetch request/response codec (API key 9, v0–v8), fetch committed offsets, nullable topics (v7+), leader epoch (v5+)                                                                                            |
| `join-group.ts`        | JoinGroup request/response codec (API key 11, v0–v9), consumer group coordination protocol, rebalance timeout, static membership (v5+)                                                                               |
| `sync-group.ts`        | SyncGroup request/response codec (API key 14, v0–v5), partition assignment distribution, protocol type negotiation (v5+)                                                                                             |
| `heartbeat.ts`         | Heartbeat request/response codec (API key 12, v0–v4), group session keepalive, rebalance detection                                                                                                                   |
| `leave-group.ts`       | LeaveGroup request/response codec (API key 13, v0–v5), individual member leave, batch member leave (v3+), leave reason (v5+)                                                                                         |
| `consumer.ts`          | KafkaConsumer class with group coordination, partition assignment, offset management, rebalance listener, auto-commit, offset reset strategies (earliest/latest/none), range assignor                                |
| `sasl-handshake.ts`    | SaslHandshake request/response codec (API key 17, v0–v1), `buildSaslHandshakeRequest`, `decodeSaslHandshakeResponse`                                                                                                 |
| `sasl-authenticate.ts` | SaslAuthenticate request/response codec (API key 36, v0–v2), `buildSaslAuthenticateRequest`, `decodeSaslAuthenticateResponse`                                                                                        |
| `sasl.ts`              | SASL mechanism implementations: PLAIN (RFC 4616), SCRAM-SHA-256/512 (RFC 5802), `SaslAuthenticator` type, `createSaslAuthenticator` factory                                                                          |
| `bun-socket.ts`        | Bun runtime socket adapter via `Bun.connect()`, TCP and TLS support, backpressure handling                                                                                                                           |
| `node-socket.ts`       | Node.js runtime socket adapter via `net`/`tls`, TCP and TLS support, backpressure handling                                                                                                                           |
| `deno-socket.ts`       | Deno runtime socket adapter via `Deno.connect()`, TCP and TLS support                                                                                                                                                |
| `serialization.ts`     | `Serializer<T>`, `Deserializer<T>`, `Serde<T>` types; built-in `stringSerializer` (UTF-8) and `jsonSerializer<T>()` factory                                                                                          |

### Features

| Feature          | Status      | Notes                                                                                                                                                                                                                                                                                  |
| ---------------- | ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Core Types       | Complete    | DecodeResult, errors, config, messages, API keys                                                                                                                                                                                                                                       |
| Binary Codec     | Complete    | BinaryReader, BinaryWriter, varints, strings, bytes, arrays, tagged fields                                                                                                                                                                                                             |
| Protocol Framing | Complete    | Request header v0–v2, response header v0–v1, size-prefixed framing, header version selection                                                                                                                                                                                           |
| Producer         | Complete    | Produce codec, KafkaProducer class with send/partitioning/record batch encoding/batching/retries/idempotent (PID, epoch, sequence numbers)                                                                                                                                             |
| Consumer         | Complete    | OffsetCommit, OffsetFetch, JoinGroup, SyncGroup, Heartbeat, LeaveGroup codecs; KafkaConsumer class with group coordination, offset management, rebalance listener, auto-commit, offset reset strategies                                                                                |
| Consumer Groups  | Complete    | Consumer group protocol (JoinGroup then SyncGroup then Heartbeat then Fetch then OffsetCommit then LeaveGroup), range partition assignor, rebalance listener pattern                                                                                                                   |
| Protocol Layer   | Complete    | ApiVersions, Metadata, Fetch, Produce, InitProducerId, OffsetCommit, OffsetFetch, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, FindCoordinator, ListOffsets, SaslHandshake, SaslAuthenticate, CreateTopics, DeleteTopics, CreatePartitions, DescribeConfigs, AlterConfigs all complete |
| Admin Client     | Complete    | CreateTopics, DeleteTopics, CreatePartitions, DescribeConfigs, AlterConfigs codecs; KafkaAdmin class with retry, listTopics/describeTopics via Metadata                                                                                                                                |
| Record Batches   | Complete    | RecordBatch v2, Record codec, CRC-32C, all compression types                                                                                                                                                                                                                           |
| Connection       | Complete    | Socket adapter interface, single-broker connection with correlation and timeouts, Bun/Node.js/Deno runtime adapters, broker discovery from metadata, connection pool with per-broker limits                                                                                            |
| Connection Pool  | Complete    | Per-broker pooling, idle/active tracking, waiter queue, metadata refresh                                                                                                                                                                                                               |
| API Design       | Complete    | `Kafka` class, `createKafka()` factory, connect/disconnect lifecycle, `producer()`, `consumer()`, and `admin()` factory methods                                                                                                                                                        |
| SASL Auth        | Complete    | SaslHandshake/SaslAuthenticate codecs, PLAIN/SCRAM-SHA-256/SCRAM-SHA-512 mechanisms, connection-level `authenticate()` method                                                                                                                                                          |
| SSL/TLS          | Not started |                                                                                                                                                                                                                                                                                        |
| Serialization    | Partial     | Serializer/Deserializer/Serde types, built-in string and JSON serializers; Avro and Protobuf not yet implemented                                                                                                                                                                       |
| Compression      | Complete    | gzip, snappy (Xerial), lz4 (frame), zstd                                                                                                                                                                                                                                               |

### File Structure

| Directory   | Purpose                 |
| ----------- | ----------------------- |
| `bench/`    | Benchmarks with stats   |
| `examples/` | Runnable usage examples |
| `scripts/`  | Development utilities   |
| `src/`      | Source code             |

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

- [ ] Property-based testing setup with fast-check
- [ ] Spec section reference pattern for tests
- [ ] Integration test harness (testcontainers or docker-compose for broker)

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
- [ ] SSL/TLS via socket adapter

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
- [ ] Avro serializer (requires Schema Registry — see OD-1)
- [ ] Protobuf serializer (requires Schema Registry — see OD-1)

---

## Learnings

> Append-only. Never edit or delete existing entries.

| Date       | Learning                                                                                                                                                                                                                                                                 |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2026-03-06 | TypeScript `override readonly name = "SubclassName"` on Error subclasses causes TS2416 when the base uses a string literal type; declare `name` as `string` in the base class to allow overriding with narrower literals                                                 |
| 2026-03-06 | BinaryWriter/BinaryReader treat all strings as nullable — `writeString(string \| null)` and `readString(): DecodeResult<string \| null>` — no separate nullable variants needed                                                                                          |
| 2026-03-10 | Producer batching with async metadata fetch requires tracking in-flight accumulations (`pendingEnqueues` set) so `flush()` waits for messages to be accumulated before draining; otherwise `flush()` races with `send()` and finds an empty accumulator                  |
| 2026-03-10 | Web Crypto API in TypeScript ES2023 lib rejects `Uint8Array<ArrayBufferLike>` as `BufferSource` — use `.buffer.slice(offset, end) as ArrayBuffer` helper to extract a plain `ArrayBuffer` before passing to `crypto.subtle.importKey` / `deriveBits` / `sign` / `digest` |
