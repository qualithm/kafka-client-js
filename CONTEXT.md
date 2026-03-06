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

| Module                | Purpose                                                                                                                    |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `index.ts`            | Main entry point, barrel exports                                                                                           |
| `greet.ts`            | Greeting utility (template placeholder)                                                                                    |
| `result.ts`           | `DecodeResult<T>` discriminated union and factory helpers                                                                  |
| `errors.ts`           | Error hierarchy: `KafkaError` base, protocol/connection/timeout/config subclasses                                          |
| `config.ts`           | `KafkaConfig`, `BrokerAddress`, SASL/TLS types, `parseBrokerAddress`                                                       |
| `messages.ts`         | `Message`, `TopicPartition`, `Offset`, `ConsumerRecord`, `ProduceResult`                                                   |
| `api-keys.ts`         | API key enum, version ranges, flexible version thresholds, `negotiateVersion`                                              |
| `binary-reader.ts`    | `BinaryReader` bounds-checked cursor over `Uint8Array`, varint/string/bytes/array/tagged field decoding                    |
| `binary-writer.ts`    | `BinaryWriter` auto-growing buffer builder, varint/string/bytes/array/tagged field encoding                                |
| `protocol-framing.ts` | Request header v0–v2 encoding, response header v0–v1 decoding, size-prefixed framing, header version selection             |
| `api-versions.ts`     | ApiVersions request/response codec (API key 18, v0–v3), `buildApiVersionsRequest`, `decodeApiVersionsResponse`             |
| `metadata.ts`         | Metadata request/response codec (API key 3, v0–v12), `buildMetadataRequest`, `decodeMetadataResponse`                      |
| `find-coordinator.ts` | FindCoordinator request/response codec (API key 10, v0–v4), `buildFindCoordinatorRequest`, `decodeFindCoordinatorResponse` |
| `record-batch.ts`     | RecordBatch v2 (magic=2) encoding/decoding, Record codec, CRC-32C, compression provider registry                           |
| `compression.ts`      | Compression providers for record batches: gzip, deflate, snappy, lz4, zstd                                                 |

### Features

| Feature          | Status      | Notes                                                                                        |
| ---------------- | ----------- | -------------------------------------------------------------------------------------------- |
| Core Types       | Complete    | DecodeResult, errors, config, messages, API keys                                             |
| Binary Codec     | Complete    | BinaryReader, BinaryWriter, varints, strings, bytes, arrays, tagged fields                   |
| Protocol Framing | Complete    | Request header v0–v2, response header v0–v1, size-prefixed framing, header version selection |
| Producer         | Not started |                                                                                              |
| Consumer         | Not started |                                                                                              |
| Consumer Groups  | Not started |                                                                                              |
| Admin Client     | Not started |                                                                                              |
| Protocol Layer   | In progress | ApiVersions (v0–v3), Metadata (v0–v12) complete                                              |
| Record Batches   | Complete    | RecordBatch v2, Record codec, CRC-32C, all compression types                                 |
| Connection Pool  | Not started |                                                                                              |
| SASL Auth        | Not started |                                                                                              |
| SSL/TLS          | Not started |                                                                                              |
| Serialization    | Not started |                                                                                              |
| Compression      | Complete    | gzip, snappy (Xerial), lz4 (frame), zstd                                                     |

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

---

## Open Decisions & Risks

### Open Decisions

| ID   | Question                           | Context                      |
| ---- | ---------------------------------- | ---------------------------- |
| OD-1 | Schema Registry integration scope? | Built-in vs separate package |

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
- [ ] ListOffsets request/response (needed by consumer offset reset)

### Connection

- [ ] Socket adapter interface (runtime provides TCP/TLS)
- [ ] Bun runtime adapter (`Bun.connect`)
- [ ] Node.js runtime adapter (`net`/`tls`)
- [ ] Deno runtime adapter (`Deno.connect`)
- [ ] Request/response correlation (correlation ID mapping)
- [ ] Broker discovery from metadata
- [ ] Connection pool — max connections per broker, lifecycle management

Acceptance: Socket adapter interface is runtime-agnostic; at least one adapter passes integration
tests against a real broker; connection pool manages connect/disconnect cleanly.

### API Design

- [ ] Factory function `createKafka()` alongside class constructor
- [ ] Resource lifecycle interface (connect/disconnect patterns)

### Testing Infrastructure

- [ ] Property-based testing setup with fast-check
- [ ] Spec section reference pattern for tests
- [ ] Integration test harness (testcontainers or docker-compose for broker)

Acceptance: `bun test` runs unit + property tests; integration tests can spin up a Kafka broker and
run basic produce/consume.

### Producer

- [ ] Produce request/response encoding
- [ ] Basic send functionality
- [ ] Partitioning strategies (round-robin, key-hash, custom)
- [ ] Batching with configurable linger and batch size
- [ ] Retry logic with configurable attempts and backoff
- [ ] Idempotent producer (InitProducerId, sequence numbers)

Acceptance: Can produce messages to a topic, messages land in expected partitions, retries recover
from transient failures, idempotent mode prevents duplicates.

### Consumer

- [ ] Fetch request/response decoding
- [ ] Subscribe/poll loop
- [ ] Offset commit (OffsetCommit request/response)
- [ ] Offset reset strategies (earliest, latest, none)
- [ ] JoinGroup request/response
- [ ] SyncGroup request/response
- [ ] Heartbeat request/response
- [ ] LeaveGroup request/response
- [ ] Rebalance listener callback support

Acceptance: Consumer can subscribe, poll, commit offsets; group rebalancing works with multiple
consumers; offset reset behaves correctly per strategy.

### Authentication

- [ ] SaslHandshake request/response
- [ ] SaslAuthenticate request/response
- [ ] SASL/PLAIN mechanism
- [ ] SASL/SCRAM-SHA-256 and SCRAM-SHA-512 mechanisms
- [ ] SSL/TLS via socket adapter

### Admin Client

- [ ] CreateTopics request/response
- [ ] DeleteTopics request/response
- [ ] CreatePartitions request/response
- [ ] DescribeTopics / DescribeCluster request/response
- [ ] DescribeConfigs / AlterConfigs request/response
- [ ] ListTopics (via Metadata)

### Serialization

- [ ] Serializer/Deserializer interface
- [ ] Built-in JSON serializer
- [ ] Built-in string serializer
- [ ] Avro serializer (requires Schema Registry — see OD-1)
- [ ] Protobuf serializer (requires Schema Registry — see OD-1)

---

## Learnings

> Append-only. Never edit or delete existing entries.

| Date       | Learning                                                                                                                                                                                                                 |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2026-03-06 | TypeScript `override readonly name = "SubclassName"` on Error subclasses causes TS2416 when the base uses a string literal type; declare `name` as `string` in the base class to allow overriding with narrower literals |
