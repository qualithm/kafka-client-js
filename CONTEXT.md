# CONTEXT.md

> **Single source of truth.** CONTEXT.md > Code > README > Comments.

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

| Module                | Purpose                                                                                       |
| --------------------- | --------------------------------------------------------------------------------------------- |
| Core (`src/*.ts`)     | Entry point, `DecodeResult<T>`, error hierarchy, config/SASL types, message types             |
| `codec/`              | Binary reader/writer, protocol framing (headers v0–v2), record batch v2, CRC-32C, compression |
| `protocol/`           | 57 Kafka API codecs (versions negotiated per broker)                                          |
| `client/kafka.ts`     | `Kafka` class, `createKafka` factory, lifecycle, producer/consumer/admin methods              |
| `client/producer.ts`  | Batching, partitioning, retries, idempotent mode, transactional support                       |
| `client/consumer.ts`  | Group coordination, offset management, rebalance listener, auto-commit                        |
| `client/admin.ts`     | Topic/partition/config/group/ACL/token/quota/log-dir/cluster management                       |
| `client/assignors.ts` | Range, round-robin, cooperative sticky partition assignors                                    |
| `client/telemetry.ts` | Opt-in KIP-714 client telemetry reporter                                                      |
| `network/`            | Socket adapters, `KafkaConnection`, `ConnectionPool`, Bun/Node.js/Deno adapters, SASL         |
| `serialization/`      | Sync/async serde types, string/JSON serializers, Schema Registry, Avro/Protobuf               |
| `testing/`            | Testing utilities subpath for test fixtures                                                   |

### Features

| Feature          | Notes                                                                        |
| ---------------- | ---------------------------------------------------------------------------- |
| Core Types       | DecodeResult, errors, config, messages, API keys                             |
| Binary Codec     | BinaryReader, BinaryWriter, varints, tagged fields                           |
| Protocol Framing | Request/response headers v0–v2, size-prefixed framing                        |
| Producer         | Batching, partitioning, retries, idempotent, transactions                    |
| Consumer         | Group coordination, offset management, rebalance, auto-commit                |
| Consumer Groups  | Classic and KIP-848 protocols, pluggable assignors                           |
| Protocol Layer   | 57 API codecs including KIP-848 and KIP-714                                  |
| Admin Client     | Topics, partitions, configs, groups, ACLs, tokens, quotas, log dirs, cluster |
| Record Batches   | v2 format, CRC-32C, gzip/snappy/lz4/zstd                                     |
| Connection       | Runtime-agnostic sockets, connection pool, opt-in auto-reconnect             |
| SASL Auth        | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512                                          |
| SSL/TLS          | mTLS, Bun/Node.js/Deno native TLS                                            |
| Serialization    | String, JSON, Schema Registry, Avro, Protobuf                                |
| Compression      | gzip, snappy (Xerial), lz4 (frame), zstd                                     |
| Telemetry        | KIP-714 opt-in client metrics                                                |
| Testing          | Property-based (fast-check), integration harness                             |

### File Structure

| Directory            | Purpose                                                      |
| -------------------- | ------------------------------------------------------------ |
| `bench/`             | Benchmarks                                                   |
| `examples/`          | Runnable usage examples                                      |
| `scripts/`           | Development utilities (bundle analysis, runtime validation)  |
| `src/`               | Source code root                                             |
| `src/codec/`         | Binary encoding/decoding, protocol framing, record batches   |
| `src/protocol/`      | Kafka API request/response codecs (57 APIs)                  |
| `src/client/`        | High-level client classes (Kafka, Producer, Consumer, Admin) |
| `src/network/`       | Socket adapters, connection management, broker pool, SASL    |
| `src/serialization/` | Serialization framework, Schema Registry, Avro/Protobuf      |
| `src/testing/`       | Testing utilities subpath                                    |

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
11. **Schema Registry built-in** — Ship in this package with pluggable providers; users only pay for
    what they import

---

## Open Decisions & Risks

### Open Decisions

| ID  | Question | Context |
| --- | -------- | ------- |

### Risks

| ID  | Risk                        | Impact | Mitigation                                        |
| --- | --------------------------- | ------ | ------------------------------------------------- |
| R-1 | Kafka protocol complexity   | High   | Incremental implementation, start with basics     |
| R-2 | Binary encoding performance | Medium | Benchmark early, consider WebAssembly as fallback |

---

## Work In Flight

> Claim work before starting. Include start timestamp. Remove within 24 hours of completion.

| ID  | Agent | Started | Task | Files |
| --- | ----- | ------- | ---- | ----- |

---

## Work Queue
