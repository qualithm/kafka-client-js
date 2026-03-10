# Kafka Client

[![CI](https://github.com/qualithm/kafka-client-js/actions/workflows/ci.yaml/badge.svg)](https://github.com/qualithm/kafka-client-js/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/qualithm/kafka-client-js/graph/badge.svg)](https://codecov.io/gh/qualithm/kafka-client-js)
[![npm](https://img.shields.io/npm/v/@qualithm/kafka-client)](https://www.npmjs.com/package/@qualithm/kafka-client)

Native Apache Kafka client for JavaScript and TypeScript runtimes. Implements the Kafka binary
protocol directly for producing, consuming, and administering Kafka clusters.

## Features

- **Zero native dependencies** — pure TypeScript binary protocol implementation
- **Multi-runtime** — Bun, Node.js 20+, and Deno
- **Producer** — batching, partitioning (murmur2/round-robin/custom), retries, idempotent mode
- **Consumer** — group coordination, offset management, rebalance listeners, auto-commit
- **Admin** — topic/partition CRUD, config describe/alter
- **SASL authentication** — PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
- **SSL/TLS** — mutual TLS support via runtime socket adapters
- **Serialisation** — built-in JSON/string, pluggable Avro and Protobuf via Schema Registry
- **Compression** — gzip, snappy, lz4, zstd

## Quick Start

```bash
bun add @qualithm/kafka-client
# or
npm install @qualithm/kafka-client
```

### Producing Messages

```ts
import { createKafka, createNodeSocketFactory } from "@qualithm/kafka-client"

const kafka = createKafka({
  config: { brokers: ["localhost:9092"], clientId: "my-app" },
  socketFactory: createNodeSocketFactory()
})
await kafka.connect()

const producer = kafka.producer()
await producer.send("my-topic", [
  { key: new TextEncoder().encode("key-1"), value: new TextEncoder().encode("hello") }
])
await producer.close()
await kafka.disconnect()
```

### Consuming Messages

```ts
import { createKafka, createNodeSocketFactory } from "@qualithm/kafka-client"

const kafka = createKafka({
  config: { brokers: ["localhost:9092"], clientId: "my-app" },
  socketFactory: createNodeSocketFactory()
})
await kafka.connect()

const consumer = kafka.consumer({ groupId: "my-group" })
consumer.subscribe(["my-topic"])
await consumer.connect()

const records = await consumer.poll()
for (const record of records) {
  console.log(new TextDecoder().decode(record.message.value!))
}

await consumer.close()
await kafka.disconnect()
```

### Admin Operations

```ts
const admin = kafka.admin()
await admin.createTopics({
  topics: [{ name: "new-topic", numPartitions: 3, replicationFactor: 1 }],
  timeoutMs: 30000
})
const topics = await admin.listTopics()
```

### Runtime Adapters

```ts
// Bun
import { createBunSocketFactory } from "@qualithm/kafka-client"
const socketFactory = createBunSocketFactory()

// Node.js
import { createNodeSocketFactory } from "@qualithm/kafka-client"
const socketFactory = createNodeSocketFactory()

// Deno
import { createDenoSocketFactory } from "@qualithm/kafka-client"
const socketFactory = createDenoSocketFactory()
```

### Compression

Register compression providers before producing or consuming compressed record batches:

```ts
import { registerCompressionProvider, createSnappyProvider } from "@qualithm/kafka-client"
import snappy from "snappy" // bring your own codec

registerCompressionProvider(createSnappyProvider(snappy))
```

Available: `gzipProvider`, `deflateProvider`, `createSnappyProvider`, `createLz4Provider`,
`createZstdProvider`.

### Schema Registry

```ts
import { SchemaRegistry, createAvroSerde } from "@qualithm/kafka-client"

const registry = new SchemaRegistry({ baseUrl: "http://localhost:8081" })
const serde = createAvroSerde<MyType>({ registry, subject: "my-topic-value", codec: avroCodec })

// Serialize for producing
const encoded = await serde.serialize("my-topic", myData)

// Deserialize when consuming
const decoded = await serde.deserialize("my-topic", record.message.value!)
```

## API Documentation

Full API reference is generated with [TypeDoc](https://typedoc.org/):

```bash
bun run docs
```

Output is written to `docs/`.

## Development

### Prerequisites

- [Bun](https://bun.sh/) (recommended), Node.js 20+, or [Deno](https://deno.land/)

### Setup

```bash
bun install
```

### Building

```bash
bun run build
```

### Testing

```bash
# Unit tests
bun run test:unit

# Integration tests (requires a running broker)
docker compose up -d
bun run test:integration

# All tests with coverage
bun run test:coverage
```

### Linting & Formatting

```bash
bun run lint
bun run format
bun run typecheck
```

### Benchmarks

```bash
bun run bench
```

## Publishing

The package is automatically published to NPM when CI passes on main. Update the version in
`package.json` before merging to trigger a new release.

## Licence

Apache-2.0
