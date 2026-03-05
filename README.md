# Kafka Client

[![CI](https://github.com/qualithm/kafka-client-js/actions/workflows/ci.yaml/badge.svg)](https://github.com/qualithm/kafka-client-js/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/qualithm/kafka-client-js/graph/badge.svg)](https://codecov.io/gh/qualithm/kafka-client-js)
[![npm](https://img.shields.io/npm/v/@qualithm/kafka-client)](https://www.npmjs.com/package/@qualithm/kafka-client)

Native Apache Kafka client for JavaScript and TypeScript runtimes. Implements the Kafka binary
protocol directly for producing, consuming, and administering Kafka clusters.

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

### Running

```bash
bun run start
```

### Testing

```bash
bun test
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
