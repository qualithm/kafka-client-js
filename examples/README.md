# Examples

Runnable examples demonstrating Kafka client usage.

## Prerequisites

The `produce-consume` example requires a running Kafka broker (default: `localhost:9092`).

## Running Examples

```bash
bun run examples/basic-usage.ts
bun run examples/error-handling.ts
bun run examples/batch-processing.ts
bun run examples/produce-consume.ts
```

## Example Files

| File                                       | Description                                       |
| ------------------------------------------ | ------------------------------------------------- |
| [basic-usage.ts](basic-usage.ts)           | Core types, broker parsing, version negotiation   |
| [error-handling.ts](error-handling.ts)     | Error hierarchy and type narrowing                |
| [batch-processing.ts](batch-processing.ts) | Constructing and encoding record batches          |
| [produce-consume.ts](produce-consume.ts)   | End-to-end produce and consume with a live broker |
