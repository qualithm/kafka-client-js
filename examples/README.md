# Examples

This directory contains runnable examples demonstrating Kafka client usage.

## Running Examples

```bash
# Basic usage — types, broker parsing, version negotiation
bun run examples/basic-usage.ts

# Error handling — error hierarchy and type narrowing
bun run examples/error-handling.ts

# Batch processing — constructing and encoding record batches
bun run examples/batch-processing.ts

# Produce and consume — end-to-end with a live broker
bun run examples/produce-consume.ts
```

## Example Files

| File                                       | Description                                       |
| ------------------------------------------ | ------------------------------------------------- |
| [basic-usage.ts](basic-usage.ts)           | Core types, broker parsing, version negotiation   |
| [error-handling.ts](error-handling.ts)     | Error hierarchy and type narrowing                |
| [batch-processing.ts](batch-processing.ts) | Constructing and encoding record batches          |
| [produce-consume.ts](produce-consume.ts)   | End-to-end produce and consume with a live broker |
