/**
 * Benchmarks entry point.
 *
 * Run with: bun run bench
 *
 * Example with configuration:
 *   WARMUP_ITERATIONS=20 BENCH_ITERATIONS=1000 bun run bench
 */

/* eslint-disable no-console */

import { BinaryReader } from "../src/binary-reader"
import { BinaryWriter } from "../src/binary-writer"
import {
  buildRecordBatch,
  createRecord,
  decodeRecordBatch,
  encodeRecordBatch
} from "../src/record-batch"

const config = {
  warmupIterations: parseInt(process.env.WARMUP_ITERATIONS ?? "15", 10),
  benchmarkIterations: parseInt(process.env.BENCH_ITERATIONS ?? "100000", 10)
}

type BenchmarkResult = {
  name: string
  iterations: number
  totalMs: number
  avgMs: number
  minMs: number
  maxMs: number
  stdDev: number
  cv: number // coefficient of variation (%)
}

function calculateStats(times: number[]): {
  avg: number
  min: number
  max: number
  stdDev: number
  cv: number
} {
  const avg = times.reduce((a, b) => a + b, 0) / times.length
  const min = Math.min(...times)
  const max = Math.max(...times)
  const variance = times.reduce((sum, t) => sum + (t - avg) ** 2, 0) / times.length
  const stdDev = Math.sqrt(variance)
  const cv = (stdDev / avg) * 100

  return { avg, min, max, stdDev, cv }
}

function runBenchmark(
  name: string,
  fn: () => void,
  iterations: number,
  warmupIterations: number
): BenchmarkResult {
  // Warmup phase
  for (let i = 0; i < warmupIterations; i++) {
    fn()
  }

  // Execute benchmark in batches for timing
  const batchSize = Math.max(1, Math.floor(iterations / 100))
  const batchTimes: number[] = []
  let totalMs = 0

  let remaining = iterations
  while (remaining > 0) {
    const batch = Math.min(batchSize, remaining)
    const start = performance.now()
    for (let i = 0; i < batch; i++) {
      fn()
    }
    const elapsed = performance.now() - start
    totalMs += elapsed
    batchTimes.push(elapsed / batch)
    remaining -= batch
  }

  const stats = calculateStats(batchTimes)

  return {
    name,
    iterations,
    totalMs,
    avgMs: stats.avg,
    minMs: stats.min,
    maxMs: stats.max,
    stdDev: stats.stdDev,
    cv: stats.cv
  }
}

function formatResult(result: BenchmarkResult): void {
  console.log(`${result.name}:`)
  console.log(`  Iterations: ${result.iterations.toLocaleString()}`)
  console.log(`  Total time: ${result.totalMs.toFixed(2)}ms`)
  console.log(`  Per call:   ${(result.avgMs * 1000).toFixed(3)}μs`)
  console.log(`  Min:        ${(result.minMs * 1000).toFixed(3)}μs`)
  console.log(`  Max:        ${(result.maxMs * 1000).toFixed(3)}μs`)
  console.log(`  Std Dev:    ${(result.stdDev * 1000).toFixed(3)}μs`)
  console.log(`  CV:         ${result.cv.toFixed(2)}%`)
  console.log()
}

// ---------------------------------------------------------------------------
// Benchmark fixtures
// ---------------------------------------------------------------------------

const encoder = new TextEncoder()
const sampleKey = encoder.encode("user-12345")
const sampleValue = encoder.encode(JSON.stringify({ event: "login", timestamp: 1710100000000 }))
const sampleHeader = { key: "source", value: encoder.encode("bench") }

// Pre-built record batch for decode benchmarks
const singleRecord = createRecord(sampleKey, sampleValue, [sampleHeader])
const singleBatch = buildRecordBatch([singleRecord])
const encodedSingleBatch = encodeRecordBatch(singleBatch)

const tenRecords = Array.from({ length: 10 }, (_, i) =>
  createRecord(encoder.encode(`key-${String(i)}`), sampleValue, [sampleHeader], i)
)
const tenBatch = buildRecordBatch(tenRecords)
const encodedTenBatch = encodeRecordBatch(tenBatch)

// Pre-built binary payload for reader benchmarks
const writerFixture = new BinaryWriter(256)
writerFixture
  .writeInt32(42)
  .writeInt64(123456789n)
  .writeCompactString("hello-kafka")
  .writeCompactBytes(sampleKey)
  .writeBoolean(true)
  .writeSignedVarInt(-1000)
  .writeUnsignedVarInt(2000)
const binaryPayload = writerFixture.finish()

function main(): void {
  console.log("=== Kafka Client Benchmarks ===\n")
  console.log(`Warmup iterations: ${String(config.warmupIterations)}`)
  console.log(`Benchmark iterations: ${config.benchmarkIterations.toLocaleString()}\n`)

  const results: BenchmarkResult[] = []

  // --- BinaryWriter ---

  const writerResult = runBenchmark(
    "BinaryWriter (int32 + int64 + string + bytes + bool + varints)",
    () => {
      const w = new BinaryWriter(256)
      w.writeInt32(42)
        .writeInt64(123456789n)
        .writeCompactString("hello-kafka")
        .writeCompactBytes(sampleKey)
        .writeBoolean(true)
        .writeSignedVarInt(-1000)
        .writeUnsignedVarInt(2000)
      w.finish()
    },
    config.benchmarkIterations,
    config.warmupIterations
  )
  results.push(writerResult)
  formatResult(writerResult)

  // --- BinaryReader ---

  const readerResult = runBenchmark(
    "BinaryReader (int32 + int64 + string + bytes + bool + varints)",
    () => {
      const r = new BinaryReader(binaryPayload)
      r.readInt32()
      r.readInt64()
      r.readCompactString()
      r.readCompactBytes()
      r.readBoolean()
      r.readSignedVarInt()
      r.readUnsignedVarInt()
    },
    config.benchmarkIterations,
    config.warmupIterations
  )
  results.push(readerResult)
  formatResult(readerResult)

  // --- Record creation ---

  const recordResult = runBenchmark(
    "createRecord (key + value + 1 header)",
    () => {
      createRecord(sampleKey, sampleValue, [sampleHeader])
    },
    config.benchmarkIterations,
    config.warmupIterations
  )
  results.push(recordResult)
  formatResult(recordResult)

  // --- RecordBatch encode (1 record) ---

  const encodeSingleResult = runBenchmark(
    "encodeRecordBatch (1 record)",
    () => {
      encodeRecordBatch(singleBatch)
    },
    config.benchmarkIterations,
    config.warmupIterations
  )
  results.push(encodeSingleResult)
  formatResult(encodeSingleResult)

  // --- RecordBatch decode (1 record) ---

  const decodeSingleResult = runBenchmark(
    "decodeRecordBatch (1 record)",
    () => {
      decodeRecordBatch(encodedSingleBatch)
    },
    config.benchmarkIterations,
    config.warmupIterations
  )
  results.push(decodeSingleResult)
  formatResult(decodeSingleResult)

  // --- RecordBatch encode (10 records) ---

  const encodeTenResult = runBenchmark(
    "encodeRecordBatch (10 records)",
    () => {
      encodeRecordBatch(tenBatch)
    },
    config.benchmarkIterations,
    config.warmupIterations
  )
  results.push(encodeTenResult)
  formatResult(encodeTenResult)

  // --- RecordBatch decode (10 records) ---

  const decodeTenResult = runBenchmark(
    "decodeRecordBatch (10 records)",
    () => {
      decodeRecordBatch(encodedTenBatch)
    },
    config.benchmarkIterations,
    config.warmupIterations
  )
  results.push(decodeTenResult)
  formatResult(decodeTenResult)

  // --- Full round-trip: create + build + encode + decode (1 record) ---

  const roundTripResult = runBenchmark(
    "round-trip: create → build → encode → decode (1 record)",
    () => {
      const rec = createRecord(sampleKey, sampleValue, [sampleHeader])
      const batch = buildRecordBatch([rec])
      const encoded = encodeRecordBatch(batch)
      decodeRecordBatch(encoded)
    },
    config.benchmarkIterations,
    config.warmupIterations
  )
  results.push(roundTripResult)
  formatResult(roundTripResult)

  // Summary
  console.log("=== Summary ===")
  console.log("Benchmark".padEnd(58) + "Avg (μs)".padStart(12) + "CV (%)".padStart(10))
  console.log("-".repeat(80))
  for (const r of results) {
    console.log(
      r.name.padEnd(58) + (r.avgMs * 1000).toFixed(3).padStart(12) + r.cv.toFixed(2).padStart(10)
    )
  }

  console.log("\nBenchmarks complete.")
}

main()
