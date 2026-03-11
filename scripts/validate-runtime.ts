#!/usr/bin/env bun
/**
 * Cross-runtime validation script.
 *
 * Tests that the library can be imported and used correctly across
 * different JavaScript runtimes (Bun, Node.js, Deno).
 *
 * Run with: bun run scripts/validate-runtime.ts
 */

import { spawn } from "node:child_process"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"

// ── Test Code ─────────────────────────────────────────────────────────

function getTestCode(importPath: string): string {
  return `
import {
  // Result types
  decodeFailure,
  decodeSuccess,

  // Errors
  KafkaError,
  KafkaConnectionError,
  KafkaProtocolError,
  KafkaTimeoutError,
  KafkaConfigError,

  // Configuration
  parseBrokerAddress,

  // Protocol
  ApiKey,
  CLIENT_API_VERSIONS,
  negotiateVersion,

  // Binary codec
  BinaryReader,
  BinaryWriter,

  // Protocol framing
  encodeRequestHeader,
  decodeResponseHeader,
  frameRequest,

  // API codecs
  buildApiVersionsRequest,
  buildMetadataRequest,
  buildFetchRequest,
  buildProduceRequest,

  // Record batches
  CompressionCodec,
  RECORD_BATCH_MAGIC,

  // Connection
  KafkaConnection,

  // Broker pool
  ConnectionPool,

  // Kafka client
  Kafka,
  createKafka,

  // Producer
  KafkaProducer,
  createProducer,

  // Consumer
  KafkaConsumer,
  createConsumer,
  OffsetResetStrategy,

  // Admin
  KafkaAdmin,
  createAdmin,

  // Serialization
  stringSerializer,
  jsonSerializer,

  // Schema Registry
  SchemaRegistry,
} from "${importPath}";

// Verify exports exist
const checks = [
  // Result
  ["decodeFailure", typeof decodeFailure === "function"],
  ["decodeSuccess", typeof decodeSuccess === "function"],

  // Errors
  ["KafkaError", typeof KafkaError === "function"],
  ["KafkaConnectionError", typeof KafkaConnectionError === "function"],
  ["KafkaProtocolError", typeof KafkaProtocolError === "function"],
  ["KafkaTimeoutError", typeof KafkaTimeoutError === "function"],
  ["KafkaConfigError", typeof KafkaConfigError === "function"],

  // Configuration
  ["parseBrokerAddress", typeof parseBrokerAddress === "function"],

  // Protocol
  ["ApiKey", typeof ApiKey === "object"],
  ["CLIENT_API_VERSIONS", typeof CLIENT_API_VERSIONS === "object"],
  ["negotiateVersion", typeof negotiateVersion === "function"],

  // Binary codec
  ["BinaryReader", typeof BinaryReader === "function"],
  ["BinaryWriter", typeof BinaryWriter === "function"],

  // Protocol framing
  ["encodeRequestHeader", typeof encodeRequestHeader === "function"],
  ["decodeResponseHeader", typeof decodeResponseHeader === "function"],
  ["frameRequest", typeof frameRequest === "function"],

  // API codecs
  ["buildApiVersionsRequest", typeof buildApiVersionsRequest === "function"],
  ["buildMetadataRequest", typeof buildMetadataRequest === "function"],
  ["buildFetchRequest", typeof buildFetchRequest === "function"],
  ["buildProduceRequest", typeof buildProduceRequest === "function"],

  // Record batches
  ["CompressionCodec", typeof CompressionCodec === "object"],
  ["RECORD_BATCH_MAGIC", typeof RECORD_BATCH_MAGIC === "number"],

  // Connection
  ["KafkaConnection", typeof KafkaConnection === "function"],

  // Broker pool
  ["ConnectionPool", typeof ConnectionPool === "function"],

  // Client classes
  ["Kafka", typeof Kafka === "function"],
  ["createKafka", typeof createKafka === "function"],
  ["KafkaProducer", typeof KafkaProducer === "function"],
  ["createProducer", typeof createProducer === "function"],
  ["KafkaConsumer", typeof KafkaConsumer === "function"],
  ["createConsumer", typeof createConsumer === "function"],
  ["OffsetResetStrategy", typeof OffsetResetStrategy === "object"],
  ["KafkaAdmin", typeof KafkaAdmin === "function"],
  ["createAdmin", typeof createAdmin === "function"],

  // Serialization
  ["stringSerializer", typeof stringSerializer === "object"],
  ["jsonSerializer", typeof jsonSerializer === "function"],

  // Schema Registry
  ["SchemaRegistry", typeof SchemaRegistry === "function"],
];

let passed = 0;
let failed = 0;

for (const [name, ok] of checks) {
  if (ok) {
    passed++;
  } else {
    failed++;
    console.error(\`FAIL: \${name} not exported correctly\`);
  }
}

// Test BinaryWriter/Reader round-trip
try {
  const writer = new BinaryWriter();
  writer.writeInt32(42);
  writer.writeInt16(7);
  writer.writeString("hello");
  const bytes = writer.finish();

  const reader = new BinaryReader(bytes);
  const i32 = reader.readInt32();
  const i16 = reader.readInt16();
  const str = reader.readString();

  if (i32.ok && i32.value === 42 && i16.ok && i16.value === 7 && str.ok && str.value === "hello") {
    passed++;
  } else {
    throw new Error("BinaryWriter/Reader round-trip failed");
  }
} catch (error) {
  failed++;
  console.error("FAIL: BinaryWriter/Reader threw:", error);
}

// Test parseBrokerAddress
try {
  const addr = parseBrokerAddress("localhost:9092");
  if (addr.host === "localhost" && addr.port === 9092) {
    passed++;
  } else {
    throw new Error("parseBrokerAddress returned incorrect result");
  }
} catch (error) {
  failed++;
  console.error("FAIL: parseBrokerAddress threw:", error);
}

// Test error classes
try {
  const connError = new KafkaConnectionError("test");
  const protoError = new KafkaProtocolError("test", 1);
  const timeoutError = new KafkaTimeoutError("test");

  if (
    connError instanceof KafkaError &&
    protoError instanceof KafkaError &&
    timeoutError instanceof KafkaError
  ) {
    passed++;
  } else {
    throw new Error("Error hierarchy incorrect");
  }
} catch (error) {
  failed++;
  console.error("FAIL: Error classes threw:", error);
}

// Test serialization round-trip
try {
  const encoded = stringSerializer.serialize("hello kafka", "test-topic");
  const decoded = stringSerializer.deserialize(encoded, "test-topic");
  if (decoded === "hello kafka") {
    passed++;
  } else {
    throw new Error("stringSerializer round-trip failed");
  }
} catch (error) {
  failed++;
  console.error("FAIL: stringSerializer threw:", error);
}

console.log(\`Passed: \${passed}, Failed: \${failed}\`);
process.exit(failed > 0 ? 1 : 0);
`.trim()
}

// ── Runtime Detection ─────────────────────────────────────────────────

type RuntimeInfo = {
  name: string
  command: string
  args: string[]
  available: boolean
  version?: string
}

async function checkRuntime(
  name: string,
  command: string,
  versionArg: string
): Promise<RuntimeInfo> {
  return new Promise((resolve) => {
    const proc = spawn(command, [versionArg], { stdio: ["ignore", "pipe", "ignore"] })
    let version = ""

    proc.stdout.on("data", (data: Buffer) => {
      version += data.toString()
    })

    proc.on("error", () => {
      resolve({ name, command, args: [], available: false })
    })

    proc.on("close", (code) => {
      resolve({
        name,
        command,
        args: [],
        available: code === 0,
        version: version.trim().split("\n")[0]
      })
    })
  })
}

// ── Test Runner ───────────────────────────────────────────────────────

async function runTest(
  runtime: RuntimeInfo,
  testFile: string,
  testDir: string,
  importMapPath: string
): Promise<{ success: boolean; output: string }> {
  return new Promise((resolve) => {
    let args = [...runtime.args]
    // Add import map for Deno
    if (runtime.name === "Deno") {
      args = ["run", "--allow-read", "--allow-env", "--allow-net", `--import-map=${importMapPath}`]
    }
    args.push(testFile)

    const proc = spawn(runtime.command, args, { cwd: testDir, stdio: ["ignore", "pipe", "pipe"] })
    let output = ""

    proc.stdout.on("data", (data: Buffer) => {
      output += data.toString()
    })
    proc.stderr.on("data", (data: Buffer) => {
      output += data.toString()
    })

    proc.on("error", (error) => {
      resolve({ success: false, output: error.message })
    })

    proc.on("close", (code) => {
      resolve({ success: code === 0, output: output.trim() })
    })
  })
}

// ── Main ──────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log("Cross-Runtime Validation")
  console.log("========================\n")

  // Check available runtimes
  const runtimes: RuntimeInfo[] = await Promise.all([
    checkRuntime("Bun", "bun", "--version").then((r) => ({ ...r, args: ["run"] })),
    checkRuntime("Node.js", "node", "--version").then((r) => ({
      ...r,
      args: ["--experimental-vm-modules"]
    })),
    checkRuntime("Deno", "deno", "--version").then((r) => ({
      ...r,
      args: ["run", "--allow-read", "--allow-env", "--allow-net", "--node-modules-dir=auto"]
    }))
  ])

  console.log("Available runtimes:")
  for (const runtime of runtimes) {
    const status = runtime.available ? `✓ ${runtime.version ?? "unknown"}` : "✗ not found"
    console.log(`  ${runtime.name}: ${status}`)
  }
  console.log()

  const available = runtimes.filter((r) => r.available)
  if (available.length === 0) {
    console.error("No runtimes available for testing")
    process.exit(1)
  }

  // Create temporary test directory
  const tmpDir = await mkdtemp(join(tmpdir(), "kafka-client-test-"))
  const distPath = join(process.cwd(), "dist", "index.js")

  try {
    // Write test file that imports directly from dist using absolute path
    const testFile = join(tmpDir, "test.mjs")
    await writeFile(testFile, getTestCode(distPath))

    // Create import map for Deno (no external dependencies needed)
    const importMapPath = join(tmpDir, "import_map.json")
    await writeFile(
      importMapPath,
      JSON.stringify({
        imports: {}
      })
    )

    // Run tests
    console.log("Running validation tests:")
    console.log("-".repeat(40))

    let passed = 0
    let failed = 0

    for (const runtime of available) {
      process.stdout.write(`${runtime.name}: `)
      const result = await runTest(runtime, testFile, tmpDir, importMapPath)

      if (result.success) {
        console.log("✓ PASS")
        passed++
      } else {
        console.log("✗ FAIL")
        console.log(`  Output: ${result.output}`)
        failed++
      }
    }

    console.log("-".repeat(40))
    console.log()
    console.log(`Results: ${String(passed)} passed, ${String(failed)} failed`)
    console.log()

    if (failed > 0) {
      process.exit(1)
    }
  } finally {
    // Cleanup
    await rm(tmpDir, { recursive: true, force: true })
  }
}

main().catch((error: unknown) => {
  console.error("Validation failed:", error)
  process.exit(1)
})
