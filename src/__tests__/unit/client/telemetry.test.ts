import { afterEach, describe, expect, it, vi } from "vitest"

import { type MetricsCollector, TelemetryReporter } from "../../../client/telemetry"
import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import type { ConnectionPool } from "../../../network/broker-pool"

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/** A non-zero UUID (16 bytes) returned by mock broker. */
const BROKER_UUID = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])

/**
 * Build an ApiVersions v0 response body advertising the given APIs.
 */
function buildApiVersionsBody(
  apis: { apiKey: number; minVersion: number; maxVersion: number }[]
): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt16(0) // error_code = 0
  w.writeInt32(apis.length)
  for (const api of apis) {
    w.writeInt16(api.apiKey)
    w.writeInt16(api.minVersion)
    w.writeInt16(api.maxVersion)
  }
  return w.finish()
}

/** Standard telemetry API versions. */
const TELEMETRY_APIS = [
  { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
  { apiKey: ApiKey.GetTelemetrySubscriptions, minVersion: 0, maxVersion: 0 },
  { apiKey: ApiKey.PushTelemetry, minVersion: 0, maxVersion: 0 }
]

/**
 * Build a GetTelemetrySubscriptions v0 response body (flexible encoding).
 */
function buildGetTelemetrySubscriptionsBody(opts?: {
  errorCode?: number
  clientInstanceId?: Uint8Array
  subscriptionId?: number
  acceptedCompressionTypes?: number[]
  pushIntervalMs?: number
  telemetryMaxBytes?: number
  deltaTemporality?: boolean
  requestedMetrics?: string[]
}): Uint8Array {
  const w = new BinaryWriter()
  // throttle_time_ms (INT32)
  w.writeInt32(0)
  // error_code (INT16)
  w.writeInt16(opts?.errorCode ?? 0)
  // client_instance_id (UUID)
  w.writeUuid(opts?.clientInstanceId ?? BROKER_UUID)
  // subscription_id (INT32)
  w.writeInt32(opts?.subscriptionId ?? 42)
  // accepted_compression_types (COMPACT_ARRAY of INT8)
  const compressionTypes = opts?.acceptedCompressionTypes ?? [0]
  w.writeUnsignedVarInt(compressionTypes.length + 1)
  for (const ct of compressionTypes) {
    w.writeInt8(ct)
  }
  // push_interval_ms (INT32)
  w.writeInt32(opts?.pushIntervalMs ?? 5000)
  // telemetry_max_bytes (INT32)
  w.writeInt32(opts?.telemetryMaxBytes ?? 1048576)
  // delta_temporality (BOOLEAN)
  w.writeBoolean(opts?.deltaTemporality ?? true)
  // requested_metrics (COMPACT_ARRAY of COMPACT_STRING)
  const metrics = opts?.requestedMetrics ?? []
  w.writeUnsignedVarInt(metrics.length + 1)
  for (const m of metrics) {
    w.writeCompactString(m)
  }
  // tagged fields
  w.writeUnsignedVarInt(0)
  return w.finish()
}

/**
 * Build a PushTelemetry v0 response body (flexible encoding).
 */
function buildPushTelemetryBody(errorCode = 0): Uint8Array {
  const w = new BinaryWriter()
  // throttle_time_ms (INT32)
  w.writeInt32(0)
  // error_code (INT16)
  w.writeInt16(errorCode)
  // tagged fields
  w.writeUnsignedVarInt(0)
  return w.finish()
}

/**
 * Create a mock connection that sequences through pre-built response bodies.
 */
function createMockConnection(responses: Uint8Array[]): {
  send: ReturnType<typeof vi.fn>
  broker: string
  connected: boolean
} {
  let callIndex = 0
  return {
    send: vi.fn(async () => {
      if (callIndex >= responses.length) {
        return Promise.reject(new Error("no more mock responses"))
      }
      return Promise.resolve(new BinaryReader(responses[callIndex++]))
    }),
    broker: "localhost:9092",
    connected: true
  }
}

/**
 * Create a mock connection pool.
 */
function createMockPool(overrides?: Partial<ConnectionPool>): ConnectionPool {
  return {
    brokers: new Map([[1, { host: "localhost", port: 9092, rack: null }]]),
    isClosed: false,
    connect: async () => Promise.resolve(),
    refreshMetadata: async () => Promise.resolve(),
    getConnection: () => {
      throw new Error("not implemented")
    },
    getConnectionByNodeId: () => {
      throw new Error("not implemented")
    },
    releaseConnection: () => {
      /* noop */
    },
    close: async () => Promise.resolve(),
    connectionCount: () => 0,
    ...overrides
  } as unknown as ConnectionPool
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("TelemetryReporter", () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it("constructs with config", () => {
    const pool = createMockPool()
    const collector: MetricsCollector = () => new Uint8Array(0)
    const reporter = new TelemetryReporter(pool, { collector })
    expect(reporter.isRunning).toBe(false)
    expect(reporter.currentSubscription).toBeNull()
  })

  it("starts and fetches subscription from broker", async () => {
    const mockConn = createMockConnection([
      buildApiVersionsBody(TELEMETRY_APIS),
      buildGetTelemetrySubscriptionsBody()
    ])

    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const collector: MetricsCollector = () => new Uint8Array(0)
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()

    expect(reporter.isRunning).toBe(true)
    expect(reporter.currentSubscription).not.toBeNull()
    expect(reporter.currentSubscription?.subscriptionId).toBe(42)
    expect(reporter.currentSubscription?.pushIntervalMs).toBe(5000)
    expect(reporter.currentSubscription?.deltaTemporality).toBe(true)

    // Cleanup
    // Need connection for the terminating push
    mockConn.send.mockReset()
    let pushCallIndex = 0
    const pushResponses = [buildApiVersionsBody(TELEMETRY_APIS), buildPushTelemetryBody()]
    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    mockConn.send.mockImplementation(async () => {
      return Promise.resolve(new BinaryReader(pushResponses[pushCallIndex++]))
    })
    await reporter.stop()
    expect(reporter.isRunning).toBe(false)
  })

  it("remains inactive if broker does not support telemetry", async () => {
    const mockConn = createMockConnection([
      buildApiVersionsBody([
        { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 }
        // No telemetry APIs advertised
      ])
    ])

    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const collector: MetricsCollector = () => new Uint8Array(0)
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()

    expect(reporter.isRunning).toBe(false)
    expect(reporter.currentSubscription).toBeNull()
  })

  it("remains inactive if subscription error code is non-zero", async () => {
    const mockConn = createMockConnection([
      buildApiVersionsBody(TELEMETRY_APIS),
      buildGetTelemetrySubscriptionsBody({ errorCode: 87 }) // TELEMETRY_TOO_LARGE or any error
    ])

    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const collector: MetricsCollector = () => new Uint8Array(0)
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()

    expect(reporter.isRunning).toBe(false)
  })

  it("remains inactive when pushIntervalMs is 0", async () => {
    const mockConn = createMockConnection([
      buildApiVersionsBody(TELEMETRY_APIS),
      buildGetTelemetrySubscriptionsBody({ pushIntervalMs: 0 })
    ])

    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const collector: MetricsCollector = () => new Uint8Array(0)
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()

    expect(reporter.isRunning).toBe(false)
  })

  it("stop is a no-op when not running", async () => {
    const pool = createMockPool()
    const collector: MetricsCollector = () => new Uint8Array(0)
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.stop()
    expect(reporter.isRunning).toBe(false)
  })

  it("start is idempotent", async () => {
    const mockConn = createMockConnection([
      buildApiVersionsBody(TELEMETRY_APIS),
      buildGetTelemetrySubscriptionsBody()
    ])

    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const collector: MetricsCollector = () => new Uint8Array(0)
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()
    expect(reporter.isRunning).toBe(true)

    // Second start should be a no-op (no additional broker calls)
    await reporter.start()
    expect(reporter.isRunning).toBe(true)
    // Only 2 send calls total (ApiVersions + GetTelemetrySubscriptions)
    expect(mockConn.send).toHaveBeenCalledTimes(2)

    // Cleanup
    mockConn.send.mockReset()
    let idx = 0
    const cleanup = [buildApiVersionsBody(TELEMETRY_APIS), buildPushTelemetryBody()]
    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    mockConn.send.mockImplementation(async () => Promise.resolve(new BinaryReader(cleanup[idx++])))
    await reporter.stop()
  })

  it("pushes metrics on interval", async () => {
    const allResponses = [
      // Start: ApiVersions + GetTelemetrySubscriptions
      buildApiVersionsBody(TELEMETRY_APIS),
      buildGetTelemetrySubscriptionsBody({ pushIntervalMs: 50 }),
      // First push: ApiVersions + PushTelemetry
      buildApiVersionsBody(TELEMETRY_APIS),
      buildPushTelemetryBody(),
      // Terminating push: ApiVersions + PushTelemetry
      buildApiVersionsBody(TELEMETRY_APIS),
      buildPushTelemetryBody()
    ]

    let responseIdx = 0
    const mockConn = {
      send: vi.fn(async () => {
        const body = allResponses[responseIdx++] as Uint8Array | undefined
        if (body === undefined) {
          // For any extra calls after exhaustion, return a no-op push response
          return Promise.resolve(new BinaryReader(buildPushTelemetryBody()))
        }
        return Promise.resolve(new BinaryReader(body))
      }),
      broker: "localhost:9092",
      connected: true
    }

    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const metricsPayload = new Uint8Array([0xde, 0xad])
    const collector = vi.fn(() => metricsPayload)
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()
    expect(reporter.isRunning).toBe(true)
    expect(collector).not.toHaveBeenCalled()

    // Wait for the push interval to fire (50ms + buffer)
    await new Promise((resolve) => {
      setTimeout(resolve, 80)
    })

    expect(collector).toHaveBeenCalledTimes(1)
    expect(collector).toHaveBeenCalledWith(reporter.currentSubscription)

    // Stop the reporter (sends terminating push)
    await reporter.stop()
    expect(reporter.isRunning).toBe(false)
    // Collector called once more for the terminating push
    expect(collector).toHaveBeenCalledTimes(2)
  })

  it("handles collector errors gracefully", async () => {
    const mockConn = createMockConnection([
      buildApiVersionsBody(TELEMETRY_APIS),
      buildGetTelemetrySubscriptionsBody({ pushIntervalMs: 50 })
    ])

    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const collector = vi.fn(() => {
      throw new Error("collector failed")
    })
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()
    expect(reporter.isRunning).toBe(true)

    // Wait for push interval — collector throws, but reporter should not crash
    await new Promise((resolve) => {
      setTimeout(resolve, 100)
    })
    expect(collector).toHaveBeenCalled()
    expect(reporter.isRunning).toBe(true)

    // Stop without sending terminating push (collector will throw again)
    await reporter.stop()
  })

  it("releases connections on errors", async () => {
    const mockConn = createMockConnection([
      buildApiVersionsBody(TELEMETRY_APIS),
      buildGetTelemetrySubscriptionsBody()
    ])

    const releaseFn = vi.fn()
    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: releaseFn as ConnectionPool["releaseConnection"]
    })

    const collector: MetricsCollector = () => new Uint8Array(0)
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()

    // Connection should have been released after fetching subscription
    expect(releaseFn).toHaveBeenCalled()

    // Cleanup
    mockConn.send.mockReset()
    let idx = 0
    const cleanup = [buildApiVersionsBody(TELEMETRY_APIS), buildPushTelemetryBody()]
    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    mockConn.send.mockImplementation(async () => Promise.resolve(new BinaryReader(cleanup[idx++])))
    await reporter.stop()
  })

  it("remains inactive when no brokers are available", async () => {
    const pool = createMockPool({
      brokers: new Map() as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(async () =>
        Promise.reject(new Error("no brokers"))
      ) as unknown as ConnectionPool["getConnectionByNodeId"]
    })

    const collector: MetricsCollector = () => new Uint8Array(0)
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()
    expect(reporter.isRunning).toBe(false)
  })

  it("sends terminating push on stop", async () => {
    const startConn = createMockConnection([
      buildApiVersionsBody(TELEMETRY_APIS),
      buildGetTelemetrySubscriptionsBody({ pushIntervalMs: 60000 })
    ])
    const stopConn = createMockConnection([
      buildApiVersionsBody(TELEMETRY_APIS),
      buildPushTelemetryBody()
    ])

    let connIndex = 0
    const connections = [startConn, stopConn]
    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(connections[connIndex++])
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const metricsPayload = new Uint8Array([0xca, 0xfe])
    const collector = vi.fn(() => metricsPayload)
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()
    expect(reporter.isRunning).toBe(true)

    await reporter.stop()
    expect(reporter.isRunning).toBe(false)
    // Collector called once for terminating push
    expect(collector).toHaveBeenCalledTimes(1)

    // Verify the PushTelemetry call was made (second call on stopConn)
    expect(stopConn.send).toHaveBeenCalledTimes(2) // ApiVersions + PushTelemetry
    const pushCall = stopConn.send.mock.calls[1]
    expect(pushCall[0]).toBe(ApiKey.PushTelemetry)
  })

  it("handles decode failure on push response gracefully", async () => {
    // Truncated response that will fail to decode
    const truncatedPushResponse = new Uint8Array([0x00])

    const allResponses = [
      // Start: ApiVersions + GetTelemetrySubscriptions
      buildApiVersionsBody(TELEMETRY_APIS),
      buildGetTelemetrySubscriptionsBody({ pushIntervalMs: 50 }),
      // Push: ApiVersions + truncated PushTelemetry response
      buildApiVersionsBody(TELEMETRY_APIS),
      truncatedPushResponse,
      // Terminating push: ApiVersions + PushTelemetry
      buildApiVersionsBody(TELEMETRY_APIS),
      buildPushTelemetryBody()
    ]

    let responseIdx = 0
    const mockConn = {
      send: vi.fn(async () => {
        const body = allResponses[responseIdx++] as Uint8Array | undefined
        if (body === undefined) {
          return Promise.resolve(new BinaryReader(buildPushTelemetryBody()))
        }
        return Promise.resolve(new BinaryReader(body))
      }),
      broker: "localhost:9092",
      connected: true
    }

    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const collector = vi.fn(() => new Uint8Array([0x01]))
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()
    expect(reporter.isRunning).toBe(true)

    // Wait for the push interval to fire — push response decode will fail
    await new Promise((resolve) => {
      setTimeout(resolve, 80)
    })

    // Reporter should still be running despite decode failure
    expect(reporter.isRunning).toBe(true)

    await reporter.stop()
  })

  it("re-fetches subscription on non-zero push error code", async () => {
    const allResponses = [
      // Start: ApiVersions + GetTelemetrySubscriptions
      buildApiVersionsBody(TELEMETRY_APIS),
      buildGetTelemetrySubscriptionsBody({ pushIntervalMs: 50, subscriptionId: 42 }),
      // Push: ApiVersions + PushTelemetry with error
      buildApiVersionsBody(TELEMETRY_APIS),
      buildPushTelemetryBody(47), // non-zero error code
      // Re-fetch subscription: ApiVersions + GetTelemetrySubscriptions (new subscriptionId)
      buildApiVersionsBody(TELEMETRY_APIS),
      buildGetTelemetrySubscriptionsBody({ pushIntervalMs: 50, subscriptionId: 99 }),
      // Terminating push: ApiVersions + PushTelemetry
      buildApiVersionsBody(TELEMETRY_APIS),
      buildPushTelemetryBody()
    ]

    let responseIdx = 0
    const mockConn = {
      send: vi.fn(async () => {
        const body = allResponses[responseIdx++] as Uint8Array | undefined
        if (body === undefined) {
          return Promise.resolve(new BinaryReader(buildPushTelemetryBody()))
        }
        return Promise.resolve(new BinaryReader(body))
      }),
      broker: "localhost:9092",
      connected: true
    }

    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const collector = vi.fn(() => new Uint8Array([0x01]))
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()
    expect(reporter.currentSubscription?.subscriptionId).toBe(42)

    // Wait for push interval — push returns error, triggers re-subscription
    await new Promise((resolve) => {
      setTimeout(resolve, 80)
    })

    // Subscription should have been updated
    expect(reporter.currentSubscription?.subscriptionId).toBe(99)
    expect(reporter.isRunning).toBe(true)

    await reporter.stop()
  })

  it("skips version negotiation result when negotiateVersion returns null", async () => {
    // Advertise telemetry APIs with a version range that won't negotiate
    // (maxVersion < minVersion is invalid but forces negotiateVersion to return null)
    const mockConn = createMockConnection([
      buildApiVersionsBody([
        { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
        { apiKey: ApiKey.GetTelemetrySubscriptions, minVersion: 100, maxVersion: 100 },
        { apiKey: ApiKey.PushTelemetry, minVersion: 100, maxVersion: 100 }
      ])
    ])

    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const collector: MetricsCollector = () => new Uint8Array(0)
    const reporter = new TelemetryReporter(pool, { collector })

    // Start should fail gracefully — negotiation yields no usable version
    await reporter.start()
    expect(reporter.isRunning).toBe(false)
  })

  it("returns early from push when PushTelemetry version is unavailable", async () => {
    // Start: broker supports GetTelemetrySubscriptions but NOT PushTelemetry
    const startApis = [
      { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
      { apiKey: ApiKey.GetTelemetrySubscriptions, minVersion: 0, maxVersion: 0 },
      { apiKey: ApiKey.PushTelemetry, minVersion: 0, maxVersion: 0 }
    ]
    // During push: broker stops advertising PushTelemetry
    const pushApis = [
      { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
      { apiKey: ApiKey.GetTelemetrySubscriptions, minVersion: 0, maxVersion: 0 }
      // PushTelemetry missing
    ]

    const allResponses = [
      // Start: ApiVersions + GetTelemetrySubscriptions
      buildApiVersionsBody(startApis),
      buildGetTelemetrySubscriptionsBody({ pushIntervalMs: 50 }),
      // Push: ApiVersions (without PushTelemetry) — should return early
      buildApiVersionsBody(pushApis),
      // Terminating push: same (no PushTelemetry)
      buildApiVersionsBody(pushApis)
    ]

    let responseIdx = 0
    const mockConn = {
      send: vi.fn(async () => {
        const body = allResponses[responseIdx++] as Uint8Array | undefined
        if (body === undefined) {
          return Promise.resolve(new BinaryReader(buildApiVersionsBody(pushApis)))
        }
        return Promise.resolve(new BinaryReader(body))
      }),
      broker: "localhost:9092",
      connected: true
    }

    const pool = createMockPool({
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(mockConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const collector = vi.fn(() => new Uint8Array([0x01]))
    const reporter = new TelemetryReporter(pool, { collector })

    await reporter.start()
    expect(reporter.isRunning).toBe(true)

    // Wait for push interval — pushVersion undefined → returns early
    await new Promise((resolve) => {
      setTimeout(resolve, 80)
    })

    expect(reporter.isRunning).toBe(true)

    await reporter.stop()
  })
})
