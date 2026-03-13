/**
 * Opt-in client telemetry reporting (KIP-714).
 *
 * The {@link TelemetryReporter} periodically pushes client metrics to the
 * broker using the GetTelemetrySubscriptions / PushTelemetry APIs. Telemetry
 * is entirely opt-in — it only activates when a {@link TelemetryConfig} is
 * supplied to a producer or consumer.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability
 *
 * @packageDocumentation
 */

import { ApiKey, negotiateVersion } from "../codec/api-keys.js"
import type { BinaryReader } from "../codec/binary-reader.js"
import type { BinaryWriter } from "../codec/binary-writer.js"
import { KafkaError } from "../errors.js"
import type { ConnectionPool } from "../network/broker-pool.js"
import { apiVersionsToMap, decodeApiVersionsResponse } from "../protocol/api-versions.js"
import {
  decodeGetTelemetrySubscriptionsResponse,
  encodeGetTelemetrySubscriptionsRequest,
  type GetTelemetrySubscriptionsResponse
} from "../protocol/get-telemetry-subscriptions.js"
import {
  decodePushTelemetryResponse,
  encodePushTelemetryRequest
} from "../protocol/push-telemetry.js"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Telemetry subscription state received from the broker.
 */
export type TelemetrySubscription = {
  /** Broker-assigned client instance ID (UUID, 16 bytes). */
  readonly clientInstanceId: Uint8Array
  /** Subscription ID used to correlate with PushTelemetry. */
  readonly subscriptionId: number
  /** Compression types accepted by the broker for pushed metrics. */
  readonly acceptedCompressionTypes: readonly number[]
  /** Interval at which the client should push telemetry, in milliseconds. */
  readonly pushIntervalMs: number
  /** Maximum size of serialised metrics in bytes. */
  readonly telemetryMaxBytes: number
  /** Whether the broker requests delta temporality for metrics. */
  readonly deltaTemporality: boolean
  /** Metric name prefixes the broker wants reported (empty = all metrics). */
  readonly requestedMetrics: readonly string[]
}

/**
 * Callback that collects client metrics and returns them as a serialised
 * payload (OTLP format). Called on each push interval.
 *
 * @param subscription - The current telemetry subscription from the broker.
 * @returns Serialised metrics payload, or an empty `Uint8Array` to skip the push.
 */
export type MetricsCollector = (
  subscription: TelemetrySubscription
) => Uint8Array | Promise<Uint8Array>

/**
 * Configuration for opt-in client telemetry (KIP-714).
 */
export type TelemetryConfig = {
  /**
   * Callback that collects and serialises client metrics.
   *
   * The reporter calls this on each push interval. Return an empty
   * `Uint8Array` to skip the push.
   */
  readonly collector: MetricsCollector
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Zero UUID used for the initial GetTelemetrySubscriptions request. */
const ZERO_UUID = new Uint8Array(16)

// ---------------------------------------------------------------------------
// TelemetryReporter
// ---------------------------------------------------------------------------

/**
 * Client telemetry reporter (KIP-714).
 *
 * Discovers the broker's telemetry subscription via
 * `GetTelemetrySubscriptions`, then periodically pushes collected metrics
 * via `PushTelemetry`. On shutdown, sends a final push with
 * `terminating: true`.
 *
 * @example
 * ```ts
 * const reporter = new TelemetryReporter(pool, {
 *   collector: (sub) => mySerialiseMetrics(sub),
 * })
 * await reporter.start()
 * // ... later ...
 * await reporter.stop()
 * ```
 */
export class TelemetryReporter {
  private readonly pool: ConnectionPool
  private readonly collector: MetricsCollector
  private subscription: TelemetrySubscription | null = null
  private pushTimer: ReturnType<typeof setInterval> | null = null
  private running = false

  constructor(pool: ConnectionPool, config: TelemetryConfig) {
    this.pool = pool
    this.collector = config.collector
  }

  /** Whether the reporter is actively pushing telemetry. */
  get isRunning(): boolean {
    return this.running
  }

  /** The current telemetry subscription, or `null` if not yet subscribed. */
  get currentSubscription(): TelemetrySubscription | null {
    return this.subscription
  }

  /**
   * Start the telemetry reporter.
   *
   * Fetches the broker's telemetry subscription and begins periodic pushes.
   * If the broker does not support telemetry, the reporter silently remains
   * inactive.
   */
  async start(): Promise<void> {
    if (this.running) {
      return
    }

    try {
      this.subscription = await this.fetchSubscription(ZERO_UUID)
    } catch {
      // Broker does not support telemetry — remain inactive
      return
    }

    if (this.subscription.pushIntervalMs <= 0) {
      return
    }

    this.running = true
    this.startPushTimer()
  }

  /**
   * Stop the telemetry reporter.
   *
   * Sends a final terminating push and stops the interval timer.
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return
    }

    this.stopPushTimer()
    this.running = false

    if (this.subscription) {
      try {
        await this.pushMetrics(true)
      } catch {
        // best-effort terminating push
      }
    }

    this.subscription = null
  }

  // -----------------------------------------------------------------------
  // Internal
  // -----------------------------------------------------------------------

  private startPushTimer(): void {
    this.stopPushTimer()
    const intervalMs = this.subscription?.pushIntervalMs ?? 60_000
    this.pushTimer = setInterval(() => {
      void this.pushMetrics(false)
    }, intervalMs)
  }

  private stopPushTimer(): void {
    if (this.pushTimer !== null) {
      clearInterval(this.pushTimer)
      this.pushTimer = null
    }
  }

  /**
   * Fetch the telemetry subscription from the broker.
   */
  private async fetchSubscription(clientInstanceId: Uint8Array): Promise<TelemetrySubscription> {
    const conn = await this.getAnyConnection()
    try {
      const apiVersions = await this.negotiateTelemetryVersions(conn)
      const subscriptionVersion = apiVersions.get(ApiKey.GetTelemetrySubscriptions)
      if (subscriptionVersion === undefined) {
        throw new KafkaError("broker does not support GetTelemetrySubscriptions", false)
      }

      const responseReader = await conn.send(
        ApiKey.GetTelemetrySubscriptions,
        subscriptionVersion,
        (writer) => {
          encodeGetTelemetrySubscriptionsRequest(writer, { clientInstanceId }, subscriptionVersion)
        }
      )

      const result = decodeGetTelemetrySubscriptionsResponse(responseReader, subscriptionVersion)
      if (!result.ok) {
        throw new KafkaError(
          `failed to decode GetTelemetrySubscriptions response: ${result.error.message}`,
          false
        )
      }

      if (result.value.errorCode !== 0) {
        throw new KafkaError(
          `GetTelemetrySubscriptions error code ${String(result.value.errorCode)}`,
          false
        )
      }

      return toSubscription(result.value)
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Push collected metrics to the broker.
   */
  private async pushMetrics(terminating: boolean): Promise<void> {
    if (!this.subscription) {
      return
    }

    let metrics: Uint8Array
    try {
      metrics = await this.collector(this.subscription)
    } catch {
      return
    }

    const conn = await this.getAnyConnection()
    try {
      const apiVersions = await this.negotiateTelemetryVersions(conn)
      const pushVersion = apiVersions.get(ApiKey.PushTelemetry)
      if (pushVersion === undefined) {
        return
      }

      const sub = this.subscription
      const responseReader = await conn.send(ApiKey.PushTelemetry, pushVersion, (writer) => {
        encodePushTelemetryRequest(
          writer,
          {
            clientInstanceId: sub.clientInstanceId,
            subscriptionId: sub.subscriptionId,
            terminating,
            compressionType: 0,
            metrics
          },
          pushVersion
        )
      })

      const result = decodePushTelemetryResponse(responseReader, pushVersion)
      if (!result.ok) {
        return
      }

      // Re-fetch subscription if broker signals an error
      if (result.value.errorCode !== 0 && !terminating) {
        try {
          this.subscription = await this.fetchSubscription(this.subscription.clientInstanceId)
          // Restart timer with potentially updated interval
          if (this.running) {
            this.startPushTimer()
          }
        } catch {
          // best-effort re-subscribe
        }
      }
    } finally {
      this.pool.releaseConnection(conn)
    }
  }

  /**
   * Get a connection to any known broker.
   */
  private async getAnyConnection(): Promise<
    Awaited<ReturnType<ConnectionPool["getConnectionByNodeId"]>>
  > {
    const { brokers } = this.pool
    for (const [nodeId] of brokers) {
      try {
        return await this.pool.getConnectionByNodeId(nodeId)
      } catch {
        continue
      }
    }
    throw new KafkaError("no available brokers for telemetry", false)
  }

  /**
   * Negotiate API versions for telemetry APIs on the given connection.
   */
  private async negotiateTelemetryVersions(conn: {
    send: (
      apiKey: ApiKey,
      apiVersion: number,
      encode: (writer: BinaryWriter) => void
    ) => Promise<BinaryReader>
  }): Promise<Map<number, number>> {
    const versionReader = await conn.send(ApiKey.ApiVersions, 0, () => {
      // ApiVersions v0 has an empty request body
    })
    const versionResult = decodeApiVersionsResponse(versionReader, 0)
    if (!versionResult.ok) {
      throw new KafkaError("failed to decode ApiVersions response", false)
    }

    const versionMap = apiVersionsToMap(versionResult.value)
    const result = new Map<number, number>()

    const subscriptionRange = versionMap.get(ApiKey.GetTelemetrySubscriptions)
    if (subscriptionRange) {
      const version = negotiateVersion(ApiKey.GetTelemetrySubscriptions, subscriptionRange)
      if (version !== null) {
        result.set(ApiKey.GetTelemetrySubscriptions, version)
      }
    }

    const pushRange = versionMap.get(ApiKey.PushTelemetry)
    if (pushRange) {
      const version = negotiateVersion(ApiKey.PushTelemetry, pushRange)
      if (version !== null) {
        result.set(ApiKey.PushTelemetry, version)
      }
    }

    return result
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function toSubscription(response: GetTelemetrySubscriptionsResponse): TelemetrySubscription {
  return {
    clientInstanceId: response.clientInstanceId,
    subscriptionId: response.subscriptionId,
    acceptedCompressionTypes: response.acceptedCompressionTypes,
    pushIntervalMs: response.pushIntervalMs,
    telemetryMaxBytes: response.telemetryMaxBytes,
    deltaTemporality: response.deltaTemporality,
    requestedMetrics: response.requestedMetrics
  }
}
