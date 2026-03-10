/**
 * Integration test setup and helpers.
 *
 * Provides a shared Kafka connection for integration tests.
 * Requires a running Kafka broker (see docker-compose.yml).
 *
 * Usage:
 *   import { getBroker, createIntegrationConnection, isKafkaAvailable } from "./setup"
 */

import { createNodeSocketFactory } from "../../node-socket"
import type { SocketFactory } from "../../socket"

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Broker address for integration tests, configurable via env vars. */
export function getBroker(): { host: string; port: number } {
  // eslint-disable-next-line @typescript-eslint/dot-notation
  const host = process.env["KAFKA_HOST"] ?? "localhost"
  // eslint-disable-next-line @typescript-eslint/dot-notation
  const port = Number(process.env["KAFKA_PORT"] ?? "9092")
  return { host, port }
}

/** Socket factory for integration tests (Node.js runtime). */
export function getSocketFactory(): SocketFactory {
  return createNodeSocketFactory()
}

// ---------------------------------------------------------------------------
// Health check
// ---------------------------------------------------------------------------

/**
 * Check if Kafka is reachable by attempting a TCP connection.
 * Returns true if the broker accepts a connection within the timeout.
 */
export async function isKafkaAvailable(timeoutMs = 3000): Promise<boolean> {
  const { host, port } = getBroker()
  const { createConnection } = await import("node:net")

  return new Promise((resolve) => {
    const socket = createConnection({ host, port, timeout: timeoutMs })

    socket.on("connect", () => {
      socket.destroy()
      resolve(true)
    })

    socket.on("timeout", () => {
      socket.destroy()
      resolve(false)
    })

    socket.on("error", () => {
      socket.destroy()
      resolve(false)
    })
  })
}

// ---------------------------------------------------------------------------
// Topic helpers
// ---------------------------------------------------------------------------

let topicCounter = 0

/** Generate a unique topic name for a test run. */
export function uniqueTopic(prefix = "test"): string {
  return `${prefix}-${String(Date.now())}-${String(++topicCounter)}`
}
