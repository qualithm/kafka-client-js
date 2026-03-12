/**
 * Integration test setup and helpers.
 *
 * Provides a shared Kafka connection for integration tests.
 * Requires a running Kafka broker (see docker-compose.yml).
 *
 * Usage:
 *   import { getBroker, createIntegrationConnection, isKafkaAvailable } from "./setup"
 */

import type { SaslConfig } from "../../config"
import { createNodeSocketFactory } from "../../network/node-socket"
import type { SocketFactory } from "../../network/socket"

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Broker address for integration tests, configurable via env vars. */
export function getBroker(): { host: string; port: number } {
  const host = process.env.KAFKA_HOST ?? "localhost"
  const port = Number(process.env.KAFKA_PORT ?? "9092")
  return { host, port }
}

/** SASL broker address, configurable via env vars (default: localhost:9094). */
export function getSaslBroker(): { host: string; port: number } {
  const host = process.env.KAFKA_SASL_HOST ?? "localhost"
  const port = Number(process.env.KAFKA_SASL_PORT ?? "9094")
  return { host, port }
}

/** SASL/PLAIN credentials for integration tests. */
export function getSaslPlainConfig(): SaslConfig {
  return {
    mechanism: "PLAIN",
    username: process.env.KAFKA_SASL_USERNAME ?? "testuser",
    password: process.env.KAFKA_SASL_PASSWORD ?? "testpassword"
  }
}

/** SASL/SCRAM-SHA-256 credentials for integration tests. */
export function getSaslScram256Config(): SaslConfig {
  return {
    mechanism: "SCRAM-SHA-256",
    username: process.env.KAFKA_SASL_USERNAME ?? "testuser",
    password: process.env.KAFKA_SASL_PASSWORD ?? "testpassword"
  }
}

/** SASL/SCRAM-SHA-512 credentials for integration tests. */
export function getSaslScram512Config(): SaslConfig {
  return {
    mechanism: "SCRAM-SHA-512",
    username: process.env.KAFKA_SASL_USERNAME ?? "testuser",
    password: process.env.KAFKA_SASL_PASSWORD ?? "testpassword"
  }
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
  return isBrokerReachable(host, port, timeoutMs)
}

/**
 * Check if the SASL broker is reachable by attempting a TCP connection.
 * Returns true if the broker accepts a connection within the timeout.
 */
export async function isSaslBrokerAvailable(timeoutMs = 3000): Promise<boolean> {
  const { host, port } = getSaslBroker()
  return isBrokerReachable(host, port, timeoutMs)
}

/** @internal */
async function isBrokerReachable(host: string, port: number, timeoutMs: number): Promise<boolean> {
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
