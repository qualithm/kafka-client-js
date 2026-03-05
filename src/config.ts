/**
 * Configuration types for the Kafka client.
 *
 * @packageDocumentation
 */

/**
 * Address of a Kafka broker.
 */
export type BrokerAddress = {
  /** Hostname or IP address. */
  readonly host: string
  /** Port number (default: 9092). */
  readonly port: number
}

/**
 * SASL authentication mechanism.
 */
export type SaslMechanism = "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"

/**
 * SASL authentication configuration.
 */
export type SaslConfig = {
  /** SASL mechanism to use. */
  readonly mechanism: SaslMechanism
  /** SASL username. */
  readonly username: string
  /** SASL password. */
  readonly password: string
}

/**
 * TLS configuration for encrypted connections.
 */
export type TlsConfig = {
  /** Whether TLS is enabled. */
  readonly enabled: boolean
  /** CA certificate(s) in PEM format for server verification. */
  readonly ca?: string | readonly string[]
  /** Client certificate in PEM format for mutual TLS. */
  readonly cert?: string
  /** Client private key in PEM format for mutual TLS. */
  readonly key?: string
  /** Whether to reject unauthorised certificates (default: true). */
  readonly rejectUnauthorised?: boolean
}

/**
 * Top-level configuration for the Kafka client.
 */
export type KafkaConfig = {
  /** One or more bootstrap broker addresses. */
  readonly brokers: readonly BrokerAddress[] | readonly string[]
  /** Client identifier sent in all requests (default: "@qualithm/kafka-client"). */
  readonly clientId?: string
  /** Connection timeout in milliseconds (default: 30000). */
  readonly connectionTimeoutMs?: number
  /** Request timeout in milliseconds (default: 30000). */
  readonly requestTimeoutMs?: number
  /** SASL authentication configuration. */
  readonly sasl?: SaslConfig
  /** TLS configuration. */
  readonly tls?: TlsConfig
}

/**
 * Parse a broker address string in "host:port" format.
 * If no port is specified, defaults to 9092.
 */
export function parseBrokerAddress(address: string): BrokerAddress {
  const lastColon = address.lastIndexOf(":")
  if (lastColon === -1) {
    return { host: address || "localhost", port: 9092 }
  }

  const host = address.slice(0, lastColon)
  const portStr = address.slice(lastColon + 1)
  const port = Number.parseInt(portStr, 10)

  if (Number.isNaN(port) || port < 1 || port > 65535) {
    return { host: address, port: 9092 }
  }

  return { host: host || "localhost", port }
}
