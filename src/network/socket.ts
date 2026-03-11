/**
 * Socket adapter interface for runtime-agnostic TCP/TLS connections.
 *
 * Kafka requires a reliable byte stream (TCP) with optional TLS encryption.
 * This module defines the adapter types that runtime-specific implementations
 * (Bun, Node.js, Deno) must satisfy.
 *
 * The socket adapter uses a push-based data model: incoming bytes arrive via
 * the {@link SocketConnectOptions.onData} callback, while outgoing bytes are
 * sent explicitly via {@link KafkaSocket.write}.
 *
 * @packageDocumentation
 */

import type { TlsConfig } from "../config.js"

// ---------------------------------------------------------------------------
// Socket types
// ---------------------------------------------------------------------------

/**
 * A connected socket to a Kafka broker.
 *
 * Runtime adapters wrap their native socket types to implement this interface.
 * Data arrives via the {@link SocketConnectOptions.onData} callback provided
 * at connection time; this type only exposes write and close operations.
 */
export type KafkaSocket = {
  /** Send raw bytes to the broker. Resolves when the data is flushed. */
  readonly write: (data: Uint8Array) => Promise<void>
  /** Close the connection gracefully. */
  readonly close: () => Promise<void>
}

/**
 * Options for establishing a socket connection.
 */
export type SocketConnectOptions = {
  /** Hostname or IP address of the broker. */
  readonly host: string
  /** Port number of the broker. */
  readonly port: number
  /** TLS configuration. When provided with `enabled: true`, the connection uses TLS. */
  readonly tls?: TlsConfig
  /** Called when data arrives from the broker. */
  readonly onData: (data: Uint8Array) => void
  /** Called when a socket error occurs. */
  readonly onError: (error: Error) => void
  /** Called when the socket is closed (either side). */
  readonly onClose: () => void
}

/**
 * Factory function that creates a connected socket.
 *
 * Runtime adapters implement this to provide platform-specific TCP/TLS
 * connections. The returned promise must resolve only after the connection
 * (and TLS handshake, if applicable) is fully established.
 */
export type SocketFactory = (options: SocketConnectOptions) => Promise<KafkaSocket>
