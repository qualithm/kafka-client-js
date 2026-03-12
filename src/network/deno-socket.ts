/**
 * Deno runtime socket adapter for {@link SocketFactory}.
 *
 * Wraps `Deno.connect()` and `Deno.connectTls()` to provide the push-based
 * socket interface expected by {@link KafkaConnection}. Supports both plain
 * TCP and TLS connections.
 *
 * This module declares minimal Deno namespace types so it can compile without
 * `@types/deno` installed — the full Deno types are available at runtime.
 *
 * @packageDocumentation
 */

import type { KafkaSocket, SocketConnectOptions, SocketFactory } from "./socket.js"

// ---------------------------------------------------------------------------
// Minimal Deno type declarations (avoids @types/deno dependency)
// ---------------------------------------------------------------------------

/** @internal */
type DenoConn = {
  readonly readable: ReadableStream<Uint8Array>
  readonly write: (data: Uint8Array) => Promise<number>
  readonly close: () => void
}

/** @internal */
type DenoConnectOptions = {
  hostname: string
  port: number
}

/** @internal */
type DenoConnectTlsOptions = {
  hostname: string
  port: number
  caCerts?: string[]
  certChain?: string
  privateKey?: string
}

/** @internal */
type DenoNamespace = {
  readonly connect: (options: DenoConnectOptions) => Promise<DenoConn>
  readonly connectTls: (options: DenoConnectTlsOptions) => Promise<DenoConn>
}

// ---------------------------------------------------------------------------
// Deno socket adapter
// ---------------------------------------------------------------------------

/**
 * Create a {@link SocketFactory} that uses `Deno.connect()` /
 * `Deno.connectTls()` for TCP/TLS.
 *
 * @example
 * ```ts
 * import { KafkaConnection } from "@qualithm/kafka-client"
 * import { createDenoSocketFactory } from "@qualithm/kafka-client/deno-socket"
 *
 * const connection = new KafkaConnection({
 *   host: "localhost",
 *   port: 9092,
 *   socketFactory: createDenoSocketFactory(),
 * })
 * ```
 */
export function createDenoSocketFactory(): SocketFactory {
  return async (options: SocketConnectOptions): Promise<KafkaSocket> => connectDenoSocket(options)
}

/** @internal */
async function connectDenoSocket(options: SocketConnectOptions): Promise<KafkaSocket> {
  const { host, port, tls, onData, onError, onClose } = options

  // Access `Deno` from globalThis to avoid compile-time dependency
  const denoNs = (globalThis as unknown as { Deno?: DenoNamespace }).Deno
  if (denoNs === undefined) {
    throw new Error("Deno runtime not detected")
  }

  const conn =
    tls?.enabled === true
      ? await denoNs.connectTls(buildTlsOptions(host, port, tls))
      : await denoNs.connect({ hostname: host, port })

  let closed = false

  // Start reading from the connection's readable stream in background
  void readLoop(conn, onData, onError, () => {
    closed = true
    onClose()
  })

  return {
    async write(data: Uint8Array): Promise<void> {
      if (closed) {
        throw new Error("socket is closed")
      }
      let offset = 0
      while (offset < data.byteLength) {
        const written = await conn.write(data.subarray(offset))
        offset += written
      }
    },

    async close(): Promise<void> {
      if (!closed) {
        closed = true
        conn.close()
      }
      return Promise.resolve()
    }
  }
}

// ---------------------------------------------------------------------------
// Read loop
// ---------------------------------------------------------------------------

/** @internal */
async function readLoop(
  conn: DenoConn,
  onData: (data: Uint8Array) => void,
  onError: (error: Error) => void,
  onClose: () => void
): Promise<void> {
  try {
    const reader = conn.readable.getReader()
    try {
      for (;;) {
        const { done, value } = await reader.read()
        if (done) {
          break
        }
        onData(value)
      }
    } finally {
      reader.releaseLock()
    }
  } catch (error) {
    onError(error instanceof Error ? error : new Error(String(error)))
  } finally {
    onClose()
  }
}

// ---------------------------------------------------------------------------
// TLS helpers
// ---------------------------------------------------------------------------

/** @internal */
function buildTlsOptions(
  hostname: string,
  port: number,
  tls: NonNullable<SocketConnectOptions["tls"]>
): DenoConnectTlsOptions {
  const options: DenoConnectTlsOptions = {
    hostname,
    port
  }

  if (tls.ca !== undefined) {
    options.caCerts = typeof tls.ca === "string" ? [tls.ca] : Array.from(tls.ca)
  }

  if (tls.cert !== undefined) {
    options.certChain = tls.cert
  }

  if (tls.key !== undefined) {
    options.privateKey = tls.key
  }

  return options
}
