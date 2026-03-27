/**
 * Bun runtime socket adapter for {@link SocketFactory}.
 *
 * Wraps `Bun.connect()` to provide the push-based socket interface expected
 * by {@link KafkaConnection}. Supports both plain TCP and TLS connections.
 *
 * @packageDocumentation
 */

import type { KafkaSocket, SocketConnectOptions, SocketFactory } from "./socket.js"

// ---------------------------------------------------------------------------
// Bun socket adapter
// ---------------------------------------------------------------------------

/**
 * Create a {@link SocketFactory} that uses `Bun.connect()` for TCP/TLS.
 *
 * @example
 * ```ts
 * import { KafkaConnection } from "@qualithm/kafka-client"
 * import { createBunSocketFactory } from "@qualithm/kafka-client/bun-socket"
 *
 * const connection = new KafkaConnection({
 *   host: "localhost",
 *   port: 9092,
 *   socketFactory: createBunSocketFactory(),
 * })
 * ```
 */
export function createBunSocketFactory(connect?: typeof Bun.connect): SocketFactory {
  return async (options: SocketConnectOptions): Promise<KafkaSocket> =>
    connectBunSocket(options, connect ?? Bun.connect)
}

/** @internal */
async function connectBunSocket(
  options: SocketConnectOptions,
  connect: typeof Bun.connect
): Promise<KafkaSocket> {
  const { host, port, tls, onData, onError, onClose } = options

  const tlsOptions = buildTlsOptions(tls)

  // Drain resolvers queued when writes encounter backpressure
  let drainResolve: (() => void) | null = null

  const socket = await connect({
    hostname: host,
    port,
    tls: tlsOptions,
    socket: {
      binaryType: "uint8array" as const,

      data(_socket, data: Uint8Array): void {
        onData(data)
      },

      error(_socket, error: Error): void {
        onError(error)
      },

      close(): void {
        onClose()
      },

      drain(): void {
        if (drainResolve) {
          const resolve = drainResolve
          drainResolve = null
          resolve()
        }
      },

      connectError(_socket, error: Error): void {
        onError(error)
      }
    }
  })

  return {
    async write(data: Uint8Array): Promise<void> {
      let offset = 0
      while (offset < data.byteLength) {
        const written = socket.write(data.subarray(offset))
        if (written === -1) {
          throw new Error("socket is closed")
        }
        if (written === 0) {
          // Socket buffer is full — wait for drain event
          await new Promise<void>((resolve) => {
            drainResolve = resolve
          })
          continue
        }
        offset += written
      }
      socket.flush()
    },

    async close(): Promise<void> {
      socket.end()
      return Promise.resolve()
    }
  }
}

// ---------------------------------------------------------------------------
// TLS helpers
// ---------------------------------------------------------------------------

/** @internal */
function buildTlsOptions(tls: SocketConnectOptions["tls"]): Bun.TLSOptions | undefined {
  if (tls?.enabled !== true) {
    return undefined
  }

  const options: Bun.TLSOptions = {
    rejectUnauthorized: tls.rejectUnauthorised ?? true
  }

  if (tls.ca !== undefined) {
    options.ca = typeof tls.ca === "string" ? tls.ca : Array.from(tls.ca)
  }

  if (tls.cert !== undefined) {
    options.cert = tls.cert
  }

  if (tls.key !== undefined) {
    options.key = tls.key
  }

  return options
}
