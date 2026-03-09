/**
 * Node.js runtime socket adapter for {@link SocketFactory}.
 *
 * Wraps `node:net` and `node:tls` to provide the push-based socket interface
 * expected by {@link KafkaConnection}. Supports both plain TCP and TLS connections.
 *
 * @packageDocumentation
 */

import * as net from "node:net"
import * as tls from "node:tls"

import type { TlsConfig } from "./config.js"
import type { KafkaSocket, SocketConnectOptions, SocketFactory } from "./socket.js"

// ---------------------------------------------------------------------------
// Node.js socket adapter
// ---------------------------------------------------------------------------

/**
 * Create a {@link SocketFactory} that uses Node.js `net`/`tls` for TCP/TLS.
 *
 * @example
 * ```ts
 * import { KafkaConnection } from "@qualithm/kafka-client"
 * import { createNodeSocketFactory } from "@qualithm/kafka-client/node-socket"
 *
 * const connection = new KafkaConnection({
 *   host: "localhost",
 *   port: 9092,
 *   socketFactory: createNodeSocketFactory(),
 * })
 * ```
 */
export function createNodeSocketFactory(): SocketFactory {
  return async (options: SocketConnectOptions): Promise<KafkaSocket> => connectNodeSocket(options)
}

/** @internal */
async function connectNodeSocket(options: SocketConnectOptions): Promise<KafkaSocket> {
  const { host, port, tls: tlsConfig, onData, onError, onClose } = options

  return new Promise<KafkaSocket>((resolve, reject) => {
    let settled = false

    const socket = createSocket(host, port, tlsConfig)

    socket.on("data", (chunk: Buffer) => {
      // Convert Buffer to Uint8Array for the public API
      onData(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength))
    })

    socket.on("error", (error: Error) => {
      if (!settled) {
        settled = true
        reject(error)
        return
      }
      onError(error)
    })

    socket.on("close", () => {
      if (!settled) {
        settled = true
        reject(new Error("socket closed before connection established"))
        return
      }
      onClose()
    })

    socket.once("connect", () => {
      // For plain TCP, "connect" means ready
      if (tlsConfig?.enabled !== true) {
        settled = true
        resolve(wrapSocket(socket))
      }
    })

    // For TLS sockets, wait for "secureConnect" instead
    if (tlsConfig?.enabled === true) {
      socket.once("secureConnect", () => {
        settled = true
        resolve(wrapSocket(socket))
      })
    }
  })
}

// ---------------------------------------------------------------------------
// Socket creation
// ---------------------------------------------------------------------------

/** @internal */
function createSocket(host: string, port: number, tlsConfig?: TlsConfig): net.Socket {
  if (tlsConfig?.enabled !== true) {
    return net.createConnection({ host, port })
  }

  const tlsOptions: tls.ConnectionOptions = {
    host,
    port,
    rejectUnauthorized: tlsConfig.rejectUnauthorised ?? true
  }

  if (tlsConfig.ca !== undefined) {
    tlsOptions.ca = typeof tlsConfig.ca === "string" ? tlsConfig.ca : Array.from(tlsConfig.ca)
  }

  if (tlsConfig.cert !== undefined) {
    tlsOptions.cert = tlsConfig.cert
  }

  if (tlsConfig.key !== undefined) {
    tlsOptions.key = tlsConfig.key
  }

  return tls.connect(tlsOptions)
}

// ---------------------------------------------------------------------------
// Socket wrapper
// ---------------------------------------------------------------------------

/** @internal */
function wrapSocket(socket: net.Socket): KafkaSocket {
  return {
    async write(data: Uint8Array): Promise<void> {
      return new Promise<void>((resolve, reject) => {
        const ok = socket.write(data, (error) => {
          if (error) {
            reject(error)
            return
          }
          resolve()
        })

        if (!ok) {
          // Backpressure — wait for drain then the write callback resolves
          socket.once("drain", () => {
            // The write callback above handles resolve/reject
          })
        }
      })
    },

    async close(): Promise<void> {
      return new Promise<void>((resolve) => {
        socket.end(() => {
          resolve()
        })
      })
    }
  }
}
