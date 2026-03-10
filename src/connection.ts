/**
 * Kafka broker connection with request/response correlation.
 *
 * Manages a single TCP connection to a Kafka broker and provides a high-level
 * send/receive API with automatic correlation ID assignment, response header
 * decoding, and timeout management.
 *
 * The connection uses an injected {@link SocketFactory} so it is runtime-agnostic —
 * no built-in TCP dependency. Runtime adapters (Bun, Node.js, Deno) provide the
 * socket implementation.
 *
 * @see https://kafka.apache.org/protocol.html#protocol_messages
 *
 * @packageDocumentation
 */

import { ApiKey } from "./api-keys.js"
import { BinaryReader } from "./binary-reader.js"
import type { BinaryWriter } from "./binary-writer.js"
import type { SaslConfig, TlsConfig } from "./config.js"
import { KafkaConnectionError, KafkaTimeoutError } from "./errors.js"
import { decodeResponseHeader, frameRequest } from "./protocol-framing.js"
import { createSaslAuthenticator } from "./sasl.js"
import {
  decodeSaslAuthenticateResponse,
  encodeSaslAuthenticateRequest
} from "./sasl-authenticate.js"
import { decodeSaslHandshakeResponse, encodeSaslHandshakeRequest } from "./sasl-handshake.js"
import type { KafkaSocket, SocketFactory } from "./socket.js"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Options for creating a {@link KafkaConnection}.
 */
export type ConnectionOptions = {
  /** Broker hostname. */
  readonly host: string
  /** Broker port. */
  readonly port: number
  /** Client ID sent in request headers (default: "@qualithm/kafka-client"). */
  readonly clientId?: string
  /** Factory function for creating the underlying socket. */
  readonly socketFactory: SocketFactory
  /** TLS configuration. */
  readonly tls?: TlsConfig
  /** Connection timeout in milliseconds (default: 30000). */
  readonly connectTimeoutMs?: number
  /** Per-request timeout in milliseconds (default: 30000). */
  readonly requestTimeoutMs?: number
  /** SASL authentication configuration. */
  readonly sasl?: SaslConfig
}

/** @internal */
type PendingRequest = {
  readonly apiKey: ApiKey
  readonly apiVersion: number
  readonly resolve: (reader: BinaryReader) => void
  readonly reject: (error: Error) => void
  readonly timer: ReturnType<typeof setTimeout>
}

// ---------------------------------------------------------------------------
// Receive buffer
// ---------------------------------------------------------------------------

const MAX_FRAME_SIZE = 0x7fff_ffff // INT32 max (2 GiB)

/**
 * Internal buffer that reassembles TCP chunks into complete Kafka response frames.
 *
 * Kafka responses are size-prefixed: an INT32 byte count followed by that many
 * bytes of payload. Data may arrive in arbitrary chunks, so we accumulate
 * until a complete frame is available.
 *
 * @internal
 */
class ReceiveBuffer {
  private chunks: Uint8Array[] = []
  private totalLength = 0

  append(data: Uint8Array): void {
    this.chunks.push(data)
    this.totalLength += data.byteLength
  }

  /**
   * Try to extract a complete response frame.
   * Returns the frame payload (without the 4-byte size prefix), or `null`
   * if not enough data is available yet.
   */
  tryReadFrame(): Uint8Array | null {
    if (this.totalLength < 4) {
      return null
    }

    const sizeBytes = this.peek(4)
    const view = new DataView(sizeBytes.buffer, sizeBytes.byteOffset, sizeBytes.byteLength)
    const size = view.getInt32(0)

    if (size < 0 || size > MAX_FRAME_SIZE) {
      return null
    }

    const frameLength = 4 + size
    if (this.totalLength < frameLength) {
      return null
    }

    const frame = this.consume(frameLength)
    return frame.subarray(4)
  }

  reset(): void {
    this.chunks = []
    this.totalLength = 0
  }

  private peek(n: number): Uint8Array {
    const first = this.chunks[0]
    if (this.chunks.length === 1 && first.byteLength >= n) {
      return first.subarray(0, n)
    }
    return this.flatten().subarray(0, n)
  }

  private consume(n: number): Uint8Array {
    const flat = this.flatten()
    const consumed = flat.subarray(0, n)
    const remaining = flat.subarray(n)

    if (remaining.byteLength > 0) {
      this.chunks = [remaining]
      this.totalLength = remaining.byteLength
    } else {
      this.chunks = []
      this.totalLength = 0
    }

    return consumed
  }

  private flatten(): Uint8Array {
    if (this.chunks.length === 0) {
      return new Uint8Array(0)
    }

    const first = this.chunks[0]
    if (this.chunks.length === 1) {
      return first
    }

    const flat = new Uint8Array(this.totalLength)
    let offset = 0
    for (const chunk of this.chunks) {
      flat.set(chunk, offset)
      offset += chunk.byteLength
    }
    this.chunks = [flat]
    return flat
  }
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

const DEFAULT_CONNECT_TIMEOUT_MS = 30_000
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000

// ---------------------------------------------------------------------------
// KafkaConnection
// ---------------------------------------------------------------------------

/**
 * A single connection to a Kafka broker.
 *
 * Handles request/response correlation, protocol framing, and timeout
 * management. Uses the injected {@link SocketFactory} for runtime-agnostic
 * TCP/TLS connections.
 */
export class KafkaConnection {
  private socket: KafkaSocket | null = null
  private nextCorrelationId = 0
  private readonly pending = new Map<number, PendingRequest>()
  private readonly receiveBuffer = new ReceiveBuffer()
  private closed = false

  /** Broker hostname. */
  readonly host: string
  /** Broker port. */
  readonly port: number

  private readonly clientId: string
  private readonly socketFactory: SocketFactory
  private readonly tls?: TlsConfig
  private readonly sasl?: SaslConfig
  private readonly connectTimeoutMs: number
  private readonly requestTimeoutMs: number

  constructor(options: ConnectionOptions) {
    this.host = options.host
    this.port = options.port
    this.clientId = options.clientId ?? "@qualithm/kafka-client"
    this.socketFactory = options.socketFactory
    this.tls = options.tls
    this.sasl = options.sasl
    this.connectTimeoutMs = options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS
    this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS
  }

  /** Broker address as "host:port". */
  get broker(): string {
    return `${this.host}:${String(this.port)}`
  }

  /** Whether the connection is established and not closed. */
  get connected(): boolean {
    return this.socket !== null && !this.closed
  }

  /**
   * Establish the connection to the broker.
   *
   * @throws {KafkaConnectionError} If already connected or previously closed.
   * @throws {KafkaTimeoutError} If the connection times out.
   */
  async connect(): Promise<void> {
    if (this.socket !== null) {
      throw new KafkaConnectionError("already connected", { broker: this.broker })
    }
    if (this.closed) {
      throw new KafkaConnectionError("connection has been closed", { broker: this.broker })
    }

    const connectPromise = this.socketFactory({
      host: this.host,
      port: this.port,
      tls: this.tls,
      onData: (data) => {
        this.onData(data)
      },
      onError: (error) => {
        this.onError(error)
      },
      onClose: () => {
        this.onClose()
      }
    })

    try {
      this.socket = await withTimeout(
        connectPromise,
        this.connectTimeoutMs,
        `connection to ${this.broker} timed out`,
        this.broker
      )
    } catch (error) {
      if (KafkaTimeoutError.isError(error)) {
        throw error
      }
      throw new KafkaConnectionError(`failed to connect to ${this.broker}`, {
        broker: this.broker,
        cause: error
      })
    }
  }

  /**
   * Send a request and wait for the correlated response.
   *
   * Automatically assigns a correlation ID, encodes the request header,
   * frames the message, and returns a {@link BinaryReader} positioned
   * after the response header (ready for API-specific body decoding).
   *
   * @param apiKey - API key for the request.
   * @param apiVersion - API version for the request.
   * @param encodeBody - Callback to encode the request body.
   * @returns Reader positioned at the start of the response body.
   * @throws {KafkaConnectionError} If not connected.
   * @throws {KafkaTimeoutError} If the request times out.
   */
  async send(
    apiKey: ApiKey,
    apiVersion: number,
    encodeBody: (writer: BinaryWriter) => void
  ): Promise<BinaryReader> {
    if (!this.connected) {
      throw new KafkaConnectionError("not connected", { broker: this.broker })
    }

    const correlationId = this.nextCorrelationId++
    const framed = frameRequest(
      {
        apiKey,
        apiVersion,
        correlationId,
        clientId: this.clientId
      },
      encodeBody
    )

    return new Promise<BinaryReader>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(correlationId)
        reject(
          new KafkaTimeoutError(
            `request timed out (correlationId=${String(correlationId)})`,
            this.requestTimeoutMs,
            { broker: this.broker }
          )
        )
      }, this.requestTimeoutMs)

      this.pending.set(correlationId, {
        apiKey,
        apiVersion,
        resolve,
        reject,
        timer
      })

      const { socket } = this
      if (!socket) {
        clearTimeout(timer)
        this.pending.delete(correlationId)
        reject(new KafkaConnectionError("not connected", { broker: this.broker }))
        return
      }

      socket.write(framed).catch((error: unknown) => {
        clearTimeout(timer)
        this.pending.delete(correlationId)
        reject(
          new KafkaConnectionError("failed to send request", {
            broker: this.broker,
            cause: error
          })
        )
      })
    })
  }

  /**
   * Perform SASL authentication if configured.
   *
   * Must be called after `connect()` and before sending any other requests.
   * Uses SaslHandshake (v1) to negotiate the mechanism, then SaslAuthenticate
   * to exchange authentication tokens.
   *
   * @throws {KafkaConnectionError} If authentication fails.
   */
  async authenticate(): Promise<void> {
    if (!this.sasl) {
      return
    }

    const authenticator = createSaslAuthenticator(this.sasl)

    // Step 1: SaslHandshake — negotiate mechanism
    const handshakeReader = await this.send(ApiKey.SaslHandshake, 1, (writer) => {
      encodeSaslHandshakeRequest(writer, { mechanism: authenticator.mechanism }, 1)
    })

    const handshakeResult = decodeSaslHandshakeResponse(handshakeReader, 1)
    if (!handshakeResult.ok) {
      throw new KafkaConnectionError(
        `failed to decode SaslHandshake response: ${handshakeResult.error.message}`,
        { broker: this.broker, retriable: false }
      )
    }

    if (handshakeResult.value.errorCode !== 0) {
      const supported = handshakeResult.value.mechanisms.join(", ")
      throw new KafkaConnectionError(
        `SASL mechanism '${authenticator.mechanism}' not supported by broker (error code ${String(handshakeResult.value.errorCode)}, supported: ${supported})`,
        { broker: this.broker, retriable: false }
      )
    }

    // Step 2: SaslAuthenticate — exchange auth tokens
    const initialBytes = authenticator.initialAuthBytes()
    const authReader = await this.send(ApiKey.SaslAuthenticate, 1, (writer) => {
      encodeSaslAuthenticateRequest(writer, { authBytes: initialBytes }, 1)
    })

    const authResult = decodeSaslAuthenticateResponse(authReader, 1)
    if (!authResult.ok) {
      throw new KafkaConnectionError(
        `failed to decode SaslAuthenticate response: ${authResult.error.message}`,
        { broker: this.broker, retriable: false }
      )
    }

    if (authResult.value.errorCode !== 0) {
      throw new KafkaConnectionError(
        `SASL authentication failed: ${authResult.value.errorMessage ?? "unknown error"} (error code ${String(authResult.value.errorCode)})`,
        { broker: this.broker, retriable: false }
      )
    }

    // For multi-step mechanisms (SCRAM), continue exchanging tokens
    let serverBytes = authResult.value.authBytes
    while (serverBytes && serverBytes.byteLength > 0) {
      const nextBytes = await authenticator.stepAsync(serverBytes)
      if (nextBytes === null) {
        break
      }

      const stepReader = await this.send(ApiKey.SaslAuthenticate, 1, (writer) => {
        encodeSaslAuthenticateRequest(writer, { authBytes: nextBytes }, 1)
      })

      const stepResult = decodeSaslAuthenticateResponse(stepReader, 1)
      if (!stepResult.ok) {
        throw new KafkaConnectionError(
          `failed to decode SaslAuthenticate response: ${stepResult.error.message}`,
          { broker: this.broker, retriable: false }
        )
      }

      if (stepResult.value.errorCode !== 0) {
        throw new KafkaConnectionError(
          `SASL authentication failed: ${stepResult.value.errorMessage ?? "unknown error"} (error code ${String(stepResult.value.errorCode)})`,
          { broker: this.broker, retriable: false }
        )
      }

      serverBytes = stepResult.value.authBytes
    }

    // Final step validation (e.g. SCRAM server signature verification)
    if (serverBytes && serverBytes.byteLength > 0) {
      await authenticator.stepAsync(serverBytes)
    }
  }

  /**
   * Close the connection and reject all pending requests.
   */
  async close(): Promise<void> {
    if (this.closed) {
      return
    }
    this.closed = true

    this.rejectAllPending(
      new KafkaConnectionError("connection closed", { broker: this.broker, retriable: false })
    )

    if (this.socket) {
      try {
        await this.socket.close()
      } catch {
        // Ignore close errors — the socket may already be torn down
      }
      this.socket = null
    }

    this.receiveBuffer.reset()
  }

  // -------------------------------------------------------------------------
  // Socket event handlers
  // -------------------------------------------------------------------------

  private onData(data: Uint8Array): void {
    this.receiveBuffer.append(data)
    this.processBuffer()
  }

  private onError(error: Error): void {
    this.rejectAllPending(
      new KafkaConnectionError("socket error", {
        broker: this.broker,
        cause: error
      })
    )
    void this.close()
  }

  private onClose(): void {
    if (!this.closed) {
      this.rejectAllPending(
        new KafkaConnectionError("connection closed unexpectedly", {
          broker: this.broker
        })
      )
      this.closed = true
      this.socket = null
      this.receiveBuffer.reset()
    }
  }

  // -------------------------------------------------------------------------
  // Response processing
  // -------------------------------------------------------------------------

  private processBuffer(): void {
    let payload: Uint8Array | null
    while ((payload = this.receiveBuffer.tryReadFrame()) !== null) {
      this.handleFrame(payload)
    }
  }

  private handleFrame(payload: Uint8Array): void {
    // Every response header starts with INT32 correlation ID
    if (payload.byteLength < 4) {
      this.onError(new Error("response frame too small"))
      return
    }

    // Read correlation ID from the first 4 bytes (common to all header versions)
    const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength)
    const correlationId = view.getInt32(0)

    const pending = this.pending.get(correlationId)
    if (!pending) {
      // Unknown correlation ID — stale response after timeout; discard
      return
    }

    clearTimeout(pending.timer)
    this.pending.delete(correlationId)

    // Decode the full response header to advance the reader past it
    const reader = new BinaryReader(payload)
    const headerResult = decodeResponseHeader(reader, pending.apiKey, pending.apiVersion)

    if (!headerResult.ok) {
      pending.reject(
        new KafkaConnectionError(
          `failed to decode response header: ${headerResult.error.message}`,
          { broker: this.broker }
        )
      )
      return
    }

    // Reader is now positioned right after the response header
    pending.resolve(reader)
  }

  private rejectAllPending(error: KafkaConnectionError): void {
    for (const [id, pending] of this.pending) {
      clearTimeout(pending.timer)
      this.pending.delete(id)
      pending.reject(error)
    }
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** @internal */
async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  message: string,
  broker?: string
): Promise<T> {
  if (timeoutMs <= 0) {
    return promise
  }

  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new KafkaTimeoutError(message, timeoutMs, { broker }))
    }, timeoutMs)

    promise.then(
      (value) => {
        clearTimeout(timer)
        resolve(value)
      },
      (error: unknown) => {
        clearTimeout(timer)
        reject(error instanceof Error ? error : new KafkaConnectionError(String(error), { broker }))
      }
    )
  })
}
