import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryWriter } from "../../binary-writer"
import { type ConnectionOptions, KafkaConnection } from "../../connection"
import { KafkaConnectionError, KafkaTimeoutError } from "../../errors"
import type { KafkaSocket, SocketFactory } from "../../socket"

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// eslint-disable-next-line @typescript-eslint/no-empty-function
const noop = (): void => {}

type MockSocketContext = {
  factory: SocketFactory
  callbacks: {
    onData: (data: Uint8Array) => void
    onError: (error: Error) => void
    onClose: () => void
  } | null
  written: Uint8Array[]
  socket: KafkaSocket | null
}

function createMockSocketFactory(
  connectBehaviour?: (ctx: MockSocketContext) => Promise<KafkaSocket> | KafkaSocket
): MockSocketContext {
  const ctx: MockSocketContext = {
    factory: null as unknown as SocketFactory,
    callbacks: null,
    written: [],
    socket: null
  }

  ctx.factory = async (options) => {
    ctx.callbacks = {
      onData: options.onData,
      onError: options.onError,
      onClose: options.onClose
    }

    if (connectBehaviour) {
      const sock = await connectBehaviour(ctx)
      ctx.socket = sock
      return sock
    }

    const socket: KafkaSocket = {
      // eslint-disable-next-line @typescript-eslint/require-await
      write: async (data) => {
        ctx.written.push(new Uint8Array(data))
      },
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      close: async () => {}
    }
    ctx.socket = socket
    return socket
  }

  return ctx
}

/**
 * Build a size-prefixed mock response with the given correlation ID and body.
 *
 * @param correlationId - Correlation ID for the response header.
 * @param body - Raw bytes for the response body.
 * @param flexible - Whether to use flexible response header v1 (with tagged fields).
 */
function buildMockResponse(
  correlationId: number,
  body: Uint8Array = new Uint8Array(0),
  flexible = false
): Uint8Array {
  const inner = new BinaryWriter()
  // Response header: INT32 correlation ID
  inner.writeInt32(correlationId)
  if (flexible) {
    // Tagged fields: 0 fields
    inner.writeUnsignedVarInt(0)
  }
  inner.writeRawBytes(body)
  const payload = inner.finish()

  // Size-prefixed frame
  const framed = new BinaryWriter(payload.byteLength + 4)
  framed.writeInt32(payload.byteLength)
  framed.writeRawBytes(payload)
  return framed.finish()
}

function defaultOptions(factory: SocketFactory): ConnectionOptions {
  return {
    host: "localhost",
    port: 9092,
    socketFactory: factory,
    requestTimeoutMs: 1000,
    connectTimeoutMs: 1000
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("KafkaConnection", () => {
  let conn: KafkaConnection

  afterEach(async () => {
    // Ensure connection is cleaned up after each test
    try {
      await conn.close()
    } catch {
      // Ignore — may already be closed
    }
  })

  // -----------------------------------------------------------------------
  // Constructor and properties
  // -----------------------------------------------------------------------

  describe("constructor", () => {
    it("exposes host, port, and broker", () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))

      expect(conn.host).toBe("localhost")
      expect(conn.port).toBe(9092)
      expect(conn.broker).toBe("localhost:9092")
    })

    it("defaults to not connected", () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))

      expect(conn.connected).toBe(false)
    })
  })

  // -----------------------------------------------------------------------
  // connect()
  // -----------------------------------------------------------------------

  describe("connect", () => {
    it("establishes a connection via the socket factory", async () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))

      await conn.connect()

      expect(conn.connected).toBe(true)
      expect(mock.callbacks).not.toBeNull()
    })

    it("passes TLS config to the socket factory", async () => {
      let capturedTls: unknown
      // eslint-disable-next-line @typescript-eslint/require-await
      const factory: SocketFactory = async (options) => {
        capturedTls = options.tls
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        return { write: async () => {}, close: async () => {} }
      }
      conn = new KafkaConnection({
        ...defaultOptions(factory),
        tls: { enabled: true, rejectUnauthorised: false }
      })

      await conn.connect()

      expect(capturedTls).toEqual({ enabled: true, rejectUnauthorised: false })
    })

    it("throws if already connected", async () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))
      await conn.connect()

      await expect(conn.connect()).rejects.toThrow(KafkaConnectionError)
      await expect(conn.connect()).rejects.toThrow("already connected")
    })

    it("throws if previously closed", async () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))
      await conn.connect()
      await conn.close()

      await expect(conn.connect()).rejects.toThrow(KafkaConnectionError)
      await expect(conn.connect()).rejects.toThrow("connection has been closed")
    })

    it("throws KafkaTimeoutError when connection times out", async () => {
      // eslint-disable-next-line @typescript-eslint/promise-function-async
      const factory: SocketFactory = () => new Promise(noop) // Never resolves
      conn = new KafkaConnection({
        ...defaultOptions(factory),
        connectTimeoutMs: 50
      })

      await expect(conn.connect()).rejects.toThrow(KafkaTimeoutError)
    })

    it("throws KafkaConnectionError when socket factory rejects", async () => {
      // eslint-disable-next-line @typescript-eslint/promise-function-async
      const factory: SocketFactory = () => Promise.reject(new Error("ECONNREFUSED"))
      conn = new KafkaConnection(defaultOptions(factory))

      await expect(conn.connect()).rejects.toThrow(KafkaConnectionError)
      await expect(
        (async () => {
          // Re-create since first attempt may leave bad state
          conn = new KafkaConnection(defaultOptions(factory))
          await conn.connect()
        })()
      ).rejects.toThrow("failed to connect")
    })
  })

  // -----------------------------------------------------------------------
  // send()
  // -----------------------------------------------------------------------

  describe("send", () => {
    let mock: MockSocketContext

    beforeEach(async () => {
      mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))
      await conn.connect()
    })

    it("throws if not connected", async () => {
      const mock2 = createMockSocketFactory()
      const disconnected = new KafkaConnection(defaultOptions(mock2.factory))

      await expect(disconnected.send(ApiKey.ApiVersions, 0, noop)).rejects.toThrow("not connected")
    })

    it("sends a framed request and resolves with response body reader", async () => {
      const sendPromise = conn.send(ApiKey.ApiVersions, 0, noop)

      // Simulate broker response with correlation ID 0
      const body = new Uint8Array([0x00, 0x01, 0x02])
      const response = buildMockResponse(0, body)
      mock.callbacks!.onData(response)

      const reader = await sendPromise

      // Reader should be positioned after the response header (4 bytes for corr ID)
      // and contain the body bytes
      expect(reader.remaining).toBe(3)
      const byteResult = reader.readInt8()
      expect(byteResult.ok).toBe(true)
      if (byteResult.ok) {
        expect(byteResult.value).toBe(0)
      }
    })

    it("sends framed request bytes to the socket", async () => {
      const sendPromise = conn.send(ApiKey.ApiVersions, 0, (writer) => {
        writer.writeInt16(42) // dummy body
      })

      // Respond to prevent hanging
      const response = buildMockResponse(0)
      mock.callbacks!.onData(response)
      await sendPromise

      // Verify something was written
      expect(mock.written.length).toBe(1)
      expect(mock.written[0].byteLength).toBeGreaterThan(0)
    })

    it("assigns sequential correlation IDs", async () => {
      const promise1 = conn.send(ApiKey.ApiVersions, 0, noop)
      const promise2 = conn.send(ApiKey.ApiVersions, 0, noop)

      // Respond with correlation IDs 0 and 1
      mock.callbacks!.onData(buildMockResponse(0))
      mock.callbacks!.onData(buildMockResponse(1))

      await Promise.all([promise1, promise2])

      // Both resolved without error — correlation IDs matched correctly
      expect(mock.written.length).toBe(2)
    })

    it("handles responses arriving in a single chunk", async () => {
      const promise1 = conn.send(ApiKey.ApiVersions, 0, noop)
      const promise2 = conn.send(ApiKey.ApiVersions, 0, noop)

      // Concatenate both responses into a single data event
      const resp1 = buildMockResponse(0, new Uint8Array([0xaa]))
      const resp2 = buildMockResponse(1, new Uint8Array([0xbb]))
      const combined = new Uint8Array(resp1.byteLength + resp2.byteLength)
      combined.set(resp1, 0)
      combined.set(resp2, resp1.byteLength)

      mock.callbacks!.onData(combined)

      const [reader1, reader2] = await Promise.all([promise1, promise2])
      expect(reader1.remaining).toBe(1)
      expect(reader2.remaining).toBe(1)
    })

    it("handles responses split across multiple chunks", async () => {
      const promise = conn.send(ApiKey.ApiVersions, 0, noop)

      const response = buildMockResponse(0, new Uint8Array([0x01, 0x02, 0x03]))

      // Split the response into 2-byte chunks
      for (let i = 0; i < response.byteLength; i += 2) {
        const end = Math.min(i + 2, response.byteLength)
        mock.callbacks!.onData(response.subarray(i, end))
      }

      const reader = await promise
      expect(reader.remaining).toBe(3)
    })

    it("handles flexible version response headers", async () => {
      // Metadata v9+ uses flexible encoding
      const promise = conn.send(ApiKey.Metadata, 9, noop)

      const body = new Uint8Array([0xab, 0xcd])
      const response = buildMockResponse(0, body, true) // flexible = true
      mock.callbacks!.onData(response)

      const reader = await promise
      expect(reader.remaining).toBe(2)
    })

    it("rejects with KafkaTimeoutError when request times out", async () => {
      vi.useFakeTimers()

      try {
        const promise = conn.send(ApiKey.ApiVersions, 0, noop)

        vi.advanceTimersByTime(1000)

        await expect(promise).rejects.toThrow(KafkaTimeoutError)
      } finally {
        vi.useRealTimers()
      }
    })

    it("rejects when socket write fails", async () => {
      const failingMock = createMockSocketFactory(() => ({
        // eslint-disable-next-line @typescript-eslint/require-await
        write: async () => {
          throw new Error("write failed")
        },
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        close: async () => {}
      }))
      const failConn = new KafkaConnection(defaultOptions(failingMock.factory))
      await failConn.connect()

      await expect(failConn.send(ApiKey.ApiVersions, 0, noop)).rejects.toThrow(
        "failed to send request"
      )

      await failConn.close()
    })

    it("discards responses with unknown correlation IDs", async () => {
      const promise = conn.send(ApiKey.ApiVersions, 0, noop)

      // Send a response with wrong correlation ID
      mock.callbacks!.onData(buildMockResponse(999))

      // Send the correct response
      mock.callbacks!.onData(buildMockResponse(0, new Uint8Array([0x42])))

      const reader = await promise
      expect(reader.remaining).toBe(1)
    })
  })

  // -----------------------------------------------------------------------
  // close()
  // -----------------------------------------------------------------------

  describe("close", () => {
    it("rejects all pending requests", async () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))
      await conn.connect()

      const promise = conn.send(ApiKey.ApiVersions, 0, noop)
      await conn.close()

      await expect(promise).rejects.toThrow(KafkaConnectionError)
      await expect(promise).rejects.toThrow("connection closed")
    })

    it("sets connected to false", async () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))
      await conn.connect()
      expect(conn.connected).toBe(true)

      await conn.close()
      expect(conn.connected).toBe(false)
    })

    it("is idempotent", async () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))
      await conn.connect()

      await conn.close()
      await conn.close() // Should not throw

      expect(conn.connected).toBe(false)
    })
  })

  // -----------------------------------------------------------------------
  // Socket events
  // -----------------------------------------------------------------------

  describe("socket events", () => {
    it("rejects pending requests on socket error", async () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))
      await conn.connect()

      const promise = conn.send(ApiKey.ApiVersions, 0, noop)
      mock.callbacks!.onError(new Error("ECONNRESET"))

      await expect(promise).rejects.toThrow(KafkaConnectionError)
      await expect(promise).rejects.toThrow("socket error")
    })

    it("rejects pending requests on unexpected socket close", async () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))
      await conn.connect()

      const promise = conn.send(ApiKey.ApiVersions, 0, noop)
      mock.callbacks!.onClose()

      await expect(promise).rejects.toThrow(KafkaConnectionError)
      await expect(promise).rejects.toThrow("connection closed unexpectedly")
    })

    it("marks connection as not connected after socket close", async () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))
      await conn.connect()
      expect(conn.connected).toBe(true)

      mock.callbacks!.onClose()
      expect(conn.connected).toBe(false)
    })
  })

  // -----------------------------------------------------------------------
  // authenticate()
  // -----------------------------------------------------------------------

  describe("authenticate", () => {
    /**
     * Create a mock socket factory that automatically responds to each write
     * with the next pre-staged response frame.
     */
    function createAutoRespondingMock(responseFrames: Uint8Array[]): MockSocketContext {
      let responseIndex = 0
      let correlationId = 0

      // eslint-disable-next-line @typescript-eslint/promise-function-async
      return createMockSocketFactory((ctx) => {
        const socket: KafkaSocket = {
          // eslint-disable-next-line @typescript-eslint/require-await
          write: async (data) => {
            ctx.written.push(new Uint8Array(data))
            if (responseIndex < responseFrames.length) {
              const body = responseFrames[responseIndex++]
              const frame = buildMockResponse(correlationId++, body)
              // Schedule response delivery to avoid re-entrancy
              queueMicrotask(() => {
                ctx.callbacks!.onData(frame)
              })
            }
          },
          // eslint-disable-next-line @typescript-eslint/no-empty-function
          close: async () => {}
        }
        return Promise.resolve(socket)
      })
    }

    /** Build a SaslHandshake v1 response body. */
    function buildHandshakeResponse(errorCode: number, mechanisms: string[]): Uint8Array {
      const w = new BinaryWriter()
      w.writeInt16(errorCode)
      w.writeInt32(mechanisms.length)
      for (const m of mechanisms) {
        w.writeString(m)
      }
      return w.finish()
    }

    /** Build a SaslAuthenticate v1 response body. */
    function buildAuthResponse(
      errorCode: number,
      errorMessage: string | null,
      authBytes: Uint8Array | null,
      sessionLifetimeMs = 0n
    ): Uint8Array {
      const w = new BinaryWriter()
      w.writeInt16(errorCode)
      w.writeString(errorMessage)
      w.writeBytes(authBytes)
      w.writeInt64(sessionLifetimeMs)
      return w.finish()
    }

    it("is a no-op when no SASL config is set", async () => {
      const mock = createMockSocketFactory()
      conn = new KafkaConnection(defaultOptions(mock.factory))
      await conn.connect()

      await conn.authenticate()

      // No requests should have been sent
      expect(mock.written.length).toBe(0)
    })

    it("completes PLAIN authentication successfully", async () => {
      const handshakeBody = buildHandshakeResponse(0, ["PLAIN", "SCRAM-SHA-256"])
      const authBody = buildAuthResponse(0, null, new Uint8Array(0))

      const mock = createAutoRespondingMock([handshakeBody, authBody])
      conn = new KafkaConnection({
        ...defaultOptions(mock.factory),
        sasl: { mechanism: "PLAIN", username: "user", password: "pass" }
      })
      await conn.connect()

      await conn.authenticate()

      // Two requests: handshake + authenticate
      expect(mock.written.length).toBe(2)
    })

    it("throws when handshake returns non-zero error code", async () => {
      // Error code 33 = UNSUPPORTED_SASL_MECHANISM
      const handshakeBody = buildHandshakeResponse(33, ["SCRAM-SHA-256"])

      const mock = createAutoRespondingMock([handshakeBody])
      conn = new KafkaConnection({
        ...defaultOptions(mock.factory),
        sasl: { mechanism: "PLAIN", username: "user", password: "pass" }
      })
      await conn.connect()

      await expect(conn.authenticate()).rejects.toThrow("not supported by broker")
    })

    it("throws when authenticate returns non-zero error code", async () => {
      const handshakeBody = buildHandshakeResponse(0, ["PLAIN"])
      const authBody = buildAuthResponse(58, "authentication failed", null)

      const mock = createAutoRespondingMock([handshakeBody, authBody])
      conn = new KafkaConnection({
        ...defaultOptions(mock.factory),
        sasl: { mechanism: "PLAIN", username: "user", password: "bad" }
      })
      await conn.connect()

      await expect(conn.authenticate()).rejects.toThrow("SASL authentication failed")
    })

    it("throws when handshake response is truncated", async () => {
      // Provide a truncated body that will fail to decode
      const truncated = new Uint8Array([0x00])

      const mock = createAutoRespondingMock([truncated])
      conn = new KafkaConnection({
        ...defaultOptions(mock.factory),
        sasl: { mechanism: "PLAIN", username: "u", password: "p" }
      })
      await conn.connect()

      await expect(conn.authenticate()).rejects.toThrow("failed to decode SaslHandshake")
    })

    it("throws when authenticate response is truncated", async () => {
      const handshakeBody = buildHandshakeResponse(0, ["PLAIN"])
      const truncated = new Uint8Array([0x00])

      const mock = createAutoRespondingMock([handshakeBody, truncated])
      conn = new KafkaConnection({
        ...defaultOptions(mock.factory),
        sasl: { mechanism: "PLAIN", username: "u", password: "p" }
      })
      await conn.connect()

      await expect(conn.authenticate()).rejects.toThrow("failed to decode SaslAuthenticate")
    })

    it("handles multi-step SCRAM authentication", async () => {
      const handshakeBody = buildHandshakeResponse(0, ["SCRAM-SHA-256"])

      // We need to provide proper SCRAM server responses.
      // The first SaslAuthenticate response returns the server-first-message.
      // We build a fake server-first containing our client nonce + server nonce.
      // Since we don't know the client nonce ahead of time, we use a dynamic
      // auto-responding mock instead.

      let requestCount = 0
      let clientNonce = ""

      // eslint-disable-next-line @typescript-eslint/promise-function-async
      const mock = createMockSocketFactory((ctx) => {
        let correlationId = 0
        const socket: KafkaSocket = {
          // eslint-disable-next-line @typescript-eslint/require-await
          write: async (data) => {
            ctx.written.push(new Uint8Array(data))
            requestCount++

            let body: Uint8Array

            if (requestCount === 1) {
              // SaslHandshake response
              body = handshakeBody
            } else if (requestCount === 2) {
              // First SaslAuthenticate — extract client nonce from request
              // The auth bytes in the request contain: n,,n=user,r=<nonce>
              const dataStr = new TextDecoder().decode(data)
              const nonceMatch = /r=([A-Za-z0-9+/=]+)/.exec(dataStr)
              clientNonce = nonceMatch ? nonceMatch[1] : "unknown"

              // Server-first-message
              const serverFirst = `r=${clientNonce}servernonce,s=${btoa("salt1234")},i=4096`
              body = buildAuthResponse(0, null, new TextEncoder().encode(serverFirst))
            } else if (requestCount === 3) {
              // Second SaslAuthenticate — server-final with a matching verifier
              // For simplicity, send an empty server-final (v= with wrong verifier
              // would be rejected, so send e= to cleanly signal completion).
              // In a real test we'd compute the verifier, but this tests the multi-step
              // loop in authenticate(). The SCRAM logic itself is tested in sasl.test.ts.
              //
              // Actually use v= with a verifier that won't match — the authenticate()
              // method will call stepAsync() which will throw, and that tests error propagation.
              // Instead, let's just provide a valid response with empty authBytes to terminate.
              body = buildAuthResponse(0, null, new Uint8Array(0))
            } else {
              body = buildAuthResponse(0, null, new Uint8Array(0))
            }

            const frame = buildMockResponse(correlationId++, body)
            queueMicrotask(() => {
              ctx.callbacks!.onData(frame)
            })
          },
          // eslint-disable-next-line @typescript-eslint/no-empty-function
          close: async () => {}
        }
        return Promise.resolve(socket)
      })

      conn = new KafkaConnection({
        ...defaultOptions(mock.factory),
        sasl: { mechanism: "SCRAM-SHA-256", username: "user", password: "pencil" }
      })
      await conn.connect()

      // The SCRAM exchange happens: handshake + first auth + second auth
      // The second auth returns empty authBytes so the loop terminates.
      // stepAsync processes the server-first and emits client-final.
      // When empty authBytes comes back, the loop exits without calling stepAsync again.
      await conn.authenticate()

      // At least 3 requests: handshake + auth round 1 + auth round 2
      expect(mock.written.length).toBeGreaterThanOrEqual(3)
    })

    it("propagates SCRAM step error during multi-step auth", async () => {
      const handshakeBody = buildHandshakeResponse(0, ["SCRAM-SHA-256"])

      let requestCount = 0

      // eslint-disable-next-line @typescript-eslint/promise-function-async
      const mock = createMockSocketFactory((ctx) => {
        let correlationId = 0
        const socket: KafkaSocket = {
          // eslint-disable-next-line @typescript-eslint/require-await
          write: async (data) => {
            ctx.written.push(new Uint8Array(data))
            requestCount++

            let body: Uint8Array

            if (requestCount === 1) {
              body = handshakeBody
            } else if (requestCount === 2) {
              // Return garbage server-first so SCRAM stepAsync throws
              body = buildAuthResponse(0, null, new TextEncoder().encode("garbage"))
            } else {
              body = buildAuthResponse(0, null, new Uint8Array(0))
            }

            const frame = buildMockResponse(correlationId++, body)
            queueMicrotask(() => {
              ctx.callbacks!.onData(frame)
            })
          },
          // eslint-disable-next-line @typescript-eslint/no-empty-function
          close: async () => {}
        }
        return Promise.resolve(socket)
      })

      conn = new KafkaConnection({
        ...defaultOptions(mock.factory),
        sasl: { mechanism: "SCRAM-SHA-256", username: "user", password: "pass" }
      })
      await conn.connect()

      await expect(conn.authenticate()).rejects.toThrow("missing required attributes")
    })

    it("throws when multi-step auth returns error code", async () => {
      const handshakeBody = buildHandshakeResponse(0, ["SCRAM-SHA-256"])

      let requestCount = 0

      // eslint-disable-next-line @typescript-eslint/promise-function-async
      const mock = createMockSocketFactory((ctx) => {
        let correlationId = 0
        const socket: KafkaSocket = {
          // eslint-disable-next-line @typescript-eslint/require-await
          write: async (data) => {
            ctx.written.push(new Uint8Array(data))
            requestCount++

            let body: Uint8Array

            if (requestCount === 1) {
              body = handshakeBody
            } else if (requestCount === 2) {
              // First auth response with server-first
              const dataStr = new TextDecoder().decode(data)
              const nonceMatch = /r=([A-Za-z0-9+/=]+)/.exec(dataStr)
              const clientNonce = nonceMatch ? nonceMatch[1] : "unknown"
              const serverFirst = `r=${clientNonce}srv,s=${btoa("salt")},i=4096`
              body = buildAuthResponse(0, null, new TextEncoder().encode(serverFirst))
            } else {
              // Second auth returns error
              body = buildAuthResponse(58, "bad credentials", null)
            }

            const frame = buildMockResponse(correlationId++, body)
            queueMicrotask(() => {
              ctx.callbacks!.onData(frame)
            })
          },
          // eslint-disable-next-line @typescript-eslint/no-empty-function
          close: async () => {}
        }
        return Promise.resolve(socket)
      })

      conn = new KafkaConnection({
        ...defaultOptions(mock.factory),
        sasl: { mechanism: "SCRAM-SHA-256", username: "user", password: "pass" }
      })
      await conn.connect()

      await expect(conn.authenticate()).rejects.toThrow("SASL authentication failed")
    })
  })
})
