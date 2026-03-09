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
})
