/* eslint-disable @typescript-eslint/require-await */
/**
 * Property-based tests for connection-level chunk reassembly.
 *
 * Uses fast-check to verify that KafkaConnection correctly reassembles
 * response frames regardless of how TCP chunks split the data.
 *
 * Kafka protocol spec §Protocol Framing:
 * "Requests and responses are both prefixed with a 4-byte big-endian
 * length field." TCP provides no message boundaries, so the connection
 * layer must buffer and reassemble arbitrarily chunked data.
 *
 * @see https://kafka.apache.org/protocol.html#protocol_common
 */

import fc from "fast-check"
import { afterEach, describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryWriter } from "../../../codec/binary-writer"
import { type ConnectionOptions, KafkaConnection } from "../../../network/connection"
import type { KafkaSocket, SocketFactory } from "../../../network/socket"

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

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

function createMockSocketFactory(): MockSocketContext {
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

    const socket: KafkaSocket = {
      write: async (data) => {
        ctx.written.push(new Uint8Array(data))
        return Promise.resolve()
      },
      close: async () => Promise.resolve()
    }
    ctx.socket = socket
    return socket
  }

  return ctx
}

function buildMockResponse(
  correlationId: number,
  body: Uint8Array = new Uint8Array(0)
): Uint8Array {
  const inner = new BinaryWriter()
  inner.writeInt32(correlationId)
  inner.writeRawBytes(body)
  const payload = inner.finish()

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
    requestTimeoutMs: 5000,
    connectTimeoutMs: 1000
  }
}

const noop = (): void => {
  /* noop */
}

/**
 * Split a Uint8Array into chunks at the given split points.
 * Split points are indices where a new chunk begins.
 */
function splitAtPoints(data: Uint8Array, points: number[]): Uint8Array[] {
  const sorted = [
    ...new Set([0, ...points.filter((p) => p > 0 && p < data.byteLength), data.byteLength])
  ].sort((a, b) => a - b)
  const chunks: Uint8Array[] = []
  for (let i = 0; i < sorted.length - 1; i++) {
    chunks.push(data.subarray(sorted[i], sorted[i + 1]))
  }
  return chunks
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("property-based: connection chunk reassembly", () => {
  let conn: KafkaConnection
  let mock: MockSocketContext

  afterEach(async () => {
    try {
      await conn.close()
    } catch {
      // Ignore — may already be closed
    }
  })

  /**
   * @see https://kafka.apache.org/protocol.html#protocol_common — Size-prefixed framing
   *
   * Verifies that a single response delivered in arbitrary chunks
   * reassembles correctly and resolves the pending request.
   */
  it("reassembles single response from arbitrary chunk splits", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.uint8Array({ minLength: 0, maxLength: 50 }),
        fc.array(fc.integer({ min: 1, max: 100 }), { minLength: 0, maxLength: 10 }),
        async (body, splitPoints) => {
          mock = createMockSocketFactory()
          conn = new KafkaConnection(defaultOptions(mock.factory))
          await conn.connect()

          const promise = conn.send(ApiKey.ApiVersions, 0, noop)
          const response = buildMockResponse(0, body)
          const chunks = splitAtPoints(response, splitPoints)

          for (const chunk of chunks) {
            mock.callbacks!.onData(chunk)
          }

          const reader = await promise
          expect(reader.remaining).toBe(body.byteLength)

          await conn.close()
        }
      ),
      { numRuns: 50 }
    )
  })

  /**
   * @see https://kafka.apache.org/protocol.html#protocol_common — Size-prefixed framing
   *
   * Verifies that multiple responses concatenated into a single chunk
   * are correctly demultiplexed to their respective pending requests.
   */
  it("demultiplexes multiple responses from a single data event", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.uint8Array({ minLength: 0, maxLength: 30 }), { minLength: 1, maxLength: 3 }),
        async (bodies) => {
          mock = createMockSocketFactory()
          conn = new KafkaConnection(defaultOptions(mock.factory))
          await conn.connect()

          // Send all requests
          const promises = bodies.map(async () => conn.send(ApiKey.ApiVersions, 0, noop))

          // Build responses and concatenate into one buffer
          const responses = bodies.map((body, i) => buildMockResponse(i, body))
          const totalLength = responses.reduce((sum, r) => sum + r.byteLength, 0)
          const combined = new Uint8Array(totalLength)
          let offset = 0
          for (const r of responses) {
            combined.set(r, offset)
            offset += r.byteLength
          }

          // Deliver all at once
          mock.callbacks!.onData(combined)

          // All should resolve with correct body sizes
          const readers = await Promise.all(promises)
          for (let i = 0; i < bodies.length; i++) {
            expect(readers[i].remaining).toBe(bodies[i].byteLength)
          }

          await conn.close()
        }
      ),
      { numRuns: 30 }
    )
  })

  /**
   * @see https://kafka.apache.org/protocol.html#protocol_common — Size-prefixed framing
   *
   * Verifies that multiple responses split across arbitrary chunk
   * boundaries all resolve correctly.
   */
  it("reassembles multiple responses from arbitrary chunk splits", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.uint8Array({ minLength: 0, maxLength: 20 }), { minLength: 1, maxLength: 3 }),
        fc.array(fc.integer({ min: 1, max: 200 }), { minLength: 0, maxLength: 8 }),
        async (bodies, splitPoints) => {
          mock = createMockSocketFactory()
          conn = new KafkaConnection(defaultOptions(mock.factory))
          await conn.connect()

          const promises = bodies.map(async () => conn.send(ApiKey.ApiVersions, 0, noop))

          // Build all responses and concatenate
          const responses = bodies.map((body, i) => buildMockResponse(i, body))
          const totalLength = responses.reduce((sum, r) => sum + r.byteLength, 0)
          const combined = new Uint8Array(totalLength)
          let offset = 0
          for (const r of responses) {
            combined.set(r, offset)
            offset += r.byteLength
          }

          // Split at arbitrary points and deliver
          const chunks = splitAtPoints(combined, splitPoints)
          for (const chunk of chunks) {
            mock.callbacks!.onData(chunk)
          }

          const readers = await Promise.all(promises)
          for (let i = 0; i < bodies.length; i++) {
            expect(readers[i].remaining).toBe(bodies[i].byteLength)
          }

          await conn.close()
        }
      ),
      { numRuns: 30 }
    )
  })
})
