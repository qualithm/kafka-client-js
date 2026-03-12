/* eslint-disable @typescript-eslint/require-await, @typescript-eslint/no-empty-function */
import { afterEach, describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryWriter } from "../../../codec/binary-writer"
import { KafkaConnectionError } from "../../../errors"
import {
  ConnectionPool,
  type ConnectionPoolOptions,
  discoverBrokers
} from "../../../network/broker-pool"
import { KafkaConnection } from "../../../network/connection"
import type { KafkaSocket, SocketFactory } from "../../../network/socket"

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/**
 * Build a size-prefixed mock response with the given correlation ID and body.
 */
function buildMockResponse(
  correlationId: number,
  body: Uint8Array = new Uint8Array(0),
  flexible = false
): Uint8Array {
  const inner = new BinaryWriter()
  inner.writeInt32(correlationId)
  if (flexible) {
    inner.writeUnsignedVarInt(0)
  }
  inner.writeRawBytes(body)
  const payload = inner.finish()

  const framed = new BinaryWriter(payload.byteLength + 4)
  framed.writeInt32(payload.byteLength)
  framed.writeRawBytes(payload)
  return framed.finish()
}

/**
 * Build an ApiVersions v0 response body with Metadata support.
 *
 * Response format (v0):
 *   error_code: INT16
 *   api_versions: ARRAY(api_key: INT16, min_version: INT16, max_version: INT16)
 */
function buildApiVersionsResponseBody(
  apis: { apiKey: number; minVersion: number; maxVersion: number }[]
): Uint8Array {
  const writer = new BinaryWriter()
  // error_code = 0
  writer.writeInt16(0)
  // array length
  writer.writeInt32(apis.length)
  for (const api of apis) {
    writer.writeInt16(api.apiKey)
    writer.writeInt16(api.minVersion)
    writer.writeInt16(api.maxVersion)
  }
  return writer.finish()
}

/**
 * Build a Metadata v1 response body with the given brokers and no topics.
 *
 * Response format (v1):
 *   throttle_time_ms: INT32 (v3+, omitted for v1)
 *   brokers: ARRAY(node_id: INT32, host: STRING, port: INT32, rack: NULLABLE_STRING)
 *   controller_id: INT32
 *   topics: ARRAY(...)
 *
 * v1 format:
 *   brokers: ARRAY(node_id: INT32, host: INT16+bytes, port: INT32, rack: INT16+bytes/-1)
 *   controller_id: INT32
 *   topics: ARRAY(...)
 */
function buildMetadataV1ResponseBody(
  brokers: { nodeId: number; host: string; port: number; rack: string | null }[]
): Uint8Array {
  const writer = new BinaryWriter()

  // brokers array
  writer.writeInt32(brokers.length)
  for (const b of brokers) {
    writer.writeInt32(b.nodeId)
    writer.writeString(b.host)
    writer.writeInt32(b.port)
    writer.writeString(b.rack)
  }

  // controller_id
  writer.writeInt32(0)

  // topics array (empty)
  writer.writeInt32(0)

  return writer.finish()
}

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

/**
 * Create a mock socket factory that auto-responds to ApiVersions and Metadata requests.
 *
 * The mock responds with:
 * - ApiVersions v0: supports Metadata v0–v12
 * - Metadata v1: returns the provided broker list
 */
function createAutoRespondingSocketFactory(
  brokerList: { nodeId: number; host: string; port: number; rack: string | null }[]
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

    const socket: KafkaSocket = {
      write: async (data) => {
        ctx.written.push(new Uint8Array(data))

        // Parse the request to figure out which API was called
        // Frame: INT32 size, then header: INT16 apiKey, INT16 apiVersion, INT32 correlationId
        const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
        const apiKey = view.getInt16(4)
        const correlationId = view.getInt32(8)

        // Respond based on API key
        if (apiKey === ApiKey.ApiVersions) {
          const body = buildApiVersionsResponseBody([
            { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
            // Advertise only v0–v1 so negotiation picks v1 (non-flexible)
            { apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 1 }
          ])
          const response = buildMockResponse(correlationId, body)
          // Deliver response asynchronously to simulate network
          queueMicrotask(() => {
            ctx.callbacks?.onData(response)
          })
        } else if (apiKey === ApiKey.Metadata) {
          const body = buildMetadataV1ResponseBody(brokerList)
          const response = buildMockResponse(correlationId, body)
          queueMicrotask(() => {
            ctx.callbacks?.onData(response)
          })
        }
      },
      close: async () => {}
    }
    ctx.socket = socket
    return socket
  }

  return ctx
}

/**
 * Create a simple mock socket factory for connection pool tests
 * (no auto-responding needed for basic pool lifecycle tests).
 */
function createSimpleSocketFactory(): MockSocketContext {
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
      },
      close: async () => {}
    }
    ctx.socket = socket
    return socket
  }

  return ctx
}

function defaultPoolOptions(factory: SocketFactory): ConnectionPoolOptions {
  return {
    bootstrapBrokers: [{ host: "localhost", port: 9092 }],
    socketFactory: factory,
    connectTimeoutMs: 1000,
    requestTimeoutMs: 1000
  }
}

/**
 * Create a socket factory that returns a custom ApiVersions response.
 * Only handles ApiVersions requests — does not respond to Metadata.
 */
function createCustomApiVersionsSocketFactory(
  buildResponse: (correlationId: number) => Uint8Array
): SocketFactory {
  const factory: SocketFactory = async (options) => {
    const socket: KafkaSocket = {
      write: async (data) => {
        const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
        const correlationId = view.getInt32(8)
        const response = buildResponse(correlationId)
        queueMicrotask(() => {
          options.onData(response)
        })
      },
      close: async () => {}
    }
    return socket
  }
  return factory
}

/**
 * Create a socket factory that dispatches to different response builders
 * based on the API key in the request.
 */
function createCustomRespondingSocketFactory(
  buildResponse: (apiKey: number, correlationId: number) => Uint8Array
): SocketFactory {
  const factory: SocketFactory = async (options) => {
    const socket: KafkaSocket = {
      write: async (data) => {
        const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
        const apiKey = view.getInt16(4)
        const correlationId = view.getInt32(8)
        const response = buildResponse(apiKey, correlationId)
        queueMicrotask(() => {
          options.onData(response)
        })
      },
      close: async () => {}
    }
    return socket
  }
  return factory
}

// ---------------------------------------------------------------------------
// discoverBrokers
// ---------------------------------------------------------------------------

describe("discoverBrokers", () => {
  it("throws when no bootstrap brokers are provided", async () => {
    const mock = createSimpleSocketFactory()
    await expect(discoverBrokers([], mock.factory)).rejects.toThrow("no bootstrap brokers provided")
  })

  it("discovers brokers from metadata response", async () => {
    const expectedBrokers = [
      { nodeId: 1, host: "broker-1", port: 9092, rack: null },
      { nodeId: 2, host: "broker-2", port: 9093, rack: "rack-a" },
      { nodeId: 3, host: "broker-3", port: 9094, rack: "rack-b" }
    ]
    const mock = createAutoRespondingSocketFactory(expectedBrokers)

    const result = await discoverBrokers([{ host: "localhost", port: 9092 }], mock.factory, {
      requestTimeoutMs: 1000
    })

    expect(result).toEqual(expectedBrokers)
  })

  it("tries the next bootstrap broker if the first fails", async () => {
    const expectedBrokers = [{ nodeId: 1, host: "broker-1", port: 9092, rack: null }]

    let callCount = 0
    const factory: SocketFactory = async (options) => {
      callCount++
      if (callCount === 1) {
        throw new Error("ECONNREFUSED")
      }

      // Second call succeeds
      const mockCtx = createAutoRespondingSocketFactory(expectedBrokers)
      return mockCtx.factory(options)
    }

    const result = await discoverBrokers(
      [
        { host: "unreachable", port: 9092 },
        { host: "localhost", port: 9092 }
      ],
      factory,
      { requestTimeoutMs: 1000 }
    )

    expect(result).toEqual(expectedBrokers)
    expect(callCount).toBe(2)
  })

  it("throws KafkaConnectionError when all bootstrap brokers fail", async () => {
    const factory: SocketFactory = async () => {
      throw new Error("ECONNREFUSED")
    }

    await expect(
      discoverBrokers(
        [
          { host: "bad-1", port: 9092 },
          { host: "bad-2", port: 9092 }
        ],
        factory,
        { requestTimeoutMs: 1000 }
      )
    ).rejects.toThrow(KafkaConnectionError)

    await expect(
      discoverBrokers([{ host: "bad-1", port: 9092 }], factory, { requestTimeoutMs: 1000 })
    ).rejects.toThrow("none of the bootstrap brokers are reachable")
  })

  it("throws when api versions response has non-zero error code", async () => {
    const factory = createCustomApiVersionsSocketFactory((correlationId) => {
      // Return an ApiVersions response with error_code = 33 (INVALID_REQUEST)
      const writer = new BinaryWriter()
      writer.writeInt16(33)
      writer.writeInt32(0) // empty array
      return buildMockResponse(correlationId, writer.finish())
    })

    await expect(
      discoverBrokers([{ host: "localhost", port: 9092 }], factory, { requestTimeoutMs: 1000 })
    ).rejects.toThrow("none of the bootstrap brokers are reachable")

    try {
      await discoverBrokers([{ host: "localhost", port: 9092 }], factory, {
        requestTimeoutMs: 1000
      })
    } catch (error) {
      expect(error).toBeInstanceOf(KafkaConnectionError)
      expect((error as KafkaConnectionError).cause).toBeInstanceOf(KafkaConnectionError)
      expect(((error as KafkaConnectionError).cause as Error).message).toContain(
        "api versions request failed with error code 33"
      )
    }
  })

  it("throws when broker does not support metadata api", async () => {
    const factory = createCustomApiVersionsSocketFactory((correlationId) => {
      // Return ApiVersions response with only ApiVersions key (no Metadata)
      const body = buildApiVersionsResponseBody([
        { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 }
      ])
      return buildMockResponse(correlationId, body)
    })

    try {
      await discoverBrokers([{ host: "localhost", port: 9092 }], factory, {
        requestTimeoutMs: 1000
      })
      expect.unreachable("should have thrown")
    } catch (error) {
      expect(error).toBeInstanceOf(KafkaConnectionError)
      expect(((error as KafkaConnectionError).cause as Error).message).toContain(
        "broker does not support metadata api"
      )
    }
  })

  it("throws when no compatible metadata api version exists", async () => {
    const factory = createCustomApiVersionsSocketFactory((correlationId) => {
      // Return ApiVersions with Metadata range that has no overlap with client
      // Client supports 0–12; advertise min 99–100 so negotiation returns null
      const body = buildApiVersionsResponseBody([
        { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
        { apiKey: ApiKey.Metadata, minVersion: 99, maxVersion: 100 }
      ])
      return buildMockResponse(correlationId, body)
    })

    try {
      await discoverBrokers([{ host: "localhost", port: 9092 }], factory, {
        requestTimeoutMs: 1000
      })
      expect.unreachable("should have thrown")
    } catch (error) {
      expect(error).toBeInstanceOf(KafkaConnectionError)
      expect(((error as KafkaConnectionError).cause as Error).message).toContain(
        "no compatible metadata api version"
      )
    }
  })

  it("throws when api versions response is truncated", async () => {
    const factory = createCustomApiVersionsSocketFactory((correlationId) => {
      // Return a truncated body (just 1 byte — not enough for error_code INT16)
      return buildMockResponse(correlationId, new Uint8Array([0x00]))
    })

    try {
      await discoverBrokers([{ host: "localhost", port: 9092 }], factory, {
        requestTimeoutMs: 1000
      })
      expect.unreachable("should have thrown")
    } catch (error) {
      expect(error).toBeInstanceOf(KafkaConnectionError)
      expect(((error as KafkaConnectionError).cause as Error).message).toContain(
        "failed to decode api versions response"
      )
    }
  })

  it("throws when metadata response is malformed", async () => {
    const factory = createCustomRespondingSocketFactory((apiKey, correlationId) => {
      if (apiKey === ApiKey.ApiVersions) {
        const body = buildApiVersionsResponseBody([
          { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
          { apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 1 }
        ])
        return buildMockResponse(correlationId, body)
      }
      // Metadata response: truncated body
      return buildMockResponse(correlationId, new Uint8Array([0xff]))
    })

    try {
      await discoverBrokers([{ host: "localhost", port: 9092 }], factory, {
        requestTimeoutMs: 1000
      })
      expect.unreachable("should have thrown")
    } catch (error) {
      expect(error).toBeInstanceOf(KafkaConnectionError)
      expect(((error as KafkaConnectionError).cause as Error).message).toContain(
        "failed to decode metadata response"
      )
    }
  })
})

// ---------------------------------------------------------------------------
// ConnectionPool
// ---------------------------------------------------------------------------

describe("ConnectionPool", () => {
  let pool: ConnectionPool

  afterEach(async () => {
    try {
      await pool.close()
    } catch {
      // Ignore — may already be closed
    }
  })

  // -----------------------------------------------------------------------
  // Constructor and properties
  // -----------------------------------------------------------------------

  describe("constructor", () => {
    it("defaults to not closed", () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool(defaultPoolOptions(mock.factory))
      expect(pool.isClosed).toBe(false)
    })

    it("starts with no known brokers", () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool(defaultPoolOptions(mock.factory))
      expect(pool.brokers.size).toBe(0)
    })
  })

  // -----------------------------------------------------------------------
  // connect()
  // -----------------------------------------------------------------------

  describe("connect", () => {
    it("discovers brokers on connect", async () => {
      const brokerList = [
        { nodeId: 1, host: "broker-1", port: 9092, rack: null },
        { nodeId: 2, host: "broker-2", port: 9093, rack: "rack-a" }
      ]
      const mock = createAutoRespondingSocketFactory(brokerList)
      pool = new ConnectionPool(defaultPoolOptions(mock.factory))

      await pool.connect()

      expect(pool.brokers.size).toBe(2)
      expect(pool.brokers.get(1)).toEqual(brokerList[0])
      expect(pool.brokers.get(2)).toEqual(brokerList[1])
    })

    it("throws if pool is closed", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool(defaultPoolOptions(mock.factory))
      await pool.close()

      await expect(pool.connect()).rejects.toThrow("connection pool has been closed")
    })
  })

  // -----------------------------------------------------------------------
  // getConnection()
  // -----------------------------------------------------------------------

  describe("getConnection", () => {
    it("creates a new connection to a broker", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: []
      })

      const conn = await pool.getConnection("localhost", 9092)

      expect(conn).toBeInstanceOf(KafkaConnection)
      expect(conn.connected).toBe(true)
      expect(conn.host).toBe("localhost")
      expect(conn.port).toBe(9092)
    })

    it("reuses idle connections", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 2
      })

      const conn1 = await pool.getConnection("localhost", 9092)
      pool.releaseConnection(conn1)

      const conn2 = await pool.getConnection("localhost", 9092)
      expect(conn2).toBe(conn1) // Same instance reused
    })

    it("creates up to maxConnectionsPerBroker connections", async () => {
      let connectCount = 0
      const factory: SocketFactory = async () => {
        connectCount++
        return {
          write: async () => {},
          close: async () => {}
        }
      }

      pool = new ConnectionPool({
        ...defaultPoolOptions(factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 3
      })

      const conns = await Promise.all([
        pool.getConnection("localhost", 9092),
        pool.getConnection("localhost", 9092),
        pool.getConnection("localhost", 9092)
      ])

      expect(connectCount).toBe(3)
      expect(conns[0]).not.toBe(conns[1])
      expect(conns[1]).not.toBe(conns[2])

      for (const c of conns) {
        pool.releaseConnection(c)
      }
    })

    it("throws if pool is closed", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: []
      })
      await pool.close()

      await expect(pool.getConnection("localhost", 9092)).rejects.toThrow(
        "connection pool has been closed"
      )
    })
  })

  // -----------------------------------------------------------------------
  // getConnectionByNodeId()
  // -----------------------------------------------------------------------

  describe("getConnectionByNodeId", () => {
    it("returns a connection to a known broker", async () => {
      const brokerList = [{ nodeId: 1, host: "broker-1", port: 9092, rack: null }]
      const mock = createAutoRespondingSocketFactory(brokerList)
      pool = new ConnectionPool(defaultPoolOptions(mock.factory))
      await pool.connect()

      const conn = await pool.getConnectionByNodeId(1)

      expect(conn).toBeInstanceOf(KafkaConnection)
      expect(conn.host).toBe("broker-1")
      expect(conn.port).toBe(9092)

      pool.releaseConnection(conn)
    })

    it("throws for unknown node IDs", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: []
      })

      await expect(pool.getConnectionByNodeId(999)).rejects.toThrow("unknown broker node id 999")
    })
  })

  // -----------------------------------------------------------------------
  // releaseConnection()
  // -----------------------------------------------------------------------

  describe("releaseConnection", () => {
    it("returns connection to idle pool", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: []
      })

      const conn = await pool.getConnection("localhost", 9092)
      expect(pool.connectionCount("localhost", 9092)).toBe(1)

      pool.releaseConnection(conn)
      expect(pool.connectionCount("localhost", 9092)).toBe(1) // Still tracked, but idle
    })

    it("gives connection to waiting callers", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 1
      })

      const conn1 = await pool.getConnection("localhost", 9092)

      // This will block since we're at maxConnectionsPerBroker
      let resolved = false
      const waiterPromise = pool.getConnection("localhost", 9092).then((conn) => {
        resolved = true
        return conn
      })

      // Not resolved yet
      await new Promise((r) => setTimeout(r, 10))
      expect(resolved).toBe(false)

      // Release the connection — should wake the waiter
      pool.releaseConnection(conn1)
      const conn2 = await waiterPromise
      expect(resolved).toBe(true)
      expect(conn2).toBe(conn1)

      pool.releaseConnection(conn2)
    })

    it("discards disconnected connections", async () => {
      const closedSockets: KafkaSocket[] = []
      const factory: SocketFactory = async () => {
        const socket: KafkaSocket = {
          write: async () => {},
          close: async () => {
            closedSockets.push(socket)
          }
        }
        return socket
      }

      pool = new ConnectionPool({
        ...defaultPoolOptions(factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 2
      })

      const conn = await pool.getConnection("localhost", 9092)
      await conn.close()
      pool.releaseConnection(conn)
    })

    it("ignores release of connection with unknown broker key", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: []
      })

      // Create a connection directly (not through the pool)
      const directConn = new KafkaConnection({
        host: "unknown-host",
        port: 1234,
        socketFactory: mock.factory,
        requestTimeoutMs: 1000
      })
      await directConn.connect()

      // Release should not throw even though this broker was never tracked
      pool.releaseConnection(directConn)
      expect(pool.connectionCount("unknown-host", 1234)).toBe(0)

      await directConn.close()
    })

    it("wakes waiter with new connection when dead connection is released", async () => {
      const factory: SocketFactory = async (_options) => ({
        write: async () => {},
        close: async () => {}
      })

      pool = new ConnectionPool({
        ...defaultPoolOptions(factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 1
      })

      const conn1 = await pool.getConnection("localhost", 9092)

      // Queue a waiter
      const waiterPromise = pool.getConnection("localhost", 9092)

      // Close the first connection, then release it — should trigger wakeWaiter
      await conn1.close()
      pool.releaseConnection(conn1)

      const conn2 = await waiterPromise
      expect(conn2).not.toBe(conn1)
      expect(conn2.connected).toBe(true)

      pool.releaseConnection(conn2)
    })

    it("rejects waiter when wakeWaiter connection fails", async () => {
      let connectCount = 0
      const factory: SocketFactory = async () => {
        connectCount++
        if (connectCount > 1) {
          throw new Error("ECONNREFUSED")
        }
        return {
          write: async () => {},
          close: async () => {}
        }
      }

      pool = new ConnectionPool({
        ...defaultPoolOptions(factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 1
      })

      const conn1 = await pool.getConnection("localhost", 9092)

      // Queue a waiter
      const waiterPromise = pool.getConnection("localhost", 9092)

      // Close and release — wakeWaiter will try to connect and fail
      await conn1.close()
      pool.releaseConnection(conn1)

      await expect(waiterPromise).rejects.toThrow("failed to connect to localhost:9092")
    })

    it("skips idle connections that were closed externally", async () => {
      const factory: SocketFactory = async (_options) => ({
        write: async () => {},
        close: async () => {}
      })

      pool = new ConnectionPool({
        ...defaultPoolOptions(factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 3
      })

      // Get two connections and idle them
      const conn1 = await pool.getConnection("localhost", 9092)
      const conn2 = await pool.getConnection("localhost", 9092)
      pool.releaseConnection(conn1)
      pool.releaseConnection(conn2)

      // Close conn2 externally while it's idle
      await conn2.close()

      // Next getConnection should skip dead conn2 and return live conn1
      const conn3 = await pool.getConnection("localhost", 9092)
      expect(conn3).toBe(conn1)
      expect(conn3.connected).toBe(true)

      pool.releaseConnection(conn3)
    })
  })

  // -----------------------------------------------------------------------
  // refreshMetadata()
  // -----------------------------------------------------------------------

  describe("refreshMetadata", () => {
    it("updates known brokers", async () => {
      const brokerList1 = [{ nodeId: 1, host: "broker-1", port: 9092, rack: null }]
      const brokerList2 = [
        { nodeId: 1, host: "broker-1", port: 9092, rack: null },
        { nodeId: 2, host: "broker-2", port: 9093, rack: "rack-a" }
      ]

      let callCount = 0
      const factory: SocketFactory = async (options) => {
        callCount++
        const brokers = callCount <= 1 ? brokerList1 : brokerList2
        const mock = createAutoRespondingSocketFactory(brokers)
        return mock.factory(options)
      }

      pool = new ConnectionPool(defaultPoolOptions(factory))
      await pool.connect()
      expect(pool.brokers.size).toBe(1)

      await pool.refreshMetadata()
      expect(pool.brokers.size).toBe(2)
    })

    it("throws if pool is closed", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool(defaultPoolOptions(mock.factory))
      await pool.close()

      await expect(pool.refreshMetadata()).rejects.toThrow("connection pool has been closed")
    })
  })

  // -----------------------------------------------------------------------
  // close()
  // -----------------------------------------------------------------------

  describe("close", () => {
    it("closes all connections", async () => {
      let closeCount = 0
      const factory: SocketFactory = async () => ({
        write: async () => {},
        close: async () => {
          closeCount++
        }
      })

      pool = new ConnectionPool({
        ...defaultPoolOptions(factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 3
      })

      const conn1 = await pool.getConnection("localhost", 9092)
      await pool.getConnection("localhost", 9092)
      pool.releaseConnection(conn1) // idle

      await pool.close()

      expect(pool.isClosed).toBe(true)
      expect(closeCount).toBe(2)
    })

    it("rejects pending waiters", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 1
      })

      await pool.getConnection("localhost", 9092)

      // This will block
      const waiterPromise = pool.getConnection("localhost", 9092)

      await pool.close()

      await expect(waiterPromise).rejects.toThrow("connection pool closed")
    })

    it("is idempotent", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: []
      })

      await pool.close()
      await pool.close() // Should not throw
    })

    it("clears known brokers", async () => {
      const brokerList = [{ nodeId: 1, host: "broker-1", port: 9092, rack: null }]
      const mock = createAutoRespondingSocketFactory(brokerList)
      pool = new ConnectionPool(defaultPoolOptions(mock.factory))
      await pool.connect()
      expect(pool.brokers.size).toBe(1)

      await pool.close()
      expect(pool.brokers.size).toBe(0)
    })
  })

  // -----------------------------------------------------------------------
  // connectionCount()
  // -----------------------------------------------------------------------

  describe("connectionCount", () => {
    it("returns 0 for unknown brokers", () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool(defaultPoolOptions(mock.factory))
      expect(pool.connectionCount("unknown", 1234)).toBe(0)
    })

    it("tracks active and idle connections", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 3
      })

      expect(pool.connectionCount("localhost", 9092)).toBe(0)

      const conn1 = await pool.getConnection("localhost", 9092)
      expect(pool.connectionCount("localhost", 9092)).toBe(1)

      const conn2 = await pool.getConnection("localhost", 9092)
      expect(pool.connectionCount("localhost", 9092)).toBe(2)

      pool.releaseConnection(conn1)
      expect(pool.connectionCount("localhost", 9092)).toBe(2)

      pool.releaseConnection(conn2)
      expect(pool.connectionCount("localhost", 9092)).toBe(2)
    })
  })

  describe("auto-reconnection", () => {
    it("does not reconnect when reconnect is not configured", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 2
      })

      const conn = await pool.getConnection("localhost", 9092)
      expect(pool.connectionCount("localhost", 9092)).toBe(1)

      // Simulate socket close so connection is dead
      mock.callbacks?.onClose()

      pool.releaseConnection(conn)

      // Dead connection is removed — no reconnection attempted
      expect(pool.connectionCount("localhost", 9092)).toBe(0)

      // Wait a bit to ensure no background reconnect
      await new Promise((resolve) => {
        setTimeout(resolve, 50)
      })
      expect(pool.connectionCount("localhost", 9092)).toBe(0)
    })

    it("replenishes pool in background when reconnect is configured", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 2,
        reconnect: { initialDelayMs: 10, maxRetries: 3, multiplier: 1 }
      })

      const conn = await pool.getConnection("localhost", 9092)
      expect(pool.connectionCount("localhost", 9092)).toBe(1)

      // Simulate socket close so connection is dead
      mock.callbacks?.onClose()

      pool.releaseConnection(conn)

      // Dead connection removed immediately
      expect(pool.connectionCount("localhost", 9092)).toBe(0)

      // Wait for background reconnect (initial delay 10ms + connect time)
      await new Promise((resolve) => {
        setTimeout(resolve, 100)
      })
      expect(pool.connectionCount("localhost", 9092)).toBe(1)
    })

    it("does not reconnect after pool is closed", async () => {
      const mock = createSimpleSocketFactory()
      pool = new ConnectionPool({
        ...defaultPoolOptions(mock.factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 2,
        reconnect: { initialDelayMs: 50, maxRetries: 3 }
      })

      const conn = await pool.getConnection("localhost", 9092)
      mock.callbacks?.onClose()
      pool.releaseConnection(conn)

      // Close pool before reconnect fires
      await pool.close()

      await new Promise((resolve) => {
        setTimeout(resolve, 150)
      })

      // Pool is closed, no new connections
      expect(pool.connectionCount("localhost", 9092)).toBe(0)
    })

    it("gives reconnected connection to waiting caller", async () => {
      let connectCount = 0
      const factory: SocketFactory = async (_options) => {
        connectCount++
        const socket: KafkaSocket = {
          write: async (data) => {
            void data
          },
          close: async () => {}
        }
        return socket
      }

      pool = new ConnectionPool({
        ...defaultPoolOptions(factory),
        bootstrapBrokers: [],
        maxConnectionsPerBroker: 1,
        reconnect: { initialDelayMs: 10, maxRetries: 3, multiplier: 1 }
      })

      const conn = await pool.getConnection("localhost", 9092)
      expect(connectCount).toBe(1)

      // Request another connection — will be queued since max is 1
      const waiterPromise = pool.getConnection("localhost", 9092)

      // Simulate the current connection dying
      // Access through connection internals: close the connection
      await conn.close()
      pool.releaseConnection(conn)

      // The waiter should be woken with a new connection (from wakeWaiter, not reconnect)
      const newConn = await waiterPromise
      expect(newConn.connected).toBe(true)
      expect(connectCount).toBe(2)

      pool.releaseConnection(newConn)
    })
  })
})
