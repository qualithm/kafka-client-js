import { describe, expect, it } from "vitest"

import { KafkaAdmin } from "../../../client/admin"
import { KafkaConsumer } from "../../../client/consumer"
import { createKafka, Kafka, type KafkaOptions } from "../../../client/kafka"
import { KafkaProducer } from "../../../client/producer"
import { ApiKey } from "../../../codec/api-keys"
import { BinaryWriter } from "../../../codec/binary-writer"
import { KafkaConfigError, KafkaConnectionError } from "../../../errors"
import type { KafkaSocket, SocketFactory } from "../../../network/socket"

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/**
 * Build a size-prefixed mock response with the given correlation ID and body.
 */
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

/**
 * Build an ApiVersions v0 response body with Metadata support.
 */
function buildApiVersionsResponseBody(
  apis: { apiKey: number; minVersion: number; maxVersion: number }[]
): Uint8Array {
  const writer = new BinaryWriter()
  writer.writeInt16(0) // error_code = 0
  writer.writeInt32(apis.length)
  for (const api of apis) {
    writer.writeInt16(api.apiKey)
    writer.writeInt16(api.minVersion)
    writer.writeInt16(api.maxVersion)
  }
  return writer.finish()
}

/**
 * Build a Metadata v1 response body.
 */
function buildMetadataV1ResponseBody(
  brokers: { nodeId: number; host: string; port: number; rack: string | null }[]
): Uint8Array {
  const writer = new BinaryWriter()
  writer.writeInt32(brokers.length)
  for (const b of brokers) {
    writer.writeInt32(b.nodeId)
    writer.writeString(b.host)
    writer.writeInt32(b.port)
    writer.writeString(b.rack)
  }
  writer.writeInt32(0) // controller_id
  writer.writeInt32(0) // empty topics array
  return writer.finish()
}

type MockSocketCallbacks = {
  onData: (data: Uint8Array) => void
  onError: (error: Error) => void
  onClose: () => void
}

/**
 * Create a mock socket factory that auto-responds to ApiVersions and Metadata requests.
 */
function createAutoRespondingSocketFactory(
  brokerList: { nodeId: number; host: string; port: number; rack: string | null }[]
): { factory: SocketFactory; callbacks: MockSocketCallbacks | null } {
  let callbacks: MockSocketCallbacks | null = null

  const factory: SocketFactory = (options) => {
    callbacks = {
      onData: options.onData,
      onError: options.onError,
      onClose: options.onClose
    }

    const socket: KafkaSocket = {
      write: (data) => {
        const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
        const apiKey = view.getInt16(4)
        const correlationId = view.getInt32(8)

        if (apiKey === ApiKey.ApiVersions) {
          const body = buildApiVersionsResponseBody([
            { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
            { apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 1 }
          ])
          const response = buildMockResponse(correlationId, body)
          queueMicrotask(() => callbacks?.onData(response))
        } else if (apiKey === ApiKey.Metadata) {
          const body = buildMetadataV1ResponseBody(brokerList)
          const response = buildMockResponse(correlationId, body)
          queueMicrotask(() => callbacks?.onData(response))
        }
        return Promise.resolve()
      },
      close: () => Promise.resolve()
    }
    return Promise.resolve(socket)
  }

  return {
    factory,
    get callbacks() {
      return callbacks
    }
  }
}

/**
 * Create a socket factory that rejects all connections.
 */
function createFailingSocketFactory(): SocketFactory {
  return () => {
    return Promise.reject(new Error("connection refused"))
  }
}

/**
 * Create a socket factory that never resolves (hangs forever).
 * Useful for keeping the client stuck in "connecting" state.
 */
function createHangingSocketFactory(): SocketFactory {
  return () =>
    new Promise<KafkaSocket>(() => {
      /* never resolves */
    })
}

/**
 * Wrap a socket factory so that socket.close() blocks on the **second and
 * subsequent** sockets. The first socket (used by discoverBrokers during
 * pool.connect()) closes normally. Useful for keeping the client in
 * "disconnecting" state.
 */
function createSlowCloseSocketFactory(
  brokerList: {
    nodeId: number
    host: string
    port: number
    rack: string | null
  }[]
): { factory: SocketFactory; resolveAllCloses: () => void } {
  const closeResolvers: (() => void)[] = []
  let socketCount = 0

  const inner = createAutoRespondingSocketFactory(brokerList)
  const factory: SocketFactory = async (options) => {
    const socket = await inner.factory(options)
    const index = socketCount++
    return {
      write: socket.write,
      close:
        index === 0
          ? socket.close
          : async () =>
              new Promise<void>((resolve) => {
                closeResolvers.push(resolve)
              })
    }
  }

  return {
    factory,
    resolveAllCloses: () => {
      for (const resolve of closeResolvers) {
        resolve()
      }
      closeResolvers.length = 0
    }
  }
}

const DEFAULT_BROKERS = [{ nodeId: 1, host: "localhost", port: 9092, rack: null }]

function defaultOptions(overrides?: Partial<KafkaOptions>): KafkaOptions {
  const mock = createAutoRespondingSocketFactory(DEFAULT_BROKERS)
  return {
    config: { brokers: ["localhost:9092"] },
    socketFactory: mock.factory,
    ...overrides
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Kafka", () => {
  describe("constructor", () => {
    it("should throw KafkaConfigError when brokers list is empty", () => {
      const mock = createAutoRespondingSocketFactory(DEFAULT_BROKERS)
      expect(() => new Kafka({ config: { brokers: [] }, socketFactory: mock.factory })).toThrow(
        KafkaConfigError
      )
    })

    it("should accept string broker addresses", () => {
      const mock = createAutoRespondingSocketFactory(DEFAULT_BROKERS)
      const kafka = new Kafka({
        config: { brokers: ["localhost:9092", "broker2:9093"] },
        socketFactory: mock.factory
      })
      expect(kafka.state).toBe("disconnected")
    })

    it("should accept BrokerAddress objects", () => {
      const mock = createAutoRespondingSocketFactory(DEFAULT_BROKERS)
      const kafka = new Kafka({
        config: { brokers: [{ host: "localhost", port: 9092 }] },
        socketFactory: mock.factory
      })
      expect(kafka.state).toBe("disconnected")
    })
  })

  describe("state", () => {
    it("should start in disconnected state", () => {
      const kafka = new Kafka(defaultOptions())
      expect(kafka.state).toBe("disconnected")
    })

    it("should return empty brokers map when disconnected", () => {
      const kafka = new Kafka(defaultOptions())
      expect(kafka.brokers.size).toBe(0)
    })
  })

  describe("connect", () => {
    it("should transition to connected state", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      expect(kafka.state).toBe("connected")
      await kafka.disconnect()
    })

    it("should discover brokers on connect", async () => {
      const brokerList = [
        { nodeId: 1, host: "broker1", port: 9092, rack: null },
        { nodeId: 2, host: "broker2", port: 9093, rack: "rack-a" }
      ]
      const mock = createAutoRespondingSocketFactory(brokerList)
      const kafka = new Kafka({
        config: { brokers: ["broker1:9092"] },
        socketFactory: mock.factory
      })

      await kafka.connect()
      expect(kafka.brokers.size).toBe(2)
      expect(kafka.brokers.get(1)).toEqual({
        nodeId: 1,
        host: "broker1",
        port: 9092,
        rack: null
      })
      expect(kafka.brokers.get(2)).toEqual({
        nodeId: 2,
        host: "broker2",
        port: 9093,
        rack: "rack-a"
      })
      await kafka.disconnect()
    })

    it("should be a no-op when already connected", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      await kafka.connect() // should not throw
      expect(kafka.state).toBe("connected")
      await kafka.disconnect()
    })

    it("should throw when called while connecting", async () => {
      const kafka = new Kafka({
        config: { brokers: ["localhost:9092"] },
        socketFactory: createHangingSocketFactory()
      })

      void kafka.connect()
      expect(kafka.state).toBe("connecting")
      await expect(kafka.connect()).rejects.toThrow("client is already connecting")
    })

    it("should throw when called while disconnecting", async () => {
      const slow = createSlowCloseSocketFactory(DEFAULT_BROKERS)
      const kafka = new Kafka({
        config: { brokers: ["localhost:9092"] },
        socketFactory: slow.factory
      })

      await kafka.connect()
      // Acquire a pool connection so pool.close() has something to wait on
      await kafka.connectionPool.getConnection("localhost", 9092)
      const disconnectPromise = kafka.disconnect()
      expect(kafka.state).toBe("disconnecting")
      await expect(kafka.connect()).rejects.toThrow("client is disconnecting")
      slow.resolveAllCloses()
      await disconnectPromise
    })

    it("should reset to disconnected on connection failure", async () => {
      const kafka = new Kafka({
        config: { brokers: ["localhost:9092"] },
        socketFactory: createFailingSocketFactory()
      })

      await expect(kafka.connect()).rejects.toThrow()
      expect(kafka.state).toBe("disconnected")
    })
  })

  describe("disconnect", () => {
    it("should transition to disconnected state", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      await kafka.disconnect()
      expect(kafka.state).toBe("disconnected")
    })

    it("should be a no-op when already disconnected", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.disconnect() // should not throw
      expect(kafka.state).toBe("disconnected")
    })

    it("should handle disconnect when pool is null (defensive)", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      // Force pool to null to cover the defensive branch
      ;(kafka as any).pool = null
      await kafka.disconnect()
      expect(kafka.state).toBe("disconnected")
    })

    it("should clear brokers after disconnect", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      expect(kafka.brokers.size).toBeGreaterThan(0)
      await kafka.disconnect()
      expect(kafka.brokers.size).toBe(0)
    })

    it("should allow reconnect after disconnect", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      await kafka.disconnect()
      await kafka.connect()
      expect(kafka.state).toBe("connected")
      await kafka.disconnect()
    })

    it("should throw when called while connecting", async () => {
      const kafka = new Kafka({
        config: { brokers: ["localhost:9092"] },
        socketFactory: createHangingSocketFactory()
      })

      void kafka.connect()
      expect(kafka.state).toBe("connecting")
      await expect(kafka.disconnect()).rejects.toThrow("client is currently connecting")
    })

    it("should throw when called while already disconnecting", async () => {
      const slow = createSlowCloseSocketFactory(DEFAULT_BROKERS)
      const kafka = new Kafka({
        config: { brokers: ["localhost:9092"] },
        socketFactory: slow.factory
      })

      await kafka.connect()
      // Acquire a pool connection so pool.close() has something to wait on
      await kafka.connectionPool.getConnection("localhost", 9092)
      const disconnectPromise = kafka.disconnect()
      expect(kafka.state).toBe("disconnecting")
      await expect(kafka.disconnect()).rejects.toThrow("client is already disconnecting")
      slow.resolveAllCloses()
      await disconnectPromise
    })
  })

  describe("refreshMetadata", () => {
    it("should throw when not connected", async () => {
      const kafka = new Kafka(defaultOptions())
      await expect(kafka.refreshMetadata()).rejects.toThrow(KafkaConnectionError)
    })

    it("should refresh broker metadata", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      await kafka.refreshMetadata() // should not throw
      await kafka.disconnect()
    })
  })

  describe("connectionPool", () => {
    it("should throw when accessed before connect", () => {
      const kafka = new Kafka(defaultOptions())
      expect(() => kafka.connectionPool).toThrow(KafkaConnectionError)
    })

    it("should return pool when connected", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      expect(kafka.connectionPool).toBeDefined()
      await kafka.disconnect()
    })
  })

  describe("producer", () => {
    it("should create a KafkaProducer when connected", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      const producer = kafka.producer()
      expect(producer).toBeInstanceOf(KafkaProducer)
      await producer.close()
      await kafka.disconnect()
    })

    it("should throw when not connected", () => {
      const kafka = new Kafka(defaultOptions())
      expect(() => kafka.producer()).toThrow(KafkaConnectionError)
    })

    it("should accept producer options", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      const producer = kafka.producer({ timeoutMs: 5000 })
      expect(producer).toBeInstanceOf(KafkaProducer)
      await producer.close()
      await kafka.disconnect()
    })
  })

  describe("consumer", () => {
    it("should create a KafkaConsumer when connected", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      const consumer = kafka.consumer({ groupId: "test-group" })
      expect(consumer).toBeInstanceOf(KafkaConsumer)
      await consumer.close()
      await kafka.disconnect()
    })

    it("should throw when not connected", () => {
      const kafka = new Kafka(defaultOptions())
      expect(() => kafka.consumer({ groupId: "test-group" })).toThrow(KafkaConnectionError)
    })
  })

  describe("admin", () => {
    it("should create a KafkaAdmin when connected", async () => {
      const kafka = new Kafka(defaultOptions())
      await kafka.connect()
      const admin = kafka.admin()
      expect(admin).toBeDefined()
      expect(admin).toBeInstanceOf(KafkaAdmin)
      admin.close()
      await kafka.disconnect()
    })

    it("should throw when not connected", () => {
      const kafka = new Kafka(defaultOptions())
      expect(() => kafka.admin()).toThrow(KafkaConnectionError)
    })
  })
})

describe("createKafka", () => {
  it("should create a Kafka instance", () => {
    const mock = createAutoRespondingSocketFactory(DEFAULT_BROKERS)
    const kafka = createKafka({
      config: { brokers: ["localhost:9092"] },
      socketFactory: mock.factory
    })
    expect(kafka).toBeInstanceOf(Kafka)
    expect(kafka.state).toBe("disconnected")
  })

  it("should pass config options through", async () => {
    const mock = createAutoRespondingSocketFactory(DEFAULT_BROKERS)
    const kafka = createKafka({
      config: {
        brokers: ["localhost:9092"],
        clientId: "test-client"
      },
      socketFactory: mock.factory,
      maxConnectionsPerBroker: 3
    })
    await kafka.connect()
    expect(kafka.state).toBe("connected")
    await kafka.disconnect()
  })
})
