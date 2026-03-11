import { EventEmitter } from "node:events"

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import { createNodeSocketFactory } from "../../../network/node-socket"
import type { SocketConnectOptions } from "../../../network/socket"

// ---------------------------------------------------------------------------
// Mock node:net and node:tls
// ---------------------------------------------------------------------------

class MockSocket extends EventEmitter {
  write = vi.fn()
  end = vi.fn()
  destroy = vi.fn()
}

let mockSocket: MockSocket
let netCreateConnectionSpy: ReturnType<typeof vi.fn<(...args: unknown[]) => MockSocket>>
let tlsConnectSpy: ReturnType<typeof vi.fn<(...args: unknown[]) => MockSocket>>

vi.mock("node:net", () => {
  return {
    createConnection: (...args: unknown[]) => netCreateConnectionSpy(...args)
  }
})

vi.mock("node:tls", () => {
  return {
    connect: (...args: unknown[]) => tlsConnectSpy(...args)
  }
})

beforeEach(() => {
  mockSocket = new MockSocket()
  netCreateConnectionSpy = vi.fn().mockReturnValue(mockSocket)
  tlsConnectSpy = vi.fn().mockReturnValue(mockSocket)
})

afterEach(() => {
  vi.restoreAllMocks()
  mockSocket.removeAllListeners()
})

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildConnectOptions(overrides?: Partial<SocketConnectOptions>): SocketConnectOptions {
  return {
    host: "localhost",
    port: 9092,
    onData: vi.fn(),
    onError: vi.fn(),
    onClose: vi.fn(),
    ...overrides
  }
}

/** Simulate the socket becoming connected (plain TCP). */
function emitConnect(): void {
  mockSocket.emit("connect")
}

/** Simulate the socket completing TLS handshake. */
function emitSecureConnect(): void {
  mockSocket.emit("secureConnect")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("createNodeSocketFactory", () => {
  describe("connection", () => {
    it("calls net.createConnection with host and port", async () => {
      const factory = createNodeSocketFactory()
      const promise = factory(buildConnectOptions({ host: "broker-1", port: 9093 }))
      emitConnect()
      await promise

      expect(netCreateConnectionSpy).toHaveBeenCalledWith({ host: "broker-1", port: 9093 })
    })

    it("returns a KafkaSocket with write and close methods", async () => {
      const factory = createNodeSocketFactory()
      const promise = factory(buildConnectOptions())
      emitConnect()
      const socket = await promise

      expect(typeof socket.write).toBe("function")
      expect(typeof socket.close).toBe("function")
    })

    it("uses net.createConnection when tls is undefined", async () => {
      const factory = createNodeSocketFactory()
      const promise = factory(buildConnectOptions({ tls: undefined }))
      emitConnect()
      await promise

      expect(netCreateConnectionSpy).toHaveBeenCalled()
      expect(tlsConnectSpy).not.toHaveBeenCalled()
    })

    it("uses net.createConnection when tls.enabled is false", async () => {
      const factory = createNodeSocketFactory()
      const promise = factory(buildConnectOptions({ tls: { enabled: false } }))
      emitConnect()
      await promise

      expect(netCreateConnectionSpy).toHaveBeenCalled()
      expect(tlsConnectSpy).not.toHaveBeenCalled()
    })

    it("uses tls.connect when tls.enabled is true", async () => {
      const factory = createNodeSocketFactory()
      const promise = factory(
        buildConnectOptions({
          tls: { enabled: true }
        })
      )
      emitSecureConnect()
      await promise

      expect(tlsConnectSpy).toHaveBeenCalled()
      expect(netCreateConnectionSpy).not.toHaveBeenCalled()
    })

    it("passes tls options to tls.connect", async () => {
      const factory = createNodeSocketFactory()
      const promise = factory(
        buildConnectOptions({
          tls: {
            enabled: true,
            ca: "ca-cert",
            cert: "client-cert",
            key: "client-key",
            rejectUnauthorised: false
          }
        })
      )
      emitSecureConnect()
      await promise

      expect(tlsConnectSpy).toHaveBeenCalledWith({
        host: "localhost",
        port: 9092,
        rejectUnauthorized: false,
        ca: "ca-cert",
        cert: "client-cert",
        key: "client-key"
      })
    })

    it("defaults rejectUnauthorized to true", async () => {
      const factory = createNodeSocketFactory()
      const promise = factory(
        buildConnectOptions({
          tls: { enabled: true }
        })
      )
      emitSecureConnect()
      await promise

      expect(tlsConnectSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          rejectUnauthorized: true
        })
      )
    })

    it("handles array CA certificates", async () => {
      const factory = createNodeSocketFactory()
      const promise = factory(
        buildConnectOptions({
          tls: { enabled: true, ca: ["cert-a", "cert-b"] }
        })
      )
      emitSecureConnect()
      await promise

      expect(tlsConnectSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          ca: ["cert-a", "cert-b"]
        })
      )
    })

    it("rejects when socket emits error before connect", async () => {
      const factory = createNodeSocketFactory()
      const promise = factory(buildConnectOptions())
      mockSocket.emit("error", new Error("ECONNREFUSED"))

      await expect(promise).rejects.toThrow("ECONNREFUSED")
    })

    it("rejects when socket closes before connect", async () => {
      const factory = createNodeSocketFactory()
      const promise = factory(buildConnectOptions())
      mockSocket.emit("close")

      await expect(promise).rejects.toThrow("socket closed before connection established")
    })
  })

  describe("data forwarding", () => {
    it("forwards incoming data to onData callback as Uint8Array", async () => {
      const onData = vi.fn()
      const factory = createNodeSocketFactory()
      const promise = factory(buildConnectOptions({ onData }))
      emitConnect()
      await promise

      const chunk = Buffer.from([1, 2, 3])
      mockSocket.emit("data", chunk)

      expect(onData).toHaveBeenCalledTimes(1)
      const received = onData.mock.calls[0][0] as Uint8Array
      expect(received).toBeInstanceOf(Uint8Array)
      expect(Array.from(received)).toEqual([1, 2, 3])
    })

    it("forwards socket errors to onError callback after connected", async () => {
      const onError = vi.fn()
      const factory = createNodeSocketFactory()
      const promise = factory(buildConnectOptions({ onError }))
      emitConnect()
      await promise

      const error = new Error("connection reset")
      mockSocket.emit("error", error)

      expect(onError).toHaveBeenCalledWith(error)
    })

    it("forwards close events to onClose callback after connected", async () => {
      const onClose = vi.fn()
      const factory = createNodeSocketFactory()
      const promise = factory(buildConnectOptions({ onClose }))
      emitConnect()
      await promise

      mockSocket.emit("close")

      expect(onClose).toHaveBeenCalled()
    })
  })

  describe("write", () => {
    it("writes data to the underlying socket", async () => {
      mockSocket.write.mockImplementation(
        (_data: Uint8Array, cb: (error?: Error | null) => void) => {
          cb(null)
          return true
        }
      )
      const factory = createNodeSocketFactory()
      const promise = factory(buildConnectOptions())
      emitConnect()
      const socket = await promise

      const data = new Uint8Array([0x00, 0x01, 0x02, 0x03])
      await socket.write(data)

      expect(mockSocket.write).toHaveBeenCalled()
    })

    it("handles backpressure via drain event", async () => {
      mockSocket.write.mockImplementation(
        (_data: Uint8Array, cb: (error?: Error | null) => void) => {
          // Simulate backpressure: callback still fires after drain
          setTimeout(() => {
            cb(null)
          }, 0)
          return false // backpressure
        }
      )
      const factory = createNodeSocketFactory()
      const connectPromise = factory(buildConnectOptions())
      emitConnect()
      const socket = await connectPromise

      const data = new Uint8Array([0x00, 0x01])
      const writePromise = socket.write(data)

      // Emit drain to unblock
      mockSocket.emit("drain")
      await writePromise

      expect(mockSocket.write).toHaveBeenCalled()
    })

    it("rejects when write encounters an error", async () => {
      mockSocket.write.mockImplementation(
        (_data: Uint8Array, cb: (error?: Error | null) => void) => {
          cb(new Error("write failed"))
          return true
        }
      )
      const factory = createNodeSocketFactory()
      const connectPromise = factory(buildConnectOptions())
      emitConnect()
      const socket = await connectPromise

      await expect(socket.write(new Uint8Array([1]))).rejects.toThrow("write failed")
    })
  })

  describe("close", () => {
    it("calls end on the underlying socket", async () => {
      mockSocket.end.mockImplementation((cb: () => void) => {
        cb()
      })
      const factory = createNodeSocketFactory()
      const promise = factory(buildConnectOptions())
      emitConnect()
      const socket = await promise

      await socket.close()

      expect(mockSocket.end).toHaveBeenCalled()
    })
  })
})
