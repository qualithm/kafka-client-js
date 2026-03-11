import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import { createBunSocketFactory } from "../../../network/bun-socket"
import type { SocketConnectOptions } from "../../../network/socket"

// ---------------------------------------------------------------------------
// Mock Bun.connect
// ---------------------------------------------------------------------------

type MockBunSocket = {
  write: ReturnType<typeof vi.fn>
  end: ReturnType<typeof vi.fn>
  flush: ReturnType<typeof vi.fn>
}

type SocketHandlers = {
  data: (socket: MockBunSocket, data: Uint8Array) => void
  error: (socket: MockBunSocket, error: Error) => void
  close: () => void
  drain: () => void
  connectError: (socket: MockBunSocket, error: Error) => void
}

let mockSocket: MockBunSocket
let capturedHandlers: SocketHandlers | null
let connectBehaviour: (() => MockBunSocket | Promise<MockBunSocket>) | null

function createDefaultMockSocket(): MockBunSocket {
  return {
    write: vi.fn().mockReturnValue(0),
    end: vi.fn(),
    flush: vi.fn()
  }
}

beforeEach(() => {
  mockSocket = createDefaultMockSocket()
  capturedHandlers = null
  connectBehaviour = null

  // Mock Bun.connect globally
  const bunMock = {
    connect: vi
      .fn()
      .mockImplementation(
        async (options: {
          hostname: string
          port: number
          tls?: unknown
          socket: SocketHandlers
        }) => {
          capturedHandlers = options.socket
          if (connectBehaviour) {
            return connectBehaviour()
          }
          return mockSocket
        }
      )
  }

  vi.stubGlobal("Bun", bunMock)
})

afterEach(() => {
  vi.restoreAllMocks()
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("createBunSocketFactory", () => {
  describe("connection", () => {
    it("calls Bun.connect with hostname and port", async () => {
      const factory = createBunSocketFactory()
      await factory(buildConnectOptions({ host: "broker-1", port: 9093 }))

      expect(Bun.connect).toHaveBeenCalledWith(
        expect.objectContaining({
          hostname: "broker-1",
          port: 9093
        })
      )
    })

    it("returns a KafkaSocket with write and close methods", async () => {
      const factory = createBunSocketFactory()
      const socket = await factory(buildConnectOptions())

      expect(typeof socket.write).toBe("function")
      expect(typeof socket.close).toBe("function")
    })

    it("passes no tls option when tls is undefined", async () => {
      const factory = createBunSocketFactory()
      await factory(buildConnectOptions({ tls: undefined }))

      expect(Bun.connect).toHaveBeenCalledWith(
        expect.objectContaining({
          tls: undefined
        })
      )
    })

    it("passes no tls option when tls.enabled is false", async () => {
      const factory = createBunSocketFactory()
      await factory(buildConnectOptions({ tls: { enabled: false } }))

      expect(Bun.connect).toHaveBeenCalledWith(
        expect.objectContaining({
          tls: undefined
        })
      )
    })

    it("passes tls options when tls.enabled is true", async () => {
      const factory = createBunSocketFactory()
      await factory(
        buildConnectOptions({
          tls: {
            enabled: true,
            ca: "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
            cert: "client-cert",
            key: "client-key",
            rejectUnauthorised: false
          }
        })
      )

      expect(Bun.connect).toHaveBeenCalledWith(
        expect.objectContaining({
          tls: {
            rejectUnauthorized: false,
            ca: "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
            cert: "client-cert",
            key: "client-key"
          }
        })
      )
    })

    it("defaults rejectUnauthorized to true", async () => {
      const factory = createBunSocketFactory()
      await factory(
        buildConnectOptions({
          tls: { enabled: true }
        })
      )

      expect(Bun.connect).toHaveBeenCalledWith(
        expect.objectContaining({
          tls: expect.objectContaining({
            rejectUnauthorized: true
          })
        })
      )
    })

    it("handles array CA certificates", async () => {
      const factory = createBunSocketFactory()
      const cas = ["cert-a", "cert-b"]
      await factory(
        buildConnectOptions({
          tls: { enabled: true, ca: cas }
        })
      )

      expect(Bun.connect).toHaveBeenCalledWith(
        expect.objectContaining({
          tls: expect.objectContaining({
            ca: ["cert-a", "cert-b"]
          })
        })
      )
    })
  })

  describe("data forwarding", () => {
    it("forwards incoming data to onData callback", async () => {
      const onData = vi.fn()
      const factory = createBunSocketFactory()
      await factory(buildConnectOptions({ onData }))

      const chunk = new Uint8Array([1, 2, 3])
      capturedHandlers!.data(mockSocket, chunk)

      expect(onData).toHaveBeenCalledWith(chunk)
    })

    it("forwards socket errors to onError callback", async () => {
      const onError = vi.fn()
      const factory = createBunSocketFactory()
      await factory(buildConnectOptions({ onError }))

      const error = new Error("connection reset")
      capturedHandlers!.error(mockSocket, error)

      expect(onError).toHaveBeenCalledWith(error)
    })

    it("forwards close events to onClose callback", async () => {
      const onClose = vi.fn()
      const factory = createBunSocketFactory()
      await factory(buildConnectOptions({ onClose }))

      capturedHandlers!.close()

      expect(onClose).toHaveBeenCalled()
    })

    it("forwards connectError to onError callback", async () => {
      const onError = vi.fn()
      const factory = createBunSocketFactory()
      await factory(buildConnectOptions({ onError }))

      const error = new Error("ECONNREFUSED")
      capturedHandlers!.connectError(mockSocket, error)

      expect(onError).toHaveBeenCalledWith(error)
    })
  })

  describe("write", () => {
    it("writes data to the underlying socket", async () => {
      mockSocket.write.mockReturnValue(4)
      const factory = createBunSocketFactory()
      const socket = await factory(buildConnectOptions())

      const data = new Uint8Array([0x00, 0x01, 0x02, 0x03])
      await socket.write(data)

      expect(mockSocket.write).toHaveBeenCalled()
      expect(mockSocket.flush).toHaveBeenCalled()
    })

    it("handles partial writes with backpressure", async () => {
      // First call: write 2 of 4 bytes. Second call (after drain): write remaining 2.
      mockSocket.write.mockReturnValueOnce(2).mockReturnValueOnce(0).mockReturnValueOnce(2)
      const factory = createBunSocketFactory()
      const socket = await factory(buildConnectOptions())

      const data = new Uint8Array([0x00, 0x01, 0x02, 0x03])
      const writePromise = socket.write(data)

      // The write should be waiting for drain
      // Simulate the drain event
      await vi.waitFor(() => {
        expect(capturedHandlers!.drain).toBeDefined()
      })
      capturedHandlers!.drain()

      await writePromise

      expect(mockSocket.write).toHaveBeenCalledTimes(3)
      expect(mockSocket.flush).toHaveBeenCalled()
    })

    it("throws when socket returns -1 (closed)", async () => {
      mockSocket.write.mockReturnValue(-1)
      const factory = createBunSocketFactory()
      const socket = await factory(buildConnectOptions())

      await expect(socket.write(new Uint8Array([1]))).rejects.toThrow("socket is closed")
    })
  })

  describe("close", () => {
    it("calls end on the underlying socket", async () => {
      const factory = createBunSocketFactory()
      const socket = await factory(buildConnectOptions())

      await socket.close()

      expect(mockSocket.end).toHaveBeenCalled()
    })
  })
})
