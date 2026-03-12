import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import { createDenoSocketFactory } from "../../../network/deno-socket"
import type { SocketConnectOptions } from "../../../network/socket"

// ---------------------------------------------------------------------------
// Mock Deno namespace
// ---------------------------------------------------------------------------

type MockDenoConn = {
  readable: ReadableStream<Uint8Array>
  write: ReturnType<typeof vi.fn>
  close: ReturnType<typeof vi.fn>
}

// Keep a direct reference to the real Deno global (if present) so we can
// preserve runtime internals like `unrefTimer` that Deno's Node.js compat
// layer relies on. A spread would miss non-enumerable properties.
const realDenoRef = (globalThis as Record<string, unknown>).Deno as object | undefined

let mockConn: MockDenoConn
let readableController: ReadableStreamDefaultController<Uint8Array>
let denoConnectSpy: ReturnType<typeof vi.fn<(...args: unknown[]) => Promise<MockDenoConn>>>
let denoConnectTlsSpy: ReturnType<typeof vi.fn<(...args: unknown[]) => Promise<MockDenoConn>>>

function createMockConn(): MockDenoConn {
  let ctrl: ReadableStreamDefaultController<Uint8Array>
  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      ctrl = controller
    }
  })
  readableController = ctrl!

  return {
    readable,
    write: vi.fn<(data: Uint8Array) => Promise<number>>().mockImplementation(
      async (data: Uint8Array) => data.byteLength
    ),
    close: vi.fn()
  }
}

beforeEach(() => {
  mockConn = createMockConn()
  denoConnectSpy = vi
    .fn<(...args: unknown[]) => Promise<MockDenoConn>>()
    .mockResolvedValue(mockConn)
  denoConnectTlsSpy = vi
    .fn<(...args: unknown[]) => Promise<MockDenoConn>>()
    .mockResolvedValue(mockConn)

  // Object.create preserves non-enumerable Deno properties (e.g. unrefTimer)
  // via the prototype chain, avoiding breakage in Deno's Node.js compat layer.
  const mockDeno =
    realDenoRef !== undefined
      ? (Object.create(realDenoRef) as Record<string, unknown>)
      : ({} as Record<string, unknown>)
  mockDeno.connect = denoConnectSpy
  mockDeno.connectTls = denoConnectTlsSpy
  vi.stubGlobal("Deno", mockDeno)
})

afterEach(() => {
  vi.unstubAllGlobals()
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

describe("createDenoSocketFactory", () => {
  describe("connection", () => {
    it("calls Deno.connect with hostname and port", async () => {
      const factory = createDenoSocketFactory()
      await factory(buildConnectOptions({ host: "broker-1", port: 9093 }))

      expect(denoConnectSpy).toHaveBeenCalledWith({ hostname: "broker-1", port: 9093 })
      expect(denoConnectTlsSpy).not.toHaveBeenCalled()
    })

    it("returns a KafkaSocket with write and close methods", async () => {
      const factory = createDenoSocketFactory()
      const socket = await factory(buildConnectOptions())

      expect(typeof socket.write).toBe("function")
      expect(typeof socket.close).toBe("function")
    })

    it("uses Deno.connect when tls is undefined", async () => {
      const factory = createDenoSocketFactory()
      await factory(buildConnectOptions({ tls: undefined }))

      expect(denoConnectSpy).toHaveBeenCalled()
      expect(denoConnectTlsSpy).not.toHaveBeenCalled()
    })

    it("uses Deno.connect when tls.enabled is false", async () => {
      const factory = createDenoSocketFactory()
      await factory(buildConnectOptions({ tls: { enabled: false } }))

      expect(denoConnectSpy).toHaveBeenCalled()
      expect(denoConnectTlsSpy).not.toHaveBeenCalled()
    })

    it("uses Deno.connectTls when tls.enabled is true", async () => {
      const factory = createDenoSocketFactory()
      await factory(buildConnectOptions({ tls: { enabled: true } }))

      expect(denoConnectTlsSpy).toHaveBeenCalled()
      expect(denoConnectSpy).not.toHaveBeenCalled()
    })

    it("passes tls options to Deno.connectTls", async () => {
      const factory = createDenoSocketFactory()
      await factory(
        buildConnectOptions({
          host: "broker-1",
          port: 9093,
          tls: {
            enabled: true,
            ca: "ca-cert",
            cert: "client-cert",
            key: "client-key"
          }
        })
      )

      expect(denoConnectTlsSpy).toHaveBeenCalledWith({
        hostname: "broker-1",
        port: 9093,
        caCerts: ["ca-cert"],
        certChain: "client-cert",
        privateKey: "client-key"
      })
    })

    it("handles array CA certificates", async () => {
      const factory = createDenoSocketFactory()
      await factory(
        buildConnectOptions({
          tls: { enabled: true, ca: ["cert-a", "cert-b"] }
        })
      )

      expect(denoConnectTlsSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          caCerts: ["cert-a", "cert-b"]
        })
      )
    })

    it("throws when Deno global is not available", async () => {
      vi.stubGlobal("Deno", undefined)
      const factory = createDenoSocketFactory()
      const promise = factory(buildConnectOptions())

      // Restore immediately so Deno runtime timer internals remain functional
      vi.unstubAllGlobals()

      await expect(promise).rejects.toThrow("Deno runtime not detected")
    })
  })

  describe("data forwarding", () => {
    it("forwards incoming data to onData callback", async () => {
      const onData = vi.fn()
      const factory = createDenoSocketFactory()
      await factory(buildConnectOptions({ onData }))

      const chunk = new Uint8Array([1, 2, 3])
      readableController.enqueue(chunk)

      // Allow microtask for the read loop to process
      await new Promise((resolve) => {
        setTimeout(resolve, 10)
      })

      expect(onData).toHaveBeenCalledWith(chunk)
    })

    it("calls onClose when readable stream ends", async () => {
      const onClose = vi.fn()
      const factory = createDenoSocketFactory()
      await factory(buildConnectOptions({ onClose }))

      readableController.close()

      await new Promise((resolve) => {
        setTimeout(resolve, 10)
      })

      expect(onClose).toHaveBeenCalled()
    })

    it("forwards stream errors to onError callback", async () => {
      const onError = vi.fn()
      const onClose = vi.fn()
      const factory = createDenoSocketFactory()
      await factory(buildConnectOptions({ onError, onClose }))

      const error = new Error("connection reset")
      readableController.error(error)

      await new Promise((resolve) => {
        setTimeout(resolve, 10)
      })

      expect(onError).toHaveBeenCalledWith(error)
      expect(onClose).toHaveBeenCalled()
    })
  })

  describe("write", () => {
    it("writes data to the underlying connection", async () => {
      const factory = createDenoSocketFactory()
      const socket = await factory(buildConnectOptions())

      const data = new Uint8Array([0x00, 0x01, 0x02, 0x03])
      await socket.write(data)

      expect(mockConn.write).toHaveBeenCalled()
    })

    it("handles partial writes", async () => {
      mockConn.write.mockResolvedValueOnce(2).mockResolvedValueOnce(2)
      const factory = createDenoSocketFactory()
      const socket = await factory(buildConnectOptions())

      const data = new Uint8Array([0x00, 0x01, 0x02, 0x03])
      await socket.write(data)

      expect(mockConn.write).toHaveBeenCalledTimes(2)
    })

    it("throws when socket is closed", async () => {
      const factory = createDenoSocketFactory()
      const socket = await factory(buildConnectOptions())

      // Close the readable stream to trigger the closed state
      readableController.close()
      await new Promise((resolve) => {
        setTimeout(resolve, 10)
      })

      await expect(socket.write(new Uint8Array([1]))).rejects.toThrow("socket is closed")
    })
  })

  describe("close", () => {
    it("calls close on the underlying connection", async () => {
      const factory = createDenoSocketFactory()
      const socket = await factory(buildConnectOptions())

      await socket.close()

      expect(mockConn.close).toHaveBeenCalled()
    })

    it("does not call close twice", async () => {
      const factory = createDenoSocketFactory()
      const socket = await factory(buildConnectOptions())

      await socket.close()
      await socket.close()

      expect(mockConn.close).toHaveBeenCalledTimes(1)
    })
  })
})
