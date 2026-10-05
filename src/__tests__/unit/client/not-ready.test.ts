import { describe, expect, it, vi } from "vitest"

import { isNotReadyError, retryWhileNotReady } from "../../../client/not-ready"
import { KafkaConnectionError, KafkaProtocolError } from "../../../errors"

const FAST = { windowMs: 1_000, initialDelayMs: 1, maxDelayMs: 2 }

function protocolError(code: number): KafkaProtocolError {
  return new KafkaProtocolError(`error code ${String(code)}`, code, true)
}

describe("isNotReadyError", () => {
  it.each([3, 5, 14, 15, 16])("treats code %i as not ready", (code) => {
    expect(isNotReadyError(protocolError(code))).toBe(true)
  })

  it("rejects other codes and other error types", () => {
    expect(isNotReadyError(protocolError(29))).toBe(false)
    expect(isNotReadyError(new KafkaConnectionError("reset", { retriable: true }))).toBe(false)
    expect(isNotReadyError(new Error("boom"))).toBe(false)
  })
})

describe("retryWhileNotReady", () => {
  it("retries not-ready errors until the operation succeeds", async () => {
    const fn = vi
      .fn<() => Promise<string>>()
      .mockRejectedValueOnce(protocolError(15))
      .mockRejectedValueOnce(protocolError(14))
      .mockResolvedValue("ok")
    const onRetry = vi.fn()

    await expect(retryWhileNotReady(fn, onRetry, FAST)).resolves.toBe("ok")
    expect(fn).toHaveBeenCalledTimes(3)
    expect(onRetry).toHaveBeenCalledTimes(2)
  })

  it("throws any other error at once", async () => {
    const fn = vi.fn<() => Promise<string>>().mockRejectedValue(protocolError(29))

    await expect(retryWhileNotReady(fn, undefined, FAST)).rejects.toThrow("error code 29")
    expect(fn).toHaveBeenCalledTimes(1)
  })

  it("throws the last not-ready error once the window is spent", async () => {
    const fn = vi.fn<() => Promise<string>>().mockRejectedValue(protocolError(3))

    await expect(
      retryWhileNotReady(fn, undefined, { windowMs: 300, initialDelayMs: 10, maxDelayMs: 10 })
    ).rejects.toThrow("error code 3")
    expect(fn.mock.calls.length).toBeGreaterThan(1)
  })
})
