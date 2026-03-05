import { describe, expect, it } from "vitest"

import {
  KafkaConfigError,
  KafkaConnectionError,
  KafkaError,
  KafkaProtocolError,
  KafkaTimeoutError
} from "../../errors"

describe("KafkaError", () => {
  it("creates an error with message and retriable flag", () => {
    const err = new KafkaError("something went wrong", true)

    expect(err.message).toBe("something went wrong")
    expect(err.retriable).toBe(true)
    expect(err.name).toBe("KafkaError")
    expect(err).toBeInstanceOf(Error)
  })

  it("supports cause option", () => {
    const cause = new Error("root cause")
    const err = new KafkaError("wrapped", false, { cause })

    expect(err.cause).toBe(cause)
  })

  describe("isError", () => {
    it("returns true for KafkaError instances", () => {
      expect(KafkaError.isError(new KafkaError("test", false))).toBe(true)
    })

    it("returns true for subclass instances", () => {
      expect(KafkaError.isError(new KafkaProtocolError("test", 1, true))).toBe(true)
      expect(KafkaError.isError(new KafkaConnectionError("test"))).toBe(true)
    })

    it("returns false for plain Error", () => {
      expect(KafkaError.isError(new Error("test"))).toBe(false)
    })

    it("returns false for non-errors", () => {
      expect(KafkaError.isError("string")).toBe(false)
      expect(KafkaError.isError(null)).toBe(false)
      expect(KafkaError.isError(undefined)).toBe(false)
      expect(KafkaError.isError(42)).toBe(false)
    })
  })
})

describe("KafkaProtocolError", () => {
  it("stores error code and retriable flag", () => {
    const err = new KafkaProtocolError("not leader for partition", 6, true)

    expect(err.errorCode).toBe(6)
    expect(err.retriable).toBe(true)
    expect(err.name).toBe("KafkaProtocolError")
    expect(err).toBeInstanceOf(KafkaError)
  })

  it("creates fatal protocol errors", () => {
    const err = new KafkaProtocolError("topic authorisation failed", 29, false)

    expect(err.errorCode).toBe(29)
    expect(err.retriable).toBe(false)
  })

  describe("isError", () => {
    it("returns true for KafkaProtocolError", () => {
      expect(KafkaProtocolError.isError(new KafkaProtocolError("test", 1, true))).toBe(true)
    })

    it("returns false for base KafkaError", () => {
      expect(KafkaProtocolError.isError(new KafkaError("test", true))).toBe(false)
    })

    it("returns false for other subclasses", () => {
      expect(KafkaProtocolError.isError(new KafkaConnectionError("test"))).toBe(false)
    })
  })
})

describe("KafkaConnectionError", () => {
  it("defaults to retriable", () => {
    const err = new KafkaConnectionError("connection refused")

    expect(err.retriable).toBe(true)
    expect(err.name).toBe("KafkaConnectionError")
    expect(err).toBeInstanceOf(KafkaError)
  })

  it("stores broker address", () => {
    const err = new KafkaConnectionError("connection refused", {
      broker: "kafka-1:9092"
    })

    expect(err.broker).toBe("kafka-1:9092")
  })

  it("can be non-retriable", () => {
    const err = new KafkaConnectionError("authentication failed", {
      retriable: false
    })

    expect(err.retriable).toBe(false)
  })

  it("supports cause", () => {
    const cause = new Error("ECONNREFUSED")
    const err = new KafkaConnectionError("connection refused", { cause })

    expect(err.cause).toBe(cause)
  })

  describe("isError", () => {
    it("returns true for KafkaConnectionError", () => {
      expect(KafkaConnectionError.isError(new KafkaConnectionError("test"))).toBe(true)
    })

    it("returns true for KafkaTimeoutError (subclass)", () => {
      expect(KafkaConnectionError.isError(new KafkaTimeoutError("test", 5000))).toBe(true)
    })

    it("returns false for base KafkaError", () => {
      expect(KafkaConnectionError.isError(new KafkaError("test", true))).toBe(false)
    })
  })
})

describe("KafkaTimeoutError", () => {
  it("stores timeout duration and defaults to retriable", () => {
    const err = new KafkaTimeoutError("request timed out", 30000)

    expect(err.timeoutMs).toBe(30000)
    expect(err.retriable).toBe(true)
    expect(err.name).toBe("KafkaTimeoutError")
    expect(err).toBeInstanceOf(KafkaConnectionError)
    expect(err).toBeInstanceOf(KafkaError)
  })

  it("stores broker address", () => {
    const err = new KafkaTimeoutError("timed out", 5000, { broker: "kafka-1:9092" })

    expect(err.broker).toBe("kafka-1:9092")
  })

  describe("isError", () => {
    it("returns true for KafkaTimeoutError", () => {
      expect(KafkaTimeoutError.isError(new KafkaTimeoutError("test", 5000))).toBe(true)
    })

    it("returns false for KafkaConnectionError", () => {
      expect(KafkaTimeoutError.isError(new KafkaConnectionError("test"))).toBe(false)
    })
  })
})

describe("KafkaConfigError", () => {
  it("is never retriable", () => {
    const err = new KafkaConfigError("invalid broker address")

    expect(err.retriable).toBe(false)
    expect(err.name).toBe("KafkaConfigError")
    expect(err).toBeInstanceOf(KafkaError)
  })

  describe("isError", () => {
    it("returns true for KafkaConfigError", () => {
      expect(KafkaConfigError.isError(new KafkaConfigError("test"))).toBe(true)
    })

    it("returns false for base KafkaError", () => {
      expect(KafkaConfigError.isError(new KafkaError("test", false))).toBe(false)
    })
  })
})
