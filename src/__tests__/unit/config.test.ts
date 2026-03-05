import { describe, expect, it } from "vitest"

import { parseBrokerAddress } from "../../config"

describe("parseBrokerAddress", () => {
  it("parses host:port format", () => {
    const result = parseBrokerAddress("kafka-1:9092")

    expect(result).toEqual({ host: "kafka-1", port: 9092 })
  })

  it("parses non-standard port", () => {
    const result = parseBrokerAddress("localhost:19092")

    expect(result).toEqual({ host: "localhost", port: 19092 })
  })

  it("defaults to port 9092 when no port specified", () => {
    const result = parseBrokerAddress("kafka-broker")

    expect(result).toEqual({ host: "kafka-broker", port: 9092 })
  })

  it("defaults to localhost when host is empty", () => {
    const result = parseBrokerAddress(":9092")

    expect(result).toEqual({ host: "localhost", port: 9092 })
  })

  it("handles empty string", () => {
    const result = parseBrokerAddress("")

    expect(result).toEqual({ host: "localhost", port: 9092 })
  })

  it("handles IPv4 address with port", () => {
    const result = parseBrokerAddress("192.168.1.100:9092")

    expect(result).toEqual({ host: "192.168.1.100", port: 9092 })
  })

  it("defaults to port 9092 for invalid port number", () => {
    const result = parseBrokerAddress("kafka:abc")

    expect(result).toEqual({ host: "kafka:abc", port: 9092 })
  })

  it("defaults to port 9092 for out-of-range port", () => {
    const result = parseBrokerAddress("kafka:99999")

    expect(result).toEqual({ host: "kafka:99999", port: 9092 })
  })

  it("defaults to port 9092 for port zero", () => {
    const result = parseBrokerAddress("kafka:0")

    expect(result).toEqual({ host: "kafka:0", port: 9092 })
  })
})
