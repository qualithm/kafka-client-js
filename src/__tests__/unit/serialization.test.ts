import { describe, expect, it } from "vitest"

import { jsonSerializer, stringSerializer } from "../../serialization"

// ---------------------------------------------------------------------------
// stringSerializer
// ---------------------------------------------------------------------------

describe("stringSerializer", () => {
  it("round trips a simple string", () => {
    const value = "hello world"
    const bytes = stringSerializer.serialize(value, "test-topic")
    const result = stringSerializer.deserialize(bytes, "test-topic")
    expect(result).toBe(value)
  })

  it("round trips an empty string", () => {
    const bytes = stringSerializer.serialize("", "test-topic")
    const result = stringSerializer.deserialize(bytes, "test-topic")
    expect(result).toBe("")
  })

  it("round trips unicode characters", () => {
    const value = "こんにちは世界 🌍 café"
    const bytes = stringSerializer.serialize(value, "test-topic")
    const result = stringSerializer.deserialize(bytes, "test-topic")
    expect(result).toBe(value)
  })

  it("round trips multi-byte emoji", () => {
    const value = "👨‍👩‍👧‍👦"
    const bytes = stringSerializer.serialize(value, "test-topic")
    const result = stringSerializer.deserialize(bytes, "test-topic")
    expect(result).toBe(value)
  })

  it("serializes to Uint8Array", () => {
    const bytes = stringSerializer.serialize("test", "topic")
    expect(bytes).toBeInstanceOf(Uint8Array)
  })

  it("serializes to valid UTF-8 bytes", () => {
    const bytes = stringSerializer.serialize("abc", "topic")
    expect(bytes).toEqual(new Uint8Array([0x61, 0x62, 0x63]))
  })
})

// ---------------------------------------------------------------------------
// jsonSerializer
// ---------------------------------------------------------------------------

describe("jsonSerializer", () => {
  const serde = jsonSerializer<{ name: string; count: number }>()

  it("round trips an object", () => {
    const value = { name: "test", count: 42 }
    const bytes = serde.serialize(value, "test-topic")
    const result = serde.deserialize(bytes, "test-topic")
    expect(result).toEqual(value)
  })

  it("round trips an array", () => {
    const arraySerde = jsonSerializer<number[]>()
    const value = [1, 2, 3]
    const bytes = arraySerde.serialize(value, "test-topic")
    const result = arraySerde.deserialize(bytes, "test-topic")
    expect(result).toEqual(value)
  })

  it("round trips null", () => {
    const nullSerde = jsonSerializer<null>()
    const bytes = nullSerde.serialize(null, "test-topic")
    const result = nullSerde.deserialize(bytes, "test-topic")
    expect(result).toBeNull()
  })

  it("round trips nested objects", () => {
    const nestedSerde = jsonSerializer<{ a: { b: { c: number } } }>()
    const value = { a: { b: { c: 99 } } }
    const bytes = nestedSerde.serialize(value, "test-topic")
    const result = nestedSerde.deserialize(bytes, "test-topic")
    expect(result).toEqual(value)
  })

  it("round trips unicode values in JSON strings", () => {
    const unicodeSerde = jsonSerializer<{ text: string }>()
    const value = { text: "café ☕" }
    const bytes = unicodeSerde.serialize(value, "test-topic")
    const result = unicodeSerde.deserialize(bytes, "test-topic")
    expect(result).toEqual(value)
  })

  it("serializes to Uint8Array", () => {
    const bytes = serde.serialize({ name: "x", count: 1 }, "topic")
    expect(bytes).toBeInstanceOf(Uint8Array)
  })

  it("serializes to valid JSON bytes", () => {
    const bytes = serde.serialize({ name: "x", count: 1 }, "topic")
    const json = new TextDecoder().decode(bytes)
    expect(JSON.parse(json)).toEqual({ name: "x", count: 1 })
  })

  it("throws on invalid JSON during deserialize", () => {
    const bytes = new TextEncoder().encode("not valid json {{{")
    expect(() => serde.deserialize(bytes, "topic")).toThrow()
  })
})
