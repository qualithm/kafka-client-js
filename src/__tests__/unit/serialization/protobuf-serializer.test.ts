import { describe, expect, it, vi } from "vitest"

import {
  createProtobufSerde,
  decodeMessageIndexes,
  encodeMessageIndexes,
  type ProtobufCodec
} from "../../../serialization/protobuf-serializer"
import {
  SchemaRegistry,
  SchemaRegistryError,
  WIRE_FORMAT_HEADER_SIZE,
  WIRE_FORMAT_MAGIC
} from "../../../serialization/schema-registry"

// ---------------------------------------------------------------------------
// Test Helpers
// ---------------------------------------------------------------------------

/** Simple Protobuf codec that encodes/decodes JSON bytes (for testing only). */
const testProtobufCodec: ProtobufCodec = {
  parse(_schemaString: string, _messageName: string) {
    const encoder = new TextEncoder()
    const decoder = new TextDecoder()
    return {
      encode: (value: unknown) => encoder.encode(JSON.stringify(value)),
      decode: (data: Uint8Array) => JSON.parse(decoder.decode(data)) as unknown,
      messageIndexes: [0]
    }
  }
}

function mockFetch(
  responses: { status: number; body: unknown }[]
): ReturnType<
  typeof vi.fn<(input: string | URL | Request, init?: RequestInit) => Promise<Response>>
> {
  let callIndex = 0
  return vi.fn(async (_input: string | URL | Request, _init?: RequestInit) => {
    const response = responses[callIndex++]
    return Promise.resolve({
      ok: response.status >= 200 && response.status < 300,
      status: response.status,
      json: async () => Promise.resolve(response.body)
    } as Response)
  })
}

// ---------------------------------------------------------------------------
// Message Index Encoding
// ---------------------------------------------------------------------------

describe("encodeMessageIndexes", () => {
  it("encodes [0] as a single varint 0", () => {
    const bytes = encodeMessageIndexes([0])
    // Special case: length=0 means [0]
    expect(bytes).toEqual(new Uint8Array([0x00]))
  })

  it("encodes [1] as length=1 followed by varint 1", () => {
    const bytes = encodeMessageIndexes([1])
    // length=1, index=1
    expect(bytes).toEqual(new Uint8Array([0x01, 0x01]))
  })

  it("encodes [0, 2] as length=2 followed by two varints", () => {
    const bytes = encodeMessageIndexes([0, 2])
    // length=2, index=0, index=2
    expect(bytes).toEqual(new Uint8Array([0x02, 0x00, 0x02]))
  })

  it("encodes large index values", () => {
    const bytes = encodeMessageIndexes([128])
    // length=1, 128 = 0x80 0x01 in varint
    expect(bytes).toEqual(new Uint8Array([0x01, 0x80, 0x01]))
  })
})

describe("decodeMessageIndexes", () => {
  it("decodes [0] from single varint 0", () => {
    const data = new Uint8Array([0x00, 0xde, 0xad])
    const { indexes, bytesRead } = decodeMessageIndexes(data, 0)
    expect(indexes).toEqual([0])
    expect(bytesRead).toBe(1)
  })

  it("decodes [1] from length=1 + varint 1", () => {
    const data = new Uint8Array([0x01, 0x01])
    const { indexes, bytesRead } = decodeMessageIndexes(data, 0)
    expect(indexes).toEqual([1])
    expect(bytesRead).toBe(2)
  })

  it("decodes [0, 2] from length=2 + two varints", () => {
    const data = new Uint8Array([0x02, 0x00, 0x02])
    const { indexes, bytesRead } = decodeMessageIndexes(data, 0)
    expect(indexes).toEqual([0, 2])
    expect(bytesRead).toBe(3)
  })

  it("respects offset parameter", () => {
    const data = new Uint8Array([0xff, 0xff, 0x00])
    const { indexes, bytesRead } = decodeMessageIndexes(data, 2)
    expect(indexes).toEqual([0])
    expect(bytesRead).toBe(1)
  })

  it("round trips with encodeMessageIndexes", () => {
    const testCases: readonly number[][] = [[0], [1], [5], [0, 2], [1, 3, 5], [128]]
    for (const indexes of testCases) {
      const encoded = encodeMessageIndexes(indexes)
      const { indexes: decoded } = decodeMessageIndexes(encoded, 0)
      expect(decoded).toEqual(indexes)
    }
  })
})

// ---------------------------------------------------------------------------
// createProtobufSerde
// ---------------------------------------------------------------------------

describe("createProtobufSerde", () => {
  it("serializes with Confluent wire format including message indexes", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 42 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createProtobufSerde<{ name: string }>({
      registry,
      codec: testProtobufCodec,
      schema: 'syntax = "proto3"; message Test { string name = 1; }',
      messageName: "Test"
    })

    const bytes = await serde.serialize({ name: "test" }, "my-topic")

    // Check wire format header
    expect(bytes[0]).toBe(WIRE_FORMAT_MAGIC)
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    expect(view.getInt32(1, false)).toBe(42)

    // Check message indexes (single varint 0 for [0])
    expect(bytes[WIRE_FORMAT_HEADER_SIZE]).toBe(0x00)

    // Check payload
    const payload = bytes.subarray(WIRE_FORMAT_HEADER_SIZE + 1) // +1 for message index byte
    const decoded = JSON.parse(new TextDecoder().decode(payload))
    expect(decoded).toEqual({ name: "test" })

    vi.unstubAllGlobals()
  })

  it("deserializes from Confluent wire format", async () => {
    const protoSchema = 'syntax = "proto3"; message Test { string name = 1; }'
    const fetchMock = mockFetch([
      { status: 200, body: { id: 42 } },
      { status: 200, body: { schema: protoSchema, schemaType: "PROTOBUF" } }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createProtobufSerde<{ name: string }>({
      registry,
      codec: testProtobufCodec,
      schema: protoSchema,
      messageName: "Test"
    })

    const serialized = await serde.serialize({ name: "hello" }, "my-topic")
    const deserialized = await serde.deserialize(serialized, "my-topic")

    expect(deserialized).toEqual({ name: "hello" })

    vi.unstubAllGlobals()
  })

  it("uses topicNameStrategy by default", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 1 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createProtobufSerde<unknown>({
      registry,
      codec: testProtobufCodec,
      schema: 'syntax = "proto3";',
      messageName: "Test"
    })

    await serde.serialize({}, "my-topic")

    const url = fetchMock.mock.calls[0][0] as string
    expect(url).toContain("/subjects/my-topic-value/versions")

    vi.unstubAllGlobals()
  })

  it("registers with PROTOBUF schema type", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 1 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createProtobufSerde<unknown>({
      registry,
      codec: testProtobufCodec,
      schema: 'syntax = "proto3";',
      messageName: "Test"
    })

    await serde.serialize({}, "topic")

    const body = JSON.parse(fetchMock.mock.calls[0][1]!.body as string)
    expect(body.schemaType).toBe("PROTOBUF")

    vi.unstubAllGlobals()
  })

  it("handles non-zero message indexes", async () => {
    const codecWithIndexes: ProtobufCodec = {
      parse() {
        const encoder = new TextEncoder()
        const decoder = new TextDecoder()
        return {
          encode: (value: unknown) => encoder.encode(JSON.stringify(value)),
          decode: (data: Uint8Array) => JSON.parse(decoder.decode(data)) as unknown,
          messageIndexes: [1, 3]
        }
      }
    }

    const fetchMock = mockFetch([
      { status: 200, body: { id: 10 } },
      { status: 200, body: { schema: "proto", schemaType: "PROTOBUF" } }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createProtobufSerde<{ x: number }>({
      registry,
      codec: codecWithIndexes,
      schema: "proto",
      messageName: "Nested"
    })

    const bytes = await serde.serialize({ x: 1 }, "topic")

    // Verify message indexes are encoded after the header
    // [1, 3] → length=2, 1, 3 → bytes [0x02, 0x01, 0x03]
    expect(bytes[WIRE_FORMAT_HEADER_SIZE]).toBe(0x02)
    expect(bytes[WIRE_FORMAT_HEADER_SIZE + 1]).toBe(0x01)
    expect(bytes[WIRE_FORMAT_HEADER_SIZE + 2]).toBe(0x03)

    const deserialized = await serde.deserialize(bytes, "topic")
    expect(deserialized).toEqual({ x: 1 })

    vi.unstubAllGlobals()
  })

  it("caches schema ID after first serialize", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 5 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createProtobufSerde<string>({
      registry,
      codec: testProtobufCodec,
      schema: "proto",
      messageName: "Test"
    })

    await serde.serialize("a", "topic")
    await serde.serialize("b", "topic")

    expect(fetchMock).toHaveBeenCalledOnce()

    vi.unstubAllGlobals()
  })

  it("throws SchemaRegistryError on encode failure", async () => {
    const failCodec: ProtobufCodec = {
      parse() {
        return {
          encode: () => {
            throw new Error("encode boom")
          },
          decode: () => null,
          messageIndexes: [0]
        }
      }
    }

    const fetchMock = mockFetch([{ status: 200, body: { id: 1 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createProtobufSerde<string>({
      registry,
      codec: failCodec,
      schema: "proto",
      messageName: "Test"
    })

    await expect(serde.serialize("x", "topic")).rejects.toThrow(SchemaRegistryError)

    vi.unstubAllGlobals()
  })

  it("throws SchemaRegistryError on decode failure", async () => {
    const failCodec: ProtobufCodec = {
      parse() {
        return {
          encode: () => new Uint8Array([0x01]),
          decode: () => {
            throw new Error("decode boom")
          },
          messageIndexes: [0]
        }
      }
    }

    const fetchMock = mockFetch([
      { status: 200, body: { id: 1 } },
      { status: 200, body: { schema: "proto", schemaType: "PROTOBUF" } }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createProtobufSerde<string>({
      registry,
      codec: failCodec,
      schema: "proto",
      messageName: "Test"
    })

    const bytes = await serde.serialize("x", "topic")
    await expect(serde.deserialize(bytes, "topic")).rejects.toThrow(SchemaRegistryError)

    vi.unstubAllGlobals()
  })

  it("looks up latest schema when autoRegister is false", async () => {
    const fetchMock = mockFetch([
      { status: 200, body: { id: 5, schema: "proto", subject: "t-value", version: 1 } }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createProtobufSerde<string>({
      registry,
      codec: testProtobufCodec,
      schema: "proto",
      messageName: "Test",
      autoRegister: false
    })

    await serde.serialize("test", "t")

    const url = fetchMock.mock.calls[0][0] as string
    expect(url).toContain("/subjects/t-value/versions/latest")

    vi.unstubAllGlobals()
  })
})
