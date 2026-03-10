import { describe, expect, it, vi } from "vitest"

import { type AvroCodec, createAvroSerde } from "../../avro-serializer"
import {
  SchemaRegistry,
  SchemaRegistryError,
  WIRE_FORMAT_HEADER_SIZE,
  WIRE_FORMAT_MAGIC
} from "../../schema-registry"

// ---------------------------------------------------------------------------
// Test Helpers
// ---------------------------------------------------------------------------

/** Simple Avro codec that encodes/decodes JSON bytes (for testing only). */
const testAvroCodec: AvroCodec = {
  parse(_schemaJson: string) {
    const encoder = new TextEncoder()
    const decoder = new TextDecoder()
    return {
      encode: (value: unknown) => encoder.encode(JSON.stringify(value)),
      decode: (data: Uint8Array) => JSON.parse(decoder.decode(data)) as unknown
    }
  }
}

function mockFetch(
  responses: { status: number; body: unknown }[]
): ReturnType<
  typeof vi.fn<(input: string | URL | Request, init?: RequestInit) => Promise<Response>>
> {
  let callIndex = 0
  // eslint-disable-next-line @typescript-eslint/require-await
  return vi.fn(async (_input: string | URL | Request, _init?: RequestInit) => {
    const response = responses[callIndex++]
    return {
      ok: response.status >= 200 && response.status < 300,
      status: response.status,
      // eslint-disable-next-line @typescript-eslint/require-await
      json: async () => response.body
    } as Response
  })
}

// ---------------------------------------------------------------------------
// createAvroSerde
// ---------------------------------------------------------------------------

describe("createAvroSerde", () => {
  it("serializes with Confluent wire format", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 42 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createAvroSerde<{ name: string }>({
      registry,
      codec: testAvroCodec,
      schema: '{"type":"record","name":"Test","fields":[]}'
    })

    const bytes = await serde.serialize({ name: "test" }, "my-topic")

    // Check wire format: magic byte + schema ID + payload
    expect(bytes[0]).toBe(WIRE_FORMAT_MAGIC)
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    expect(view.getInt32(1, false)).toBe(42)

    // Check payload is valid JSON
    const payload = bytes.subarray(WIRE_FORMAT_HEADER_SIZE)
    const decoded = JSON.parse(new TextDecoder().decode(payload))
    expect(decoded).toEqual({ name: "test" })

    vi.unstubAllGlobals()
  })

  it("deserializes from Confluent wire format", async () => {
    const schemaJson = '{"type":"record","name":"Test","fields":[]}'
    const fetchMock = mockFetch([
      { status: 200, body: { id: 42 } },
      { status: 200, body: { schema: schemaJson, schemaType: "AVRO" } }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createAvroSerde<{ name: string }>({
      registry,
      codec: testAvroCodec,
      schema: schemaJson
    })

    const serialized = await serde.serialize({ name: "hello" }, "my-topic")
    const deserialized = await serde.deserialize(serialized, "my-topic")

    expect(deserialized).toEqual({ name: "hello" })

    vi.unstubAllGlobals()
  })

  it("caches schema ID after first serialize", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 10 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createAvroSerde<string>({
      registry,
      codec: testAvroCodec,
      schema: '{"type":"string"}'
    })

    await serde.serialize("a", "topic")
    await serde.serialize("b", "topic")

    // Only one register call
    expect(fetchMock).toHaveBeenCalledOnce()

    vi.unstubAllGlobals()
  })

  it("uses topicNameStrategy by default", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 1 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createAvroSerde<string>({
      registry,
      codec: testAvroCodec,
      schema: '{"type":"string"}'
    })

    await serde.serialize("test", "my-topic")

    const url = fetchMock.mock.calls[0][0] as string
    expect(url).toContain("/subjects/my-topic-value/versions")

    vi.unstubAllGlobals()
  })

  it("uses isKey for subject naming", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 1 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createAvroSerde<string>({
      registry,
      codec: testAvroCodec,
      schema: '{"type":"string"}',
      isKey: true
    })

    await serde.serialize("test", "my-topic")

    const url = fetchMock.mock.calls[0][0] as string
    expect(url).toContain("/subjects/my-topic-key/versions")

    vi.unstubAllGlobals()
  })

  it("looks up latest schema when autoRegister is false", async () => {
    const fetchMock = mockFetch([
      {
        status: 200,
        body: { id: 5, schema: '{"type":"string"}', subject: "t-value", version: 1 }
      }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createAvroSerde<string>({
      registry,
      codec: testAvroCodec,
      schema: '{"type":"string"}',
      autoRegister: false
    })

    await serde.serialize("test", "t")

    const url = fetchMock.mock.calls[0][0] as string
    expect(url).toContain("/subjects/t-value/versions/latest")

    vi.unstubAllGlobals()
  })

  it("caches reader schema by ID for deserialization", async () => {
    const schemaJson = '{"type":"string"}'
    // register() caches schema by ID, so getSchema() during
    // deserialization hits the cache — only 1 HTTP call total.
    const fetchMock = mockFetch([{ status: 200, body: { id: 7 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createAvroSerde<string>({
      registry,
      codec: testAvroCodec,
      schema: schemaJson
    })

    const bytes = await serde.serialize("value", "topic")
    await serde.deserialize(bytes, "topic")
    await serde.deserialize(bytes, "topic")

    // Only the register call; all deserialization uses cached schemas
    expect(fetchMock).toHaveBeenCalledOnce()

    vi.unstubAllGlobals()
  })

  it("throws SchemaRegistryError on encode failure", async () => {
    const failCodec: AvroCodec = {
      parse() {
        return {
          encode: () => {
            throw new Error("encode boom")
          },
          decode: () => null
        }
      }
    }

    const fetchMock = mockFetch([{ status: 200, body: { id: 1 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createAvroSerde<string>({
      registry,
      codec: failCodec,
      schema: '{"type":"string"}'
    })

    await expect(serde.serialize("x", "topic")).rejects.toThrow(SchemaRegistryError)

    vi.unstubAllGlobals()
  })

  it("throws SchemaRegistryError on decode failure", async () => {
    const failCodec: AvroCodec = {
      parse() {
        return {
          encode: () => new Uint8Array([0x01]),
          decode: () => {
            throw new Error("decode boom")
          }
        }
      }
    }

    const fetchMock = mockFetch([
      { status: 200, body: { id: 1 } },
      { status: 200, body: { schema: "{}", schemaType: "AVRO" } }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createAvroSerde<string>({
      registry,
      codec: failCodec,
      schema: "{}"
    })

    const bytes = await serde.serialize("x", "topic")
    await expect(serde.deserialize(bytes, "topic")).rejects.toThrow(SchemaRegistryError)

    vi.unstubAllGlobals()
  })

  it("throws on invalid wire format during deserialization", async () => {
    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const serde = createAvroSerde<string>({
      registry,
      codec: testAvroCodec,
      schema: '{"type":"string"}'
    })

    // Invalid magic byte
    await expect(
      serde.deserialize(new Uint8Array([0x01, 0x00, 0x00, 0x00, 0x01, 0x00]), "topic")
    ).rejects.toThrow(SchemaRegistryError)

    // Too short
    await expect(serde.deserialize(new Uint8Array([0x00, 0x01]), "topic")).rejects.toThrow(
      SchemaRegistryError
    )
  })
})
