import { describe, expect, it, vi } from "vitest"

import {
  decodeWireFormatHeader,
  encodeWireFormatHeader,
  recordNameStrategy,
  SchemaRegistry,
  SchemaRegistryError,
  topicNameStrategy,
  topicRecordNameStrategy,
  WIRE_FORMAT_HEADER_SIZE,
  WIRE_FORMAT_MAGIC
} from "../../../serialization/schema-registry"

// ---------------------------------------------------------------------------
// Subject Naming Strategies
// ---------------------------------------------------------------------------

describe("topicNameStrategy", () => {
  it("returns topic-value for value position", () => {
    expect(topicNameStrategy("my-topic", false)).toBe("my-topic-value")
  })

  it("returns topic-key for key position", () => {
    expect(topicNameStrategy("my-topic", true)).toBe("my-topic-key")
  })
})

describe("recordNameStrategy", () => {
  it("always returns the fixed record name", () => {
    const strategy = recordNameStrategy("com.example.User")
    expect(strategy("topic-a", false)).toBe("com.example.User")
    expect(strategy("topic-b", true)).toBe("com.example.User")
  })
})

describe("topicRecordNameStrategy", () => {
  it("combines topic and record name", () => {
    const strategy = topicRecordNameStrategy("User")
    expect(strategy("my-topic", false)).toBe("my-topic-User")
    expect(strategy("other-topic", true)).toBe("other-topic-User")
  })
})

// ---------------------------------------------------------------------------
// Wire Format Header
// ---------------------------------------------------------------------------

describe("encodeWireFormatHeader", () => {
  it("encodes magic byte and schema ID", () => {
    const header = encodeWireFormatHeader(42)
    expect(header.length).toBe(WIRE_FORMAT_HEADER_SIZE)
    expect(header[0]).toBe(WIRE_FORMAT_MAGIC)

    const view = new DataView(header.buffer)
    expect(view.getInt32(1, false)).toBe(42)
  })

  it("encodes schema ID 0", () => {
    const header = encodeWireFormatHeader(0)
    expect(header).toEqual(new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x00]))
  })

  it("encodes large schema ID", () => {
    const header = encodeWireFormatHeader(0x7fffffff)
    expect(header[0]).toBe(0x00)
    const view = new DataView(header.buffer)
    expect(view.getInt32(1, false)).toBe(0x7fffffff)
  })
})

describe("decodeWireFormatHeader", () => {
  it("decodes a valid header", () => {
    const data = new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x2a, 0xff, 0xff])
    const { schemaId, offset } = decodeWireFormatHeader(data)
    expect(schemaId).toBe(42)
    expect(offset).toBe(WIRE_FORMAT_HEADER_SIZE)
  })

  it("round trips with encodeWireFormatHeader", () => {
    for (const id of [0, 1, 42, 1000, 0x7fffffff]) {
      const header = encodeWireFormatHeader(id)
      const payload = new Uint8Array([...header, 0xde, 0xad])
      const { schemaId, offset } = decodeWireFormatHeader(payload)
      expect(schemaId).toBe(id)
      expect(offset).toBe(WIRE_FORMAT_HEADER_SIZE)
    }
  })

  it("throws on data too short", () => {
    expect(() => decodeWireFormatHeader(new Uint8Array([0x00, 0x01]))).toThrow(SchemaRegistryError)
  })

  it("throws on invalid magic byte", () => {
    expect(() => decodeWireFormatHeader(new Uint8Array([0x01, 0x00, 0x00, 0x00, 0x2a]))).toThrow(
      SchemaRegistryError
    )
  })

  it("throws on empty data", () => {
    expect(() => decodeWireFormatHeader(new Uint8Array([]))).toThrow(SchemaRegistryError)
  })
})

// ---------------------------------------------------------------------------
// SchemaRegistryError
// ---------------------------------------------------------------------------

describe("SchemaRegistryError", () => {
  it("extends KafkaError", () => {
    const error = new SchemaRegistryError("test error")
    expect(error).toBeInstanceOf(Error)
    expect(error.name).toBe("SchemaRegistryError")
    expect(error.message).toBe("test error")
    expect(error.retriable).toBe(false)
  })

  it("stores error code and status code", () => {
    const error = new SchemaRegistryError("not found", {
      errorCode: 40401,
      statusCode: 404
    })
    expect(error.errorCode).toBe(40401)
    expect(error.statusCode).toBe(404)
  })

  it("supports retriable flag", () => {
    const error = new SchemaRegistryError("timeout", { retriable: true })
    expect(error.retriable).toBe(true)
  })

  it("isError narrows type", () => {
    const error = new SchemaRegistryError("test")
    expect(SchemaRegistryError.isError(error)).toBe(true)
    expect(SchemaRegistryError.isError(new Error("other"))).toBe(false)
  })
})

// ---------------------------------------------------------------------------
// SchemaRegistry Client
// ---------------------------------------------------------------------------

describe("SchemaRegistry", () => {
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

  it("registers a schema", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 1 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const result = await registry.register("test-topic-value", '{"type":"string"}', "AVRO")

    expect(result.id).toBe(1)
    expect(fetchMock).toHaveBeenCalledOnce()

    const [url, options] = fetchMock.mock.calls[0]
    expect(url).toBe("http://localhost:8081/subjects/test-topic-value/versions")
    expect(options!.method).toBe("POST")
    expect(JSON.parse(options!.body as string)).toEqual({ schema: '{"type":"string"}' })

    vi.unstubAllGlobals()
  })

  it("includes schemaType for non-AVRO schemas", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 2 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    await registry.register("test-topic-value", 'syntax = "proto3";', "PROTOBUF")

    const body = JSON.parse(fetchMock.mock.calls[0][1]!.body as string)
    expect(body.schemaType).toBe("PROTOBUF")

    vi.unstubAllGlobals()
  })

  it("fetches a schema by ID", async () => {
    const fetchMock = mockFetch([
      { status: 200, body: { schema: '{"type":"string"}', schemaType: "AVRO" } }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const schema = await registry.getSchema(1)

    expect(schema.id).toBe(1)
    expect(schema.schema).toBe('{"type":"string"}')
    expect(fetchMock.mock.calls[0][0]).toBe("http://localhost:8081/schemas/ids/1")

    vi.unstubAllGlobals()
  })

  it("caches schema by ID", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { schema: '{"type":"string"}' } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    await registry.getSchema(1)
    await registry.getSchema(1)

    expect(fetchMock).toHaveBeenCalledOnce()

    vi.unstubAllGlobals()
  })

  it("fetches latest schema for subject", async () => {
    const fetchMock = mockFetch([
      {
        status: 200,
        body: {
          id: 3,
          schema: '{"type":"string"}',
          subject: "test-value",
          version: 2
        }
      }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const schema = await registry.getLatestSchema("test-value")

    expect(schema.id).toBe(3)
    expect(schema.version).toBe(2)
    expect(fetchMock.mock.calls[0][0]).toBe(
      "http://localhost:8081/subjects/test-value/versions/latest"
    )

    vi.unstubAllGlobals()
  })

  it("fetches specific schema version", async () => {
    const fetchMock = mockFetch([
      {
        status: 200,
        body: { id: 5, schema: '{"type":"int"}', subject: "s", version: 3 }
      }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const schema = await registry.getSchemaVersion("s", 3)

    expect(schema.id).toBe(5)
    expect(schema.version).toBe(3)
    expect(fetchMock.mock.calls[0][0]).toBe("http://localhost:8081/subjects/s/versions/3")

    vi.unstubAllGlobals()
  })

  it("lists subjects", async () => {
    const fetchMock = mockFetch([{ status: 200, body: ["subject-a", "subject-b"] }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    const subjects = await registry.listSubjects()

    expect(subjects).toEqual(["subject-a", "subject-b"])

    vi.unstubAllGlobals()
  })

  it("sends auth header when configured", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 1 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({
      url: "http://localhost:8081",
      auth: { username: "user", password: "pass" }
    })
    await registry.register("s", "{}", "AVRO")

    const headers = fetchMock.mock.calls[0][1]!.headers as Record<string, string>
    expect(headers.Authorization).toBe(`Basic ${btoa("user:pass")}`)

    vi.unstubAllGlobals()
  })

  it("strips trailing slash from URL", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { schema: '{"type":"string"}' } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081/" })
    await registry.getSchema(1)

    expect(fetchMock.mock.calls[0][0]).toBe("http://localhost:8081/schemas/ids/1")

    vi.unstubAllGlobals()
  })

  it("throws SchemaRegistryError on HTTP error with error body", async () => {
    const fetchMock = mockFetch([
      {
        status: 404,
        body: { error_code: 40401, message: "subject not found" }
      }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })

    await expect(registry.getLatestSchema("missing")).rejects.toThrow(SchemaRegistryError)
    try {
      await registry.getLatestSchema("missing")
    } catch (error) {
      if (SchemaRegistryError.isError(error)) {
        // Only the first call threw (second is also an error but fetch already consumed)
      }
    }

    vi.unstubAllGlobals()
  })

  it("marks 5xx errors as retriable", async () => {
    const fetchMock = mockFetch([
      { status: 503, body: { error_code: 50001, message: "backend error" } }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })

    try {
      await registry.getSchema(999)
    } catch (error) {
      expect(SchemaRegistryError.isError(error)).toBe(true)
      if (SchemaRegistryError.isError(error)) {
        expect(error.retriable).toBe(true)
        expect(error.statusCode).toBe(503)
        expect(error.errorCode).toBe(50001)
      }
    }

    vi.unstubAllGlobals()
  })

  it("throws retriable error on network failure", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => {
        throw new TypeError("fetch failed")
      })
    )

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })

    try {
      await registry.getSchema(1)
    } catch (error) {
      expect(SchemaRegistryError.isError(error)).toBe(true)
      if (SchemaRegistryError.isError(error)) {
        expect(error.retriable).toBe(true)
        expect(error.message).toContain("failed to connect")
      }
    }

    vi.unstubAllGlobals()
  })

  it("clears cache", async () => {
    const fetchMock = mockFetch([
      { status: 200, body: { schema: '{"type":"string"}' } },
      { status: 200, body: { schema: '{"type":"int"}' } }
    ])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    await registry.getSchema(1)
    registry.clearCache()
    await registry.getSchema(1)

    expect(fetchMock).toHaveBeenCalledTimes(2)

    vi.unstubAllGlobals()
  })

  it("URL-encodes subject names", async () => {
    const fetchMock = mockFetch([{ status: 200, body: { id: 1 } }])
    vi.stubGlobal("fetch", fetchMock)

    const registry = new SchemaRegistry({ url: "http://localhost:8081" })
    await registry.register("my/subject", "{}", "AVRO")

    expect(fetchMock.mock.calls[0][0]).toBe("http://localhost:8081/subjects/my%2Fsubject/versions")

    vi.unstubAllGlobals()
  })
})
