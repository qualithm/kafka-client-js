import { describe, expect, it, vi } from "vitest"

import {
  createProducer,
  defaultPartitioner,
  KafkaProducer,
  type ProducerOptions,
  roundRobinPartitioner
} from "../../../client/producer"
import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import { CompressionCodec } from "../../../codec/record-batch"
import { KafkaConnectionError, KafkaError, KafkaProtocolError } from "../../../errors"
import type { Message } from "../../../messages"
import type { ConnectionPool } from "../../../network/broker-pool"
import { Acks } from "../../../protocol/produce"

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/**
 * Build an ApiVersions v0 response body advertising the given APIs.
 */
function buildApiVersionsBody(
  apis: { apiKey: number; minVersion: number; maxVersion: number }[]
): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt16(0) // error_code = 0
  w.writeInt32(apis.length)
  for (const api of apis) {
    w.writeInt16(api.apiKey)
    w.writeInt16(api.minVersion)
    w.writeInt16(api.maxVersion)
  }
  return w.finish()
}

/**
 * Build a Metadata v1 response body with brokers and topics.
 */
function buildMetadataV1Body(
  brokers: { nodeId: number; host: string; port: number }[],
  topics: {
    errorCode: number
    name: string
    isInternal: boolean
    partitions: {
      errorCode: number
      partitionIndex: number
      leaderId: number
      replicaNodes: number[]
      isrNodes: number[]
    }[]
  }[]
): Uint8Array {
  const w = new BinaryWriter()
  // brokers
  w.writeInt32(brokers.length)
  for (const b of brokers) {
    w.writeInt32(b.nodeId)
    w.writeString(b.host)
    w.writeInt32(b.port)
    w.writeString(null) // rack
  }
  w.writeInt32(0) // controller_id
  // topics
  w.writeInt32(topics.length)
  for (const t of topics) {
    w.writeInt16(t.errorCode)
    w.writeString(t.name)
    w.writeBoolean(t.isInternal)
    w.writeInt32(t.partitions.length)
    for (const p of t.partitions) {
      w.writeInt16(p.errorCode)
      w.writeInt32(p.partitionIndex)
      w.writeInt32(p.leaderId)
      w.writeInt32(p.replicaNodes.length)
      for (const r of p.replicaNodes) {
        w.writeInt32(r)
      }
      w.writeInt32(p.isrNodes.length)
      for (const n of p.isrNodes) {
        w.writeInt32(n)
      }
    }
  }
  return w.finish()
}

/**
 * Build a Produce response body (non-flexible, v0–v8).
 */
function buildProduceResponseBody(
  topics: {
    name: string
    partitions: {
      partitionIndex: number
      errorCode: number
      baseOffset: bigint
      logAppendTimeMs?: bigint
      logStartOffset?: bigint
    }[]
  }[],
  apiVersion: number
): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt32(topics.length)
  for (const t of topics) {
    w.writeString(t.name)
    w.writeInt32(t.partitions.length)
    for (const p of t.partitions) {
      w.writeInt32(p.partitionIndex)
      w.writeInt16(p.errorCode)
      w.writeInt64(p.baseOffset)
      if (apiVersion >= 2) {
        w.writeInt64(p.logAppendTimeMs ?? -1n)
      }
      if (apiVersion >= 5) {
        w.writeInt64(p.logStartOffset ?? -1n)
      }
      if (apiVersion >= 8) {
        w.writeInt32(0) // record_errors (empty)
        w.writeString(null) // error_message
      }
    }
  }
  if (apiVersion >= 1) {
    w.writeInt32(0) // throttle_time_ms
  }
  return w.finish()
}

/** Default broker list for tests. */
const TEST_BROKERS = [{ nodeId: 1, host: "localhost", port: 9092, rack: null }]

/** Standard API versions advertised by test broker (non-flexible produce). */
const STANDARD_APIS = [
  { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
  { apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 1 },
  { apiKey: ApiKey.Produce, minVersion: 0, maxVersion: 8 }
]

/** Standard APIs including InitProducerId for idempotent tests. */
const STANDARD_APIS_WITH_INIT_PID = [
  ...STANDARD_APIS,
  { apiKey: ApiKey.InitProducerId, minVersion: 0, maxVersion: 4 }
]

/**
 * Build an InitProducerId response body (flexible v4 format).
 * Includes trailing tagged fields varint (0 = empty).
 */
function buildInitProducerIdBody(
  producerId: bigint,
  producerEpoch: number,
  errorCode = 0,
  throttleTimeMs = 0
): Uint8Array {
  const w = new BinaryWriter()
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  w.writeInt64(producerId)
  w.writeInt16(producerEpoch)
  w.writeUnsignedVarInt(0) // empty tagged fields (flexible v2+)
  return w.finish()
}

/**
 * A mock connection that sequences through pre-built response bodies.
 * Each call to `send()` returns the next response as a BinaryReader.
 */
function createMockConnection(
  responses: Uint8Array[],
  brokerAddr = "localhost:9092"
): {
  send: ReturnType<typeof vi.fn>
  broker: string
  connected: boolean
} {
  let callIndex = 0
  return {
    send: vi.fn(async () => {
      if (callIndex >= responses.length) {
        return Promise.reject(new Error("no more mock responses"))
      }
      return Promise.resolve(new BinaryReader(responses[callIndex++]))
    }),
    broker: brokerAddr,
    connected: true
  }
}

/**
 * Create a minimal mock connection pool for unit testing.
 */
function createMockPool(overrides?: Partial<ConnectionPool>): ConnectionPool {
  return {
    brokers: new Map(),
    isClosed: false,
    connect: async () => Promise.resolve(),
    refreshMetadata: async () => Promise.resolve(),
    getConnection: () => {
      throw new Error("not implemented")
    },
    getConnectionByNodeId: () => {
      throw new Error("not implemented")
    },
    releaseConnection: () => {
      /* noop */
    },
    close: async () => Promise.resolve(),
    connectionCount: () => 0,
    ...overrides
  } as unknown as ConnectionPool
}

/**
 * Create a pool with a broker and a mock connection that returns
 * standard API versions + metadata + produce responses.
 */
function createPoolWithBroker(
  metadataTopics: Parameters<typeof buildMetadataV1Body>[1],
  produceTopics: Parameters<typeof buildProduceResponseBody>[0],
  produceApiVersion = 8,
  brokers = TEST_BROKERS
): { pool: ConnectionPool; mockConn: ReturnType<typeof createMockConnection> } {
  // For metadata: ApiVersions response, then Metadata response
  // For produce: ApiVersions response, then Produce response
  const metadataConn = createMockConnection([
    buildApiVersionsBody(STANDARD_APIS),
    buildMetadataV1Body(brokers, metadataTopics)
  ])
  const produceConn = createMockConnection([
    buildApiVersionsBody(STANDARD_APIS),
    buildProduceResponseBody(produceTopics, produceApiVersion)
  ])

  const brokerMap = new Map(brokers.map((b) => [b.nodeId, b]))

  // First getConnectionByNodeId call is for metadata (any broker),
  // subsequent calls route to the produce connection by leader nodeId
  let getConnCallCount = 0
  const pool = createMockPool({
    brokers: brokerMap,
    getConnectionByNodeId: vi.fn(async () => {
      getConnCallCount++
      // First call is for metadata refresh, remaining are for produce
      if (getConnCallCount === 1) {
        return Promise.resolve(metadataConn)
      }
      return Promise.resolve(produceConn)
    }) as unknown as ConnectionPool["getConnectionByNodeId"],
    releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
  })

  return { pool, mockConn: produceConn }
}

function defaultProducerOptions(overrides?: Partial<ProducerOptions>): ProducerOptions {
  return {
    connectionPool: createMockPool(),
    ...overrides
  }
}

// ---------------------------------------------------------------------------
// Partitioners
// ---------------------------------------------------------------------------

describe("defaultPartitioner", () => {
  it("returns a function", () => {
    const partitioner = defaultPartitioner()
    expect(typeof partitioner).toBe("function")
  })

  it("returns 0 when partition count is 0", () => {
    const partitioner = defaultPartitioner()
    expect(partitioner("topic", null, 0)).toBe(0)
  })

  it("round-robins for null keys", () => {
    const partitioner = defaultPartitioner()
    const results = new Set<number>()
    for (let i = 0; i < 10; i++) {
      results.add(partitioner("topic", null, 3))
    }
    // Should hit multiple partitions
    expect(results.size).toBeGreaterThan(1)
  })

  it("distributes null-key messages across partitions", () => {
    const partitioner = defaultPartitioner()
    const counts = [0, 0, 0]
    for (let i = 0; i < 9; i++) {
      counts[partitioner("topic", null, 3)]++
    }
    // Each partition should get exactly 3 messages (round-robin)
    expect(counts).toEqual([3, 3, 3])
  })

  it("consistently hashes the same key to the same partition", () => {
    const partitioner = defaultPartitioner()
    const key = new TextEncoder().encode("user-123")
    const partition1 = partitioner("topic", key, 10)
    const partition2 = partitioner("topic", key, 10)
    expect(partition1).toBe(partition2)
  })

  it("produces valid partition indices for various keys", () => {
    const partitioner = defaultPartitioner()
    const encoder = new TextEncoder()
    for (let i = 0; i < 100; i++) {
      const key = encoder.encode(`key-${String(i)}`)
      const partition = partitioner("topic", key, 6)
      expect(partition).toBeGreaterThanOrEqual(0)
      expect(partition).toBeLessThan(6)
    }
  })

  it("hashes key with byteLength % 4 == 1 (trailing 1 byte)", () => {
    const partitioner = defaultPartitioner()
    // "a" = 1 byte → length % 4 == 1: hits case 1 in murmur2
    const key = new Uint8Array([0x61])
    const partition = partitioner("topic", key, 100)
    expect(partition).toBeGreaterThanOrEqual(0)
    expect(partition).toBeLessThan(100)
  })

  it("hashes key with byteLength % 4 == 2 (trailing 2 bytes)", () => {
    const partitioner = defaultPartitioner()
    // "ab" = 2 bytes → length % 4 == 2: hits case 2 in murmur2
    const key = new Uint8Array([0x61, 0x62])
    const partition = partitioner("topic", key, 100)
    expect(partition).toBeGreaterThanOrEqual(0)
    expect(partition).toBeLessThan(100)
  })

  it("hashes key with byteLength % 4 == 3 (trailing 3 bytes)", () => {
    const partitioner = defaultPartitioner()
    // "abc" = 3 bytes → length % 4 == 3: hits case 3 in murmur2
    const key = new Uint8Array([0x61, 0x62, 0x63])
    const partition = partitioner("topic", key, 100)
    expect(partition).toBeGreaterThanOrEqual(0)
    expect(partition).toBeLessThan(100)
  })

  it("hashes key with byteLength % 4 == 0 (no trailing bytes)", () => {
    const partitioner = defaultPartitioner()
    // "abcd" = 4 bytes → length % 4 == 0: default case (no switch branch)
    const key = new Uint8Array([0x61, 0x62, 0x63, 0x64])
    const partition = partitioner("topic", key, 100)
    expect(partition).toBeGreaterThanOrEqual(0)
    expect(partition).toBeLessThan(100)
  })
})

describe("roundRobinPartitioner", () => {
  it("returns a function", () => {
    const partitioner = roundRobinPartitioner()
    expect(typeof partitioner).toBe("function")
  })

  it("cycles through partitions in order", () => {
    const partitioner = roundRobinPartitioner()
    const results = []
    for (let i = 0; i < 6; i++) {
      results.push(partitioner("topic", null, 3))
    }
    expect(results).toEqual([0, 1, 2, 0, 1, 2])
  })

  it("ignores the message key", () => {
    const partitioner = roundRobinPartitioner()
    const key = new TextEncoder().encode("same-key")
    const results = []
    for (let i = 0; i < 3; i++) {
      results.push(partitioner("topic", key, 3))
    }
    expect(results).toEqual([0, 1, 2])
  })

  it("returns 0 when partition count is 0", () => {
    const partitioner = roundRobinPartitioner()
    expect(partitioner("topic", null, 0)).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// KafkaProducer construction
// ---------------------------------------------------------------------------

describe("KafkaProducer", () => {
  describe("constructor", () => {
    it("creates with default options", () => {
      const producer = new KafkaProducer(defaultProducerOptions())
      expect(producer).toBeInstanceOf(KafkaProducer)
    })

    it("accepts custom acks", () => {
      const producer = new KafkaProducer(defaultProducerOptions({ acks: Acks.None }))
      expect(producer).toBeInstanceOf(KafkaProducer)
    })

    it("accepts custom timeout", () => {
      const producer = new KafkaProducer(defaultProducerOptions({ timeoutMs: 5000 }))
      expect(producer).toBeInstanceOf(KafkaProducer)
    })

    it("accepts custom partitioner", () => {
      const producer = new KafkaProducer(
        defaultProducerOptions({ partitioner: roundRobinPartitioner() })
      )
      expect(producer).toBeInstanceOf(KafkaProducer)
    })

    it("accepts compression option", () => {
      const producer = new KafkaProducer(
        defaultProducerOptions({ compression: CompressionCodec.GZIP })
      )
      expect(producer).toBeInstanceOf(KafkaProducer)
    })
  })

  describe("close", () => {
    it("prevents sending after close", async () => {
      const producer = new KafkaProducer(defaultProducerOptions())
      await producer.close()

      const message: Message = {
        key: null,
        value: new TextEncoder().encode("test")
      }

      await expect(producer.send("topic", [message])).rejects.toThrow(KafkaConnectionError)
    })

    it("can be called multiple times", async () => {
      const producer = new KafkaProducer(defaultProducerOptions())
      await producer.close()
      await producer.close()
    })
  })

  describe("send", () => {
    it("returns empty array for empty messages", async () => {
      const producer = new KafkaProducer(defaultProducerOptions())
      const results = await producer.send("topic", [])
      expect(results).toEqual([])
    })

    it("rejects when producer is closed", async () => {
      const producer = new KafkaProducer(defaultProducerOptions())
      await producer.close()

      await expect(
        producer.send("topic", [{ key: null, value: new Uint8Array([1]) }])
      ).rejects.toThrow("producer is closed")
    })

    it("rejects when no brokers available", async () => {
      const pool = createMockPool({
        brokers: new Map()
      })
      const producer = new KafkaProducer({ connectionPool: pool })

      await expect(
        producer.send("topic", [{ key: null, value: new Uint8Array([1]) }])
      ).rejects.toThrow("no brokers available")
    })
  })

  describe("sendToPartition", () => {
    it("returns synthetic result for empty messages", async () => {
      const producer = new KafkaProducer(defaultProducerOptions())
      const result = await producer.sendToPartition({ topic: "topic", partition: 0 }, [])
      expect(result.topicPartition).toEqual({ topic: "topic", partition: 0 })
      expect(result.baseOffset).toBe(-1n)
    })

    it("rejects when producer is closed", async () => {
      const producer = new KafkaProducer(defaultProducerOptions())
      await producer.close()

      await expect(
        producer.sendToPartition({ topic: "topic", partition: 0 }, [
          { key: null, value: new Uint8Array([1]) }
        ])
      ).rejects.toThrow("producer is closed")
    })
  })
})

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

describe("createProducer", () => {
  it("creates a KafkaProducer instance", () => {
    const producer = createProducer(defaultProducerOptions())
    expect(producer).toBeInstanceOf(KafkaProducer)
  })
})

// ---------------------------------------------------------------------------
// send() — full produce path
// ---------------------------------------------------------------------------

describe("KafkaProducer send (integration path)", () => {
  const encoder = new TextEncoder()

  it("sends a single message and returns produce result", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "test-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "test-topic",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 42n }]
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool, acks: Acks.All })
    const results = await producer.send("test-topic", [
      { key: null, value: encoder.encode("hello") }
    ])

    expect(results).toHaveLength(1)
    expect(results[0].topicPartition).toEqual({ topic: "test-topic", partition: 0 })
    expect(results[0].baseOffset).toBe(42n)
  })

  it("sends messages to multiple partitions on the same leader", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "multi-part",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] },
            { errorCode: 0, partitionIndex: 1, leaderId: 1, replicaNodes: [1], isrNodes: [1] },
            { errorCode: 0, partitionIndex: 2, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "multi-part",
          partitions: [
            { partitionIndex: 0, errorCode: 0, baseOffset: 10n },
            { partitionIndex: 1, errorCode: 0, baseOffset: 20n },
            { partitionIndex: 2, errorCode: 0, baseOffset: 30n }
          ]
        }
      ]
    )

    const producer = new KafkaProducer({
      connectionPool: pool,
      partitioner: roundRobinPartitioner()
    })
    const messages: Message[] = [
      { key: null, value: encoder.encode("a") },
      { key: null, value: encoder.encode("b") },
      { key: null, value: encoder.encode("c") }
    ]
    const results = await producer.send("multi-part", messages)

    expect(results).toHaveLength(3)
    const partitions = results.map((r) => r.topicPartition.partition).sort()
    expect(partitions).toEqual([0, 1, 2])
  })

  it("uses metadata cache on second send to same topic", async () => {
    // Build connections that can handle two produce requests
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "cached-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])

    const produceConn1 = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildProduceResponseBody(
        [
          {
            name: "cached-topic",
            partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 1n }]
          }
        ],
        8
      )
    ])
    const produceConn2 = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildProduceResponseBody(
        [
          {
            name: "cached-topic",
            partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 2n }]
          }
        ],
        8
      )
    ])

    const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])
    let getConnCall = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnCall++
        if (getConnCall === 1) {
          return Promise.resolve(metadataConn)
        }
        if (getConnCall === 2) {
          return Promise.resolve(produceConn1)
        }
        return Promise.resolve(produceConn2)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await producer.send("cached-topic", [{ key: null, value: encoder.encode("first") }])
    const results = await producer.send("cached-topic", [
      { key: null, value: encoder.encode("second") }
    ])

    // Second send should skip metadata fetch — only 3 getConnectionByNodeId calls total
    // (1 metadata + 1 produce + 1 produce from cache)
    expect(getConnCall).toBe(3)
    expect(results[0].baseOffset).toBe(2n)
  })

  it("returns synthetic results for acks=0 (fire-and-forget)", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "fire-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      // Response body won't be read for acks=0, but send() still returns a reader
      [
        {
          name: "fire-topic",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 0n }]
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool, acks: Acks.None })
    const results = await producer.send("fire-topic", [
      { key: null, value: encoder.encode("fire") }
    ])

    expect(results).toHaveLength(1)
    expect(results[0].baseOffset).toBe(-1n)
    expect(results[0].topicPartition).toEqual({ topic: "fire-topic", partition: 0 })
  })

  it("sends messages with keys, headers, and timestamps", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "full-msg",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "full-msg",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 100n }]
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool })
    const results = await producer.send("full-msg", [
      {
        key: encoder.encode("user-1"),
        value: encoder.encode("payload"),
        headers: [{ key: "trace-id", value: encoder.encode("abc-123") }],
        timestamp: 1700000000000n
      }
    ])

    expect(results).toHaveLength(1)
    expect(results[0].baseOffset).toBe(100n)
  })

  it("includes logAppendTimeMs as timestamp when non-negative", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "ts-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "ts-topic",
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              baseOffset: 50n,
              logAppendTimeMs: 1700000000000n
            }
          ]
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool })
    const results = await producer.send("ts-topic", [
      { key: null, value: encoder.encode("ts-test") }
    ])

    expect(results[0].timestamp).toBe(1700000000000n)
  })

  it("throws KafkaProtocolError when produce partition has error code", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "err-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "err-topic",
          partitions: [{ partitionIndex: 0, errorCode: 7, baseOffset: 0n }] // REQUEST_TIMED_OUT
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("err-topic", [{ key: null, value: encoder.encode("fail") }])
    ).rejects.toThrow(KafkaProtocolError)
  })

  it("throws when no leader available for a partition", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "no-leader",
          isInternal: false,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 0,
              leaderId: -1,
              replicaNodes: [],
              isrNodes: []
            }
          ]
        }
      ],
      [] // won't reach produce
    )

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("no-leader", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow(KafkaConnectionError)
  })

  it("throws when topic not found in metadata", async () => {
    // Return metadata with no matching topic
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, []) // empty topics
    ])

    const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(metadataConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("missing-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow(KafkaError)
  })

  it("throws when metadata topic has error code", async () => {
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
          name: "bad-topic",
          isInternal: false,
          partitions: []
        }
      ])
    ])

    const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(metadataConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("bad-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow(KafkaProtocolError)
  })

  it("throws when broker does not support produce api", async () => {
    // Advertise only ApiVersions, not Produce
    const apisWithoutProduce = [{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 }]
    const metadataConn = createMockConnection([
      buildApiVersionsBody([
        ...apisWithoutProduce,
        { apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 1 }
      ]),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "test-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])
    const produceConn = createMockConnection([buildApiVersionsBody(apisWithoutProduce)])

    const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])
    let callN = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        callN++
        return Promise.resolve(callN === 1 ? metadataConn : produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("test-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("broker does not support produce api")
  })

  it("throws when broker does not support metadata api", async () => {
    // Advertise only ApiVersions, not Metadata
    const metadataConn = createMockConnection([
      buildApiVersionsBody([{ apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 }])
    ])

    const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(metadataConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("test-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("broker does not support metadata api")
  })

  it("releases connection on success", async () => {
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "rel-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])
    const produceConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildProduceResponseBody(
        [
          {
            name: "rel-topic",
            partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 0n }]
          }
        ],
        8
      )
    ])

    const releaseFn = vi.fn()
    const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])
    let n = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return Promise.resolve(n === 1 ? metadataConn : produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: releaseFn as unknown as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await producer.send("rel-topic", [{ key: null, value: encoder.encode("x") }])

    // Two releases: one for metadata conn, one for produce conn
    expect(releaseFn).toHaveBeenCalledTimes(2)
  })

  it("releases connection on error", async () => {
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "rel-err",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])
    const produceConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildProduceResponseBody(
        [
          {
            name: "rel-err",
            partitions: [{ partitionIndex: 0, errorCode: 7, baseOffset: 0n }]
          }
        ],
        8
      )
    ])

    const releaseFn = vi.fn()
    const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])
    let n = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return Promise.resolve(n === 1 ? metadataConn : produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: releaseFn as unknown as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("rel-err", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow()

    // Both connections should still be released
    expect(releaseFn).toHaveBeenCalledTimes(2)
  })

  it("forces acks=All when idempotent is true", async () => {
    // For idempotent producers, we need an InitProducerId connection + metadata + produce
    const initPidConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildInitProducerIdBody(1000n, 0)
    ])
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "idemp-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])
    const produceConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildProduceResponseBody(
        [
          {
            name: "idemp-topic",
            partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 5n }]
          }
        ],
        8
      )
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    let getConnCall = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnCall++
        // 1st call: InitProducerId, 2nd: metadata, 3rd: produce
        if (getConnCall === 1) {
          return Promise.resolve(initPidConn)
        }
        if (getConnCall === 2) {
          return Promise.resolve(metadataConn)
        }
        return Promise.resolve(produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    // Even though acks=1 is requested, idempotent forces All
    const producer = new KafkaProducer({
      connectionPool: pool,
      acks: Acks.Leader,
      idempotent: true
    })
    const results = await producer.send("idemp-topic", [{ key: null, value: encoder.encode("x") }])
    expect(results).toHaveLength(1)
    // The produce request was sent (not fire-and-forget acks=0), so we get real results
    expect(results[0].baseOffset).toBe(5n)
  })

  it("forces acks=All when transactionalId is set", () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      acks: Acks.None,
      transactionalId: "tx-1"
    })
    // Acks are forced to All for transactional producers
    expect((producer as unknown as Record<string, unknown>).acks).toBe(Acks.All)
  })

  it("throws when metadata retriable error occurs", async () => {
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 5, // LEADER_NOT_AVAILABLE — retriable
          name: "meta-err",
          isInternal: false,
          partitions: []
        }
      ])
    ])

    const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(metadataConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    const error = await producer
      .send("meta-err", [{ key: null, value: encoder.encode("x") }])
      .catch((e: unknown) => e as KafkaProtocolError)

    expect(error).toBeInstanceOf(KafkaProtocolError)
    expect((error as KafkaProtocolError).retriable).toBe(true)
  })

  it("throws non-retriable error for unknown metadata error code", async () => {
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 29, // TOPIC_AUTHORIZATION_FAILED — not retriable
          name: "auth-topic",
          isInternal: false,
          partitions: []
        }
      ])
    ])

    const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(metadataConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    const error = await producer
      .send("auth-topic", [{ key: null, value: encoder.encode("x") }])
      .catch((e: unknown) => e as KafkaProtocolError)

    expect(error).toBeInstanceOf(KafkaProtocolError)
    expect((error as KafkaProtocolError).retriable).toBe(false)
  })

  it("throws retriable KafkaProtocolError for retriable produce error code", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "ret-err",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "ret-err",
          partitions: [{ partitionIndex: 0, errorCode: 6, baseOffset: 0n }] // NOT_LEADER_OR_FOLLOWER
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool })
    const error = await producer
      .send("ret-err", [{ key: null, value: encoder.encode("x") }])
      .catch((e: unknown) => e as KafkaProtocolError)

    expect(error).toBeInstanceOf(KafkaProtocolError)
    expect((error as KafkaProtocolError).retriable).toBe(true)
  })

  it("throws non-retriable KafkaProtocolError for non-retriable produce error", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "nonret-err",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "nonret-err",
          partitions: [{ partitionIndex: 0, errorCode: 2, baseOffset: 0n }] // CORRUPT_MESSAGE
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool })
    const error = await producer
      .send("nonret-err", [{ key: null, value: encoder.encode("x") }])
      .catch((e: unknown) => e as KafkaProtocolError)

    expect(error).toBeInstanceOf(KafkaProtocolError)
    expect((error as KafkaProtocolError).retriable).toBe(false)
  })

  it("treats DUPLICATE_SEQUENCE_NUMBER (45) as non-retriable", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "dup-seq",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "dup-seq",
          partitions: [{ partitionIndex: 0, errorCode: 45, baseOffset: 0n }]
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool })
    const error = await producer
      .send("dup-seq", [{ key: null, value: encoder.encode("x") }])
      .catch((e: unknown) => e as KafkaProtocolError)

    expect(error).toBeInstanceOf(KafkaProtocolError)
    expect((error as KafkaProtocolError).retriable).toBe(false)
  })

  it("treats OUT_OF_ORDER_SEQUENCE_NUMBER (46) as retriable", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "ooo-seq",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "ooo-seq",
          partitions: [{ partitionIndex: 0, errorCode: 46, baseOffset: 0n }]
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool })
    const error = await producer
      .send("ooo-seq", [{ key: null, value: encoder.encode("x") }])
      .catch((e: unknown) => e as KafkaProtocolError)

    expect(error).toBeInstanceOf(KafkaProtocolError)
    expect((error as KafkaProtocolError).retriable).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// sendToPartition() — full produce path
// ---------------------------------------------------------------------------

describe("KafkaProducer sendToPartition (integration path)", () => {
  const encoder = new TextEncoder()

  it("sends to a specific partition and returns result", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "stp-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] },
            { errorCode: 0, partitionIndex: 1, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "stp-topic",
          partitions: [{ partitionIndex: 1, errorCode: 0, baseOffset: 99n }]
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool })
    const result = await producer.sendToPartition({ topic: "stp-topic", partition: 1 }, [
      { key: null, value: encoder.encode("direct") }
    ])

    expect(result.topicPartition).toEqual({ topic: "stp-topic", partition: 1 })
    expect(result.baseOffset).toBe(99n)
  })

  it("throws when no leader for specified partition", async () => {
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "no-leader-stp",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: -1, replicaNodes: [], isrNodes: [] }
          ]
        }
      ])
    ])

    const brokerMap = new Map([[1, { nodeId: 1, host: "localhost", port: 9092, rack: null }]])
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(metadataConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.sendToPartition({ topic: "no-leader-stp", partition: 0 }, [
        { key: null, value: encoder.encode("x") }
      ])
    ).rejects.toThrow(KafkaConnectionError)
  })
})

// ---------------------------------------------------------------------------
// Edge-case error paths
// ---------------------------------------------------------------------------

describe("KafkaProducer edge-case errors", () => {
  const encoder = new TextEncoder()

  it("throws when api versions decode fails during produce", async () => {
    // Return a truncated/corrupt ApiVersions response for the produce connection
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "edge-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])
    // Return corrupt response (just error code, no array length — causes decode failure)
    const corruptApiVersions = new Uint8Array([0x00, 0x00])
    const produceConn = createMockConnection([corruptApiVersions])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    let n = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return Promise.resolve(n === 1 ? metadataConn : produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("edge-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("failed to decode api versions response")
  })

  it("throws when api versions decode fails during metadata", async () => {
    // Return corrupt ApiVersions response for metadata connection
    const corruptApiVersions = new Uint8Array([0x00, 0x00])
    const metadataConn = createMockConnection([corruptApiVersions])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(metadataConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("fail-meta", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("failed to decode api versions response")
  })

  it("throws when no compatible metadata api version", async () => {
    // Advertise metadata versions outside client range
    const metadataConn = createMockConnection([
      buildApiVersionsBody([
        { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
        { apiKey: ApiKey.Metadata, minVersion: 99, maxVersion: 100 }
      ])
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(metadataConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("no-compat", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("no compatible metadata api version")
  })

  it("throws when no compatible produce api version", async () => {
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "no-ver",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])
    // Produce connection advertises only incompatible versions
    const produceConn = createMockConnection([
      buildApiVersionsBody([
        { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
        { apiKey: ApiKey.Produce, minVersion: 99, maxVersion: 100 }
      ])
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    let n = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return Promise.resolve(n === 1 ? metadataConn : produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("no-ver", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("no compatible produce api version")
  })

  it("throws when metadata response decode fails", async () => {
    // Return valid ApiVersions, then corrupt Metadata response
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      new Uint8Array([0xff]) // corrupt metadata response
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(metadataConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("corrupt-meta", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("failed to decode metadata response")
  })

  it("throws when produce response decode fails", async () => {
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "corrupt-prod",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])
    // Valid ApiVersions followed by corrupt produce response
    const produceConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      new Uint8Array([0xff]) // corrupt
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    let n = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return Promise.resolve(n === 1 ? metadataConn : produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.send("corrupt-prod", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("failed to decode produce response")
  })
})

// ---------------------------------------------------------------------------
// Batching
// ---------------------------------------------------------------------------

describe("KafkaProducer batching", () => {
  const encoder = new TextEncoder()

  it("flushes messages on explicit flush()", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "batch-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "batch-topic",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 10n }]
        }
      ]
    )

    const producer = new KafkaProducer({
      connectionPool: pool,
      batch: { lingerMs: 5000 }
    })

    // send() should not immediately produce — it returns a promise that resolves on flush
    const resultPromise = producer.send("batch-topic", [
      { key: null, value: encoder.encode("batched-message") }
    ])

    // The produce connection shouldn't have been called yet (only metadata)
    // Force flush
    await producer.flush()

    const results = await resultPromise
    expect(results).toHaveLength(1)
    expect(results[0].topicPartition).toEqual({ topic: "batch-topic", partition: 0 })
    expect(results[0].baseOffset).toBe(10n)
  })

  it("flushes when batch size is exceeded", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "size-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "size-topic",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 20n }]
        }
      ]
    )

    const producer = new KafkaProducer({
      connectionPool: pool,
      batch: { lingerMs: 60_000, batchBytes: 100 }
    })

    // Send a large message that exceeds batchBytes — should trigger auto-flush
    const largeValue = new Uint8Array(200)
    const results = await producer.send("size-topic", [{ key: null, value: largeValue }])
    expect(results).toHaveLength(1)
    expect(results[0].baseOffset).toBe(20n)
  })

  it("close() flushes pending batches", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "close-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "close-topic",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 30n }]
        }
      ]
    )

    const producer = new KafkaProducer({
      connectionPool: pool,
      batch: { lingerMs: 60_000 }
    })

    const resultPromise = producer.send("close-topic", [
      { key: null, value: encoder.encode("close-msg") }
    ])

    // close() should flush before closing
    await producer.close()

    const results = await resultPromise
    expect(results).toHaveLength(1)
    expect(results[0].baseOffset).toBe(30n)
  })

  it("sends immediately when lingerMs is 0 (default)", async () => {
    const { pool, mockConn } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "immediate-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "immediate-topic",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 0n }]
        }
      ]
    )

    // No batch config — should behave like original immediate send
    const producer = new KafkaProducer({ connectionPool: pool })

    const results = await producer.send("immediate-topic", [
      { key: null, value: encoder.encode("immediate") }
    ])
    expect(results).toHaveLength(1)
    // Produce connection should have been called
    expect(mockConn.send).toHaveBeenCalled()
  })

  it("flush() is a no-op when accumulator is empty", async () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      batch: { lingerMs: 1000 }
    })

    // Should resolve immediately without errors
    await producer.flush()
  })

  it("rejects batched message deferreds on send failure", async () => {
    // Create a pool where produce fails
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "fail-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])
    // Produce connection that returns a protocol error
    const produceConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildProduceResponseBody(
        [
          {
            name: "fail-topic",
            partitions: [{ partitionIndex: 0, errorCode: 2, baseOffset: 0n }]
          }
        ],
        8
      )
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    let n = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return Promise.resolve(n === 1 ? metadataConn : produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      batch: { lingerMs: 60_000 }
    })

    const resultPromise = producer.send("fail-topic", [
      { key: null, value: encoder.encode("will-fail") }
    ])

    await producer.flush()

    await expect(resultPromise).rejects.toThrow()
  })

  it("flushes on linger timer expiry", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "linger-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "linger-topic",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 50n }]
        }
      ]
    )

    const producer = new KafkaProducer({
      connectionPool: pool,
      batch: { lingerMs: 5 }
    })

    // send() accumulates; linger timer fires after 5ms and flushes automatically
    const results = await producer.send("linger-topic", [
      { key: null, value: encoder.encode("linger-msg") }
    ])

    expect(results).toHaveLength(1)
    expect(results[0].baseOffset).toBe(50n)
  })
})

// ---------------------------------------------------------------------------
// Retry
// ---------------------------------------------------------------------------

describe("KafkaProducer retry", () => {
  const encoder = new TextEncoder()

  it("retries on retriable errors", async () => {
    // First produce attempt fails with retriable error, second succeeds
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "retry-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ]),
      // Second metadata fetch on retry
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "retry-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])

    const failProduceBody = buildProduceResponseBody(
      [
        {
          name: "retry-topic",
          partitions: [{ partitionIndex: 0, errorCode: 7, baseOffset: 0n }] // REQUEST_TIMED_OUT
        }
      ],
      8
    )
    const successProduceBody = buildProduceResponseBody(
      [
        {
          name: "retry-topic",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 42n }]
        }
      ],
      8
    )

    const produceConn = createMockConnection([
      // First attempt: ApiVersions + fail
      buildApiVersionsBody(STANDARD_APIS),
      failProduceBody,
      // Second attempt (retry): ApiVersions + success
      buildApiVersionsBody(STANDARD_APIS),
      successProduceBody
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    let getConnCalls = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnCalls++
        // Odd calls for metadata, even calls for produce
        return Promise.resolve(getConnCalls % 2 === 1 ? metadataConn : produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      retry: { maxRetries: 2, initialRetryMs: 1 }
    })

    const results = await producer.send("retry-topic", [
      { key: null, value: encoder.encode("retry-msg") }
    ])

    expect(results).toHaveLength(1)
    expect(results[0].baseOffset).toBe(42n)
  })

  it("does not retry non-retriable errors", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "no-retry-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "no-retry-topic",
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 17, // INVALID_PARTITIONS (non-retriable)
              baseOffset: 0n
            }
          ]
        }
      ]
    )

    const producer = new KafkaProducer({
      connectionPool: pool,
      retry: { maxRetries: 3, initialRetryMs: 1 }
    })

    await expect(
      producer.send("no-retry-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow(KafkaProtocolError)
  })

  it("gives up after maxRetries attempts", async () => {
    // Create a pool that always fails with retriable error
    const metadataBody = buildMetadataV1Body(TEST_BROKERS, [
      {
        errorCode: 0,
        name: "exhaust-topic",
        isInternal: false,
        partitions: [
          { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
        ]
      }
    ])
    const failBody = buildProduceResponseBody(
      [
        {
          name: "exhaust-topic",
          partitions: [{ partitionIndex: 0, errorCode: 7, baseOffset: 0n }]
        }
      ],
      8
    )

    // Enough responses for 3 attempts (metadata + produce each time)
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      metadataBody,
      buildApiVersionsBody(STANDARD_APIS),
      metadataBody,
      buildApiVersionsBody(STANDARD_APIS),
      metadataBody
    ])
    const produceConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      failBody,
      buildApiVersionsBody(STANDARD_APIS),
      failBody,
      buildApiVersionsBody(STANDARD_APIS),
      failBody
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    let getConnCalls = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnCalls++
        return Promise.resolve(getConnCalls % 2 === 1 ? metadataConn : produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      retry: { maxRetries: 2, initialRetryMs: 1 }
    })

    // Should exhaust retries (attempt 0 + 2 retries = 3 attempts, all fail)
    await expect(
      producer.send("exhaust-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow(KafkaProtocolError)
  })

  it("retries sendToPartition on retriable errors", async () => {
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "stp-retry",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ]),
      // Retry metadata fetch
      buildApiVersionsBody(STANDARD_APIS),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "stp-retry",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])

    const produceConn = createMockConnection([
      // First attempt fails
      buildApiVersionsBody(STANDARD_APIS),
      buildProduceResponseBody(
        [
          {
            name: "stp-retry",
            partitions: [{ partitionIndex: 0, errorCode: 6, baseOffset: 0n }] // NOT_LEADER_OR_FOLLOWER
          }
        ],
        8
      ),
      // Retry succeeds
      buildApiVersionsBody(STANDARD_APIS),
      buildProduceResponseBody(
        [
          {
            name: "stp-retry",
            partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 55n }]
          }
        ],
        8
      )
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    let getConnCalls = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnCalls++
        return Promise.resolve(getConnCalls % 2 === 1 ? metadataConn : produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      retry: { maxRetries: 2, initialRetryMs: 1 }
    })

    const result = await producer.sendToPartition({ topic: "stp-retry", partition: 0 }, [
      { key: null, value: encoder.encode("retry") }
    ])

    expect(result.baseOffset).toBe(55n)
  })

  it("does not retry when maxRetries is 0 (default)", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "no-config-retry",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "no-config-retry",
          partitions: [{ partitionIndex: 0, errorCode: 7, baseOffset: 0n }]
        }
      ]
    )

    // No retry config — default maxRetries=0
    const producer = new KafkaProducer({ connectionPool: pool })

    await expect(
      producer.send("no-config-retry", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow(KafkaProtocolError)
  })
})

// ---------------------------------------------------------------------------
// Idempotent producer
// ---------------------------------------------------------------------------

describe("KafkaProducer idempotent", () => {
  const encoder = new TextEncoder()

  /**
   * Helper to create a pool with InitProducerId + metadata + produce connections
   * for idempotent tests.
   */
  function createIdempotentPoolWithBroker(
    metadataTopics: Parameters<typeof buildMetadataV1Body>[1],
    produceTopics: Parameters<typeof buildProduceResponseBody>[0],
    producerId = 1000n,
    producerEpoch = 0,
    produceApiVersion = 8
  ): { pool: ConnectionPool; initPidConn: ReturnType<typeof createMockConnection> } {
    const initPidConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildInitProducerIdBody(producerId, producerEpoch)
    ])
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildMetadataV1Body(TEST_BROKERS, metadataTopics)
    ])
    const produceConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildProduceResponseBody(produceTopics, produceApiVersion)
    ])

    const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
    let getConnCall = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnCall++
        // 1st: InitProducerId, 2nd: metadata, 3rd+: produce
        if (getConnCall === 1) {
          return Promise.resolve(initPidConn)
        }
        if (getConnCall === 2) {
          return Promise.resolve(metadataConn)
        }
        return Promise.resolve(produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    return { pool, initPidConn }
  }

  it("calls InitProducerId on first send", async () => {
    const { pool, initPidConn } = createIdempotentPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "pid-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "pid-topic",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 10n }]
        }
      ]
    )

    const producer = new KafkaProducer({
      connectionPool: pool,
      idempotent: true
    })
    const results = await producer.send("pid-topic", [
      { key: null, value: encoder.encode("hello") }
    ])

    expect(results).toHaveLength(1)
    expect(results[0].baseOffset).toBe(10n)
    // InitProducerId connection should have been called
    expect(initPidConn.send).toHaveBeenCalledTimes(2) // ApiVersions + InitProducerId
  })

  it("does not call InitProducerId for non-idempotent producer", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "non-pid",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "non-pid",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 1n }]
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool })
    const results = await producer.send("non-pid", [{ key: null, value: encoder.encode("hello") }])

    expect(results).toHaveLength(1)
  })

  it("calls InitProducerId only once across multiple sends", async () => {
    const initPidConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildInitProducerIdBody(1000n, 0)
    ])
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "multi-send",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])
    const produceConn1 = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildProduceResponseBody(
        [
          {
            name: "multi-send",
            partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 1n }]
          }
        ],
        8
      )
    ])
    const produceConn2 = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildProduceResponseBody(
        [
          {
            name: "multi-send",
            partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 2n }]
          }
        ],
        8
      )
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    let getConnCall = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnCall++
        if (getConnCall === 1) {
          return Promise.resolve(initPidConn)
        }
        if (getConnCall === 2) {
          return Promise.resolve(metadataConn)
        }
        if (getConnCall === 3) {
          return Promise.resolve(produceConn1)
        }
        return Promise.resolve(produceConn2)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      idempotent: true
    })

    await producer.send("multi-send", [{ key: null, value: encoder.encode("first") }])
    await producer.send("multi-send", [{ key: null, value: encoder.encode("second") }])

    // InitProducerId should only be called once (2 send calls: ApiVersions + InitProdId)
    expect(initPidConn.send).toHaveBeenCalledTimes(2)
  })

  it("sets default maxRetries to 5 for idempotent producer", () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      idempotent: true
    })
    // Access private field for testing — verify default retry count is 5
    expect((producer as unknown as Record<string, unknown>).maxRetries).toBe(5)
  })

  it("allows explicit maxRetries override for idempotent producer", () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      idempotent: true,
      retry: { maxRetries: 10 }
    })
    expect((producer as unknown as Record<string, unknown>).maxRetries).toBe(10)
  })

  it("enables idempotent when transactionalId is set", () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "my-txn"
    })
    expect((producer as unknown as Record<string, unknown>).idempotent).toBe(true)
  })

  it("throws when InitProducerId returns error code", async () => {
    const initPidConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildInitProducerIdBody(-1n, -1, 15) // NOT_COORDINATOR
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(initPidConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      idempotent: true
    })

    await expect(
      producer.send("any-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("init producer id failed")
  })

  it("throws when broker does not support InitProducerId api", async () => {
    // Return APIs without InitProducerId
    const initPidConn = createMockConnection([buildApiVersionsBody(STANDARD_APIS)])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(initPidConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      idempotent: true
    })

    await expect(
      producer.send("any-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("broker does not support init producer id api")
  })

  it("resets sequence numbers on close", async () => {
    const { pool } = createIdempotentPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "close-seq",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "close-seq",
          partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 1n }]
        }
      ]
    )

    const producer = new KafkaProducer({
      connectionPool: pool,
      idempotent: true
    })

    await producer.send("close-seq", [{ key: null, value: encoder.encode("x") }])
    await producer.close()

    expect((producer as unknown as Record<string, unknown>).producerId).toBe(-1n)
    expect((producer as unknown as Record<string, unknown>).producerEpoch).toBe(-1)
  })
})

// ---------------------------------------------------------------------------
// Transactional producer
// ---------------------------------------------------------------------------

describe("KafkaProducer transactional", () => {
  const encoder = new TextEncoder()

  /**
   * API versions for transactional producer tests.
   * maxVersion is kept below flexible-encoding thresholds so our
   * hand-built response helpers stay simple (no tagged-field suffix).
   */
  const txnApis = [
    { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 2 },
    { apiKey: ApiKey.Metadata, minVersion: 0, maxVersion: 1 },
    { apiKey: ApiKey.Produce, minVersion: 0, maxVersion: 8 },
    { apiKey: ApiKey.InitProducerId, minVersion: 0, maxVersion: 1 },
    { apiKey: ApiKey.FindCoordinator, minVersion: 0, maxVersion: 2 },
    { apiKey: ApiKey.AddPartitionsToTxn, minVersion: 0, maxVersion: 2 },
    { apiKey: ApiKey.EndTxn, minVersion: 0, maxVersion: 2 },
    { apiKey: ApiKey.AddOffsetsToTxn, minVersion: 0, maxVersion: 2 },
    { apiKey: ApiKey.TxnOffsetCommit, minVersion: 0, maxVersion: 2 }
  ]

  /** Build a FindCoordinator v1–v2 response body (includes throttle + error message). */
  function buildFindCoordinatorBody(
    nodeId: number,
    host: string,
    port: number,
    errorCode = 0
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(errorCode) // error_code
    w.writeString(null) // error_message (nullable string)
    w.writeInt32(nodeId) // node_id
    w.writeString(host) // host
    w.writeInt32(port) // port
    return w.finish()
  }

  /** Build an AddPartitionsToTxn v0 response body. */
  function buildAddPartitionsToTxnBody(
    topics: { name: string; partitions: { partitionIndex: number; errorCode: number }[] }[]
  ): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt32(topics.length) // topics count
    for (const topic of topics) {
      w.writeString(topic.name)
      w.writeInt32(topic.partitions.length)
      for (const p of topic.partitions) {
        w.writeInt32(p.partitionIndex)
        w.writeInt16(p.errorCode)
      }
    }
    return w.finish()
  }

  /** Build an EndTxn v0 response body. */
  function buildEndTxnBody(errorCode = 0, throttleTimeMs = 0): Uint8Array {
    const w = new BinaryWriter()
    w.writeInt32(throttleTimeMs)
    w.writeInt16(errorCode)
    return w.finish()
  }

  /**
   * Build coordinator responses for the standard transactional send flow:
   * FindCoordinator → InitProducerId → AddPartitionsToTxn.
   * Optionally appends EndTxn responses.
   */
  function buildCoordinatorResponses(
    metadataTopics: Parameters<typeof buildMetadataV1Body>[1],
    options?: { endTxnErrorCode?: number; producerId?: bigint; producerEpoch?: number }
  ): Uint8Array[] {
    const responses: Uint8Array[] = [
      // FindCoordinator discovery
      buildApiVersionsBody(txnApis),
      buildFindCoordinatorBody(1, "localhost", 9092),
      // InitProducerId via coordinator
      buildApiVersionsBody(txnApis),
      buildInitProducerIdBody(options?.producerId ?? 3000n, options?.producerEpoch ?? 0),
      // AddPartitionsToTxn
      buildApiVersionsBody(txnApis),
      buildAddPartitionsToTxnBody(
        metadataTopics.map((t) => ({
          name: t.name,
          partitions: t.partitions.map((p) => ({
            partitionIndex: p.partitionIndex,
            errorCode: 0
          }))
        }))
      )
    ]

    if (options?.endTxnErrorCode !== undefined) {
      responses.push(buildApiVersionsBody(txnApis), buildEndTxnBody(options.endTxnErrorCode))
    }

    return responses
  }

  /** Standard metadata topic used by most transactional tests. */
  const standardMetaTopic = [
    {
      errorCode: 0,
      name: "tx-topic",
      isInternal: false,
      partitions: [
        { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
      ]
    }
  ]

  /** Standard produce response for most transactional tests. */
  const standardProduceTopic = [
    { name: "tx-topic", partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 1n }] }
  ]

  /**
   * Create a full transactional producer pool.
   */
  function createTransactionalPool(
    coordinatorResponses: Uint8Array[],
    metadataTopics: Parameters<typeof buildMetadataV1Body>[1],
    produceTopics: Parameters<typeof buildProduceResponseBody>[0],
    produceApiVersion = 8
  ): { pool: ConnectionPool } {
    const coordinatorConn = createMockConnection(coordinatorResponses)

    const metadataConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildMetadataV1Body(TEST_BROKERS, metadataTopics)
    ])

    const produceConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildProduceResponseBody(produceTopics, produceApiVersion)
    ])

    const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
    let getConnByIdCall = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnByIdCall++
        if (getConnByIdCall === 1) {
          return Promise.resolve(coordinatorConn)
        }
        if (getConnByIdCall === 2) {
          return Promise.resolve(metadataConn)
        }
        return Promise.resolve(produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      getConnection: vi.fn(async () => {
        return Promise.resolve(coordinatorConn)
      }) as unknown as ConnectionPool["getConnection"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    return { pool }
  }

  // -----------------------------------------------------------------------
  // beginTransaction
  // -----------------------------------------------------------------------

  it("throws when beginTransaction called on non-transactional producer", () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({ connectionPool: pool })
    expect(() => {
      producer.beginTransaction()
    }).toThrow("beginTransaction requires a transactional producer")
  })

  it("throws when beginTransaction called twice", () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({ connectionPool: pool, transactionalId: "tx-1" })
    producer.beginTransaction()
    expect(() => {
      producer.beginTransaction()
    }).toThrow("transaction already in progress")
  })

  it("allows beginTransaction after commit", async () => {
    const responses = buildCoordinatorResponses(standardMetaTopic, { endTxnErrorCode: 0 })
    const { pool } = createTransactionalPool(responses, standardMetaTopic, standardProduceTopic)

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    await producer.send("tx-topic", [{ key: null, value: encoder.encode("x") }])
    await producer.commitTransaction()

    // Should be able to begin again
    expect(() => {
      producer.beginTransaction()
    }).not.toThrow()
  })

  // -----------------------------------------------------------------------
  // send requires beginTransaction for transactional producers
  // -----------------------------------------------------------------------

  it("throws when send called without beginTransaction", async () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    await expect(
      producer.send("topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("transactional producer requires beginTransaction before send")
  })

  it("throws when sendToPartition called without beginTransaction", async () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    await expect(
      producer.sendToPartition({ topic: "t", partition: 0 }, [
        { key: null, value: encoder.encode("x") }
      ])
    ).rejects.toThrow("transactional producer requires beginTransaction before send")
  })

  // -----------------------------------------------------------------------
  // Transactional send flow
  // -----------------------------------------------------------------------

  it("sends messages within a transaction", async () => {
    const produceTopics = [
      { name: "tx-topic", partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 42n }] }
    ]
    const responses = buildCoordinatorResponses(standardMetaTopic)
    const { pool } = createTransactionalPool(responses, standardMetaTopic, produceTopics)

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    const results = await producer.send("tx-topic", [{ key: null, value: encoder.encode("hello") }])

    expect(results).toHaveLength(1)
    expect(results[0].baseOffset).toBe(42n)
  })

  // -----------------------------------------------------------------------
  // commitTransaction
  // -----------------------------------------------------------------------

  it("throws when commitTransaction called on non-transactional producer", async () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(producer.commitTransaction()).rejects.toThrow(
      "commitTransaction requires a transactional producer"
    )
  })

  it("throws when commitTransaction called without beginTransaction", async () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })
    await expect(producer.commitTransaction()).rejects.toThrow("no transaction in progress")
  })

  it("throws when commitTransaction returns error", async () => {
    // EndTxn returns CONCURRENT_TRANSACTIONS (51)
    const responses = buildCoordinatorResponses(standardMetaTopic, { endTxnErrorCode: 51 })
    const { pool } = createTransactionalPool(responses, standardMetaTopic, standardProduceTopic)

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    await producer.send("tx-topic", [{ key: null, value: encoder.encode("x") }])

    await expect(producer.commitTransaction()).rejects.toThrow("end transaction failed")
  })

  // -----------------------------------------------------------------------
  // abortTransaction
  // -----------------------------------------------------------------------

  it("throws when abortTransaction called on non-transactional producer", async () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(producer.abortTransaction()).rejects.toThrow(
      "abortTransaction requires a transactional producer"
    )
  })

  it("throws when abortTransaction called without beginTransaction", async () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })
    await expect(producer.abortTransaction()).rejects.toThrow("no transaction in progress")
  })

  it("aborts transaction and allows new one", async () => {
    const responses = buildCoordinatorResponses(standardMetaTopic, { endTxnErrorCode: 0 })
    const { pool } = createTransactionalPool(responses, standardMetaTopic, standardProduceTopic)

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    await producer.send("tx-topic", [{ key: null, value: encoder.encode("x") }])
    await producer.abortTransaction()

    // Transaction state should be reset
    expect(() => {
      producer.beginTransaction()
    }).not.toThrow()
  })

  // -----------------------------------------------------------------------
  // sendOffsetsToTransaction
  // -----------------------------------------------------------------------

  it("throws when sendOffsetsToTransaction called without transaction", async () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })
    await expect(
      producer.sendOffsetsToTransaction([{ topic: "t", partition: 0, offset: 10n }], "group-1")
    ).rejects.toThrow("no transaction in progress")
  })

  it("throws when sendOffsetsToTransaction called on non-transactional producer", async () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({ connectionPool: pool })
    await expect(
      producer.sendOffsetsToTransaction([{ topic: "t", partition: 0, offset: 10n }], "group-1")
    ).rejects.toThrow("sendOffsetsToTransaction requires a transactional producer")
  })

  // -----------------------------------------------------------------------
  // close with active transaction
  // -----------------------------------------------------------------------

  it("aborts transaction on close", async () => {
    const responses = buildCoordinatorResponses(standardMetaTopic, { endTxnErrorCode: 0 })
    const { pool } = createTransactionalPool(responses, standardMetaTopic, standardProduceTopic)

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    await producer.send("tx-topic", [{ key: null, value: encoder.encode("x") }])

    // Close should abort the in-flight transaction
    await producer.close()

    // Producer state should be cleaned up
    expect((producer as unknown as Record<string, unknown>).producerId).toBe(-1n)
    expect((producer as unknown as Record<string, unknown>).coordinatorHost).toBeNull()
  })

  it("clears coordinator state on close", async () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    // Non-transactional close (no active transaction)
    await producer.close()

    expect((producer as unknown as Record<string, unknown>).coordinatorHost).toBeNull()
    expect((producer as unknown as Record<string, unknown>).coordinatorPort).toBeNull()
  })

  // -----------------------------------------------------------------------
  // AddPartitionsToTxn error handling
  // -----------------------------------------------------------------------

  it("throws on AddPartitionsToTxn error", async () => {
    // Discovery connection (getConnectionByNodeId call 1: FindCoordinator)
    const discoveryConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildFindCoordinatorBody(1, "localhost", 9092)
    ])

    // Metadata connection (getConnectionByNodeId call 2: Metadata)
    const metadataConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "tx-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])

    // Coordinator connection (getConnection: InitProducerId + AddPartitionsToTxn)
    const coordinatorConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildInitProducerIdBody(3000n, 0),
      buildApiVersionsBody(txnApis),
      buildAddPartitionsToTxnBody([
        {
          name: "tx-topic",
          partitions: [{ partitionIndex: 0, errorCode: 48 }] // INVALID_TXN_STATE
        }
      ])
    ])

    const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
    let getConnByIdCall = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnByIdCall++
        if (getConnByIdCall === 1) {
          return Promise.resolve(discoveryConn)
        }
        return Promise.resolve(metadataConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      getConnection: vi.fn(async () => {
        return Promise.resolve(coordinatorConn)
      }) as unknown as ConnectionPool["getConnection"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    await expect(
      producer.send("tx-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("add partitions to txn failed")
  })

  // -----------------------------------------------------------------------
  // Coordinator discovery error handling
  // -----------------------------------------------------------------------

  it("throws when FindCoordinator returns error", async () => {
    const coordinatorConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildFindCoordinatorBody(0, "", 0, 15) // NOT_COORDINATOR error
    ])

    const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        return Promise.resolve(coordinatorConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    await expect(
      producer.send("topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("find transaction coordinator failed")
  })

  it("invalidates coordinator cache on NOT_COORDINATOR error", () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    // Manually set coordinator
    const internal = producer as unknown as Record<string, unknown>
    internal.coordinatorHost = "old-host"
    internal.coordinatorPort = 1234

    // Call invalidateCoordinatorOnError with NOT_COORDINATOR (15)
    ;(
      producer as unknown as { invalidateCoordinatorOnError: (code: number) => void }
    ).invalidateCoordinatorOnError(15)

    expect(internal.coordinatorHost).toBeNull()
    expect(internal.coordinatorPort).toBeNull()
  })

  it("does not invalidate coordinator cache on non-coordinator errors", () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    const internal = producer as unknown as Record<string, unknown>
    internal.coordinatorHost = "host"
    internal.coordinatorPort = 1234
    ;(
      producer as unknown as { invalidateCoordinatorOnError: (code: number) => void }
    ).invalidateCoordinatorOnError(51) // CONCURRENT_TRANSACTIONS

    expect(internal.coordinatorHost).toBe("host")
    expect(internal.coordinatorPort).toBe(1234)
  })

  // -----------------------------------------------------------------------
  // sendOffsetsToTransaction full flow
  // -----------------------------------------------------------------------

  it("sends offsets to transaction (full flow)", async () => {
    /** Build an AddOffsetsToTxn v0 response body. */
    function buildAddOffsetsToTxnBody(errorCode = 0, throttleTimeMs = 0): Uint8Array {
      const w = new BinaryWriter()
      w.writeInt32(throttleTimeMs)
      w.writeInt16(errorCode)
      return w.finish()
    }

    /** Build a TxnOffsetCommit v0 response body. */
    function buildTxnOffsetCommitBody(
      topics: { name: string; partitions: { partitionIndex: number; errorCode: number }[] }[]
    ): Uint8Array {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(topics.length)
      for (const t of topics) {
        w.writeString(t.name)
        w.writeInt32(t.partitions.length)
        for (const p of t.partitions) {
          w.writeInt32(p.partitionIndex)
          w.writeInt16(p.errorCode)
        }
      }
      return w.finish()
    }

    // Coordinator connection: FindCoordinator + InitProducerId + AddPartitionsToTxn
    // + AddOffsetsToTxn
    const coordinatorConn = createMockConnection([
      // FindCoordinator
      buildApiVersionsBody(txnApis),
      buildFindCoordinatorBody(1, "localhost", 9092),
      // InitProducerId
      buildApiVersionsBody(txnApis),
      buildInitProducerIdBody(3000n, 0),
      // AddPartitionsToTxn
      buildApiVersionsBody(txnApis),
      buildAddPartitionsToTxnBody([
        { name: "tx-topic", partitions: [{ partitionIndex: 0, errorCode: 0 }] }
      ]),
      // AddOffsetsToTxn
      buildApiVersionsBody(txnApis),
      buildAddOffsetsToTxnBody(0)
    ])

    // Group coordinator (for FindGroupCoordinator + TxnOffsetCommit)
    const discoveryConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildFindCoordinatorBody(2, "localhost", 9093)
    ])

    const groupCoordinatorConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildTxnOffsetCommitBody([
        { name: "tx-topic", partitions: [{ partitionIndex: 0, errorCode: 0 }] }
      ])
    ])

    const metadataConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildMetadataV1Body(TEST_BROKERS, standardMetaTopic)
    ])

    const produceConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildProduceResponseBody(standardProduceTopic, 8)
    ])

    const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
    let getConnByIdCall = 0
    let getConnCall = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnByIdCall++
        if (getConnByIdCall === 1) {
          return Promise.resolve(coordinatorConn)
        } // FindCoordinator discovery
        if (getConnByIdCall === 2) {
          return Promise.resolve(metadataConn)
        } // Metadata (getTopicMetadata)
        if (getConnByIdCall === 3) {
          return Promise.resolve(produceConn)
        } // sendToLeader
        if (getConnByIdCall === 4) {
          return Promise.resolve(discoveryConn)
        } // FindGroupCoordinator discovery
        return Promise.resolve(produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      getConnection: vi.fn(async () => {
        getConnCall++
        if (getConnCall <= 3) {
          return Promise.resolve(coordinatorConn)
        } // InitProducerId, AddPartitions, AddOffsets
        return Promise.resolve(groupCoordinatorConn) // TxnOffsetCommit
      }) as unknown as ConnectionPool["getConnection"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    await producer.send("tx-topic", [{ key: null, value: encoder.encode("x") }])
    await producer.sendOffsetsToTransaction(
      [{ topic: "tx-topic", partition: 0, offset: 10n }],
      "my-group"
    )

    // Verify the group coordinator was called (ApiVersions + TxnOffsetCommit)
    expect(groupCoordinatorConn.send).toHaveBeenCalledTimes(2)
  })

  // -----------------------------------------------------------------------
  // sendToPartition within a transaction
  // -----------------------------------------------------------------------

  it("sends to specific partition within a transaction", async () => {
    const responses = buildCoordinatorResponses(standardMetaTopic)
    const produceTopics = [
      { name: "tx-topic", partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 99n }] }
    ]
    const { pool } = createTransactionalPool(responses, standardMetaTopic, produceTopics)

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    const result = await producer.sendToPartition({ topic: "tx-topic", partition: 0 }, [
      { key: null, value: encoder.encode("direct") }
    ])

    expect(result.baseOffset).toBe(99n)
  })

  // -----------------------------------------------------------------------
  // initProducerId via transaction coordinator
  // -----------------------------------------------------------------------

  it("routes InitProducerId through transaction coordinator", async () => {
    const coordinatorConn = createMockConnection([
      // FindCoordinator
      buildApiVersionsBody(txnApis),
      buildFindCoordinatorBody(1, "coord-host", 9095),
      // InitProducerId (via getConnection to coordinator)
      buildApiVersionsBody(txnApis),
      buildInitProducerIdBody(5000n, 1),
      // AddPartitionsToTxn
      buildApiVersionsBody(txnApis),
      buildAddPartitionsToTxnBody([
        { name: "tx-topic", partitions: [{ partitionIndex: 0, errorCode: 0 }] }
      ])
    ])

    const metadataConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildMetadataV1Body(TEST_BROKERS, standardMetaTopic)
    ])

    const produceConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildProduceResponseBody(standardProduceTopic, 8)
    ])

    const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
    let getConnByIdCall = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnByIdCall++
        if (getConnByIdCall === 1) {
          return Promise.resolve(coordinatorConn)
        }
        if (getConnByIdCall === 2) {
          return Promise.resolve(metadataConn)
        }
        return Promise.resolve(produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      getConnection: vi.fn(async () =>
        Promise.resolve(coordinatorConn)
      ) as unknown as ConnectionPool["getConnection"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    const results = await producer.send("tx-topic", [
      { key: null, value: encoder.encode("txn-msg") }
    ])
    expect(results).toHaveLength(1)
    expect((producer as unknown as Record<string, unknown>).producerId).toBe(5000n)
    expect((producer as unknown as Record<string, unknown>).producerEpoch).toBe(1)
  })

  // -----------------------------------------------------------------------
  // Skips already-registered partitions in AddPartitionsToTxn
  // -----------------------------------------------------------------------

  it("skips AddPartitionsToTxn for already-registered partitions", async () => {
    // First send: full flow including AddPartitionsToTxn
    // Second send: should skip AddPartitionsToTxn for the same partition
    const coordinatorConn = createMockConnection([
      // FindCoordinator
      buildApiVersionsBody(txnApis),
      buildFindCoordinatorBody(1, "localhost", 9092),
      // InitProducerId
      buildApiVersionsBody(txnApis),
      buildInitProducerIdBody(3000n, 0),
      // AddPartitionsToTxn (first send)
      buildApiVersionsBody(txnApis),
      buildAddPartitionsToTxnBody([
        { name: "tx-topic", partitions: [{ partitionIndex: 0, errorCode: 0 }] }
      ])
      // Second send should NOT call AddPartitionsToTxn again
    ])

    const metadataConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildMetadataV1Body(TEST_BROKERS, standardMetaTopic)
    ])

    const produceConn1 = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildProduceResponseBody(standardProduceTopic, 8)
    ])
    const produceConn2 = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildProduceResponseBody(standardProduceTopic, 8)
    ])

    const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
    let getConnByIdCall = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnByIdCall++
        if (getConnByIdCall === 1) {
          return Promise.resolve(coordinatorConn)
        }
        if (getConnByIdCall === 2) {
          return Promise.resolve(metadataConn)
        }
        if (getConnByIdCall === 3) {
          return Promise.resolve(produceConn1)
        }
        return Promise.resolve(produceConn2)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      getConnection: vi.fn(async () =>
        Promise.resolve(coordinatorConn)
      ) as unknown as ConnectionPool["getConnection"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    await producer.send("tx-topic", [{ key: null, value: encoder.encode("first") }])
    // Second send to same partition — should reuse cached metadata and skip AddPartitionsToTxn
    await producer.send("tx-topic", [{ key: null, value: encoder.encode("second") }])

    // coordinatorConn should still have only 6 send calls (ApiVersions+FindCoord + ApiVersions+InitPID + ApiVersions+AddPartitions)
    expect(coordinatorConn.send).toHaveBeenCalledTimes(6)
  })

  // -----------------------------------------------------------------------
  // negotiateApiVersion error paths (on coordinator)
  // -----------------------------------------------------------------------

  it("throws when coordinator negotiateApiVersion decode fails", async () => {
    const coordinatorConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildFindCoordinatorBody(1, "localhost", 9092),
      // InitProducerId: return invalid response
      new Uint8Array([0x00]) // truncated / invalid
    ])

    const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () =>
        Promise.resolve(coordinatorConn)
      ) as unknown as ConnectionPool["getConnectionByNodeId"],
      getConnection: vi.fn(async () =>
        Promise.resolve(coordinatorConn)
      ) as unknown as ConnectionPool["getConnection"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    await expect(
      producer.send("tx-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("failed to decode api versions response")
  })

  it("throws when coordinator does not support required API key", async () => {
    const limitedApis = [
      { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 2 },
      { apiKey: ApiKey.FindCoordinator, minVersion: 0, maxVersion: 2 },
      { apiKey: ApiKey.InitProducerId, minVersion: 0, maxVersion: 1 }
      // Missing AddPartitionsToTxn
    ]
    const coordinatorConn = createMockConnection([
      buildApiVersionsBody(limitedApis),
      buildFindCoordinatorBody(1, "localhost", 9092),
      buildApiVersionsBody(limitedApis),
      buildInitProducerIdBody(3000n, 0),
      // Negotiate AddPartitionsToTxn — API not in version map
      buildApiVersionsBody(limitedApis)
    ])

    const metadataConn = createMockConnection([
      buildApiVersionsBody(txnApis),
      buildMetadataV1Body(TEST_BROKERS, standardMetaTopic)
    ])

    const brokerMap = new Map(TEST_BROKERS.map((b) => [b.nodeId, b]))
    let getConnByIdCall = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnByIdCall++
        if (getConnByIdCall === 1) {
          return Promise.resolve(coordinatorConn)
        }
        return Promise.resolve(metadataConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      getConnection: vi.fn(async () =>
        Promise.resolve(coordinatorConn)
      ) as unknown as ConnectionPool["getConnection"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    await expect(
      producer.send("tx-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("broker does not support api key")
  })

  // -----------------------------------------------------------------------
  // discardBufferedMessages
  // -----------------------------------------------------------------------

  it("discardBufferedMessages rejects pending deferreds", () => {
    const pool = createMockPool()
    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1",
      batch: { lingerMs: 10_000 }
    })

    // Directly add to accumulator for testing
    type TestPartitionAccumulator = {
      messages: Message[]
      deferreds: { resolve: ReturnType<typeof vi.fn>; reject: ReturnType<typeof vi.fn> }[]
      sizeBytes: number
    }
    const internal = producer as unknown as {
      accumulator: Map<string, Map<number, TestPartitionAccumulator>>
      discardBufferedMessages: () => void
    }

    const deferred1 = {
      resolve: vi.fn(),
      reject: vi.fn()
    }
    const deferred2 = {
      resolve: vi.fn(),
      reject: vi.fn()
    }

    const partMap = new Map<number, TestPartitionAccumulator>()
    partMap.set(0, {
      messages: [{ key: null, value: encoder.encode("a") }],
      deferreds: [deferred1],
      sizeBytes: 100
    })
    partMap.set(1, {
      messages: [{ key: null, value: encoder.encode("b") }],
      deferreds: [deferred2],
      sizeBytes: 100
    })
    internal.accumulator.set("test-topic", partMap)

    internal.discardBufferedMessages()

    expect(deferred1.reject).toHaveBeenCalledTimes(1)
    expect(deferred2.reject).toHaveBeenCalledTimes(1)
    expect(internal.accumulator.size).toBe(0)
  })

  // -----------------------------------------------------------------------
  // acks=0 fire-and-forget in sendToLeader
  // -----------------------------------------------------------------------

  it("returns synthetic results for acks=0", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "ack0-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [] // No produce response needed for acks=0
    )

    const producer = new KafkaProducer({
      connectionPool: pool,
      acks: Acks.None
    })

    const results = await producer.send("ack0-topic", [
      { key: null, value: encoder.encode("fire-and-forget") }
    ])

    expect(results).toHaveLength(1)
    expect(results[0].baseOffset).toBe(-1n)
    expect(results[0].topicPartition.topic).toBe("ack0-topic")
  })

  // -----------------------------------------------------------------------
  // idempotent producer requires produce API v3+
  // -----------------------------------------------------------------------

  it("throws when idempotent producer encounters produce API < v3", async () => {
    const initPidConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildInitProducerIdBody(1000n, 0)
    ])
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildMetadataV1Body(TEST_BROKERS, [
        {
          errorCode: 0,
          name: "v2-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ])
    ])
    // Produce connection only supports up to v2
    const oldApis = [
      { apiKey: ApiKey.ApiVersions, minVersion: 0, maxVersion: 3 },
      { apiKey: ApiKey.Produce, minVersion: 0, maxVersion: 2 }
    ]
    const produceConn = createMockConnection([buildApiVersionsBody(oldApis)])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    let getConnCall = 0
    const pool = createMockPool({
      brokers: brokerMap,
      getConnectionByNodeId: vi.fn(async () => {
        getConnCall++
        if (getConnCall === 1) {
          return Promise.resolve(initPidConn)
        }
        if (getConnCall === 2) {
          return Promise.resolve(metadataConn)
        }
        return Promise.resolve(produceConn)
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      idempotent: true
    })

    await expect(
      producer.send("v2-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("idempotent producer requires produce api v3+")
  })

  // -----------------------------------------------------------------------
  // initProducerId with no brokers available
  // -----------------------------------------------------------------------

  it("throws when no brokers available for InitProducerId", async () => {
    const pool = createMockPool({
      brokers: new Map() as ConnectionPool["brokers"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      idempotent: true
    })

    await expect(
      producer.send("any-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("no brokers available")
  })

  // -----------------------------------------------------------------------
  // Produce response error handling
  // -----------------------------------------------------------------------

  it("throws on produce partition error code", async () => {
    const { pool } = createPoolWithBroker(
      [
        {
          errorCode: 0,
          name: "err-topic",
          isInternal: false,
          partitions: [
            { errorCode: 0, partitionIndex: 0, leaderId: 1, replicaNodes: [1], isrNodes: [1] }
          ]
        }
      ],
      [
        {
          name: "err-topic",
          partitions: [{ partitionIndex: 0, errorCode: 6, baseOffset: -1n }] // NOT_LEADER_OR_FOLLOWER
        }
      ]
    )

    const producer = new KafkaProducer({ connectionPool: pool })

    await expect(
      producer.send("err-topic", [{ key: null, value: encoder.encode("x") }])
    ).rejects.toThrow("produce failed")
  })

  // -----------------------------------------------------------------------
  // EndTxn error invalidates coordinator
  // -----------------------------------------------------------------------

  it("invalidates coordinator on EndTxn NOT_COORDINATOR error", async () => {
    const responses = buildCoordinatorResponses(standardMetaTopic, { endTxnErrorCode: 15 })
    const { pool } = createTransactionalPool(responses, standardMetaTopic, standardProduceTopic)

    const producer = new KafkaProducer({
      connectionPool: pool,
      transactionalId: "tx-1"
    })

    producer.beginTransaction()
    await producer.send("tx-topic", [{ key: null, value: encoder.encode("x") }])

    const internal = producer as unknown as Record<string, unknown>
    // Coordinator should be set after FindCoordinator
    expect(internal.coordinatorHost).not.toBeNull()

    await expect(producer.commitTransaction()).rejects.toThrow("end transaction failed")
    // After error, coordinator should be invalidated
    expect(internal.coordinatorHost).toBeNull()
  })
})
