import { describe, expect, it, vi } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import type { ConnectionPool } from "../../broker-pool"
import { KafkaConnectionError, KafkaError, KafkaProtocolError } from "../../errors"
import type { Message } from "../../messages"
import { Acks } from "../../produce"
import {
  createProducer,
  defaultPartitioner,
  KafkaProducer,
  type ProducerOptions,
  roundRobinPartitioner
} from "../../producer"
import { CompressionCodec } from "../../record-batch"

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
    // eslint-disable-next-line @typescript-eslint/require-await
    send: vi.fn(async () => {
      if (callIndex >= responses.length) {
        throw new Error("no more mock responses")
      }
      return new BinaryReader(responses[callIndex++])
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
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    connect: async () => {},
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    refreshMetadata: async () => {},
    getConnection: () => {
      throw new Error("not implemented")
    },
    getConnectionByNodeId: () => {
      throw new Error("not implemented")
    },
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    releaseConnection: () => {},
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    close: async () => {},
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
    brokers: brokerMap as ConnectionPool["brokers"],
    // eslint-disable-next-line @typescript-eslint/require-await
    getConnectionByNodeId: vi.fn(async () => {
      getConnCallCount++
      // First call is for metadata refresh, remaining are for produce
      if (getConnCallCount === 1) {
        return metadataConn
      }
      return produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        getConnCall++
        if (getConnCall === 1) {
          return metadataConn
        }
        if (getConnCall === 2) {
          return produceConn1
        }
        return produceConn2
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
      brokers: brokerMap as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => metadataConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => metadataConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        callN++
        return callN === 1 ? metadataConn : produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => metadataConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return n === 1 ? metadataConn : produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return n === 1 ? metadataConn : produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        getConnCall++
        // 1st call: InitProducerId, 2nd: metadata, 3rd: produce
        if (getConnCall === 1) {
          return initPidConn
        }
        if (getConnCall === 2) {
          return metadataConn
        }
        return produceConn
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

  it("forces acks=All when transactionalId is set", async () => {
    // For transactional producers, need InitProducerId connection
    const initPidConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildInitProducerIdBody(2000n, 0)
    ])
    const metadataConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
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
    const produceConn = createMockConnection([
      buildApiVersionsBody(STANDARD_APIS_WITH_INIT_PID),
      buildProduceResponseBody(
        [
          {
            name: "tx-topic",
            partitions: [{ partitionIndex: 0, errorCode: 0, baseOffset: 7n }]
          }
        ],
        8
      )
    ])

    const brokerMap = new Map([[1, TEST_BROKERS[0]]])
    let getConnCall = 0
    const pool = createMockPool({
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        getConnCall++
        if (getConnCall === 1) {
          return initPidConn
        }
        if (getConnCall === 2) {
          return metadataConn
        }
        return produceConn
      }) as unknown as ConnectionPool["getConnectionByNodeId"],
      releaseConnection: vi.fn() as ConnectionPool["releaseConnection"]
    })

    const producer = new KafkaProducer({
      connectionPool: pool,
      acks: Acks.None,
      transactionalId: "tx-1"
    })
    const results = await producer.send("tx-topic", [{ key: null, value: encoder.encode("x") }])
    // Should NOT be fire-and-forget — acks forced to All
    expect(results[0].baseOffset).toBe(7n)
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
      brokers: brokerMap as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => metadataConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => metadataConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => metadataConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return n === 1 ? metadataConn : produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => metadataConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => metadataConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return n === 1 ? metadataConn : produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => metadataConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return n === 1 ? metadataConn : produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        n++
        return n === 1 ? metadataConn : produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        getConnCalls++
        // Odd calls for metadata, even calls for produce
        return getConnCalls % 2 === 1 ? metadataConn : produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        getConnCalls++
        return getConnCalls % 2 === 1 ? metadataConn : produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        getConnCalls++
        return getConnCalls % 2 === 1 ? metadataConn : produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        getConnCall++
        // 1st: InitProducerId, 2nd: metadata, 3rd+: produce
        if (getConnCall === 1) {
          return initPidConn
        }
        if (getConnCall === 2) {
          return metadataConn
        }
        return produceConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      // eslint-disable-next-line @typescript-eslint/require-await
      getConnectionByNodeId: vi.fn(async () => {
        getConnCall++
        if (getConnCall === 1) {
          return initPidConn
        }
        if (getConnCall === 2) {
          return metadataConn
        }
        if (getConnCall === 3) {
          return produceConn1
        }
        return produceConn2
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
      brokers: brokerMap as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => initPidConn
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
      brokers: brokerMap as ConnectionPool["brokers"],
      getConnectionByNodeId: vi.fn(
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => initPidConn
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
