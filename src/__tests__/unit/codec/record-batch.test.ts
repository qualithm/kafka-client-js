/**
 * RecordBatch v2 (magic=2) encoding/decoding tests.
 *
 * @see https://kafka.apache.org/documentation/#recordbatch — RecordBatch format
 * @see https://kafka.apache.org/documentation/#record — Record format
 */

import fc from "fast-check"
import { beforeAll, describe, expect, it } from "vitest"

import { gzipProvider } from "../../../codec/compression"
import {
  buildRecordBatch,
  CompressionCodec,
  crc32c,
  createRecord,
  decodeAttributes,
  decodeRecordBatch,
  encodeAttributes,
  encodeRecordBatch,
  hasCompressionProvider,
  type Record,
  RECORD_BATCH_MAGIC,
  type RecordBatch,
  type RecordBatchAttributes,
  registerCompressionProvider,
  TimestampType
} from "../../../codec/record-batch"

// ---------------------------------------------------------------------------
// Test Helpers
// ---------------------------------------------------------------------------

function recordBatchRoundTrip(batch: RecordBatch): RecordBatch {
  const encoded = encodeRecordBatch(batch)
  const result = decodeRecordBatch(encoded)
  if (!result.ok) {
    throw new Error(`decode failed: ${result.error.message}`)
  }
  return result.value
}

function createTestRecord(
  key: string | null,
  value: string | null,
  offsetDelta = 0,
  timestampDelta = 0n
): Record {
  return createRecord(
    key !== null ? new TextEncoder().encode(key) : null,
    value !== null ? new TextEncoder().encode(value) : null,
    [],
    offsetDelta,
    timestampDelta
  )
}

// ---------------------------------------------------------------------------
// CRC-32C
// ---------------------------------------------------------------------------

describe("crc32c", () => {
  it("computes correct CRC for empty data", () => {
    const crc = crc32c(new Uint8Array(0))
    expect(crc).toBe(0x00000000)
  })

  it("computes correct CRC for known test vector", () => {
    // "123456789" is a standard CRC test vector
    // CRC-32C of "123456789" = 0xE3069283
    const data = new TextEncoder().encode("123456789")
    const crc = crc32c(data)
    expect(crc).toBe(0xe3069283)
  })

  it("computes correct CRC for single byte", () => {
    // CRC-32C of [0x00] = 0x527D5351
    const crc = crc32c(new Uint8Array([0x00]))
    expect(crc).toBe(0x527d5351)
  })
})

// ---------------------------------------------------------------------------
// Attributes Encoding
// ---------------------------------------------------------------------------

describe("encodeAttributes / decodeAttributes", () => {
  it("round trips compression codec", () => {
    for (const codec of [
      CompressionCodec.NONE,
      CompressionCodec.GZIP,
      CompressionCodec.SNAPPY,
      CompressionCodec.LZ4,
      CompressionCodec.ZSTD
    ]) {
      const attrs: RecordBatchAttributes = {
        compression: codec,
        timestampType: TimestampType.CREATE_TIME,
        isTransactional: false,
        isControlBatch: false,
        hasDeleteHorizon: false
      }
      const encoded = encodeAttributes(attrs)
      const decoded = decodeAttributes(encoded)
      expect(decoded.compression).toBe(codec)
    }
  })

  it("round trips timestamp type", () => {
    for (const tsType of [TimestampType.CREATE_TIME, TimestampType.LOG_APPEND_TIME]) {
      const attrs: RecordBatchAttributes = {
        compression: CompressionCodec.NONE,
        timestampType: tsType,
        isTransactional: false,
        isControlBatch: false,
        hasDeleteHorizon: false
      }
      const encoded = encodeAttributes(attrs)
      const decoded = decodeAttributes(encoded)
      expect(decoded.timestampType).toBe(tsType)
    }
  })

  it("round trips transactional flag", () => {
    for (const isTransactional of [true, false]) {
      const attrs: RecordBatchAttributes = {
        compression: CompressionCodec.NONE,
        timestampType: TimestampType.CREATE_TIME,
        isTransactional,
        isControlBatch: false,
        hasDeleteHorizon: false
      }
      const encoded = encodeAttributes(attrs)
      const decoded = decodeAttributes(encoded)
      expect(decoded.isTransactional).toBe(isTransactional)
    }
  })

  it("round trips control batch flag", () => {
    for (const isControlBatch of [true, false]) {
      const attrs: RecordBatchAttributes = {
        compression: CompressionCodec.NONE,
        timestampType: TimestampType.CREATE_TIME,
        isTransactional: false,
        isControlBatch,
        hasDeleteHorizon: false
      }
      const encoded = encodeAttributes(attrs)
      const decoded = decodeAttributes(encoded)
      expect(decoded.isControlBatch).toBe(isControlBatch)
    }
  })

  it("round trips all flags combined", () => {
    const attrs: RecordBatchAttributes = {
      compression: CompressionCodec.LZ4,
      timestampType: TimestampType.LOG_APPEND_TIME,
      isTransactional: true,
      isControlBatch: true,
      hasDeleteHorizon: true
    }
    const encoded = encodeAttributes(attrs)
    const decoded = decodeAttributes(encoded)
    expect(decoded).toEqual(attrs)
  })
})

// ---------------------------------------------------------------------------
// RecordBatch Encoding/Decoding
// ---------------------------------------------------------------------------

describe("RecordBatch", () => {
  describe("basic round-trip", () => {
    it("encodes and decodes empty batch", () => {
      const batch = buildRecordBatch([])
      const decoded = recordBatchRoundTrip(batch)

      expect(decoded.magic).toBe(RECORD_BATCH_MAGIC)
      expect(decoded.records.length).toBe(0)
    })

    it("encodes and decodes single record", () => {
      const record = createTestRecord("key1", "value1")
      const batch = buildRecordBatch([record])
      const decoded = recordBatchRoundTrip(batch)

      expect(decoded.records.length).toBe(1)
      expect(decoded.records[0].key).toEqual(new TextEncoder().encode("key1"))
      expect(decoded.records[0].value).toEqual(new TextEncoder().encode("value1"))
    })

    it("encodes and decodes multiple records", () => {
      const records = [
        createTestRecord("key1", "value1", 0, 0n),
        createTestRecord("key2", "value2", 1, 10n),
        createTestRecord("key3", "value3", 2, 20n)
      ]
      const batch = buildRecordBatch(records)
      const decoded = recordBatchRoundTrip(batch)

      expect(decoded.records.length).toBe(3)
      expect(decoded.lastOffsetDelta).toBe(2)

      for (let i = 0; i < 3; i++) {
        expect(decoded.records[i].offsetDelta).toBe(i)
        expect(decoded.records[i].key).toEqual(new TextEncoder().encode(`key${String(i + 1)}`))
      }
    })

    it("encodes and decodes record with null key", () => {
      const record = createTestRecord(null, "value-only")
      const batch = buildRecordBatch([record])
      const decoded = recordBatchRoundTrip(batch)

      expect(decoded.records[0].key).toBeNull()
      expect(decoded.records[0].value).toEqual(new TextEncoder().encode("value-only"))
    })

    it("encodes and decodes tombstone (null value)", () => {
      const record = createTestRecord("key", null)
      const batch = buildRecordBatch([record])
      const decoded = recordBatchRoundTrip(batch)

      expect(decoded.records[0].key).toEqual(new TextEncoder().encode("key"))
      expect(decoded.records[0].value).toBeNull()
    })

    it("encodes and decodes record with headers", () => {
      const record = createRecord(
        new TextEncoder().encode("key"),
        new TextEncoder().encode("value"),
        [
          { key: "header1", value: new TextEncoder().encode("hvalue1") },
          { key: "header2", value: new TextEncoder().encode("hvalue2") }
        ]
      )
      const batch = buildRecordBatch([record])
      const decoded = recordBatchRoundTrip(batch)

      expect(decoded.records[0].headers.length).toBe(2)
      expect(decoded.records[0].headers[0].key).toBe("header1")
      expect(decoded.records[0].headers[0].value).toEqual(new TextEncoder().encode("hvalue1"))
    })
  })

  describe("batch metadata", () => {
    it("preserves baseOffset", () => {
      const batch = buildRecordBatch([createTestRecord("k", "v")], {
        baseOffset: 12345n
      })
      const decoded = recordBatchRoundTrip(batch)
      expect(decoded.baseOffset).toBe(12345n)
    })

    it("preserves partitionLeaderEpoch", () => {
      const batch = buildRecordBatch([createTestRecord("k", "v")], {
        partitionLeaderEpoch: 42
      })
      const decoded = recordBatchRoundTrip(batch)
      expect(decoded.partitionLeaderEpoch).toBe(42)
    })

    it("preserves producer fields", () => {
      const batch = buildRecordBatch([createTestRecord("k", "v")], {
        producerId: 999n,
        producerEpoch: 5,
        baseSequence: 100
      })
      const decoded = recordBatchRoundTrip(batch)
      expect(decoded.producerId).toBe(999n)
      expect(decoded.producerEpoch).toBe(5)
      expect(decoded.baseSequence).toBe(100)
    })

    it("computes maxTimestamp correctly", () => {
      const records = [
        createTestRecord("k1", "v1", 0, 0n),
        createTestRecord("k2", "v2", 1, 100n),
        createTestRecord("k3", "v3", 2, 50n)
      ]
      const batch = buildRecordBatch(records, { baseTimestamp: 1000n })
      const decoded = recordBatchRoundTrip(batch)

      expect(decoded.baseTimestamp).toBe(1000n)
      expect(decoded.maxTimestamp).toBe(1100n) // 1000 + 100
    })
  })

  describe("CRC validation", () => {
    it("validates CRC on decode", () => {
      const batch = buildRecordBatch([createTestRecord("k", "v")])
      const encoded = encodeRecordBatch(batch)

      // Corrupt a byte in the middle
      encoded[encoded.length - 5] ^= 0xff

      const result = decodeRecordBatch(encoded, true)
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("CRC_MISMATCH")
      }
    })

    it("skips CRC validation when disabled", () => {
      const batch = buildRecordBatch([createTestRecord("k", "v")])
      const encoded = encodeRecordBatch(batch)

      // Corrupt a byte
      encoded[encoded.length - 5] ^= 0xff

      // Should succeed with CRC validation disabled
      const result = decodeRecordBatch(encoded, false)
      // Note: May still fail if corruption hits critical metadata
      // This test just verifies the flag is respected
      expect(result.ok || result.error.code !== "CRC_MISMATCH").toBe(true)
    })
  })

  describe("error handling", () => {
    it("rejects unsupported magic byte", () => {
      const batch = buildRecordBatch([createTestRecord("k", "v")])
      const encoded = encodeRecordBatch(batch)

      // Change magic byte (at offset 12 + 4 = 16: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4))
      encoded[16] = 0

      const result = decodeRecordBatch(encoded, false)
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("UNSUPPORTED_VERSION")
      }
    })

    it("rejects truncated batch", () => {
      const batch = buildRecordBatch([createTestRecord("k", "v")])
      const encoded = encodeRecordBatch(batch)

      // Truncate the buffer
      const truncated = encoded.slice(0, 20)

      const result = decodeRecordBatch(truncated)
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("BUFFER_UNDERFLOW")
      }
    })

    it("rejects empty buffer", () => {
      const result = decodeRecordBatch(new Uint8Array(0))
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("BUFFER_UNDERFLOW")
      }
    })

    it("rejects buffer too small for baseOffset", () => {
      const result = decodeRecordBatch(new Uint8Array(4))
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("BUFFER_UNDERFLOW")
      }
    })

    it("rejects buffer too small for batchLength", () => {
      // 8 bytes for baseOffset, but not enough for batchLength
      const result = decodeRecordBatch(new Uint8Array(10))
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("BUFFER_UNDERFLOW")
      }
    })

    it("rejects batch length that is too small", () => {
      const batch = buildRecordBatch([createTestRecord("k", "v")])
      const encoded = encodeRecordBatch(batch)

      // Set batchLength to a value smaller than minimum (49 bytes)
      const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength)
      view.setInt32(8, 10) // offset 8 is batchLength, set to 10 (too small)

      const result = decodeRecordBatch(encoded, false)
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("INVALID_DATA")
        expect(result.error.message).toContain("too small")
      }
    })

    it("rejects when batch payload is truncated", () => {
      const batch = buildRecordBatch([createTestRecord("k", "v")])
      const encoded = encodeRecordBatch(batch)

      // Keep header intact but truncate payload
      // baseOffset(8) + batchLength(4) + partial data
      const truncated = encoded.slice(0, 30)

      const result = decodeRecordBatch(truncated, false)
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("BUFFER_UNDERFLOW")
      }
    })

    it("rejects negative records count", () => {
      const batch = buildRecordBatch([createTestRecord("k", "v")])
      const encoded = encodeRecordBatch(batch)

      // Records count is at offset: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) +
      // magic(1) + crc(4) + attributes(2) + lastOffsetDelta(4) + baseTimestamp(8) +
      // maxTimestamp(8) + producerId(8) + producerEpoch(2) + baseSequence(4) = 57
      const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength)
      view.setInt32(57, -5) // Set records count to -5

      const result = decodeRecordBatch(encoded, false)
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("INVALID_DATA")
        expect(result.error.message).toContain("records count")
      }
    })

    it("throws when encoding with unregistered compression codec", () => {
      const batch = buildRecordBatch([createTestRecord("k", "v")], {
        compression: CompressionCodec.SNAPPY // Not registered
      })

      expect(() => encodeRecordBatch(batch)).toThrow("compression codec")
    })

    it("rejects decoding with unregistered compression codec", () => {
      // Encode without compression, then change attributes to indicate an unregistered codec
      const batch = buildRecordBatch([createTestRecord("k", "v")], {
        compression: CompressionCodec.NONE
      })
      const encoded = encodeRecordBatch(batch)

      // Attributes are at offset: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) +
      // magic(1) + crc(4) = 21
      const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength)
      const currentAttrs = view.getInt16(21)
      // Change compression from none (0) to lz4 (3): clear bits 0-2, set to 3
      const newAttrs = (currentAttrs & ~0x07) | CompressionCodec.LZ4
      view.setInt16(21, newAttrs)

      const result = decodeRecordBatch(encoded, false) // Skip CRC since we modified data
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("INVALID_DATA")
        expect(result.error.message).toContain("compression codec")
      }
    })

    // Test truncation at various points in batch metadata
    // Batch structure: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) + crc(4) + metadata...
    it.each([
      { name: "truncated at partitionLeaderEpoch", size: 13 },
      { name: "truncated at magic", size: 16 },
      { name: "truncated at crc", size: 18 },
      { name: "truncated at attributes", size: 22 },
      { name: "truncated at lastOffsetDelta", size: 25 },
      { name: "truncated at baseTimestamp", size: 30 },
      { name: "truncated at maxTimestamp", size: 40 },
      { name: "truncated at producerId", size: 50 },
      { name: "truncated at producerEpoch", size: 54 },
      { name: "truncated at baseSequence", size: 56 },
      { name: "truncated at recordsCount", size: 58 }
    ])("rejects batch $name", ({ size }) => {
      const batch = buildRecordBatch([createTestRecord("k", "v")])
      const encoded = encodeRecordBatch(batch)

      // Set batchLength to claim more data than we'll provide
      const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength)
      view.setInt32(8, encoded.length - 12) // Original valid batchLength

      // Truncate the buffer
      const truncated = encoded.slice(0, size)

      const result = decodeRecordBatch(truncated, false)
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.code).toBe("BUFFER_UNDERFLOW")
      }
    })
  })
})

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

describe("compression", () => {
  beforeAll(() => {
    registerCompressionProvider(CompressionCodec.GZIP, gzipProvider)
  })

  it("has gzip provider registered", () => {
    expect(hasCompressionProvider(CompressionCodec.GZIP)).toBe(true)
  })

  it("NONE compression requires no provider", () => {
    expect(hasCompressionProvider(CompressionCodec.NONE)).toBe(true)
  })

  it("round trips batch with gzip compression", () => {
    const records = [
      createTestRecord("key1", "value1", 0, 0n),
      createTestRecord("key2", "a longer value to benefit from compression", 1, 10n),
      createTestRecord("key3", "another value with some content", 2, 20n)
    ]
    const batch = buildRecordBatch(records, {
      compression: CompressionCodec.GZIP
    })
    const decoded = recordBatchRoundTrip(batch)

    expect(decoded.attributes.compression).toBe(CompressionCodec.GZIP)
    expect(decoded.records.length).toBe(3)
    expect(decoded.records[0].value).toEqual(new TextEncoder().encode("value1"))
  })

  it("compressed batch is smaller for repetitive data", () => {
    // Create a batch with repetitive data that should compress well
    const largeValue = "x".repeat(1000)
    const records = Array.from({ length: 10 }, (_, i) =>
      createTestRecord(`key${String(i)}`, largeValue, i, BigInt(i * 10))
    )

    const uncompressed = encodeRecordBatch(
      buildRecordBatch(records, { compression: CompressionCodec.NONE })
    )
    const compressed = encodeRecordBatch(
      buildRecordBatch(records, { compression: CompressionCodec.GZIP })
    )

    expect(compressed.length).toBeLessThan(uncompressed.length)
  })

  it("fails on corrupt compressed data", () => {
    // Create a valid compressed batch
    const records = [createTestRecord("key", "value")]
    const batch = buildRecordBatch(records, { compression: CompressionCodec.GZIP })
    const encoded = encodeRecordBatch(batch)

    // Corrupt the compressed records data (after batch metadata at offset 61)
    // Fill with invalid gzip data
    for (let i = 61; i < encoded.length; i++) {
      encoded[i] = 0xff // Invalid compressed data
    }

    // Decode with CRC validation disabled to reach decompression
    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("INVALID_DATA")
      expect(result.error.message).toContain("decompression failed")
    }
  })
})

// ---------------------------------------------------------------------------
// Property-based tests
// ---------------------------------------------------------------------------

describe("property-based: RecordBatch", () => {
  it("round trips arbitrary records", () => {
    fc.assert(
      fc.property(
        fc.array(
          fc.record({
            key: fc.option(fc.uint8Array({ minLength: 0, maxLength: 100 }), { nil: null }),
            value: fc.option(fc.uint8Array({ minLength: 0, maxLength: 100 }), { nil: null }),
            offsetDelta: fc.nat(1000),
            timestampDelta: fc.bigInt({ min: 0n, max: 10000n })
          }),
          { minLength: 0, maxLength: 10 }
        ),
        (records) => {
          const batch = buildRecordBatch(
            records.map((r, i) =>
              createRecord(r.key ?? null, r.value ?? null, [], r.offsetDelta + i, r.timestampDelta)
            )
          )
          const decoded = recordBatchRoundTrip(batch)

          expect(decoded.records.length).toBe(records.length)
        }
      ),
      { numRuns: 50 }
    )
  })

  it("CRC detects any single-bit corruption", () => {
    fc.assert(
      fc.property(fc.nat(999), (seed) => {
        const record = createTestRecord(`key-${String(seed)}`, `value-${String(seed)}`)
        const batch = buildRecordBatch([record])
        const encoded = encodeRecordBatch(batch)

        // Flip a random bit in the CRC-protected region
        // CRC-protected region starts at offset 21 (after baseOffset + batchLength + partitionLeaderEpoch + magic + crc)
        if (encoded.length > 25) {
          const byteIndex = 21 + (seed % (encoded.length - 21))
          const bitIndex = seed % 8
          encoded[byteIndex] ^= 1 << bitIndex

          const result = decodeRecordBatch(encoded, true)
          expect(result.ok).toBe(false)
        }
      }),
      { numRuns: 100 }
    )
  })
})

// ---------------------------------------------------------------------------
// Record-Level Error Paths (with CRC validation disabled)
// ---------------------------------------------------------------------------

describe("decodeRecordBatch record-level errors (validateCrc=false)", () => {
  /**
   * Helper to create a batch with corrupted record data.
   * Encodes a valid batch then corrupts bytes in the records section.
   */
  function createCorruptedBatch(corruptFn: (encoded: Uint8Array) => void): Uint8Array {
    const record = createTestRecord("key", "value")
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)
    corruptFn(encoded)
    return encoded
  }

  // Batch structure: [8:baseOffset][4:batchLength][4:partitionLeaderEpoch][1:magic][4:crc]
  //                  [2:attributes][4:lastOffsetDelta][8:baseTimestamp][8:maxTimestamp]
  //                  [8:producerId][2:producerEpoch][4:baseSequence][4:recordsCount]
  //                  [records...]
  // Total header overhead = 8+4+4+1+4+2+4+8+8+8+2+4+4 = 61 bytes
  const recordsStart = 61

  it("fails on invalid record length (negative)", () => {
    const encoded = createCorruptedBatch((data) => {
      // Record length is a signed varint at recordsStart
      // Set to -1 (0xFF 0xFF 0xFF 0xFF 0x0F in varint encoding, but simpler: 0x01 = -1 in zigzag)
      // zigzag encode of -1 = 1, but varint of 1 is 0x01
      // Actually zigzag -1 = ((-1) << 1) ^ ((-1) >> 31) = (-2) ^ (-1) = 1
      // So varint encoding of 1 is 0x01
      // For a negative value like -10, zigzag = 19, varint = 0x13
      data[recordsStart] = 0x13 // -10 in zigzag varint
    })
    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("INVALID_DATA")
      expect(result.error.message).toContain("invalid record length")
    }
  })

  it("fails on truncated record (not enough bytes)", () => {
    const encoded = createCorruptedBatch((data) => {
      // Set record length to a huge value that exceeds remaining bytes
      // varint 1000 = 0xE8 0x07
      data[recordsStart] = 0xe8
      data[recordsStart + 1] = 0x07
    })
    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("BUFFER_UNDERFLOW")
      expect(result.error.message).toContain("bytes for record")
    }
  })

  it("fails on invalid headers count (negative)", () => {
    // Create a simple record with no headers
    const record = createTestRecord(null, null)
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)

    // In a record with null key and null value:
    // [recordLen][attrs=1][timestampDelta=1][offsetDelta=1][keyLen=-1=1][valueLen=-1=1][headersCount=0=0]
    // Total record: 1+1+1+1+1+1+1 = 7 bytes-ish
    // headersCount is the last field, find it and make it negative
    // headersCount=0 is zigzag 0 = 0x00
    // Change to headersCount=-1 which is zigzag 1 = 0x01 (but that's not invalid...)
    // Actually headersCount<0 should fail. zigzag of -1 = 1, -2 = 3, -3 = 5
    // Find the 0x00 (headersCount=0) and change to 0x05 (headersCount=-3)

    // Records start at offset 61, find the headersCount byte
    // For minimal record: recordLen(~6) + payload = we need to find the last varint
    const recordsEnd = encoded.length
    // The last byte before padding should be headersCount
    // In encoded form, find 0x00 near end and corrupt it
    for (let i = recordsEnd - 1; i >= recordsStart; i--) {
      if (encoded[i] === 0x00) {
        encoded[i] = 0x05 // -3 in zigzag
        break
      }
    }

    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("INVALID_DATA")
    }
  })

  it("fails when record key read truncated", () => {
    const encoded = createCorruptedBatch((data) => {
      // Set a valid record length but corrupt the key length to be huge
      // After record length (varint) and attributes (1 byte), timestampDelta, offsetDelta comes keyLen
      // Record starts at recordsStart, length is varint, then attributes (1), timestampDelta (varlong), offsetDelta (varint), keyLen (varint)
      // Corrupt keyLen to indicate huge key that doesn't exist

      // Skip record length (assume 1 byte varint) + attributes (1) = offset +2
      // Then timestampDelta and offsetDelta are varints, typically 1 byte each = +2 more
      // keyLen is at approximately recordsStart + 4
      const keyLenOffset = recordsStart + 4
      data[keyLenOffset] = 0xfe // Large positive varint (126 in zigzag = 63)
      data[keyLenOffset + 1] = 0x01 // Continuation for even larger value
    })
    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
  })

  it("fails when record value read truncated", () => {
    // Create a batch then corrupt the value length
    const record = createTestRecord("k", "v")
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)

    // Corrupt the value length to be very large
    // This is at a variable offset, so we use a heuristic
    // After key "k" (length 1 + 1 byte data), value length comes next
    const valueLenOffset = recordsStart + 6 // Approximate
    encoded[valueLenOffset] = 0xfe
    encoded[valueLenOffset + 1] = 0x7f // Large varint

    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
  })

  it("fails on corrupted header key length", () => {
    const record = createRecord(null, null, [
      { key: "header", value: new TextEncoder().encode("val") }
    ])
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)

    // Corrupt header key length to be huge
    // Headers are at the end of the record
    const headerAreaStart = recordsStart + 5 // After minimal record fields
    for (let i = headerAreaStart; i < encoded.length - 2; i++) {
      if (encoded[i] === 0x0c) {
        // Length 6 zigzag = 12
        encoded[i] = 0xfe // Large length
        encoded[i + 1] = 0x7f
        break
      }
    }

    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
  })

  it("fails on header key null (which is invalid)", () => {
    const record = createRecord(null, null, [{ key: "h", value: null }])
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)

    // Find and replace the header key length with -1 (zigzag 1)
    // Header key length of -1 should cause "header key cannot be null" error
    // We need to find where the header starts and corrupt keyLen there

    // The header is near the end, after headersCount (which is 1 = zigzag 2)
    // Find 0x02 (headersCount=1) and corrupt the next byte
    for (let i = recordsStart; i < encoded.length - 1; i++) {
      if (encoded[i] === 0x02 && encoded[i + 1] === 0x02) {
        // Found headersCount=1, headerKeyLen=1 (for "h")
        encoded[i + 1] = 0x01 // Change to -1 (zigzag of -1 = 1)
        break
      }
    }

    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("INVALID_DATA")
    }
  })

  it("fails on truncated header value", () => {
    const record = createRecord(null, null, [
      { key: "x", value: new TextEncoder().encode("longvalue") }
    ])
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)

    // Find the header value length (which is 9 for "longvalue")
    // zigzag of 9 = 18 = 0x12
    // Change it to a huge value
    for (let i = recordsStart; i < encoded.length - 1; i++) {
      if (encoded[i] === 0x12) {
        // Found value length 9
        encoded[i] = 0xfe // Large varint (127)
        break
      }
    }

    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
  })

  it("fails when record timestamp delta read fails", () => {
    // Create a batch where timestampDelta is an invalid varlong (too many bytes)
    const record = createTestRecord("k", "v")
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)

    const recordStart = recordsStart

    // Set recordLen = 12 (attrs + 10 bytes of invalid varlong that never terminates properly)
    encoded[recordStart] = 0x18 // zigzag(12) = 24
    encoded[recordStart + 1] = 0x00 // attrs = 0
    // Fill timestampDelta with 10+ continuation bytes to trigger "exceeds 10 bytes" error
    for (let i = 0; i < 10; i++) {
      encoded[recordStart + 2 + i] = 0x80
    }

    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
  })

  it("fails when record offset delta read fails", () => {
    const record = createTestRecord("k", "v")
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)

    const recordStart = recordsStart

    // recordLen = 13: attrs(1) + timestampDelta(1) + 10 invalid offset bytes
    encoded[recordStart] = 0x1a // zigzag(13) = 26
    encoded[recordStart + 1] = 0x00 // attrs = 0
    encoded[recordStart + 2] = 0x00 // timestampDelta = 0 (complete in 1 byte)
    // Fill offsetDelta with 10+ continuation bytes
    for (let i = 0; i < 10; i++) {
      encoded[recordStart + 3 + i] = 0x80
    }

    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
  })

  it("fails when key length varint read fails", () => {
    const record = createTestRecord("k", "v")
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)

    // Set recordLen to 4, space for attrs + timestamps + incomplete keyLen
    const recordStart = recordsStart
    encoded[recordStart] = 0x08 // zigzag(4) = 8
    encoded[recordStart + 2] = 0x00 // timestampDelta = 0
    encoded[recordStart + 3] = 0x00 // offsetDelta = 0
    encoded[recordStart + 4] = 0x80 // Incomplete keyLen varint

    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
  })

  it("fails when value length varint read fails", () => {
    const record = createTestRecord(null, "v")
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)

    // recordLen = 5: attrs(1) + timestamps(2) + keyLen(-1)(1) + incomplete valueLen
    const recordStart = recordsStart
    encoded[recordStart] = 0x0a // zigzag(5) = 10
    encoded[recordStart + 2] = 0x00 // timestampDelta = 0
    encoded[recordStart + 3] = 0x00 // offsetDelta = 0
    encoded[recordStart + 4] = 0x01 // keyLen = -1 (null) zigzag
    encoded[recordStart + 5] = 0x80 // Incomplete valueLen varint

    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
  })

  it("fails when headers count varint read fails", () => {
    const record = createTestRecord(null, null)
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)

    const recordStart = recordsStart

    // Record fields: attrs(1) + timestampDelta + offsetDelta + keyLen + valueLen + headersCount
    // Minimal: 1 + 1 + 1 + 1 + 1 + 1 = 6 bytes for null key/value, 0 headers
    // Set recordLen = 6 but make headersCount an incomplete varint

    encoded[recordStart] = 0x0c // zigzag(6) = 12
    encoded[recordStart + 1] = 0x00 // attrs = 0
    encoded[recordStart + 2] = 0x00 // timestampDelta = 0
    encoded[recordStart + 3] = 0x00 // offsetDelta = 0
    encoded[recordStart + 4] = 0x01 // keyLen = -1 (null)
    encoded[recordStart + 5] = 0x01 // valueLen = -1 (null)
    encoded[recordStart + 6] = 0x80 // headersCount incomplete varint

    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
  })

  it("fails when header key bytes read fails", () => {
    // Create record with header that has key "abc"
    const record = createRecord(null, null, [{ key: "abc", value: null }])
    const batch = buildRecordBatch([record])
    const encoded = encodeRecordBatch(batch)

    // Find keyLen=3 (zigzag 6) and corrupt the following bytes
    for (let i = recordsStart; i < encoded.length - 3; i++) {
      if (encoded[i] === 0x02 && encoded[i + 1] === 0x06) {
        // headersCount=1, keyLen=3 for "abc"
        // Change keyLen to huge value so bytes read fails
        encoded[i + 1] = 0xfe
        encoded[i + 2] = 0xff
        encoded[i + 3] = 0x01 // Very large length
        break
      }
    }

    const result = decodeRecordBatch(encoded, false)
    expect(result.ok).toBe(false)
  })
})
