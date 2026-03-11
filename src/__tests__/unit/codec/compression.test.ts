import { describe, expect, it } from "vitest"

import {
  createLz4Provider,
  createSnappyProvider,
  createZstdProvider,
  deflateProvider,
  gzipProvider,
  type Lz4Codec,
  type SnappyCodec,
  type ZstdCodec
} from "../../../codec/compression"

// ---------------------------------------------------------------------------
// gzipProvider
// ---------------------------------------------------------------------------

describe("gzipProvider", () => {
  it("round trips empty data", () => {
    const data = new Uint8Array(0)
    const compressed = gzipProvider.compress(data)
    const decompressed = gzipProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("round trips small data", () => {
    const data = new TextEncoder().encode("hello world")
    const compressed = gzipProvider.compress(data)
    const decompressed = gzipProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("round trips large repetitive data", () => {
    const data = new TextEncoder().encode("x".repeat(10000))
    const compressed = gzipProvider.compress(data)
    const decompressed = gzipProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("compresses repetitive data to smaller size", () => {
    const data = new TextEncoder().encode("abcdefgh".repeat(1000))
    const compressed = gzipProvider.compress(data)
    expect(compressed.length).toBeLessThan(data.length)
  })

  it("returns Uint8Array from compress", () => {
    const data = new Uint8Array([1, 2, 3])
    const compressed = gzipProvider.compress(data)
    expect(compressed).toBeInstanceOf(Uint8Array)
  })

  it("returns Uint8Array from decompress", () => {
    const data = new Uint8Array([1, 2, 3])
    const compressed = gzipProvider.compress(data)
    const decompressed = gzipProvider.decompress(compressed)
    expect(decompressed).toBeInstanceOf(Uint8Array)
  })

  it("handles binary data with all byte values", () => {
    const data = new Uint8Array(256)
    for (let i = 0; i < 256; i++) {
      data[i] = i
    }
    const compressed = gzipProvider.compress(data)
    const decompressed = gzipProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })
})

// ---------------------------------------------------------------------------
// deflateProvider
// ---------------------------------------------------------------------------

describe("deflateProvider", () => {
  it("round trips empty data", () => {
    const data = new Uint8Array(0)
    const compressed = deflateProvider.compress(data)
    const decompressed = deflateProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("round trips small data", () => {
    const data = new TextEncoder().encode("hello world")
    const compressed = deflateProvider.compress(data)
    const decompressed = deflateProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("round trips large repetitive data", () => {
    const data = new TextEncoder().encode("y".repeat(10000))
    const compressed = deflateProvider.compress(data)
    const decompressed = deflateProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("compresses repetitive data to smaller size", () => {
    const data = new TextEncoder().encode("abcdefgh".repeat(1000))
    const compressed = deflateProvider.compress(data)
    expect(compressed.length).toBeLessThan(data.length)
  })

  it("returns Uint8Array from compress", () => {
    const data = new Uint8Array([1, 2, 3])
    const compressed = deflateProvider.compress(data)
    expect(compressed).toBeInstanceOf(Uint8Array)
  })

  it("returns Uint8Array from decompress", () => {
    const data = new Uint8Array([1, 2, 3])
    const compressed = deflateProvider.compress(data)
    const decompressed = deflateProvider.decompress(compressed)
    expect(decompressed).toBeInstanceOf(Uint8Array)
  })

  it("handles binary data with all byte values", () => {
    const data = new Uint8Array(256)
    for (let i = 0; i < 256; i++) {
      data[i] = i
    }
    const compressed = deflateProvider.compress(data)
    const decompressed = deflateProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("produces different output than gzip", () => {
    const data = new TextEncoder().encode("test data for comparison")
    const gzipCompressed = gzipProvider.compress(data)
    const deflateCompressed = deflateProvider.compress(data)
    // Raw deflate should be smaller than gzip (no gzip header/trailer)
    expect(deflateCompressed.length).toBeLessThan(gzipCompressed.length)
  })
})

// ---------------------------------------------------------------------------
// Mock Snappy Codec (for testing Xerial framing)
// ---------------------------------------------------------------------------

/**
 * Simple mock snappy codec that uses a basic RLE-like encoding.
 * This is NOT real snappy - it's just for testing the Xerial framing logic.
 */
const mockSnappyCodec: SnappyCodec = {
  compressSync(data: Uint8Array): Uint8Array {
    // Simple prefix encoding: [length (4 bytes)] + data
    const result = new Uint8Array(4 + data.length)
    result[0] = (data.length >>> 24) & 0xff
    result[1] = (data.length >>> 16) & 0xff
    result[2] = (data.length >>> 8) & 0xff
    result[3] = data.length & 0xff
    result.set(data, 4)
    return result
  },
  decompressSync(data: Uint8Array): Uint8Array {
    // Read length from first 4 bytes
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
    const length = view.getUint32(0, false)
    return data.subarray(4, 4 + length)
  }
}

// ---------------------------------------------------------------------------
// createSnappyProvider (Xerial framing)
// ---------------------------------------------------------------------------

describe("createSnappyProvider", () => {
  const snappyProvider = createSnappyProvider(mockSnappyCodec)

  it("round trips empty data", () => {
    const data = new Uint8Array(0)
    const compressed = snappyProvider.compress(data)
    const decompressed = snappyProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("round trips small data", () => {
    const data = new TextEncoder().encode("hello world")
    const compressed = snappyProvider.compress(data)
    const decompressed = snappyProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("round trips large data with multiple chunks", () => {
    // Create data larger than 32KB to trigger multiple chunks
    const data = new Uint8Array(50000)
    for (let i = 0; i < data.length; i++) {
      data[i] = i % 256
    }
    const compressed = snappyProvider.compress(data)
    const decompressed = snappyProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("produces output with Xerial magic header", () => {
    const data = new TextEncoder().encode("test")
    const compressed = snappyProvider.compress(data)
    // Xerial magic: 0x82 + "SNAPPY" + 0x00
    expect(compressed[0]).toBe(0x82)
    expect(compressed[1]).toBe(0x53) // 'S'
    expect(compressed[2]).toBe(0x4e) // 'N'
    expect(compressed[3]).toBe(0x41) // 'A'
    expect(compressed[4]).toBe(0x50) // 'P'
    expect(compressed[5]).toBe(0x50) // 'P'
    expect(compressed[6]).toBe(0x59) // 'Y'
    expect(compressed[7]).toBe(0x00)
  })

  it("includes version in header", () => {
    const data = new TextEncoder().encode("test")
    const compressed = snappyProvider.compress(data)
    // Version at offset 8-11 (big-endian)
    const view = new DataView(compressed.buffer, compressed.byteOffset, compressed.byteLength)
    const version = view.getUint32(8, false)
    expect(version).toBe(1)
  })

  it("returns Uint8Array from compress", () => {
    const data = new Uint8Array([1, 2, 3])
    const compressed = snappyProvider.compress(data)
    expect(compressed).toBeInstanceOf(Uint8Array)
  })

  it("returns Uint8Array from decompress", () => {
    const data = new Uint8Array([1, 2, 3])
    const compressed = snappyProvider.compress(data)
    const decompressed = snappyProvider.decompress(compressed)
    expect(decompressed).toBeInstanceOf(Uint8Array)
  })

  it("handles raw snappy data without Xerial framing", () => {
    // When data doesn't have Xerial magic, should pass through to underlying codec
    const rawData = new TextEncoder().encode("hello")
    const rawCompressed = mockSnappyCodec.compressSync(rawData)
    const decompressed = snappyProvider.decompress(rawCompressed)
    expect(decompressed).toEqual(rawData)
  })

  it("throws on truncated Xerial header", () => {
    const truncated = new Uint8Array(10) // Less than 16 byte header but has magic
    truncated.set([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00], 0)
    // Has Xerial magic but truncated header - should throw
    expect(() => snappyProvider.decompress(truncated)).toThrow(
      "invalid xerial snappy data: too short for header"
    )
  })
})

// ---------------------------------------------------------------------------
// Mock LZ4 Codec
// ---------------------------------------------------------------------------

const mockLz4Codec: Lz4Codec = {
  compressSync(data: Uint8Array): Uint8Array {
    // Simulate LZ4 frame format: magic + data
    const magic = new Uint8Array([0x04, 0x22, 0x4d, 0x18])
    const result = new Uint8Array(4 + data.length)
    result.set(magic, 0)
    result.set(data, 4)
    return result
  },
  decompressSync(data: Uint8Array): Uint8Array {
    // Skip LZ4 magic
    if (data[0] === 0x04 && data[1] === 0x22 && data[2] === 0x4d && data[3] === 0x18) {
      return data.subarray(4)
    }
    return data
  }
}

// ---------------------------------------------------------------------------
// createLz4Provider
// ---------------------------------------------------------------------------

describe("createLz4Provider", () => {
  const lz4Provider = createLz4Provider(mockLz4Codec)

  it("round trips empty data", () => {
    const data = new Uint8Array(0)
    const compressed = lz4Provider.compress(data)
    const decompressed = lz4Provider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("round trips small data", () => {
    const data = new TextEncoder().encode("hello world")
    const compressed = lz4Provider.compress(data)
    const decompressed = lz4Provider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("round trips binary data", () => {
    const data = new Uint8Array(256)
    for (let i = 0; i < 256; i++) {
      data[i] = i
    }
    const compressed = lz4Provider.compress(data)
    const decompressed = lz4Provider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("produces output with LZ4 frame magic", () => {
    const data = new TextEncoder().encode("test")
    const compressed = lz4Provider.compress(data)
    // LZ4 frame magic: 0x04 0x22 0x4D 0x18 (little-endian 0x184D2204)
    expect(compressed[0]).toBe(0x04)
    expect(compressed[1]).toBe(0x22)
    expect(compressed[2]).toBe(0x4d)
    expect(compressed[3]).toBe(0x18)
  })

  it("returns Uint8Array from compress", () => {
    const data = new Uint8Array([1, 2, 3])
    const compressed = lz4Provider.compress(data)
    expect(compressed).toBeInstanceOf(Uint8Array)
  })

  it("returns Uint8Array from decompress", () => {
    const data = new Uint8Array([1, 2, 3])
    const compressed = lz4Provider.compress(data)
    const decompressed = lz4Provider.decompress(compressed)
    expect(decompressed).toBeInstanceOf(Uint8Array)
  })
})

// ---------------------------------------------------------------------------
// Mock ZSTD Codec
// ---------------------------------------------------------------------------

const mockZstdCodec: ZstdCodec = {
  compressSync(data: Uint8Array): Uint8Array {
    // Simulate ZSTD format: magic + data
    const magic = new Uint8Array([0x28, 0xb5, 0x2f, 0xfd])
    const result = new Uint8Array(4 + data.length)
    result.set(magic, 0)
    result.set(data, 4)
    return result
  },
  decompressSync(data: Uint8Array): Uint8Array {
    // Skip ZSTD magic
    if (data[0] === 0x28 && data[1] === 0xb5 && data[2] === 0x2f && data[3] === 0xfd) {
      return data.subarray(4)
    }
    return data
  }
}

// ---------------------------------------------------------------------------
// createZstdProvider
// ---------------------------------------------------------------------------

describe("createZstdProvider", () => {
  const zstdProvider = createZstdProvider(mockZstdCodec)

  it("round trips empty data", () => {
    const data = new Uint8Array(0)
    const compressed = zstdProvider.compress(data)
    const decompressed = zstdProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("round trips small data", () => {
    const data = new TextEncoder().encode("hello world")
    const compressed = zstdProvider.compress(data)
    const decompressed = zstdProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("round trips binary data", () => {
    const data = new Uint8Array(256)
    for (let i = 0; i < 256; i++) {
      data[i] = i
    }
    const compressed = zstdProvider.compress(data)
    const decompressed = zstdProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })

  it("produces output with ZSTD magic", () => {
    const data = new TextEncoder().encode("test")
    const compressed = zstdProvider.compress(data)
    // ZSTD magic: 0xFD2FB528 (little-endian: 0x28 0xB5 0x2F 0xFD)
    expect(compressed[0]).toBe(0x28)
    expect(compressed[1]).toBe(0xb5)
    expect(compressed[2]).toBe(0x2f)
    expect(compressed[3]).toBe(0xfd)
  })

  it("returns Uint8Array from compress", () => {
    const data = new Uint8Array([1, 2, 3])
    const compressed = zstdProvider.compress(data)
    expect(compressed).toBeInstanceOf(Uint8Array)
  })

  it("returns Uint8Array from decompress", () => {
    const data = new Uint8Array([1, 2, 3])
    const compressed = zstdProvider.compress(data)
    const decompressed = zstdProvider.decompress(compressed)
    expect(decompressed).toBeInstanceOf(Uint8Array)
  })

  it("handles large data", () => {
    const data = new Uint8Array(100000)
    for (let i = 0; i < data.length; i++) {
      data[i] = Math.floor(Math.random() * 256)
    }
    const compressed = zstdProvider.compress(data)
    const decompressed = zstdProvider.decompress(compressed)
    expect(decompressed).toEqual(data)
  })
})
