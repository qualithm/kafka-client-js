import { describe, expect, it } from "vitest"

import { deflateProvider, gzipProvider } from "../../compression"

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
