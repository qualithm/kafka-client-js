/**
 * Compression providers for Kafka record batches.
 *
 * This module provides compression implementations that can be registered
 * with the record batch codec.
 *
 * Kafka uses specific compression formats:
 * - GZIP: Standard gzip format (RFC 1952)
 * - Snappy: Xerial snappy framing (Java library format with magic header)
 * - LZ4: LZ4 frame format (not raw blocks)
 * - ZSTD: Standard zstd format
 *
 * @packageDocumentation
 */

import { deflateSync, gunzipSync, gzipSync, inflateSync } from "node:zlib"

import type { CompressionProvider } from "./record-batch.js"

// ---------------------------------------------------------------------------
// Gzip Provider
// ---------------------------------------------------------------------------

/**
 * Gzip compression provider.
 *
 * Uses Node.js zlib for compression/decompression.
 * Compatible with Bun and Node.js runtimes.
 */
export const gzipProvider: CompressionProvider = {
  compress(data: Uint8Array): Uint8Array {
    return new Uint8Array(gzipSync(data))
  },
  decompress(data: Uint8Array): Uint8Array {
    return new Uint8Array(gunzipSync(data))
  }
}

/**
 * Raw deflate compression provider (used internally by some codecs).
 */
export const deflateProvider: CompressionProvider = {
  compress(data: Uint8Array): Uint8Array {
    return new Uint8Array(deflateSync(data))
  },
  decompress(data: Uint8Array): Uint8Array {
    return new Uint8Array(inflateSync(data))
  }
}

// ---------------------------------------------------------------------------
// Snappy Constants (Xerial Framing)
// ---------------------------------------------------------------------------

/**
 * Xerial snappy framing magic header.
 * Format: 0x82 + "SNAPPY" + 0x00 (8 bytes)
 */
const XERIAL_MAGIC = new Uint8Array([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00])

/** Xerial snappy version identifier (network byte order). */
const XERIAL_VERSION = 1

/** Xerial snappy minimum compatible version. */
const XERIAL_MIN_VERSION = 1

/** Xerial framing header size: magic(8) + version(4) + minVersion(4). */
const XERIAL_HEADER_SIZE = 16

/** Maximum chunk size for Xerial snappy framing. */
const XERIAL_MAX_CHUNK_SIZE = 32 * 1024 // 32KB

// ---------------------------------------------------------------------------
// Snappy Provider
// ---------------------------------------------------------------------------

/**
 * Snappy compression codec interface.
 *
 * This interface allows plugging in different snappy implementations
 * (pure JS, native bindings, etc.).
 */
export type SnappyCodec = {
  /** Compress raw data using snappy algorithm. */
  compressSync: (data: Uint8Array) => Uint8Array
  /** Decompress snappy-compressed data. */
  decompressSync: (data: Uint8Array) => Uint8Array
}

/**
 * Creates a Kafka-compatible snappy compression provider.
 *
 * Kafka uses Xerial snappy framing, which wraps snappy-compressed blocks
 * with a magic header and length-prefixed chunks. This is the format used
 * by the Java Kafka client (org.xerial.snappy).
 *
 * @param codec - Snappy codec implementation (e.g., from 'snappy' or 'snappyjs' package)
 * @returns Compression provider for use with registerCompressionProvider
 *
 * @example
 * ```ts
 * import snappy from 'snappy'
 * import { createSnappyProvider, registerCompressionProvider, CompressionCodec } from '@qualithm/kafka-client'
 *
 * const snappyProvider = createSnappyProvider({
 *   compressSync: snappy.compressSync,
 *   decompressSync: snappy.uncompressSync
 * })
 * registerCompressionProvider(CompressionCodec.SNAPPY, snappyProvider)
 * ```
 */
export function createSnappyProvider(codec: SnappyCodec): CompressionProvider {
  return {
    compress(data: Uint8Array): Uint8Array {
      return xerialEncode(data, codec)
    },
    decompress(data: Uint8Array): Uint8Array {
      return xerialDecode(data, codec)
    }
  }
}

/**
 * Encodes data using Xerial snappy framing.
 *
 * Format:
 * - Magic header (8 bytes): 0x82 + "SNAPPY" + 0x00
 * - Version (4 bytes, big-endian)
 * - Minimum compatible version (4 bytes, big-endian)
 * - Chunks: [length (4 bytes, big-endian) + snappy-compressed data]...
 */
function xerialEncode(data: Uint8Array, codec: SnappyCodec): Uint8Array {
  if (data.length === 0) {
    // Return just the header for empty data
    const result = new Uint8Array(XERIAL_HEADER_SIZE)
    result.set(XERIAL_MAGIC, 0)
    writeUint32BE(result, 8, XERIAL_VERSION)
    writeUint32BE(result, 12, XERIAL_MIN_VERSION)
    return result
  }

  // Compress in chunks
  const chunks: Uint8Array[] = []
  let totalSize = XERIAL_HEADER_SIZE

  for (let offset = 0; offset < data.length; offset += XERIAL_MAX_CHUNK_SIZE) {
    const end = Math.min(offset + XERIAL_MAX_CHUNK_SIZE, data.length)
    const chunk = data.subarray(offset, end)
    const compressed = codec.compressSync(chunk)
    chunks.push(compressed)
    totalSize += 4 + compressed.length // 4 bytes for length prefix
  }

  // Build output buffer
  const result = new Uint8Array(totalSize)
  result.set(XERIAL_MAGIC, 0)
  writeUint32BE(result, 8, XERIAL_VERSION)
  writeUint32BE(result, 12, XERIAL_MIN_VERSION)

  let writeOffset = XERIAL_HEADER_SIZE
  for (const chunk of chunks) {
    writeUint32BE(result, writeOffset, chunk.length)
    writeOffset += 4
    result.set(chunk, writeOffset)
    writeOffset += chunk.length
  }

  return result
}

/**
 * Decodes data using Xerial snappy framing.
 */
function xerialDecode(data: Uint8Array, codec: SnappyCodec): Uint8Array {
  // Check magic header first - if not Xerial, treat as raw snappy
  if (!isXerialMagic(data)) {
    // Not Xerial framing - might be raw snappy (some older Kafka versions)
    return codec.decompressSync(data)
  }

  if (data.length < XERIAL_HEADER_SIZE) {
    throw new Error("invalid xerial snappy data: too short for header")
  }

  // Skip header, read chunks
  const chunks: Uint8Array[] = []
  let totalSize = 0
  let offset = XERIAL_HEADER_SIZE

  while (offset < data.length) {
    if (offset + 4 > data.length) {
      throw new Error("invalid xerial snappy data: truncated chunk length")
    }

    const chunkLength = readUint32BE(data, offset)
    offset += 4

    if (offset + chunkLength > data.length) {
      throw new Error("invalid xerial snappy data: truncated chunk data")
    }

    const compressedChunk = data.subarray(offset, offset + chunkLength)
    const decompressed = codec.decompressSync(compressedChunk)
    chunks.push(decompressed)
    totalSize += decompressed.length
    offset += chunkLength
  }

  // Concatenate chunks
  if (chunks.length === 0) {
    return new Uint8Array(0)
  }
  if (chunks.length === 1) {
    const [chunk] = chunks
    return chunk
  }

  const result = new Uint8Array(totalSize)
  let writeOffset = 0
  for (const chunk of chunks) {
    result.set(chunk, writeOffset)
    writeOffset += chunk.length
  }
  return result
}

/** Check if data starts with Xerial magic header. */
function isXerialMagic(data: Uint8Array): boolean {
  if (data.length < XERIAL_MAGIC.length) {
    return false
  }
  for (let i = 0; i < XERIAL_MAGIC.length; i++) {
    if (data[i] !== XERIAL_MAGIC[i]) {
      return false
    }
  }
  return true
}

// ---------------------------------------------------------------------------
// LZ4 Provider
// ---------------------------------------------------------------------------

/**
 * LZ4 compression codec interface.
 *
 * Kafka uses the LZ4 frame format (LZ4F), not raw block compression.
 * The codec must handle frame format encoding/decoding.
 */
export type Lz4Codec = {
  /** Compress data using LZ4 frame format. */
  compressSync: (data: Uint8Array) => Uint8Array
  /** Decompress LZ4 frame format data. */
  decompressSync: (data: Uint8Array) => Uint8Array
}

/** LZ4 frame format magic number (little-endian: 0x184D2204). */
const LZ4_FRAME_MAGIC = new Uint8Array([0x04, 0x22, 0x4d, 0x18])

/**
 * Creates a Kafka-compatible LZ4 compression provider.
 *
 * Kafka uses the LZ4 frame format (LZ4F), which includes magic number,
 * frame descriptor, and content checksum. This is different from raw
 * LZ4 block compression.
 *
 * @param codec - LZ4 codec implementation (e.g., from 'lz4' or 'lz4js' package)
 * @returns Compression provider for use with registerCompressionProvider
 *
 * @example
 * ```ts
 * import lz4 from 'lz4'
 * import { createLz4Provider, registerCompressionProvider, CompressionCodec } from '@qualithm/kafka-client'
 *
 * const lz4Provider = createLz4Provider({
 *   compressSync: (data) => lz4.encode(data),
 *   decompressSync: (data) => lz4.decode(data)
 * })
 * registerCompressionProvider(CompressionCodec.LZ4, lz4Provider)
 * ```
 */
export function createLz4Provider(codec: Lz4Codec): CompressionProvider {
  return {
    compress(data: Uint8Array): Uint8Array {
      // Older Kafka versions (< 0.10) used KafkaLZ4 framing with a custom header
      // Modern Kafka uses standard LZ4 frame format
      return codec.compressSync(data)
    },
    decompress(data: Uint8Array): Uint8Array {
      // Check for LZ4 frame magic
      if (data.length >= 4 && isLz4FrameMagic(data)) {
        return codec.decompressSync(data)
      }
      // Fallback: try decompressing anyway (might be raw or KafkaLZ4)
      return codec.decompressSync(data)
    }
  }
}

/** Check if data starts with LZ4 frame magic number. */
function isLz4FrameMagic(data: Uint8Array): boolean {
  return (
    data[0] === LZ4_FRAME_MAGIC[0] &&
    data[1] === LZ4_FRAME_MAGIC[1] &&
    data[2] === LZ4_FRAME_MAGIC[2] &&
    data[3] === LZ4_FRAME_MAGIC[3]
  )
}

// ---------------------------------------------------------------------------
// ZSTD Provider
// ---------------------------------------------------------------------------

/**
 * ZSTD compression codec interface.
 */
export type ZstdCodec = {
  /** Compress data using zstd. */
  compressSync: (data: Uint8Array) => Uint8Array
  /** Decompress zstd data. */
  decompressSync: (data: Uint8Array) => Uint8Array
}

/**
 * Creates a Kafka-compatible ZSTD compression provider.
 *
 * Kafka uses standard zstd compression format.
 *
 * @param codec - ZSTD codec implementation (e.g., from 'zstd-codec' or '@aspect/zstd' package)
 * @returns Compression provider for use with registerCompressionProvider
 *
 * @example
 * ```ts
 * import { ZstdCodec } from 'zstd-codec'
 * import { createZstdProvider, registerCompressionProvider, CompressionCodec } from '@qualithm/kafka-client'
 *
 * const zstd = await ZstdCodec.run()
 * const zstdProvider = createZstdProvider({
 *   compressSync: (data) => zstd.compress(data),
 *   decompressSync: (data) => zstd.decompress(data)
 * })
 * registerCompressionProvider(CompressionCodec.ZSTD, zstdProvider)
 * ```
 */
export function createZstdProvider(codec: ZstdCodec): CompressionProvider {
  return {
    compress(data: Uint8Array): Uint8Array {
      return codec.compressSync(data)
    },
    decompress(data: Uint8Array): Uint8Array {
      return codec.decompressSync(data)
    }
  }
}

// ---------------------------------------------------------------------------
// Utility Functions
// ---------------------------------------------------------------------------

/** Write uint32 in big-endian byte order. */
function writeUint32BE(buffer: Uint8Array, offset: number, value: number): void {
  buffer[offset] = (value >>> 24) & 0xff
  buffer[offset + 1] = (value >>> 16) & 0xff
  buffer[offset + 2] = (value >>> 8) & 0xff
  buffer[offset + 3] = value & 0xff
}

/** Read uint32 in big-endian byte order. */
function readUint32BE(buffer: Uint8Array, offset: number): number {
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  return view.getUint32(offset, false) // false = big-endian
}
