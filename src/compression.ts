/**
 * Compression providers for Kafka record batches.
 *
 * This module provides compression implementations that can be registered
 * with the record batch codec.
 *
 * @packageDocumentation
 */

import { deflateSync, gunzipSync, gzipSync, inflateSync } from "node:zlib"

import type { CompressionProvider } from "./record-batch.js"

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
