/**
 * Test helpers for code that uses Kafka Client, exported at
 * `@qualithm/kafka-client/testing`.
 *
 * @packageDocumentation
 */

// Re-export codec primitives useful for building test fixtures
export { BinaryReader } from "../codec/binary-reader.js"
export { BinaryWriter } from "../codec/binary-writer.js"
export type { DecodeResult } from "../result.js"
export { decodeFailure, decodeSuccess } from "../result.js"

// Re-export protocol framing for crafting raw requests/responses in tests
export { ApiKey } from "../codec/api-keys.js"
export {
  decodeResponseHeader,
  encodeRequestHeader,
  frameRequest,
  readResponseFrame
} from "../codec/protocol-framing.js"
