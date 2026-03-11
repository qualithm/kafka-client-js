/**
 * Testing utilities for Kafka Client.
 *
 * This subpath export (`@qualithm/kafka-client/testing`) provides utilities
 * for testing code that integrates with the Kafka client library.
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
