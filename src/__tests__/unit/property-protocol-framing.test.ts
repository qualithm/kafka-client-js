/**
 * Property-based tests for protocol framing.
 *
 * Uses fast-check to verify round-trip properties of request/response
 * header encoding/decoding and size-prefixed framing.
 *
 * @see https://kafka.apache.org/protocol.html#protocol_messages — Protocol framing
 */

import fc from "fast-check"
import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  decodeResponseHeader,
  encodeRequestHeader,
  frameRequest,
  readResponseFrame,
  type RequestHeader
} from "../../protocol-framing"

// ---------------------------------------------------------------------------
// Arbitraries
// ---------------------------------------------------------------------------

/** Arbitrary API key from the real enum values. */
const arbApiKey = fc.constantFrom(
  ApiKey.Produce,
  ApiKey.Fetch,
  ApiKey.ListOffsets,
  ApiKey.Metadata,
  ApiKey.OffsetCommit,
  ApiKey.OffsetFetch,
  ApiKey.FindCoordinator,
  ApiKey.JoinGroup,
  ApiKey.Heartbeat,
  ApiKey.LeaveGroup,
  ApiKey.SyncGroup,
  ApiKey.DescribeConfigs,
  ApiKey.AlterConfigs,
  ApiKey.CreateTopics,
  ApiKey.DeleteTopics,
  ApiKey.CreatePartitions,
  ApiKey.SaslHandshake,
  ApiKey.ApiVersions,
  ApiKey.InitProducerId,
  ApiKey.SaslAuthenticate
)

/** Arbitrary non-flexible (v0–v2) API version. */
const arbNonFlexVersion = fc.integer({ min: 0, max: 2 })

/** Arbitrary flexible (v3+) API version. */
const arbFlexVersion = fc.integer({ min: 3, max: 10 })

/** Arbitrary correlation ID (full int32 range). */
const arbCorrelationId = fc.integer({ min: 0, max: 2147483647 })

/** Arbitrary client ID — nullable short ASCII string. */
const arbClientId = fc.option(fc.string({ minLength: 0, maxLength: 50 }), {
  nil: null
})

/** Arbitrary tagged field. */
const arbTaggedField = fc.record({
  tag: fc.integer({ min: 0, max: 500 }),
  data: fc.uint8Array({ minLength: 0, maxLength: 30 })
})

/** Arbitrary tagged fields array. */
const arbTaggedFields = fc.array(arbTaggedField, { maxLength: 5 })

/** Arbitrary body bytes. */
const arbBody = fc.uint8Array({ minLength: 0, maxLength: 200 })

// ---------------------------------------------------------------------------
// Property: frameRequest produces a valid size-prefixed frame
// ---------------------------------------------------------------------------

describe("property-based: frameRequest", () => {
  /**
   * Kafka protocol spec §Protocol Framing:
   * "Requests and responses are both prefixed with a 4-byte big-endian
   * length field that gives the length of the remaining message bytes."
   *
   * @see https://kafka.apache.org/protocol.html#protocol_common
   */
  it("size prefix equals remaining bytes", () => {
    fc.assert(
      fc.property(arbCorrelationId, arbClientId, arbBody, (corrId, clientId, body) => {
        const header: RequestHeader = {
          apiKey: ApiKey.ApiVersions,
          apiVersion: 0,
          correlationId: corrId,
          clientId
        }

        const framed = frameRequest(header, (w) => w.writeRawBytes(body))
        const reader = new BinaryReader(framed)
        const sizeResult = reader.readInt32()

        expect(sizeResult.ok).toBe(true)
        if (sizeResult.ok) {
          expect(sizeResult.value).toBe(framed.byteLength - 4)
        }
      }),
      { numRuns: 200 }
    )
  })

  it("framed data is fully parseable via readResponseFrame", () => {
    fc.assert(
      fc.property(arbCorrelationId, arbBody, (corrId, body) => {
        // Build a response-like frame: size prefix + correlation ID + body
        const inner = new BinaryWriter()
        inner.writeInt32(corrId)
        inner.writeRawBytes(body)
        const payload = inner.finish()

        const frame = new BinaryWriter()
        frame.writeInt32(payload.byteLength)
        frame.writeRawBytes(payload)

        const reader = new BinaryReader(frame.finish())
        const result = readResponseFrame(reader)

        expect(result.ok).toBe(true)
        if (result.ok) {
          const innerReader = result.value
          const readCorrId = innerReader.readInt32()
          expect(readCorrId.ok).toBe(true)
          if (readCorrId.ok) {
            expect(readCorrId.value).toBe(corrId)
          }
          expect(innerReader.remaining).toBe(body.byteLength)
        }
      }),
      { numRuns: 200 }
    )
  })
})

// ---------------------------------------------------------------------------
// Property: request header v1 round-trips
// ---------------------------------------------------------------------------

describe("property-based: request header v1 (non-flexible)", () => {
  /**
   * @see https://kafka.apache.org/protocol.html#protocol_messages — Request Header v1
   */
  it("encodes then decodes to identical fields", () => {
    fc.assert(
      fc.property(
        arbApiKey,
        arbNonFlexVersion,
        arbCorrelationId,
        arbClientId,
        (apiKey, apiVersion, corrId, clientId) => {
          const writer = new BinaryWriter()
          const header: RequestHeader = { apiKey, apiVersion, correlationId: corrId, clientId }
          encodeRequestHeader(writer, header, 1)

          const reader = new BinaryReader(writer.finish())
          const readApiKey = reader.readInt16()
          const readVersion = reader.readInt16()
          const readCorrId = reader.readInt32()
          const readClientId = reader.readString()

          expect(readApiKey.ok && readApiKey.value).toBe(apiKey)
          expect(readVersion.ok && readVersion.value).toBe(apiVersion)
          expect(readCorrId.ok && readCorrId.value).toBe(corrId)
          expect(readClientId.ok && readClientId.value).toBe(clientId)
          expect(reader.remaining).toBe(0)
        }
      ),
      { numRuns: 200 }
    )
  })
})

// ---------------------------------------------------------------------------
// Property: request header v2 round-trips (flexible)
// ---------------------------------------------------------------------------

describe("property-based: request header v2 (flexible)", () => {
  /**
   * @see https://kafka.apache.org/protocol.html#protocol_messages — Request Header v2
   */
  it("encodes then decodes to identical fields with tagged fields", () => {
    fc.assert(
      fc.property(
        arbApiKey,
        arbFlexVersion,
        arbCorrelationId,
        arbClientId,
        arbTaggedFields,
        (apiKey, apiVersion, corrId, clientId, taggedFields) => {
          const writer = new BinaryWriter()
          const header: RequestHeader = {
            apiKey,
            apiVersion,
            correlationId: corrId,
            clientId,
            taggedFields
          }
          encodeRequestHeader(writer, header, 2)

          const reader = new BinaryReader(writer.finish())
          const readApiKey = reader.readInt16()
          const readVersion = reader.readInt16()
          const readCorrId = reader.readInt32()
          const readClientId = reader.readCompactString()
          const readTagged = reader.readTaggedFields()

          expect(readApiKey.ok && readApiKey.value).toBe(apiKey)
          expect(readVersion.ok && readVersion.value).toBe(apiVersion)
          expect(readCorrId.ok && readCorrId.value).toBe(corrId)
          expect(readClientId.ok && readClientId.value).toBe(clientId)
          expect(readTagged.ok).toBe(true)
          if (readTagged.ok) {
            expect(readTagged.value).toHaveLength(taggedFields.length)
            for (let i = 0; i < taggedFields.length; i++) {
              expect(readTagged.value[i].tag).toBe(taggedFields[i].tag)
              expect(readTagged.value[i].data).toEqual(taggedFields[i].data)
            }
          }
          expect(reader.remaining).toBe(0)
        }
      ),
      { numRuns: 200 }
    )
  })
})

// ---------------------------------------------------------------------------
// Property: response header round-trips
// ---------------------------------------------------------------------------

describe("property-based: response header round-trips", () => {
  /**
   * @see https://kafka.apache.org/protocol.html#protocol_messages — Response Header v0
   */
  it("v0 (non-flexible) round-trips correlation ID", () => {
    fc.assert(
      fc.property(arbCorrelationId, (corrId) => {
        const writer = new BinaryWriter()
        writer.writeInt32(corrId)
        const reader = new BinaryReader(writer.finish())

        const result = decodeResponseHeader(reader, ApiKey.ApiVersions, 0)
        expect(result.ok).toBe(true)
        if (result.ok) {
          expect(result.value.correlationId).toBe(corrId)
          expect(result.value.taggedFields).toEqual([])
        }
      }),
      { numRuns: 200 }
    )
  })

  /**
   * @see https://kafka.apache.org/protocol.html#protocol_messages — Response Header v1
   */
  it("v1 (flexible) round-trips correlation ID and tagged fields", () => {
    fc.assert(
      fc.property(arbCorrelationId, arbTaggedFields, (corrId, taggedFields) => {
        const writer = new BinaryWriter()
        writer.writeInt32(corrId)
        writer.writeTaggedFields(taggedFields)
        const reader = new BinaryReader(writer.finish())

        const result = decodeResponseHeader(reader, ApiKey.ApiVersions, 3)
        expect(result.ok).toBe(true)
        if (result.ok) {
          expect(result.value.correlationId).toBe(corrId)
          expect(result.value.taggedFields).toHaveLength(taggedFields.length)
          for (let i = 0; i < taggedFields.length; i++) {
            expect(result.value.taggedFields[i].tag).toBe(taggedFields[i].tag)
            expect(result.value.taggedFields[i].data).toEqual(taggedFields[i].data)
          }
        }
      }),
      { numRuns: 200 }
    )
  })
})

// ---------------------------------------------------------------------------
// Property: chunk reassembly via readResponseFrame
// ---------------------------------------------------------------------------

describe("property-based: frame parsing with arbitrary split points", () => {
  /**
   * Kafka protocol spec §Protocol Framing:
   * TCP delivers bytes as a stream — a single Kafka response may arrive
   * in any number of chunks. The framing layer must reassemble correctly.
   *
   * This test verifies that readResponseFrame produces the same result
   * regardless of whether the full frame is provided at once or
   * concatenated from arbitrary byte boundaries.
   *
   * @see https://kafka.apache.org/protocol.html#protocol_common
   */
  it("concatenated frames parse identically to individual frames", () => {
    fc.assert(
      fc.property(fc.array(arbBody, { minLength: 1, maxLength: 5 }), (bodies) => {
        // Build multiple frames
        const frames: Uint8Array[] = bodies.map((body, i) => {
          const inner = new BinaryWriter()
          inner.writeInt32(i) // correlation ID
          inner.writeRawBytes(body)
          const payload = inner.finish()

          const frame = new BinaryWriter()
          frame.writeInt32(payload.byteLength)
          frame.writeRawBytes(payload)
          return frame.finish()
        })

        // Concatenate all frames into a single buffer
        const totalLength = frames.reduce((sum, f) => sum + f.byteLength, 0)
        const combined = new Uint8Array(totalLength)
        let offset = 0
        for (const f of frames) {
          combined.set(f, offset)
          offset += f.byteLength
        }

        // Parse sequentially from the concatenated buffer
        const reader = new BinaryReader(combined)
        for (let i = 0; i < bodies.length; i++) {
          const result = readResponseFrame(reader)
          expect(result.ok).toBe(true)
          if (result.ok) {
            const corrId = result.value.readInt32()
            expect(corrId.ok).toBe(true)
            if (corrId.ok) {
              expect(corrId.value).toBe(i)
            }
            expect(result.value.remaining).toBe(bodies[i].byteLength)
          }
        }
        expect(reader.remaining).toBe(0)
      }),
      { numRuns: 100 }
    )
  })
})
