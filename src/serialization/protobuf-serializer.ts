/**
 * Protobuf serializer/deserializer with Confluent Schema Registry integration.
 *
 * Uses the Confluent wire format for Protobuf: magic byte (0x00) + 4-byte
 * big-endian schema ID + varint-encoded message index array + Protobuf
 * binary payload.
 *
 * The message index array identifies which message type within the proto
 * file is being serialized. A single `[0]` indicates the first message,
 * which is encoded as a single varint `0`. The array is encoded as a
 * length-prefixed list of varint values.
 *
 * The Protobuf codec is pluggable — users bring their own Protobuf library
 * (e.g. `protobufjs`) and wrap it in the {@link ProtobufCodec} interface.
 *
 * @example
 * ```ts
 * import protobuf from "protobufjs"
 *
 * const codec: ProtobufCodec = {
 *   parse(schemaString, messageName) {
 *     const root = protobuf.parse(schemaString).root
 *     const MessageType = root.lookupType(messageName)
 *     return {
 *       encode: (value) => new Uint8Array(MessageType.encode(value).finish()),
 *       decode: (data) => MessageType.toObject(MessageType.decode(data)),
 *       messageIndexes: [0],
 *     }
 *   },
 * }
 *
 * const serde = createProtobufSerde<MyType>({
 *   registry: new SchemaRegistry({ url: "http://localhost:8081" }),
 *   codec,
 *   schema: 'syntax = "proto3"; message MyType { string name = 1; }',
 *   messageName: "MyType",
 * })
 * ```
 *
 * @packageDocumentation
 */

import {
  decodeWireFormatHeader,
  encodeWireFormatHeader,
  type SchemaRegistry,
  SchemaRegistryError,
  type SubjectNameStrategy,
  topicNameStrategy,
  WIRE_FORMAT_HEADER_SIZE
} from "./schema-registry.js"
import type { AsyncSerde } from "./serialization.js"

// ---------------------------------------------------------------------------
// Codec Types
// ---------------------------------------------------------------------------

/**
 * A parsed Protobuf schema with encode/decode capabilities for a specific
 * message type.
 */
export type ProtobufSchema = {
  /** Encode a value to Protobuf binary format. */
  readonly encode: (value: unknown) => Uint8Array
  /** Decode Protobuf binary data to a value. */
  readonly decode: (data: Uint8Array) => unknown
  /**
   * Message index path within the proto file.
   *
   * For the first (or only) message type, this is `[0]`.
   * For nested or non-first message types, use the path of indexes
   * (e.g. `[1]` for the second top-level message, `[0, 2]` for the
   * third nested message within the first top-level message).
   */
  readonly messageIndexes: readonly number[]
}

/**
 * Pluggable Protobuf codec interface.
 *
 * Wraps a Protobuf library (e.g. `protobufjs`) to provide schema parsing.
 * The codec parses the proto schema string and returns a
 * {@link ProtobufSchema} bound to a specific message type.
 */
export type ProtobufCodec = {
  /**
   * Parse a proto schema and bind to a specific message type.
   *
   * @param schemaString - Proto schema definition string.
   * @param messageName - Fully-qualified message type name to encode/decode.
   * @returns Parsed schema with encode/decode methods and message indexes.
   */
  readonly parse: (schemaString: string, messageName: string) => ProtobufSchema
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/** Options for creating a Protobuf serde. */
export type ProtobufSerdeOptions = {
  /** Schema Registry client instance. */
  readonly registry: SchemaRegistry
  /** Pluggable Protobuf codec (wraps user's Protobuf library). */
  readonly codec: ProtobufCodec
  /** Proto schema definition string. */
  readonly schema: string
  /** Fully-qualified message type name to encode/decode. */
  readonly messageName: string
  /**
   * Whether this serde is for the message key.
   * @default false
   */
  readonly isKey?: boolean
  /**
   * Subject name strategy for determining the Schema Registry subject.
   * @default topicNameStrategy
   */
  readonly subjectNameStrategy?: SubjectNameStrategy
  /**
   * Whether to automatically register the schema on first serialize.
   * @default true
   */
  readonly autoRegister?: boolean
}

// ---------------------------------------------------------------------------
// Varint Helpers (for message index encoding)
// ---------------------------------------------------------------------------

/**
 * Encode an unsigned integer as a protobuf-style varint.
 *
 * @internal
 */
function encodeVarint(value: number): Uint8Array {
  const bytes: number[] = []
  let v = value >>> 0
  while (v > 0x7f) {
    bytes.push((v & 0x7f) | 0x80)
    v >>>= 7
  }
  bytes.push(v & 0x7f)
  return new Uint8Array(bytes)
}

/**
 * Decode a protobuf-style varint from a buffer at the given offset.
 *
 * @internal
 * @returns The decoded value and number of bytes consumed.
 */
function decodeVarint(data: Uint8Array, offset: number): { value: number; bytesRead: number } {
  let value = 0
  let shift = 0
  let bytesRead = 0

  for (let i = offset; i < data.length; i++) {
    const byte = data[i]
    value |= (byte & 0x7f) << shift
    bytesRead++
    if ((byte & 0x80) === 0) {
      return { value: value >>> 0, bytesRead }
    }
    shift += 7
    if (shift >= 35) {
      throw new SchemaRegistryError("varint too long in protobuf message index")
    }
  }

  throw new SchemaRegistryError("unexpected end of data while decoding varint")
}

/**
 * Encode the message index array for the Confluent Protobuf wire format.
 *
 * Format: varint(array length) + varint(index0) + varint(index1) + ...
 * Special case: `[0]` is encoded as a single varint `0` (length = 0).
 *
 * @internal
 */
export function encodeMessageIndexes(indexes: readonly number[]): Uint8Array {
  // Special case: single index [0] → length 0
  if (indexes.length === 1 && indexes[0] === 0) {
    return encodeVarint(0)
  }

  const parts: Uint8Array[] = [encodeVarint(indexes.length)]
  for (const index of indexes) {
    parts.push(encodeVarint(index))
  }

  const totalLength = parts.reduce((sum, p) => sum + p.length, 0)
  const result = new Uint8Array(totalLength)
  let offset = 0
  for (const part of parts) {
    result.set(part, offset)
    offset += part.length
  }
  return result
}

/**
 * Decode the message index array from the Confluent Protobuf wire format.
 *
 * @internal
 * @returns The message indexes and total bytes consumed.
 */
export function decodeMessageIndexes(
  data: Uint8Array,
  offset: number
): { indexes: readonly number[]; bytesRead: number } {
  const { value: length, bytesRead: lengthBytes } = decodeVarint(data, offset)
  let totalBytesRead = lengthBytes

  // Special case: length 0 means [0]
  if (length === 0) {
    return { indexes: [0], bytesRead: totalBytesRead }
  }

  const indexes: number[] = []
  for (let i = 0; i < length; i++) {
    const { value, bytesRead } = decodeVarint(data, offset + totalBytesRead)
    indexes.push(value)
    totalBytesRead += bytesRead
  }

  return { indexes, bytesRead: totalBytesRead }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/**
 * Creates an async Protobuf serde with Schema Registry integration.
 *
 * **Serialization:** Encodes the value using the specified Protobuf message
 * type and prepends the Confluent wire format header (magic byte + schema ID
 * + message index array).
 *
 * **Deserialization:** Reads the schema ID and message index array from
 * the wire format header, fetches the schema from the registry, and
 * decodes the Protobuf binary payload.
 *
 * @param options - Serde configuration.
 * @returns Async serde that produces/consumes Confluent wire format messages.
 */
export function createProtobufSerde<T>(options: ProtobufSerdeOptions): AsyncSerde<T> {
  const {
    registry,
    codec,
    schema,
    messageName,
    isKey = false,
    subjectNameStrategy: namingStrategy = topicNameStrategy,
    autoRegister = true
  } = options

  // Parse the writer schema once
  const writerSchema = codec.parse(schema, messageName)

  // Cache: subject → resolved schema ID
  const schemaIdCache = new Map<string, number>()
  // Cache: schema ID → parsed ProtobufSchema
  const readerSchemaCache = new Map<number, ProtobufSchema>()

  // Pre-encode the message index bytes for the writer schema
  const writerMessageIndexBytes = encodeMessageIndexes(writerSchema.messageIndexes)

  async function resolveSchemaId(topic: string): Promise<number> {
    const subject = namingStrategy(topic, isKey)
    const cached = schemaIdCache.get(subject)
    if (cached !== undefined) {
      return cached
    }

    if (autoRegister) {
      const { id } = await registry.register(subject, schema, "PROTOBUF")
      schemaIdCache.set(subject, id)
      return id
    }

    const registered = await registry.getLatestSchema(subject)
    schemaIdCache.set(subject, registered.id)
    return registered.id
  }

  async function getReaderSchema(schemaId: number): Promise<ProtobufSchema> {
    const cached = readerSchemaCache.get(schemaId)
    if (cached) {
      return cached
    }

    const registered = await registry.getSchema(schemaId)
    const parsed = codec.parse(registered.schema, messageName)
    readerSchemaCache.set(schemaId, parsed)
    return parsed
  }

  return {
    async serialize(value: T, topic: string): Promise<Uint8Array> {
      const schemaId = await resolveSchemaId(topic)
      let payload: Uint8Array
      try {
        payload = writerSchema.encode(value)
      } catch (error) {
        throw new SchemaRegistryError("protobuf serialization failed", {
          retriable: false,
          cause: error
        })
      }

      // Confluent wire format: header + message indexes + payload
      const header = encodeWireFormatHeader(schemaId)
      const result = new Uint8Array(header.length + writerMessageIndexBytes.length + payload.length)
      result.set(header)
      result.set(writerMessageIndexBytes, WIRE_FORMAT_HEADER_SIZE)
      result.set(payload, WIRE_FORMAT_HEADER_SIZE + writerMessageIndexBytes.length)
      return result
    },

    async deserialize(data: Uint8Array, _topic: string): Promise<T> {
      const { schemaId, offset: headerEnd } = decodeWireFormatHeader(data)
      const { indexes: _indexes, bytesRead: indexBytes } = decodeMessageIndexes(data, headerEnd)
      const readerSchema = await getReaderSchema(schemaId)
      const payload = data.subarray(headerEnd + indexBytes)
      try {
        return readerSchema.decode(payload) as T
      } catch (error) {
        throw new SchemaRegistryError("protobuf deserialization failed", {
          retriable: false,
          cause: error
        })
      }
    }
  }
}
