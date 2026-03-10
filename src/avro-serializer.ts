/**
 * Avro serializer/deserializer with Confluent Schema Registry integration.
 *
 * Uses the Confluent wire format: magic byte (0x00) + 4-byte big-endian
 * schema ID + Avro binary payload.
 *
 * The Avro codec is pluggable — users bring their own Avro library
 * (e.g. `avsc`, `@avro/avsc`) and wrap it in the {@link AvroCodec} interface.
 * This follows the same pattern as compression providers.
 *
 * @example
 * ```ts
 * import avro from "avsc"
 *
 * const codec: AvroCodec = {
 *   parse(schemaJson: string) {
 *     const type = avro.Type.forSchema(JSON.parse(schemaJson))
 *     return {
 *       encode: (value) => type.toBuffer(value),
 *       decode: (data) => type.fromBuffer(data),
 *     }
 *   },
 * }
 *
 * const serde = createAvroSerde<MyType>({
 *   registry: new SchemaRegistry({ url: "http://localhost:8081" }),
 *   codec,
 *   schema: JSON.stringify({ type: "record", name: "MyType", fields: [...] }),
 * })
 *
 * const bytes = await serde.serialize(myValue, "my-topic")
 * const value = await serde.deserialize(bytes, "my-topic")
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
 * A parsed Avro schema with encode/decode capabilities.
 *
 * Returned by {@link AvroCodec.parse} after parsing a schema JSON string.
 * Implementations should cache any internal parsing state.
 */
export type AvroSchema = {
  /** Encode a value to Avro binary format. */
  readonly encode: (value: unknown) => Uint8Array
  /** Decode Avro binary data to a value. */
  readonly decode: (data: Uint8Array) => unknown
}

/**
 * Pluggable Avro codec interface.
 *
 * Wraps an Avro library (e.g. `avsc`) to provide schema parsing.
 * The codec is responsible for parsing the schema JSON string and
 * returning an {@link AvroSchema} with encode/decode methods.
 *
 * @example
 * ```ts
 * import avro from "avsc"
 *
 * const codec: AvroCodec = {
 *   parse(schemaJson: string) {
 *     const type = avro.Type.forSchema(JSON.parse(schemaJson))
 *     return {
 *       encode: (value) => new Uint8Array(type.toBuffer(value)),
 *       decode: (data) => type.fromBuffer(Buffer.from(data)),
 *     }
 *   },
 * }
 * ```
 */
export type AvroCodec = {
  /**
   * Parse an Avro schema JSON string into an encodable/decodable schema.
   *
   * @param schemaJson - Avro schema as a JSON string.
   * @returns Parsed schema with encode/decode methods.
   */
  readonly parse: (schemaJson: string) => AvroSchema
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/** Options for creating an Avro serde. */
export type AvroSerdeOptions = {
  /** Schema Registry client instance. */
  readonly registry: SchemaRegistry
  /** Pluggable Avro codec (wraps user's Avro library). */
  readonly codec: AvroCodec
  /**
   * Writer schema as a JSON string.
   *
   * Used for serialization. If `autoRegister` is true (default), this
   * schema is registered with the Schema Registry on first use.
   */
  readonly schema: string
  /**
   * Whether this serde is for the message key.
   *
   * Affects subject naming via the subject name strategy.
   * @default false
   */
  readonly isKey?: boolean
  /**
   * Subject name strategy for determining the Schema Registry subject.
   * @default topicNameStrategy
   */
  readonly subjectNameStrategy?: SubjectNameStrategy
  /**
   * Whether to automatically register the writer schema on first serialize.
   * @default true
   */
  readonly autoRegister?: boolean
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/**
 * Creates an async Avro serde with Schema Registry integration.
 *
 * **Serialization:** Encodes the value using the writer schema and prepends
 * the Confluent wire format header (magic byte + schema ID). On first call,
 * registers the schema if `autoRegister` is true.
 *
 * **Deserialization:** Reads the schema ID from the wire format header,
 * fetches the schema from the registry (cached after first fetch), and
 * decodes the Avro binary payload.
 *
 * @param options - Serde configuration.
 * @returns Async serde that produces/consumes Confluent wire format messages.
 */
export function createAvroSerde<T>(options: AvroSerdeOptions): AsyncSerde<T> {
  const {
    registry,
    codec,
    schema,
    isKey = false,
    subjectNameStrategy: namingStrategy = topicNameStrategy,
    autoRegister = true
  } = options

  // Parse the writer schema once
  const writerSchema = codec.parse(schema)

  // Cache: subject → resolved schema ID
  const schemaIdCache = new Map<string, number>()
  // Cache: schema ID → parsed AvroSchema
  const readerSchemaCache = new Map<number, AvroSchema>()

  async function resolveSchemaId(topic: string): Promise<number> {
    const subject = namingStrategy(topic, isKey)
    const cached = schemaIdCache.get(subject)
    if (cached !== undefined) {
      return cached
    }

    if (autoRegister) {
      const { id } = await registry.register(subject, schema, "AVRO")
      schemaIdCache.set(subject, id)
      return id
    }

    // Not auto-registering — look up the latest version
    const registered = await registry.getLatestSchema(subject)
    schemaIdCache.set(subject, registered.id)
    return registered.id
  }

  async function getReaderSchema(schemaId: number): Promise<AvroSchema> {
    const cached = readerSchemaCache.get(schemaId)
    if (cached) {
      return cached
    }

    const registered = await registry.getSchema(schemaId)
    const parsed = codec.parse(registered.schema)
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
        throw new SchemaRegistryError("avro serialization failed", {
          retriable: false,
          cause: error
        })
      }

      // Confluent wire format: magic + schema ID + payload
      const header = encodeWireFormatHeader(schemaId)
      const result = new Uint8Array(header.length + payload.length)
      result.set(header)
      result.set(payload, WIRE_FORMAT_HEADER_SIZE)
      return result
    },

    async deserialize(data: Uint8Array, _topic: string): Promise<T> {
      const { schemaId, offset } = decodeWireFormatHeader(data)
      const readerSchema = await getReaderSchema(schemaId)
      const payload = data.subarray(offset)
      try {
        return readerSchema.decode(payload) as T
      } catch (error) {
        throw new SchemaRegistryError("avro deserialization failed", {
          retriable: false,
          cause: error
        })
      }
    }
  }
}
