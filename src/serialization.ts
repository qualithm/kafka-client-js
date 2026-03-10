/**
 * Serializer/Deserializer interface and built-in implementations.
 *
 * Serializers convert typed values to `Uint8Array` for producing, and
 * deserializers convert `Uint8Array` back to typed values for consuming.
 * Uses `Uint8Array` exclusively per locked decision #2.
 *
 * Built-in serializers:
 * - {@link stringSerializer} — UTF-8 string encoding
 * - {@link jsonSerializer} — JSON via UTF-8 encoding
 *
 * Async variants ({@link AsyncSerializer}, {@link AsyncDeserializer},
 * {@link AsyncSerde}) support Schema Registry integration where network
 * calls are required during serialization/deserialization.
 *
 * @packageDocumentation
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Serializes a value of type `T` to bytes for producing.
 */
export type Serializer<T> = {
  /**
   * Serialize a value to bytes.
   *
   * @param value - The value to serialize.
   * @param topic - The topic the message will be produced to (for topic-aware serialization).
   * @returns The serialized bytes.
   */
  readonly serialize: (value: T, topic: string) => Uint8Array
}

/**
 * Deserializes bytes back to a value of type `T` for consuming.
 */
export type Deserializer<T> = {
  /**
   * Deserialize bytes to a value.
   *
   * @param data - The bytes to deserialize.
   * @param topic - The topic the message was consumed from (for topic-aware deserialization).
   * @returns The deserialized value.
   */
  readonly deserialize: (data: Uint8Array, topic: string) => T
}

/**
 * Combined serializer and deserializer for a type `T`.
 */
export type Serde<T> = Serializer<T> & Deserializer<T>

// ---------------------------------------------------------------------------
// Async Types (for Schema Registry integration)
// ---------------------------------------------------------------------------

/**
 * Async serializer for use with Schema Registry.
 *
 * Network calls (schema registration, ID lookup) may be required
 * during serialization.
 */
export type AsyncSerializer<T> = {
  /**
   * Serialize a value to bytes, potentially fetching/registering schemas.
   *
   * @param value - The value to serialize.
   * @param topic - The topic the message will be produced to.
   * @returns The serialized bytes including the Confluent wire format header.
   */
  readonly serialize: (value: T, topic: string) => Promise<Uint8Array>
}

/**
 * Async deserializer for use with Schema Registry.
 *
 * Network calls (schema fetch by ID) may be required during deserialization
 * when encountering a previously unseen schema ID.
 */
export type AsyncDeserializer<T> = {
  /**
   * Deserialize bytes to a value, potentially fetching schemas.
   *
   * @param data - The bytes to deserialize (including Confluent wire format header).
   * @param topic - The topic the message was consumed from.
   * @returns The deserialized value.
   */
  readonly deserialize: (data: Uint8Array, topic: string) => Promise<T>
}

/**
 * Combined async serializer and deserializer for a type `T`.
 */
export type AsyncSerde<T> = AsyncSerializer<T> & AsyncDeserializer<T>

// ---------------------------------------------------------------------------
// Shared TextEncoder / TextDecoder
// ---------------------------------------------------------------------------

const encoder = new TextEncoder()
const decoder = new TextDecoder()

// ---------------------------------------------------------------------------
// String Serializer
// ---------------------------------------------------------------------------

/**
 * UTF-8 string serializer/deserializer.
 *
 * Encodes strings to UTF-8 bytes and decodes UTF-8 bytes to strings.
 */
export const stringSerializer: Serde<string> = {
  serialize(value: string): Uint8Array {
    return encoder.encode(value)
  },
  deserialize(data: Uint8Array): string {
    return decoder.decode(data)
  }
}

// ---------------------------------------------------------------------------
// JSON Serializer
// ---------------------------------------------------------------------------

/**
 * Creates a JSON serializer/deserializer.
 *
 * Serialises values to UTF-8 JSON bytes and deserialises UTF-8 JSON bytes
 * back to typed values. The type parameter `T` is not validated at runtime;
 * callers are responsible for ensuring the JSON shape matches `T`.
 */
export function jsonSerializer<T>(): Serde<T> {
  return {
    serialize(value: T): Uint8Array {
      return encoder.encode(JSON.stringify(value))
    },
    deserialize(data: Uint8Array): T {
      return JSON.parse(decoder.decode(data)) as T
    }
  }
}
