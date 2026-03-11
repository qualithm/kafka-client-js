/**
 * Auto-growing buffer builder for writing Kafka binary protocol data.
 *
 * Uses a doubling growth strategy to amortise allocation cost.
 * Call {@link BinaryWriter.finish} to obtain the final {@link Uint8Array}.
 *
 * @packageDocumentation
 */

import type { TaggedField } from "./binary-reader.js"

const TEXT_ENCODER = new TextEncoder()
const INITIAL_CAPACITY = 256

/**
 * Auto-growing buffer builder for writing Kafka protocol primitives.
 *
 * All write methods return `this` for fluent chaining.
 */
export class BinaryWriter {
  private buffer: Uint8Array
  private view: DataView
  private pos: number

  constructor(initialCapacity = INITIAL_CAPACITY) {
    this.buffer = new Uint8Array(initialCapacity)
    this.view = new DataView(this.buffer.buffer)
    this.pos = 0
  }

  /** Current write position (number of bytes written so far). */
  get offset(): number {
    return this.pos
  }

  /** Ensure at least `bytes` more bytes can be written without reallocation. */
  private ensure(bytes: number): void {
    const required = this.pos + bytes
    if (required <= this.buffer.byteLength) {
      return
    }

    let newCapacity = this.buffer.byteLength
    while (newCapacity < required) {
      newCapacity *= 2
    }

    const newBuffer = new Uint8Array(newCapacity)
    newBuffer.set(this.buffer)
    this.buffer = newBuffer
    this.view = new DataView(this.buffer.buffer)
  }

  // ---------------------------------------------------------------------------
  // Fixed-width primitives
  // ---------------------------------------------------------------------------

  writeInt8(value: number): this {
    this.ensure(1)
    this.view.setInt8(this.pos, value)
    this.pos += 1
    return this
  }

  writeInt16(value: number): this {
    this.ensure(2)
    this.view.setInt16(this.pos, value)
    this.pos += 2
    return this
  }

  writeInt32(value: number): this {
    this.ensure(4)
    this.view.setInt32(this.pos, value)
    this.pos += 4
    return this
  }

  writeInt64(value: bigint): this {
    this.ensure(8)
    this.view.setBigInt64(this.pos, value)
    this.pos += 8
    return this
  }

  writeUint32(value: number): this {
    this.ensure(4)
    this.view.setUint32(this.pos, value)
    this.pos += 4
    return this
  }

  writeBoolean(value: boolean): this {
    this.ensure(1)
    this.view.setInt8(this.pos, value ? 1 : 0)
    this.pos += 1
    return this
  }

  writeFloat64(value: number): this {
    this.ensure(8)
    this.view.setFloat64(this.pos, value)
    this.pos += 8
    return this
  }

  /** Write 16 bytes as a Kafka UUID. */
  writeUuid(uuid: Uint8Array): this {
    return this.writeRawBytes(uuid)
  }

  // ---------------------------------------------------------------------------
  // Variable-length integers (Kafka protocol encoding)
  // ---------------------------------------------------------------------------

  /**
   * Write an unsigned variable-length integer (up to 5 bytes, 32-bit).
   */
  writeUnsignedVarInt(value: number): this {
    this.ensure(5)
    let v = value >>> 0
    while ((v & ~0x7f) !== 0) {
      this.view.setUint8(this.pos, (v & 0x7f) | 0x80)
      this.pos += 1
      v >>>= 7
    }
    this.view.setUint8(this.pos, v)
    this.pos += 1
    return this
  }

  /**
   * Write a signed variable-length integer using zigzag encoding.
   */
  writeSignedVarInt(value: number): this {
    const zigzag = ((value << 1) ^ (value >> 31)) >>> 0
    return this.writeUnsignedVarInt(zigzag)
  }

  /**
   * Write an unsigned variable-length long (up to 10 bytes, 64-bit).
   */
  writeUnsignedVarLong(value: bigint): this {
    this.ensure(10)
    let v = BigInt.asUintN(64, value)
    while ((v & ~0x7fn) !== 0n) {
      this.view.setUint8(this.pos, Number(v & 0x7fn) | 0x80)
      this.pos += 1
      v >>= 7n
    }
    this.view.setUint8(this.pos, Number(v))
    this.pos += 1
    return this
  }

  /**
   * Write a signed variable-length long using zigzag encoding.
   */
  writeSignedVarLong(value: bigint): this {
    const zigzag = BigInt.asUintN(64, (value << 1n) ^ (value >> 63n))
    return this.writeUnsignedVarLong(zigzag)
  }

  // ---------------------------------------------------------------------------
  // Strings (Kafka protocol §5.3)
  // ---------------------------------------------------------------------------

  /**
   * Write a nullable string with INT16 length prefix (non-flexible versions).
   * Null writes -1 as the length.
   */
  writeString(value: string | null): this {
    if (value === null) {
      return this.writeInt16(-1)
    }
    const encoded = TEXT_ENCODER.encode(value)
    this.writeInt16(encoded.byteLength)
    return this.writeRawBytes(encoded)
  }

  /**
   * Write a nullable compact string (flexible versions, KIP-482).
   * Null writes 0; otherwise writes (length + 1) as unsigned varint.
   */
  writeCompactString(value: string | null): this {
    if (value === null) {
      return this.writeUnsignedVarInt(0)
    }
    const encoded = TEXT_ENCODER.encode(value)
    this.writeUnsignedVarInt(encoded.byteLength + 1)
    return this.writeRawBytes(encoded)
  }

  // ---------------------------------------------------------------------------
  // Bytes (Kafka protocol §5.3)
  // ---------------------------------------------------------------------------

  /**
   * Write nullable bytes with INT32 length prefix (non-flexible versions).
   * Null writes -1 as the length.
   */
  writeBytes(value: Uint8Array | null): this {
    if (value === null) {
      return this.writeInt32(-1)
    }
    this.writeInt32(value.byteLength)
    return this.writeRawBytes(value)
  }

  /**
   * Write nullable compact bytes (flexible versions, KIP-482).
   * Null writes 0; otherwise writes (length + 1) as unsigned varint.
   */
  writeCompactBytes(value: Uint8Array | null): this {
    if (value === null) {
      return this.writeUnsignedVarInt(0)
    }
    this.writeUnsignedVarInt(value.byteLength + 1)
    return this.writeRawBytes(value)
  }

  // ---------------------------------------------------------------------------
  // Arrays (Kafka protocol §5.3)
  // ---------------------------------------------------------------------------

  /**
   * Write a nullable array with INT32 count prefix (non-flexible versions).
   * Null writes -1 as the count.
   */
  writeArray<T>(
    values: readonly T[] | null,
    writeElement: (writer: BinaryWriter, value: T) => void
  ): this {
    if (values === null) {
      return this.writeInt32(-1)
    }
    this.writeInt32(values.length)
    for (const value of values) {
      writeElement(this, value)
    }
    return this
  }

  /**
   * Write a nullable compact array (flexible versions, KIP-482).
   * Null writes 0; otherwise writes (count + 1) as unsigned varint.
   */
  writeCompactArray<T>(
    values: readonly T[] | null,
    writeElement: (writer: BinaryWriter, value: T) => void
  ): this {
    if (values === null) {
      return this.writeUnsignedVarInt(0)
    }
    this.writeUnsignedVarInt(values.length + 1)
    for (const value of values) {
      writeElement(this, value)
    }
    return this
  }

  // ---------------------------------------------------------------------------
  // Tagged fields (KIP-482 flexible versions)
  // ---------------------------------------------------------------------------

  /**
   * Write tagged fields. Format: unsigned varint count, then for each field:
   * unsigned varint tag, unsigned varint size, then the data bytes.
   */
  writeTaggedFields(fields: readonly TaggedField[]): this {
    this.writeUnsignedVarInt(fields.length)
    for (const field of fields) {
      this.writeUnsignedVarInt(field.tag)
      this.writeUnsignedVarInt(field.data.byteLength)
      this.writeRawBytes(field.data)
    }
    return this
  }

  // ---------------------------------------------------------------------------
  // Raw byte access
  // ---------------------------------------------------------------------------

  /** Write raw bytes into the buffer. */
  writeRawBytes(bytes: Uint8Array): this {
    this.ensure(bytes.byteLength)
    this.buffer.set(bytes, this.pos)
    this.pos += bytes.byteLength
    return this
  }

  /** Return a new {@link Uint8Array} containing exactly the written bytes. */
  finish(): Uint8Array {
    return this.buffer.slice(0, this.pos)
  }

  /** Reset the write position to 0, allowing buffer reuse. */
  reset(): void {
    this.pos = 0
  }
}
