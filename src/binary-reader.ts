/**
 * Bounds-checked cursor for reading Kafka binary protocol data from a {@link Uint8Array}.
 *
 * All read methods return {@link DecodeResult} to avoid exceptions in hot paths.
 * The reader advances its internal position on successful reads only.
 *
 * @packageDocumentation
 */

import { decodeFailure, type DecodeResult, decodeSuccess } from "./result.js"

const TEXT_DECODER = new TextDecoder()

/**
 * A tagged field from KIP-482 flexible versions.
 * Each field has a numeric tag and opaque data payload.
 */
export type TaggedField = {
  readonly tag: number
  readonly data: Uint8Array
}

/**
 * Bounds-checked cursor over a {@link Uint8Array} for reading Kafka protocol primitives.
 *
 * On success the reader advances its position; on failure the position is unchanged.
 */
export class BinaryReader {
  private readonly buffer: Uint8Array
  private readonly view: DataView
  private pos: number

  constructor(buffer: Uint8Array, offset = 0) {
    this.buffer = buffer
    this.view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength)
    this.pos = offset
  }

  /** Current byte offset within the buffer. */
  get offset(): number {
    return this.pos
  }

  /** Number of bytes remaining from the current position. */
  get remaining(): number {
    return this.buffer.byteLength - this.pos
  }

  // ---------------------------------------------------------------------------
  // Fixed-width primitives
  // ---------------------------------------------------------------------------

  readInt8(): DecodeResult<number> {
    if (this.remaining < 1) {
      return decodeFailure("BUFFER_UNDERFLOW", "need 1 byte for int8", this.pos)
    }
    const value = this.view.getInt8(this.pos)
    this.pos += 1
    return decodeSuccess(value, 1)
  }

  readInt16(): DecodeResult<number> {
    if (this.remaining < 2) {
      return decodeFailure("BUFFER_UNDERFLOW", "need 2 bytes for int16", this.pos)
    }
    const value = this.view.getInt16(this.pos)
    this.pos += 2
    return decodeSuccess(value, 2)
  }

  readInt32(): DecodeResult<number> {
    if (this.remaining < 4) {
      return decodeFailure("BUFFER_UNDERFLOW", "need 4 bytes for int32", this.pos)
    }
    const value = this.view.getInt32(this.pos)
    this.pos += 4
    return decodeSuccess(value, 4)
  }

  readInt64(): DecodeResult<bigint> {
    if (this.remaining < 8) {
      return decodeFailure("BUFFER_UNDERFLOW", "need 8 bytes for int64", this.pos)
    }
    const value = this.view.getBigInt64(this.pos)
    this.pos += 8
    return decodeSuccess(value, 8)
  }

  readUint32(): DecodeResult<number> {
    if (this.remaining < 4) {
      return decodeFailure("BUFFER_UNDERFLOW", "need 4 bytes for uint32", this.pos)
    }
    const value = this.view.getUint32(this.pos)
    this.pos += 4
    return decodeSuccess(value, 4)
  }

  readBoolean(): DecodeResult<boolean> {
    if (this.remaining < 1) {
      return decodeFailure("BUFFER_UNDERFLOW", "need 1 byte for boolean", this.pos)
    }
    const value = this.view.getUint8(this.pos) !== 0
    this.pos += 1
    return decodeSuccess(value, 1)
  }

  readFloat64(): DecodeResult<number> {
    if (this.remaining < 8) {
      return decodeFailure("BUFFER_UNDERFLOW", "need 8 bytes for float64", this.pos)
    }
    const value = this.view.getFloat64(this.pos)
    this.pos += 8
    return decodeSuccess(value, 8)
  }

  /** Read 16 bytes as a Kafka UUID. */
  readUuid(): DecodeResult<Uint8Array> {
    return this.readRawBytes(16)
  }

  // ---------------------------------------------------------------------------
  // Variable-length integers (Kafka protocol encoding)
  // ---------------------------------------------------------------------------

  /**
   * Read an unsigned variable-length integer (up to 5 bytes, 32-bit).
   *
   * Kafka protocol §5.2: Each byte uses 7 data bits with the high bit as
   * a continuation flag. Least-significant group first.
   */
  readUnsignedVarInt(): DecodeResult<number> {
    let value = 0
    let shift = 0
    const startPos = this.pos

    for (let i = 0; i < 5; i++) {
      if (this.remaining < 1) {
        this.pos = startPos
        return decodeFailure("BUFFER_UNDERFLOW", "truncated unsigned varint", startPos)
      }
      const byte = this.view.getUint8(this.pos)
      this.pos += 1
      value |= (byte & 0x7f) << shift
      if ((byte & 0x80) === 0) {
        return decodeSuccess(value >>> 0, this.pos - startPos)
      }
      shift += 7
    }

    this.pos = startPos
    return decodeFailure("INVALID_VARINT", "unsigned varint exceeds 5 bytes", startPos)
  }

  /**
   * Read a signed variable-length integer using zigzag encoding.
   *
   * Zigzag maps signed integers to unsigned so that small absolute values
   * have small varint encodings: 0 → 0, -1 → 1, 1 → 2, -2 → 3, …
   */
  readSignedVarInt(): DecodeResult<number> {
    const result = this.readUnsignedVarInt()
    if (!result.ok) {
      return result
    }
    const raw = result.value
    const decoded = (raw >>> 1) ^ -(raw & 1)
    return decodeSuccess(decoded, result.bytesRead)
  }

  /**
   * Read an unsigned variable-length long (up to 10 bytes, 64-bit).
   */
  readUnsignedVarLong(): DecodeResult<bigint> {
    let value = 0n
    let shift = 0n
    const startPos = this.pos

    for (let i = 0; i < 10; i++) {
      if (this.remaining < 1) {
        this.pos = startPos
        return decodeFailure("BUFFER_UNDERFLOW", "truncated unsigned varlong", startPos)
      }
      const byte = this.view.getUint8(this.pos)
      this.pos += 1
      value |= BigInt(byte & 0x7f) << shift
      if ((byte & 0x80) === 0) {
        return decodeSuccess(value, this.pos - startPos)
      }
      shift += 7n
    }

    this.pos = startPos
    return decodeFailure("INVALID_VARINT", "unsigned varlong exceeds 10 bytes", startPos)
  }

  /**
   * Read a signed variable-length long using zigzag encoding.
   */
  readSignedVarLong(): DecodeResult<bigint> {
    const result = this.readUnsignedVarLong()
    if (!result.ok) {
      return result
    }
    const raw = result.value
    const decoded = (raw >> 1n) ^ -(raw & 1n)
    return decodeSuccess(decoded, result.bytesRead)
  }

  // ---------------------------------------------------------------------------
  // Strings (Kafka protocol §5.3)
  // ---------------------------------------------------------------------------

  /**
   * Read a nullable string with INT16 length prefix (non-flexible versions).
   * Length of -1 indicates null.
   */
  readString(): DecodeResult<string | null> {
    const startPos = this.pos
    const lenResult = this.readInt16()
    if (!lenResult.ok) {
      return lenResult as DecodeResult<string | null>
    }
    const length = lenResult.value

    if (length < -1) {
      this.pos = startPos
      return decodeFailure("INVALID_DATA", `invalid string length: ${String(length)}`, startPos)
    }
    if (length === -1) {
      return decodeSuccess(null, 2)
    }
    if (this.remaining < length) {
      this.pos = startPos
      return decodeFailure("BUFFER_UNDERFLOW", `need ${String(length)} bytes for string`, startPos)
    }

    const bytes = this.buffer.subarray(this.pos, this.pos + length)
    this.pos += length
    return decodeSuccess(TEXT_DECODER.decode(bytes), this.pos - startPos)
  }

  /**
   * Read a nullable compact string (flexible versions, KIP-482).
   * Uses unsigned varint for (length + 1); value 0 indicates null.
   */
  readCompactString(): DecodeResult<string | null> {
    const startPos = this.pos
    const lenResult = this.readUnsignedVarInt()
    if (!lenResult.ok) {
      return lenResult as DecodeResult<string | null>
    }
    const rawLength = lenResult.value

    if (rawLength === 0) {
      return decodeSuccess(null, this.pos - startPos)
    }

    const length = rawLength - 1
    if (this.remaining < length) {
      this.pos = startPos
      return decodeFailure(
        "BUFFER_UNDERFLOW",
        `need ${String(length)} bytes for compact string`,
        startPos
      )
    }

    const bytes = this.buffer.subarray(this.pos, this.pos + length)
    this.pos += length
    return decodeSuccess(TEXT_DECODER.decode(bytes), this.pos - startPos)
  }

  // ---------------------------------------------------------------------------
  // Bytes (Kafka protocol §5.3)
  // ---------------------------------------------------------------------------

  /**
   * Read nullable bytes with INT32 length prefix (non-flexible versions).
   * Length of -1 indicates null. Returns a copy of the data.
   */
  readBytes(): DecodeResult<Uint8Array | null> {
    const startPos = this.pos
    const lenResult = this.readInt32()
    if (!lenResult.ok) {
      return lenResult as DecodeResult<Uint8Array | null>
    }
    const length = lenResult.value

    if (length < -1) {
      this.pos = startPos
      return decodeFailure("INVALID_DATA", `invalid bytes length: ${String(length)}`, startPos)
    }
    if (length === -1) {
      return decodeSuccess(null, 4)
    }
    if (this.remaining < length) {
      this.pos = startPos
      return decodeFailure(
        "BUFFER_UNDERFLOW",
        `need ${String(length)} bytes for bytes field`,
        startPos
      )
    }

    const bytes = this.buffer.slice(this.pos, this.pos + length)
    this.pos += length
    return decodeSuccess(bytes, this.pos - startPos)
  }

  /**
   * Read nullable compact bytes (flexible versions, KIP-482).
   * Uses unsigned varint for (length + 1); value 0 indicates null.
   * Returns a copy of the data.
   */
  readCompactBytes(): DecodeResult<Uint8Array | null> {
    const startPos = this.pos
    const lenResult = this.readUnsignedVarInt()
    if (!lenResult.ok) {
      return lenResult as DecodeResult<Uint8Array | null>
    }
    const rawLength = lenResult.value

    if (rawLength === 0) {
      return decodeSuccess(null, this.pos - startPos)
    }

    const length = rawLength - 1
    if (this.remaining < length) {
      this.pos = startPos
      return decodeFailure(
        "BUFFER_UNDERFLOW",
        `need ${String(length)} bytes for compact bytes`,
        startPos
      )
    }

    const bytes = this.buffer.slice(this.pos, this.pos + length)
    this.pos += length
    return decodeSuccess(bytes, this.pos - startPos)
  }

  // ---------------------------------------------------------------------------
  // Arrays (Kafka protocol §5.3)
  // ---------------------------------------------------------------------------

  /**
   * Read a nullable array with INT32 count prefix (non-flexible versions).
   * Count of -1 indicates null.
   */
  readArray<T>(readElement: (reader: BinaryReader) => DecodeResult<T>): DecodeResult<T[] | null> {
    const startPos = this.pos
    const countResult = this.readInt32()
    if (!countResult.ok) {
      return countResult as DecodeResult<T[] | null>
    }
    const count = countResult.value

    if (count < -1) {
      this.pos = startPos
      return decodeFailure("INVALID_DATA", `invalid array count: ${String(count)}`, startPos)
    }
    if (count === -1) {
      return decodeSuccess(null, 4)
    }

    const elements: T[] = []
    for (let i = 0; i < count; i++) {
      const elemResult = readElement(this)
      if (!elemResult.ok) {
        this.pos = startPos
        return elemResult as DecodeResult<T[] | null>
      }
      elements.push(elemResult.value)
    }

    return decodeSuccess(elements, this.pos - startPos)
  }

  /**
   * Read a nullable compact array (flexible versions, KIP-482).
   * Uses unsigned varint for (count + 1); value 0 indicates null.
   */
  readCompactArray<T>(
    readElement: (reader: BinaryReader) => DecodeResult<T>
  ): DecodeResult<T[] | null> {
    const startPos = this.pos
    const countResult = this.readUnsignedVarInt()
    if (!countResult.ok) {
      return countResult as DecodeResult<T[] | null>
    }
    const rawCount = countResult.value

    if (rawCount === 0) {
      return decodeSuccess(null, this.pos - startPos)
    }

    const count = rawCount - 1
    const elements: T[] = []
    for (let i = 0; i < count; i++) {
      const elemResult = readElement(this)
      if (!elemResult.ok) {
        this.pos = startPos
        return elemResult as DecodeResult<T[] | null>
      }
      elements.push(elemResult.value)
    }

    return decodeSuccess(elements, this.pos - startPos)
  }

  // ---------------------------------------------------------------------------
  // Tagged fields (KIP-482 flexible versions)
  // ---------------------------------------------------------------------------

  /**
   * Read tagged fields. Format: unsigned varint count, then for each field:
   * unsigned varint tag, unsigned varint size, then `size` bytes of data.
   */
  readTaggedFields(): DecodeResult<TaggedField[]> {
    const startPos = this.pos
    const countResult = this.readUnsignedVarInt()
    if (!countResult.ok) {
      return countResult as DecodeResult<TaggedField[]>
    }
    const count = countResult.value

    const fields: TaggedField[] = []
    for (let i = 0; i < count; i++) {
      const tagResult = this.readUnsignedVarInt()
      if (!tagResult.ok) {
        this.pos = startPos
        return tagResult as DecodeResult<TaggedField[]>
      }

      const sizeResult = this.readUnsignedVarInt()
      if (!sizeResult.ok) {
        this.pos = startPos
        return sizeResult as DecodeResult<TaggedField[]>
      }

      if (this.remaining < sizeResult.value) {
        this.pos = startPos
        return decodeFailure(
          "BUFFER_UNDERFLOW",
          `need ${String(sizeResult.value)} bytes for tagged field`,
          startPos
        )
      }

      const data = this.buffer.slice(this.pos, this.pos + sizeResult.value)
      this.pos += sizeResult.value
      fields.push({ tag: tagResult.value, data })
    }

    return decodeSuccess(fields, this.pos - startPos)
  }

  // ---------------------------------------------------------------------------
  // Raw byte access
  // ---------------------------------------------------------------------------

  /**
   * Read `length` raw bytes as a copy.
   */
  readRawBytes(length: number): DecodeResult<Uint8Array> {
    if (this.remaining < length) {
      return decodeFailure("BUFFER_UNDERFLOW", `need ${String(length)} bytes`, this.pos)
    }
    const bytes = this.buffer.slice(this.pos, this.pos + length)
    this.pos += length
    return decodeSuccess(bytes, length)
  }
}
