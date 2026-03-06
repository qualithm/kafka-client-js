import fc from "fast-check"
import { describe, expect, it } from "vitest"

import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Write then read, returning the decoded value. */
function roundTrip<T>(
  write: (w: BinaryWriter) => void,
  read: (r: BinaryReader) => { ok: boolean; value?: T }
): T {
  const writer = new BinaryWriter()
  write(writer)
  const buf = writer.finish()
  const reader = new BinaryReader(buf)
  const result = read(reader)
  if (!result.ok) {
    throw new Error("round trip decode failed")
  }
  return result.value as T
}

// ---------------------------------------------------------------------------
// BinaryReader — bounds checking
// ---------------------------------------------------------------------------

describe("BinaryReader", () => {
  describe("constructor", () => {
    it("starts at offset 0 by default", () => {
      const reader = new BinaryReader(new Uint8Array(8))
      expect(reader.offset).toBe(0)
      expect(reader.remaining).toBe(8)
    })

    it("accepts an initial offset", () => {
      const reader = new BinaryReader(new Uint8Array(8), 3)
      expect(reader.offset).toBe(3)
      expect(reader.remaining).toBe(5)
    })
  })

  describe("fixed-width underflow", () => {
    const empty = new Uint8Array(0)

    it.each([
      ["readInt8", 1],
      ["readInt16", 2],
      ["readInt32", 4],
      ["readInt64", 8],
      ["readUint32", 4],
      ["readBoolean", 1],
      ["readFloat64", 8]
    ] as const)("%s fails on empty buffer", (method, _bytes) => {
      const reader = new BinaryReader(empty)
      const result = (reader[method] as () => { ok: boolean })()
      expect(result.ok).toBe(false)
    })

    it("readUuid fails with fewer than 16 bytes", () => {
      const reader = new BinaryReader(new Uint8Array(15))
      const result = reader.readUuid()
      expect(result.ok).toBe(false)
    })
  })

  describe("position does not advance on failure", () => {
    it("keeps position unchanged when int32 read fails", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const before = reader.offset
      reader.readInt32()
      expect(reader.offset).toBe(before)
    })

    it("keeps position unchanged when varint read fails", () => {
      // Buffer with only continuation bytes (never terminates)
      const reader = new BinaryReader(new Uint8Array([0x80, 0x80]))
      const before = reader.offset
      reader.readUnsignedVarInt()
      expect(reader.offset).toBe(before)
    })
  })

  describe("readRawBytes", () => {
    it("reads the requested number of bytes", () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      const reader = new BinaryReader(data)
      const result = reader.readRawBytes(3)
      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value).toEqual(new Uint8Array([1, 2, 3]))
        expect(result.bytesRead).toBe(3)
      }
      expect(reader.offset).toBe(3)
    })

    it("returns a copy, not a view", () => {
      const data = new Uint8Array([10, 20, 30])
      const reader = new BinaryReader(data)
      const result = reader.readRawBytes(3)
      if (result.ok) {
        result.value[0] = 99
        expect(data[0]).toBe(10) // original unchanged
      }
    })
  })
})

// ---------------------------------------------------------------------------
// BinaryWriter — growth and basics
// ---------------------------------------------------------------------------

describe("BinaryWriter", () => {
  it("starts at offset 0", () => {
    const writer = new BinaryWriter()
    expect(writer.offset).toBe(0)
  })

  it("finish returns exactly the written bytes", () => {
    const writer = new BinaryWriter()
    writer.writeInt8(1).writeInt8(2)
    const buf = writer.finish()
    expect(buf.byteLength).toBe(2)
    expect(buf[0]).toBe(1)
    expect(buf[1]).toBe(2)
  })

  it("grows the buffer when capacity is exceeded", () => {
    const writer = new BinaryWriter(4) // tiny initial capacity
    writer.writeInt32(1).writeInt32(2).writeInt32(3)
    const buf = writer.finish()
    expect(buf.byteLength).toBe(12)
  })

  it("reset allows buffer reuse", () => {
    const writer = new BinaryWriter()
    writer.writeInt32(42)
    expect(writer.offset).toBe(4)
    writer.reset()
    expect(writer.offset).toBe(0)
    writer.writeInt8(1)
    expect(writer.finish().byteLength).toBe(1)
  })
})

// ---------------------------------------------------------------------------
// Round-trip: fixed-width primitives
// ---------------------------------------------------------------------------

describe("round-trip: fixed-width primitives", () => {
  it("int8", () => {
    fc.assert(
      fc.property(fc.integer({ min: -128, max: 127 }), (n) => {
        const result = roundTrip<number>(
          (w) => w.writeInt8(n),
          (r) => r.readInt8()
        )
        expect(result).toBe(n)
      })
    )
  })

  it("int16", () => {
    fc.assert(
      fc.property(fc.integer({ min: -32768, max: 32767 }), (n) => {
        const result = roundTrip<number>(
          (w) => w.writeInt16(n),
          (r) => r.readInt16()
        )
        expect(result).toBe(n)
      })
    )
  })

  it("int32", () => {
    fc.assert(
      fc.property(fc.integer({ min: -2147483648, max: 2147483647 }), (n) => {
        const result = roundTrip<number>(
          (w) => w.writeInt32(n),
          (r) => r.readInt32()
        )
        expect(result).toBe(n)
      })
    )
  })

  it("int64", () => {
    fc.assert(
      fc.property(fc.bigInt({ min: -9223372036854775808n, max: 9223372036854775807n }), (n) => {
        const result = roundTrip<bigint>(
          (w) => w.writeInt64(n),
          (r) => r.readInt64()
        )
        expect(result).toBe(n)
      })
    )
  })

  it("uint32", () => {
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 4294967295 }), (n) => {
        const result = roundTrip<number>(
          (w) => w.writeUint32(n),
          (r) => r.readUint32()
        )
        expect(result).toBe(n)
      })
    )
  })

  it("boolean", () => {
    fc.assert(
      fc.property(fc.boolean(), (b) => {
        const result = roundTrip<boolean>(
          (w) => w.writeBoolean(b),
          (r) => r.readBoolean()
        )
        expect(result).toBe(b)
      })
    )
  })

  it("float64", () => {
    fc.assert(
      fc.property(fc.double({ noNaN: true }), (n) => {
        const result = roundTrip<number>(
          (w) => w.writeFloat64(n),
          (r) => r.readFloat64()
        )
        expect(result).toBe(n)
      })
    )
  })

  it("uuid (16 bytes)", () => {
    fc.assert(
      fc.property(fc.uint8Array({ minLength: 16, maxLength: 16 }), (uuid) => {
        const result = roundTrip<Uint8Array>(
          (w) => w.writeUuid(uuid),
          (r) => r.readUuid()
        )
        expect(result).toEqual(uuid)
      })
    )
  })
})

// ---------------------------------------------------------------------------
// Round-trip: variable-length integers
// ---------------------------------------------------------------------------

describe("round-trip: varints", () => {
  it("unsigned varint", () => {
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 4294967295 }), (n) => {
        const result = roundTrip<number>(
          (w) => w.writeUnsignedVarInt(n),
          (r) => r.readUnsignedVarInt()
        )
        expect(result).toBe(n)
      })
    )
  })

  it("signed varint (zigzag)", () => {
    fc.assert(
      fc.property(fc.integer({ min: -2147483648, max: 2147483647 }), (n) => {
        const result = roundTrip<number>(
          (w) => w.writeSignedVarInt(n),
          (r) => r.readSignedVarInt()
        )
        expect(result).toBe(n)
      })
    )
  })

  it("unsigned varlong", () => {
    fc.assert(
      fc.property(fc.bigInt({ min: 0n, max: 18446744073709551615n }), (n) => {
        const result = roundTrip<bigint>(
          (w) => w.writeUnsignedVarLong(n),
          (r) => r.readUnsignedVarLong()
        )
        expect(result).toBe(n)
      })
    )
  })

  it("signed varlong (zigzag)", () => {
    fc.assert(
      fc.property(fc.bigInt({ min: -9223372036854775808n, max: 9223372036854775807n }), (n) => {
        const result = roundTrip<bigint>(
          (w) => w.writeSignedVarLong(n),
          (r) => r.readSignedVarLong()
        )
        expect(result).toBe(n)
      })
    )
  })

  describe("unsigned varint known values", () => {
    it.each([
      [0, [0x00]],
      [1, [0x01]],
      [127, [0x7f]],
      [128, [0x80, 0x01]],
      [16383, [0xff, 0x7f]],
      [16384, [0x80, 0x80, 0x01]],
      [300, [0xac, 0x02]]
    ])("encodes %i correctly", (value, expectedBytes) => {
      const writer = new BinaryWriter()
      writer.writeUnsignedVarInt(value)
      expect(Array.from(writer.finish())).toEqual(expectedBytes)
    })
  })

  describe("signed varint known zigzag values", () => {
    it.each([
      [0, 0],
      [-1, 1],
      [1, 2],
      [-2, 3],
      [2, 4],
      [2147483647, 4294967294],
      [-2147483648, 4294967295]
    ])("zigzag(%i) = %i", (signed, expectedUnsigned) => {
      const writer = new BinaryWriter()
      writer.writeSignedVarInt(signed)
      const buf = writer.finish()

      const reader = new BinaryReader(buf)
      const rawResult = reader.readUnsignedVarInt()
      expect(rawResult.ok).toBe(true)
      if (rawResult.ok) {
        expect(rawResult.value).toBe(expectedUnsigned)
      }
    })
  })

  it("rejects varint exceeding 5 bytes", () => {
    const bytes = new Uint8Array([0x80, 0x80, 0x80, 0x80, 0x80, 0x01])
    const reader = new BinaryReader(bytes)
    const result = reader.readUnsignedVarInt()
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("INVALID_VARINT")
    }
  })

  it("rejects varlong exceeding 10 bytes", () => {
    const bytes = new Uint8Array(11).fill(0x80)
    bytes[10] = 0x01
    const reader = new BinaryReader(bytes)
    const result = reader.readUnsignedVarLong()
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("INVALID_VARINT")
    }
  })
})

// ---------------------------------------------------------------------------
// Round-trip: strings
// ---------------------------------------------------------------------------

describe("round-trip: strings", () => {
  it("nullable string (non-flexible)", () => {
    fc.assert(
      fc.property(fc.option(fc.string(), { nil: null }), (s) => {
        const result = roundTrip<string | null>(
          (w) => w.writeString(s),
          (r) => r.readString()
        )
        expect(result).toBe(s)
      })
    )
  })

  it("compact string (flexible)", () => {
    fc.assert(
      fc.property(fc.option(fc.string(), { nil: null }), (s) => {
        const result = roundTrip<string | null>(
          (w) => w.writeCompactString(s),
          (r) => r.readCompactString()
        )
        expect(result).toBe(s)
      })
    )
  })

  it("reads empty string correctly (non-flexible)", () => {
    const result = roundTrip<string | null>(
      (w) => w.writeString(""),
      (r) => r.readString()
    )
    expect(result).toBe("")
  })

  it("reads empty string correctly (flexible)", () => {
    const result = roundTrip<string | null>(
      (w) => w.writeCompactString(""),
      (r) => r.readCompactString()
    )
    expect(result).toBe("")
  })

  it("handles multi-byte UTF-8 characters", () => {
    const str = "café ☕ 你好"
    const result = roundTrip<string | null>(
      (w) => w.writeString(str),
      (r) => r.readString()
    )
    expect(result).toBe(str)
  })

  it("rejects invalid string length", () => {
    const writer = new BinaryWriter()
    writer.writeInt16(-2) // invalid: only -1 is allowed for null
    const reader = new BinaryReader(writer.finish())
    const result = reader.readString()
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("INVALID_DATA")
    }
  })

  it("fails when string body is truncated", () => {
    const writer = new BinaryWriter()
    writer.writeInt16(10) // claims 10 bytes follow
    writer.writeRawBytes(new Uint8Array(3)) // only 3 bytes
    const reader = new BinaryReader(writer.finish())
    const result = reader.readString()
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("BUFFER_UNDERFLOW")
    }
    expect(reader.offset).toBe(0)
  })

  it("fails when compact string body is truncated", () => {
    const writer = new BinaryWriter()
    writer.writeUnsignedVarInt(11) // length + 1 = 11, so 10 bytes expected
    writer.writeRawBytes(new Uint8Array(3)) // only 3 bytes
    const reader = new BinaryReader(writer.finish())
    const result = reader.readCompactString()
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("BUFFER_UNDERFLOW")
    }
    expect(reader.offset).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Round-trip: bytes
// ---------------------------------------------------------------------------

describe("round-trip: bytes", () => {
  it("nullable bytes (non-flexible)", () => {
    fc.assert(
      fc.property(
        fc.option(fc.uint8Array({ minLength: 0, maxLength: 200 }), { nil: null }),
        (b) => {
          const result = roundTrip<Uint8Array | null>(
            (w) => w.writeBytes(b),
            (r) => r.readBytes()
          )
          if (b === null) {
            expect(result).toBeNull()
          } else {
            expect(result).toEqual(b)
          }
        }
      )
    )
  })

  it("compact bytes (flexible)", () => {
    fc.assert(
      fc.property(
        fc.option(fc.uint8Array({ minLength: 0, maxLength: 200 }), { nil: null }),
        (b) => {
          const result = roundTrip<Uint8Array | null>(
            (w) => w.writeCompactBytes(b),
            (r) => r.readCompactBytes()
          )
          if (b === null) {
            expect(result).toBeNull()
          } else {
            expect(result).toEqual(b)
          }
        }
      )
    )
  })

  it("reads empty bytes correctly", () => {
    const result = roundTrip<Uint8Array | null>(
      (w) => w.writeBytes(new Uint8Array(0)),
      (r) => r.readBytes()
    )
    expect(result).toEqual(new Uint8Array(0))
  })

  it("returned bytes are a copy", () => {
    const writer = new BinaryWriter()
    writer.writeBytes(new Uint8Array([1, 2, 3]))
    const buf = writer.finish()
    const reader = new BinaryReader(buf)
    const result = reader.readBytes()
    if (result.ok && result.value !== null) {
      result.value[0] = 99
      // the source buffer should be unchanged at the data offset (after length prefix)
      expect(buf[4]).toBe(1)
    }
  })

  it("rejects invalid bytes length", () => {
    const writer = new BinaryWriter()
    writer.writeInt32(-2)
    const reader = new BinaryReader(writer.finish())
    const result = reader.readBytes()
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("INVALID_DATA")
    }
  })

  it("fails when bytes body is truncated", () => {
    const writer = new BinaryWriter()
    writer.writeInt32(10) // claims 10 bytes follow
    writer.writeRawBytes(new Uint8Array(3)) // only 3 bytes
    const reader = new BinaryReader(writer.finish())
    const result = reader.readBytes()
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("BUFFER_UNDERFLOW")
    }
    expect(reader.offset).toBe(0)
  })

  it("fails when compact bytes body is truncated", () => {
    const writer = new BinaryWriter()
    writer.writeUnsignedVarInt(11) // length + 1 = 11, so 10 bytes expected
    writer.writeRawBytes(new Uint8Array(3))
    const reader = new BinaryReader(writer.finish())
    const result = reader.readCompactBytes()
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("BUFFER_UNDERFLOW")
    }
    expect(reader.offset).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Round-trip: arrays
// ---------------------------------------------------------------------------

describe("round-trip: arrays", () => {
  it("nullable int32 array (non-flexible)", () => {
    fc.assert(
      fc.property(
        fc.option(fc.array(fc.integer({ min: -2147483648, max: 2147483647 }), { maxLength: 50 }), {
          nil: null
        }),
        (arr) => {
          const result = roundTrip<number[] | null>(
            (w) => w.writeArray(arr, (writer, v) => writer.writeInt32(v)),
            (r) => r.readArray((reader) => reader.readInt32())
          )
          if (arr === null) {
            expect(result).toBeNull()
          } else {
            expect(result).toEqual(arr)
          }
        }
      )
    )
  })

  it("compact int32 array (flexible)", () => {
    fc.assert(
      fc.property(
        fc.option(fc.array(fc.integer({ min: -2147483648, max: 2147483647 }), { maxLength: 50 }), {
          nil: null
        }),
        (arr) => {
          const result = roundTrip<number[] | null>(
            (w) => w.writeCompactArray(arr, (writer, v) => writer.writeInt32(v)),
            (r) => r.readCompactArray((reader) => reader.readInt32())
          )
          if (arr === null) {
            expect(result).toBeNull()
          } else {
            expect(result).toEqual(arr)
          }
        }
      )
    )
  })

  it("reads empty array correctly", () => {
    const result = roundTrip<number[] | null>(
      (w) => w.writeArray([], (writer, v) => writer.writeInt32(v)),
      (r) => r.readArray((reader) => reader.readInt32())
    )
    expect(result).toEqual([])
  })

  it("nested string arrays", () => {
    const input = ["hello", "world", "kafka"]
    const result = roundTrip<(string | null)[] | null>(
      (w) => w.writeArray(input, (writer, v) => writer.writeString(v)),
      (r) => r.readArray((reader) => reader.readString())
    )
    expect(result).toEqual(input)
  })

  it("rejects invalid array count", () => {
    const writer = new BinaryWriter()
    writer.writeInt32(-2)
    const reader = new BinaryReader(writer.finish())
    const result = reader.readArray((r) => r.readInt32())
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("INVALID_DATA")
    }
  })

  it("fails when array element read fails mid-array", () => {
    const writer = new BinaryWriter()
    writer.writeInt32(3) // count = 3 elements
    writer.writeInt32(1) // element 0
    writer.writeInt32(2) // element 1
    // element 2 missing
    const reader = new BinaryReader(writer.finish())
    const result = reader.readArray((r) => r.readInt32())
    expect(result.ok).toBe(false)
    expect(reader.offset).toBe(0)
  })

  it("fails when compact array element read fails mid-array", () => {
    const writer = new BinaryWriter()
    writer.writeUnsignedVarInt(4) // count + 1 = 4, so 3 elements
    writer.writeInt32(1) // element 0
    // elements 1..2 missing
    const reader = new BinaryReader(writer.finish())
    const result = reader.readCompactArray((r) => r.readInt32())
    expect(result.ok).toBe(false)
    expect(reader.offset).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Tagged fields (KIP-482)
// ---------------------------------------------------------------------------

describe("round-trip: tagged fields", () => {
  it("empty tagged fields", () => {
    const result = roundTrip<{ tag: number; data: Uint8Array }[]>(
      (w) => w.writeTaggedFields([]),
      (r) => r.readTaggedFields()
    )
    expect(result).toEqual([])
  })

  it("single tagged field", () => {
    const field = { tag: 0, data: new Uint8Array([0xca, 0xfe]) }
    const result = roundTrip<{ tag: number; data: Uint8Array }[]>(
      (w) => w.writeTaggedFields([field]),
      (r) => r.readTaggedFields()
    )
    expect(result).toHaveLength(1)
    expect(result[0]?.tag).toBe(0)
    expect(result[0]?.data).toEqual(new Uint8Array([0xca, 0xfe]))
  })

  it("multiple tagged fields", () => {
    fc.assert(
      fc.property(
        fc.array(
          fc.record({
            tag: fc.integer({ min: 0, max: 1000 }),
            data: fc.uint8Array({ minLength: 0, maxLength: 50 })
          }),
          { maxLength: 10 }
        ),
        (fields) => {
          const result = roundTrip<{ tag: number; data: Uint8Array }[]>(
            (w) => w.writeTaggedFields(fields),
            (r) => r.readTaggedFields()
          )
          expect(result).toHaveLength(fields.length)
          for (let i = 0; i < fields.length; i++) {
            expect(result[i]?.tag).toBe(fields[i]?.tag)
            expect(result[i]?.data).toEqual(fields[i]?.data)
          }
        }
      )
    )
  })

  it("tagged field data is a copy", () => {
    const writer = new BinaryWriter()
    writer.writeTaggedFields([{ tag: 1, data: new Uint8Array([0xab]) }])
    const buf = writer.finish()
    const reader = new BinaryReader(buf)
    const result = reader.readTaggedFields()
    if (result.ok) {
      result.value[0].data[0] = 0xff
      // original buffer unmodified at the data byte position
      // (varint count=1, varint tag=1, varint size=1, data byte)
      expect(buf[3]).toBe(0xab)
    }
  })

  it("fails when tag varint is truncated", () => {
    // count=2 then one complete field, then truncated tag
    const writer = new BinaryWriter()
    writer.writeUnsignedVarInt(2) // 2 fields
    writer.writeUnsignedVarInt(0) // field 0 tag
    writer.writeUnsignedVarInt(1) // field 0 size = 1
    writer.writeRawBytes(new Uint8Array([0xff])) // field 0 data
    // field 1 tag missing
    const reader = new BinaryReader(writer.finish())
    const result = reader.readTaggedFields()
    expect(result.ok).toBe(false)
    expect(reader.offset).toBe(0)
  })

  it("fails when size varint is truncated", () => {
    const writer = new BinaryWriter()
    writer.writeUnsignedVarInt(1) // 1 field
    writer.writeUnsignedVarInt(5) // tag = 5
    // size missing
    const reader = new BinaryReader(writer.finish())
    const result = reader.readTaggedFields()
    expect(result.ok).toBe(false)
    expect(reader.offset).toBe(0)
  })

  it("fails when field data is truncated", () => {
    const writer = new BinaryWriter()
    writer.writeUnsignedVarInt(1) // 1 field
    writer.writeUnsignedVarInt(0) // tag = 0
    writer.writeUnsignedVarInt(10) // size = 10 bytes
    writer.writeRawBytes(new Uint8Array(3)) // only 3 bytes
    const reader = new BinaryReader(writer.finish())
    const result = reader.readTaggedFields()
    expect(result.ok).toBe(false)
    if (!result.ok) {
      expect(result.error.code).toBe("BUFFER_UNDERFLOW")
    }
    expect(reader.offset).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Sequential multi-field reads
// ---------------------------------------------------------------------------

describe("sequential reads", () => {
  it("reads multiple fields in order", () => {
    const writer = new BinaryWriter()
    writer.writeInt16(18).writeInt32(42).writeString("hello").writeBoolean(true)
    const buf = writer.finish()

    const reader = new BinaryReader(buf)
    const a = reader.readInt16()
    const b = reader.readInt32()
    const c = reader.readString()
    const d = reader.readBoolean()

    expect(a.ok && a.value).toBe(18)
    expect(b.ok && b.value).toBe(42)
    expect(c.ok && c.value).toBe("hello")
    expect(d.ok && d.value).toBe(true)
    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Buffer view offset handling
// ---------------------------------------------------------------------------

describe("buffer with byteOffset", () => {
  it("reads correctly from a subarray", () => {
    const base = new Uint8Array(16)
    const view = new DataView(base.buffer)
    view.setInt32(4, 42)

    const sub = base.subarray(4, 8)
    const reader = new BinaryReader(sub)
    const result = reader.readInt32()
    expect(result.ok).toBe(true)
    if (result.ok) {
      expect(result.value).toBe(42)
    }
  })
})
