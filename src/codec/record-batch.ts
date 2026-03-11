/**
 * RecordBatch v2 (magic=2) encoding and decoding for the Kafka protocol.
 *
 * References:
 * - KIP-98: Exactly Once Delivery and Transactional Messaging
 * - Kafka protocol documentation: RecordBatch
 *
 * @packageDocumentation
 */

import type { MessageHeader } from "../messages.js"
import { decodeFailure, type DecodeResult, decodeSuccess } from "../result.js"
import { BinaryReader } from "./binary-reader.js"
import { BinaryWriter } from "./binary-writer.js"

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Magic byte for RecordBatch v2 format (introduced in Kafka 0.11). */
export const RECORD_BATCH_MAGIC = 2

/** Header size before the records data: baseOffset(8) + batchLength(4). */
export const RECORD_BATCH_HEADER_OVERHEAD = 12

/** Size of fixed fields after batchLength: partitionLeaderEpoch(4) + magic(1) + crc(4) +
 *  attributes(2) + lastOffsetDelta(4) + baseTimestamp(8) + maxTimestamp(8) +
 *  producerId(8) + producerEpoch(2) + baseSequence(4) + recordsCount(4). */
export const RECORD_BATCH_METADATA_SIZE = 49

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

/**
 * Compression codec identifiers.
 * Stored in the lower 3 bits of the batch attributes.
 */
export const CompressionCodec = {
  NONE: 0,
  GZIP: 1,
  SNAPPY: 2,
  LZ4: 3,
  ZSTD: 4
} as const

export type CompressionCodec = (typeof CompressionCodec)[keyof typeof CompressionCodec]

/**
 * Compression/decompression interface for record batch data.
 */
export type CompressionProvider = {
  /** Compress data synchronously. */
  compress: (data: Uint8Array) => Uint8Array
  /** Decompress data synchronously. */
  decompress: (data: Uint8Array) => Uint8Array
}

// ---------------------------------------------------------------------------
// Timestamp Type
// ---------------------------------------------------------------------------

/**
 * Timestamp type stored in bit 3 of batch attributes.
 */
export const TimestampType = {
  CREATE_TIME: 0,
  LOG_APPEND_TIME: 1
} as const

export type TimestampType = (typeof TimestampType)[keyof typeof TimestampType]

// ---------------------------------------------------------------------------
// Record Types
// ---------------------------------------------------------------------------

/**
 * A single record within a RecordBatch.
 * Represents the logical message with deltas computed relative to the batch.
 */
export type Record = {
  /** Record attributes (currently unused, reserved for future use). */
  readonly attributes: number
  /** Millisecond delta from the batch's base timestamp. */
  readonly timestampDelta: bigint
  /** Offset delta from the batch's base offset. */
  readonly offsetDelta: number
  /** Message key, or null if no key. */
  readonly key: Uint8Array | null
  /** Message value, or null for tombstones. */
  readonly value: Uint8Array | null
  /** Record headers. */
  readonly headers: readonly MessageHeader[]
}

/**
 * RecordBatch attributes bit flags.
 */
export type RecordBatchAttributes = {
  /** Compression codec (0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd). */
  readonly compression: CompressionCodec
  /** Timestamp type (0=CreateTime, 1=LogAppendTime). */
  readonly timestampType: TimestampType
  /** Whether this is a transactional batch. */
  readonly isTransactional: boolean
  /** Whether this is a control batch (e.g., transaction markers). */
  readonly isControlBatch: boolean
  /** Whether delete horizon is set (KIP-817). */
  readonly hasDeleteHorizon: boolean
}

/**
 * A Kafka RecordBatch (magic=2).
 *
 * This is the on-wire format for batches of records in Kafka 0.11+.
 */
export type RecordBatch = {
  /** Base offset in the partition log. */
  readonly baseOffset: bigint
  /** Partition leader epoch at the time of append. */
  readonly partitionLeaderEpoch: number
  /** Magic byte (always 2 for this format). */
  readonly magic: number
  /** CRC-32C of the record batch data (from attributes through records). */
  readonly crc: number
  /** Batch attributes (compression, timestamp type, transactional). */
  readonly attributes: RecordBatchAttributes
  /** Offset delta of the last record in the batch. */
  readonly lastOffsetDelta: number
  /** Base timestamp (milliseconds since epoch). */
  readonly baseTimestamp: bigint
  /** Max timestamp in the batch. */
  readonly maxTimestamp: bigint
  /** Producer ID for idempotent/transactional producers, or -1 if none. */
  readonly producerId: bigint
  /** Producer epoch, or -1 if not using idempotent producer. */
  readonly producerEpoch: number
  /** Base sequence number, or -1 if not using idempotent producer. */
  readonly baseSequence: number
  /** Records in this batch. */
  readonly records: readonly Record[]
}

/**
 * Options for building a RecordBatch.
 */
export type RecordBatchOptions = {
  /** Base offset for the batch (default: 0). */
  baseOffset?: bigint
  /** Partition leader epoch (default: -1). */
  partitionLeaderEpoch?: number
  /** Base timestamp in milliseconds (default: Date.now()). */
  baseTimestamp?: bigint
  /** Producer ID for idempotent producers (default: -1). */
  producerId?: bigint
  /** Producer epoch (default: -1). */
  producerEpoch?: number
  /** Base sequence number (default: -1). */
  baseSequence?: number
  /** Compression codec (default: NONE). */
  compression?: CompressionCodec
  /** Timestamp type (default: CREATE_TIME). */
  timestampType?: TimestampType
  /** Whether this is a transactional batch (default: false). */
  isTransactional?: boolean
  /** Whether this is a control batch (default: false). */
  isControlBatch?: boolean
}

// ---------------------------------------------------------------------------
// Attribute Encoding/Decoding
// ---------------------------------------------------------------------------

/**
 * Encode batch attributes to a 16-bit integer.
 */
export function encodeAttributes(attrs: RecordBatchAttributes): number {
  let value = attrs.compression & 0x07
  if (attrs.timestampType === TimestampType.LOG_APPEND_TIME) {
    value |= 0x08
  }
  if (attrs.isTransactional) {
    value |= 0x10
  }
  if (attrs.isControlBatch) {
    value |= 0x20
  }
  if (attrs.hasDeleteHorizon) {
    value |= 0x40
  }
  return value
}

/**
 * Decode batch attributes from a 16-bit integer.
 */
export function decodeAttributes(value: number): RecordBatchAttributes {
  return {
    compression: (value & 0x07) as CompressionCodec,
    timestampType: ((value >> 3) & 0x01) as TimestampType,
    isTransactional: (value & 0x10) !== 0,
    isControlBatch: (value & 0x20) !== 0,
    hasDeleteHorizon: (value & 0x40) !== 0
  }
}

// ---------------------------------------------------------------------------
// CRC-32C Implementation
// ---------------------------------------------------------------------------

// CRC-32C lookup table (Castagnoli polynomial: 0x1EDC6F41)
const CRC32C_TABLE = (() => {
  const POLYNOMIAL = 0x82f63b78
  const table = new Uint32Array(256)
  for (let i = 0; i < 256; i++) {
    let crc = i
    for (let j = 0; j < 8; j++) {
      crc = (crc >>> 1) ^ (crc & 1 ? POLYNOMIAL : 0)
    }
    table[i] = crc
  }
  return table
})()

/**
 * Compute CRC-32C (Castagnoli) checksum.
 *
 * Kafka uses CRC-32C for record batch validation per KIP-98.
 */
export function crc32c(data: Uint8Array): number {
  let crc = 0xffffffff
  for (let i = 0; i < data.byteLength; i++) {
    crc = CRC32C_TABLE[(crc ^ data[i]) & 0xff] ^ (crc >>> 8)
  }
  return (crc ^ 0xffffffff) >>> 0
}

// ---------------------------------------------------------------------------
// Record Encoding
// ---------------------------------------------------------------------------

/**
 * Encode a single record (without the length prefix).
 * Returns the encoded bytes.
 */
function encodeRecordBody(record: Record): Uint8Array {
  const writer = new BinaryWriter()

  // attributes (int8, currently unused)
  writer.writeInt8(record.attributes)

  // timestampDelta (varlong)
  writer.writeSignedVarLong(record.timestampDelta)

  // offsetDelta (varint)
  writer.writeSignedVarInt(record.offsetDelta)

  // key (varint length + bytes, -1 for null)
  if (record.key === null) {
    writer.writeSignedVarInt(-1)
  } else {
    writer.writeSignedVarInt(record.key.byteLength)
    writer.writeRawBytes(record.key)
  }

  // value (varint length + bytes, -1 for null)
  if (record.value === null) {
    writer.writeSignedVarInt(-1)
  } else {
    writer.writeSignedVarInt(record.value.byteLength)
    writer.writeRawBytes(record.value)
  }

  // headers count (varint)
  writer.writeSignedVarInt(record.headers.length)

  // headers
  for (const header of record.headers) {
    // header key (varint length + string bytes)
    const keyBytes = new TextEncoder().encode(header.key)
    writer.writeSignedVarInt(keyBytes.byteLength)
    writer.writeRawBytes(keyBytes)

    // header value (varint length + bytes, -1 for null)
    if (header.value === null) {
      writer.writeSignedVarInt(-1)
    } else {
      writer.writeSignedVarInt(header.value.byteLength)
      writer.writeRawBytes(header.value)
    }
  }

  return writer.finish()
}

/**
 * Encode a single record with its varint length prefix.
 */
function encodeRecord(record: Record): Uint8Array {
  const body = encodeRecordBody(record)
  const writer = new BinaryWriter()
  writer.writeSignedVarInt(body.byteLength)
  writer.writeRawBytes(body)
  return writer.finish()
}

// ---------------------------------------------------------------------------
// Record Decoding Helpers
// ---------------------------------------------------------------------------

const TEXT_DECODER = new TextDecoder()

/**
 * Decode nullable bytes (varint length + bytes, -1 for null).
 */
function decodeNullableBytes(reader: BinaryReader): DecodeResult<Uint8Array | null> {
  const lenResult = reader.readSignedVarInt()
  if (!lenResult.ok) {
    return lenResult
  }
  if (lenResult.value < 0) {
    return decodeSuccess(null, lenResult.bytesRead)
  }
  const bytesResult = reader.readRawBytes(lenResult.value)
  if (!bytesResult.ok) {
    return bytesResult
  }
  return decodeSuccess(bytesResult.value, lenResult.bytesRead + bytesResult.bytesRead)
}

/**
 * Decode a single record header (key + nullable value).
 */
function decodeRecordHeader(reader: BinaryReader): DecodeResult<MessageHeader> {
  const startPos = reader.offset

  // header key (varint length + string, cannot be null)
  const keyLenResult = reader.readSignedVarInt()
  if (!keyLenResult.ok) {
    return keyLenResult
  }
  if (keyLenResult.value < 0) {
    return decodeFailure("INVALID_DATA", "header key cannot be null", reader.offset)
  }
  const keyBytesResult = reader.readRawBytes(keyLenResult.value)
  if (!keyBytesResult.ok) {
    return keyBytesResult
  }
  const key = TEXT_DECODER.decode(keyBytesResult.value)

  // header value (nullable bytes)
  const valueResult = decodeNullableBytes(reader)
  if (!valueResult.ok) {
    return valueResult
  }

  return decodeSuccess({ key, value: valueResult.value }, reader.offset - startPos)
}

/**
 * Decode a single record from the reader.
 * The reader must be positioned at the record's length prefix.
 */
function decodeRecord(reader: BinaryReader): DecodeResult<Record> {
  const startPos = reader.offset

  // Record length (varint)
  const lengthResult = reader.readSignedVarInt()
  if (!lengthResult.ok) {
    return lengthResult
  }

  const recordLength = lengthResult.value
  if (recordLength < 0) {
    return decodeFailure("INVALID_DATA", `invalid record length: ${String(recordLength)}`, startPos)
  }

  // Check we have enough bytes for the record
  if (reader.remaining < recordLength) {
    return decodeFailure(
      "BUFFER_UNDERFLOW",
      `need ${String(recordLength)} bytes for record`,
      reader.offset
    )
  }

  // attributes (int8)
  const attrResult = reader.readInt8()
  if (!attrResult.ok) {
    return attrResult
  }

  // timestampDelta (varlong)
  const timestampResult = reader.readSignedVarLong()
  if (!timestampResult.ok) {
    return timestampResult
  }

  // offsetDelta (varint)
  const offsetResult = reader.readSignedVarInt()
  if (!offsetResult.ok) {
    return offsetResult
  }

  // key (nullable bytes)
  const keyResult = decodeNullableBytes(reader)
  if (!keyResult.ok) {
    return keyResult
  }

  // value (nullable bytes)
  const valueResult = decodeNullableBytes(reader)
  if (!valueResult.ok) {
    return valueResult
  }

  // headers count (varint)
  const headersCountResult = reader.readSignedVarInt()
  if (!headersCountResult.ok) {
    return headersCountResult
  }
  const headersCount = headersCountResult.value

  if (headersCount < 0) {
    return decodeFailure(
      "INVALID_DATA",
      `invalid headers count: ${String(headersCount)}`,
      reader.offset
    )
  }

  // Decode headers
  const headers: MessageHeader[] = []
  for (let i = 0; i < headersCount; i++) {
    const headerResult = decodeRecordHeader(reader)
    if (!headerResult.ok) {
      return headerResult
    }
    headers.push(headerResult.value)
  }

  return decodeSuccess(
    {
      attributes: attrResult.value,
      timestampDelta: timestampResult.value,
      offsetDelta: offsetResult.value,
      key: keyResult.value,
      value: valueResult.value,
      headers
    },
    reader.offset - startPos
  )
}

// ---------------------------------------------------------------------------
// RecordBatch Encoding
// ---------------------------------------------------------------------------

/**
 * Registry of compression providers by codec.
 */
const compressionProviders = new Map<CompressionCodec, CompressionProvider>()

/**
 * Register a compression provider for a codec.
 */
export function registerCompressionProvider(
  codec: CompressionCodec,
  provider: CompressionProvider
): void {
  compressionProviders.set(codec, provider)
}

/**
 * Check if a compression provider is registered.
 */
export function hasCompressionProvider(codec: CompressionCodec): boolean {
  return codec === CompressionCodec.NONE || compressionProviders.has(codec)
}

/**
 * Encode a RecordBatch to bytes.
 *
 * @param batch - The record batch to encode.
 * @returns The encoded bytes.
 * @throws Error if compression codec is not registered.
 */
export function encodeRecordBatch(batch: RecordBatch): Uint8Array {
  const { compression } = batch.attributes

  // Encode all records
  const recordsWriter = new BinaryWriter()
  for (const record of batch.records) {
    recordsWriter.writeRawBytes(encodeRecord(record))
  }
  let recordsData = recordsWriter.finish()

  // Compress if needed
  if (compression !== CompressionCodec.NONE) {
    const provider = compressionProviders.get(compression)
    if (!provider) {
      throw new Error(`compression codec ${String(compression)} not registered`)
    }
    recordsData = provider.compress(recordsData)
  }

  // Build the batch body (from attributes through records)
  const bodyWriter = new BinaryWriter()
  bodyWriter.writeInt16(encodeAttributes(batch.attributes))
  bodyWriter.writeInt32(batch.lastOffsetDelta)
  bodyWriter.writeInt64(batch.baseTimestamp)
  bodyWriter.writeInt64(batch.maxTimestamp)
  bodyWriter.writeInt64(batch.producerId)
  bodyWriter.writeInt16(batch.producerEpoch)
  bodyWriter.writeInt32(batch.baseSequence)
  bodyWriter.writeInt32(batch.records.length)
  bodyWriter.writeRawBytes(recordsData)

  const body = bodyWriter.finish()

  // Compute CRC-32C of the body
  const crc = crc32c(body)

  // Write the full batch
  const writer = new BinaryWriter()
  writer.writeInt64(batch.baseOffset)
  // batchLength: everything after this field
  const batchLength = 4 + 1 + 4 + body.byteLength // partitionLeaderEpoch + magic + crc + body
  writer.writeInt32(batchLength)
  writer.writeInt32(batch.partitionLeaderEpoch)
  writer.writeInt8(RECORD_BATCH_MAGIC)
  writer.writeUint32(crc)
  writer.writeRawBytes(body)

  return writer.finish()
}

/**
 * Build a RecordBatch from records with the given options.
 *
 * Computes lastOffsetDelta and maxTimestamp automatically.
 */
export function buildRecordBatch(
  records: readonly Record[],
  options: RecordBatchOptions = {}
): RecordBatch {
  const baseTimestamp = options.baseTimestamp ?? BigInt(Date.now())

  // Compute lastOffsetDelta and maxTimestamp from records
  let lastOffsetDelta = 0
  let maxTimestamp = baseTimestamp
  for (const record of records) {
    if (record.offsetDelta > lastOffsetDelta) {
      lastOffsetDelta = record.offsetDelta
    }
    const recordTimestamp = baseTimestamp + record.timestampDelta
    if (recordTimestamp > maxTimestamp) {
      maxTimestamp = recordTimestamp
    }
  }

  return {
    baseOffset: options.baseOffset ?? 0n,
    partitionLeaderEpoch: options.partitionLeaderEpoch ?? -1,
    magic: RECORD_BATCH_MAGIC,
    crc: 0, // Will be computed during encoding
    attributes: {
      compression: options.compression ?? CompressionCodec.NONE,
      timestampType: options.timestampType ?? TimestampType.CREATE_TIME,
      isTransactional: options.isTransactional ?? false,
      isControlBatch: options.isControlBatch ?? false,
      hasDeleteHorizon: false
    },
    lastOffsetDelta,
    baseTimestamp,
    maxTimestamp,
    producerId: options.producerId ?? -1n,
    producerEpoch: options.producerEpoch ?? -1,
    baseSequence: options.baseSequence ?? -1,
    records
  }
}

/**
 * Create a Record from a simple key/value/headers.
 */
export function createRecord(
  key: Uint8Array | null,
  value: Uint8Array | null,
  headers: readonly MessageHeader[] = [],
  offsetDelta = 0,
  timestampDelta = 0n
): Record {
  return {
    attributes: 0,
    timestampDelta,
    offsetDelta,
    key,
    value,
    headers
  }
}

// ---------------------------------------------------------------------------
// RecordBatch Decoding Helpers
// ---------------------------------------------------------------------------

/**
 * Batch metadata fields decoded from the record batch.
 */
type BatchMetadata = {
  attributes: RecordBatchAttributes
  lastOffsetDelta: number
  baseTimestamp: bigint
  maxTimestamp: bigint
  producerId: bigint
  producerEpoch: number
  baseSequence: number
  recordsCount: number
}

/**
 * Decode batch metadata fields after CRC.
 */
function decodeBatchMetadata(reader: BinaryReader): DecodeResult<BatchMetadata> {
  const startPos = reader.offset

  // attributes (int16)
  const attributesResult = reader.readInt16()
  if (!attributesResult.ok) {
    return attributesResult
  }

  // lastOffsetDelta (int32)
  const lastOffsetDeltaResult = reader.readInt32()
  if (!lastOffsetDeltaResult.ok) {
    return lastOffsetDeltaResult
  }

  // baseTimestamp (int64)
  const baseTimestampResult = reader.readInt64()
  if (!baseTimestampResult.ok) {
    return baseTimestampResult
  }

  // maxTimestamp (int64)
  const maxTimestampResult = reader.readInt64()
  if (!maxTimestampResult.ok) {
    return maxTimestampResult
  }

  // producerId (int64)
  const producerIdResult = reader.readInt64()
  if (!producerIdResult.ok) {
    return producerIdResult
  }

  // producerEpoch (int16)
  const producerEpochResult = reader.readInt16()
  if (!producerEpochResult.ok) {
    return producerEpochResult
  }

  // baseSequence (int32)
  const baseSequenceResult = reader.readInt32()
  if (!baseSequenceResult.ok) {
    return baseSequenceResult
  }

  // recordsCount (int32)
  const recordsCountResult = reader.readInt32()
  if (!recordsCountResult.ok) {
    return recordsCountResult
  }

  if (recordsCountResult.value < 0) {
    return decodeFailure(
      "INVALID_DATA",
      `invalid records count: ${String(recordsCountResult.value)}`,
      reader.offset
    )
  }

  return decodeSuccess(
    {
      attributes: decodeAttributes(attributesResult.value),
      lastOffsetDelta: lastOffsetDeltaResult.value,
      baseTimestamp: baseTimestampResult.value,
      maxTimestamp: maxTimestampResult.value,
      producerId: producerIdResult.value,
      producerEpoch: producerEpochResult.value,
      baseSequence: baseSequenceResult.value,
      recordsCount: recordsCountResult.value
    },
    reader.offset - startPos
  )
}

/**
 * Validate CRC-32C checksum of batch data.
 */
function validateBatchCrc(
  data: Uint8Array,
  batchLength: number,
  expectedCrc: number,
  crcDataStart: number
): DecodeResult<void> {
  const crcData = data.subarray(
    RECORD_BATCH_HEADER_OVERHEAD + 4 + 1 + 4, // baseOffset + batchLength + partitionLeaderEpoch + magic + crc
    RECORD_BATCH_HEADER_OVERHEAD + batchLength
  )
  const actualCrc = crc32c(crcData)
  if (actualCrc !== expectedCrc) {
    return decodeFailure(
      "CRC_MISMATCH",
      `crc mismatch: expected ${String(expectedCrc)}, got ${String(actualCrc)}`,
      crcDataStart
    )
  }
  return decodeSuccess(undefined, 0)
}

/**
 * Decompress records data if needed.
 */
function decompressRecords(
  recordsData: Uint8Array,
  compression: CompressionCodec,
  offset: number
): DecodeResult<Uint8Array> {
  if (compression === CompressionCodec.NONE) {
    return decodeSuccess(recordsData, 0)
  }
  const provider = compressionProviders.get(compression)
  if (!provider) {
    return decodeFailure(
      "INVALID_DATA",
      `compression codec ${String(compression)} not registered`,
      offset
    )
  }
  try {
    return decodeSuccess(provider.decompress(recordsData), 0)
  } catch (err) {
    return decodeFailure(
      "INVALID_DATA",
      `decompression failed: ${err instanceof Error ? err.message : String(err)}`,
      offset
    )
  }
}

// ---------------------------------------------------------------------------
// RecordBatch Decoding
// ---------------------------------------------------------------------------

/**
 * Decode a RecordBatch from bytes.
 *
 * @param data - The bytes to decode.
 * @param validateCrc - Whether to validate the CRC (default: true).
 * @returns The decoded record batch.
 */
export function decodeRecordBatch(data: Uint8Array, validateCrc = true): DecodeResult<RecordBatch> {
  const reader = new BinaryReader(data)

  // baseOffset (int64)
  const baseOffsetResult = reader.readInt64()
  if (!baseOffsetResult.ok) {
    return baseOffsetResult
  }

  // batchLength (int32)
  const batchLengthResult = reader.readInt32()
  if (!batchLengthResult.ok) {
    return batchLengthResult
  }

  const batchLength = batchLengthResult.value
  if (batchLength < RECORD_BATCH_METADATA_SIZE) {
    return decodeFailure(
      "INVALID_DATA",
      `batch length ${String(batchLength)} too small`,
      reader.offset
    )
  }

  if (reader.remaining < batchLength) {
    return decodeFailure(
      "BUFFER_UNDERFLOW",
      `need ${String(batchLength)} bytes for batch`,
      reader.offset
    )
  }

  // partitionLeaderEpoch (int32)
  const partitionLeaderEpochResult = reader.readInt32()
  if (!partitionLeaderEpochResult.ok) {
    return partitionLeaderEpochResult
  }

  // magic (int8)
  const magicResult = reader.readInt8()
  if (!magicResult.ok) {
    return magicResult
  }

  if (magicResult.value !== RECORD_BATCH_MAGIC) {
    return decodeFailure(
      "UNSUPPORTED_VERSION",
      `unsupported record batch magic: ${String(magicResult.value)}`,
      reader.offset - 1
    )
  }

  // crc (uint32)
  const crcResult = reader.readUint32()
  if (!crcResult.ok) {
    return crcResult
  }
  const expectedCrc = crcResult.value

  // CRC covers everything from attributes to end of records
  const crcDataStart = reader.offset
  const crcDataLength = batchLength - 4 - 1 - 4 // minus partitionLeaderEpoch, magic, crc

  if (validateCrc) {
    const crcValidation = validateBatchCrc(data, batchLength, expectedCrc, crcDataStart)
    if (!crcValidation.ok) {
      return crcValidation
    }
  }

  // Decode batch metadata fields
  const metadataResult = decodeBatchMetadata(reader)
  if (!metadataResult.ok) {
    return metadataResult
  }
  const metadata = metadataResult.value

  // Records data (remaining bytes in the batch)
  const recordsDataLength = crcDataLength - 2 - 4 - 8 - 8 - 8 - 2 - 4 - 4 // subtract all fixed fields after crc
  const recordsDataResult = reader.readRawBytes(recordsDataLength)
  if (!recordsDataResult.ok) {
    return recordsDataResult
  }

  // Decompress if needed
  const decompressResult = decompressRecords(
    recordsDataResult.value,
    metadata.attributes.compression,
    reader.offset
  )
  if (!decompressResult.ok) {
    return decompressResult
  }
  const recordsData = decompressResult.value

  // Decode individual records
  const recordsReader = new BinaryReader(recordsData)
  const records: Record[] = []

  for (let i = 0; i < metadata.recordsCount; i++) {
    const recordResult = decodeRecord(recordsReader)
    if (!recordResult.ok) {
      return recordResult
    }
    records.push(recordResult.value)
  }

  return decodeSuccess(
    {
      baseOffset: baseOffsetResult.value,
      partitionLeaderEpoch: partitionLeaderEpochResult.value,
      magic: magicResult.value,
      crc: expectedCrc,
      attributes: metadata.attributes,
      lastOffsetDelta: metadata.lastOffsetDelta,
      baseTimestamp: metadata.baseTimestamp,
      maxTimestamp: metadata.maxTimestamp,
      producerId: metadata.producerId,
      producerEpoch: metadata.producerEpoch,
      baseSequence: metadata.baseSequence,
      records
    },
    reader.offset
  )
}
