import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import { ConfigResourceType } from "../../../protocol/describe-configs"
import {
  AlterConfigOp,
  buildIncrementalAlterConfigsRequest,
  decodeIncrementalAlterConfigsResponse,
  encodeIncrementalAlterConfigsRequest,
  type IncrementalAlterConfigsRequest
} from "../../../protocol/incremental-alter-configs"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeIncrementalAlterConfigsRequest", () => {
  describe("v0 — basic resources + configs + validate_only", () => {
    it("encodes a single topic resource with two configs", () => {
      const writer = new BinaryWriter()
      const request: IncrementalAlterConfigsRequest = {
        resources: [
          {
            resourceType: ConfigResourceType.Topic,
            resourceName: "my-topic",
            configs: [
              { name: "cleanup.policy", configOperation: AlterConfigOp.Set, value: "compact" },
              { name: "retention.ms", configOperation: AlterConfigOp.Set, value: "86400000" }
            ]
          }
        ],
        validateOnly: false
      }
      encodeIncrementalAlterConfigsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // resources array
      const resLen = reader.readInt32()
      expect(resLen.ok && resLen.value).toBe(1)

      // resource_type
      const resType = reader.readInt8()
      expect(resType.ok && resType.value).toBe(2) // Topic

      // resource_name
      const resName = reader.readString()
      expect(resName.ok && resName.value).toBe("my-topic")

      // configs array
      const configLen = reader.readInt32()
      expect(configLen.ok && configLen.value).toBe(2)

      // config 1
      const c1Name = reader.readString()
      expect(c1Name.ok && c1Name.value).toBe("cleanup.policy")
      const c1Op = reader.readInt8()
      expect(c1Op.ok && c1Op.value).toBe(AlterConfigOp.Set)
      const c1Value = reader.readString()
      expect(c1Value.ok && c1Value.value).toBe("compact")

      // config 2
      const c2Name = reader.readString()
      expect(c2Name.ok && c2Name.value).toBe("retention.ms")
      const c2Op = reader.readInt8()
      expect(c2Op.ok && c2Op.value).toBe(AlterConfigOp.Set)
      const c2Value = reader.readString()
      expect(c2Value.ok && c2Value.value).toBe("86400000")

      // validate_only
      const validateOnly = reader.readBoolean()
      expect(validateOnly.ok && validateOnly.value).toBe(false)

      expect(reader.remaining).toBe(0)
    })

    it("encodes validate_only = true", () => {
      const writer = new BinaryWriter()
      const request: IncrementalAlterConfigsRequest = {
        resources: [],
        validateOnly: true
      }
      encodeIncrementalAlterConfigsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const resLen = reader.readInt32()
      expect(resLen.ok && resLen.value).toBe(0)

      const validateOnly = reader.readBoolean()
      expect(validateOnly.ok && validateOnly.value).toBe(true)

      expect(reader.remaining).toBe(0)
    })

    it("encodes a null config value", () => {
      const writer = new BinaryWriter()
      const request: IncrementalAlterConfigsRequest = {
        resources: [
          {
            resourceType: ConfigResourceType.Broker,
            resourceName: "0",
            configs: [
              { name: "log.retention.hours", configOperation: AlterConfigOp.Delete, value: null }
            ]
          }
        ]
      }
      encodeIncrementalAlterConfigsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // resources
      reader.readInt8() // resource_type
      reader.readString() // resource_name
      reader.readInt32() // configs

      reader.readString() // name
      const op = reader.readInt8()
      expect(op.ok && op.value).toBe(AlterConfigOp.Delete)
      const val = reader.readString()
      expect(val.ok && val.value).toBeNull()
    })

    it("encodes all config operation types", () => {
      const writer = new BinaryWriter()
      const request: IncrementalAlterConfigsRequest = {
        resources: [
          {
            resourceType: ConfigResourceType.Topic,
            resourceName: "t",
            configs: [
              { name: "a", configOperation: AlterConfigOp.Set, value: "v1" },
              { name: "b", configOperation: AlterConfigOp.Delete, value: null },
              { name: "c", configOperation: AlterConfigOp.Append, value: "v2" },
              { name: "d", configOperation: AlterConfigOp.Subtract, value: "v3" }
            ]
          }
        ]
      }
      encodeIncrementalAlterConfigsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // resources
      reader.readInt8() // resource_type
      reader.readString() // resource_name
      reader.readInt32() // configs

      // config 1: SET
      reader.readString()
      const op1 = reader.readInt8()
      expect(op1.ok && op1.value).toBe(0)
      reader.readString()

      // config 2: DELETE
      reader.readString()
      const op2 = reader.readInt8()
      expect(op2.ok && op2.value).toBe(1)
      reader.readString()

      // config 3: APPEND
      reader.readString()
      const op3 = reader.readInt8()
      expect(op3.ok && op3.value).toBe(2)
      reader.readString()

      // config 4: SUBTRACT
      reader.readString()
      const op4 = reader.readInt8()
      expect(op4.ok && op4.value).toBe(3)
      reader.readString()

      // validate_only defaults to false
      const validateOnly = reader.readBoolean()
      expect(validateOnly.ok && validateOnly.value).toBe(false)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v1 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: IncrementalAlterConfigsRequest = {
        resources: [
          {
            resourceType: ConfigResourceType.Topic,
            resourceName: "flex-topic",
            configs: [
              { name: "max.message.bytes", configOperation: AlterConfigOp.Set, value: "1048576" }
            ]
          }
        ],
        validateOnly: false
      }
      encodeIncrementalAlterConfigsRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // resources (compact: 1+1)
      const resLen = reader.readUnsignedVarInt()
      expect(resLen.ok && resLen.value).toBe(2)

      // resource_type
      const resType = reader.readInt8()
      expect(resType.ok && resType.value).toBe(2)

      // resource_name (compact)
      const resName = reader.readCompactString()
      expect(resName.ok && resName.value).toBe("flex-topic")

      // configs (compact: 1+1)
      const configLen = reader.readUnsignedVarInt()
      expect(configLen.ok && configLen.value).toBe(2)

      // config: name
      const cName = reader.readCompactString()
      expect(cName.ok && cName.value).toBe("max.message.bytes")

      // config: operation
      const cOp = reader.readInt8()
      expect(cOp.ok && cOp.value).toBe(AlterConfigOp.Set)

      // config: value
      const cValue = reader.readCompactString()
      expect(cValue.ok && cValue.value).toBe("1048576")

      // config tagged fields
      const configTag = reader.readTaggedFields()
      expect(configTag.ok).toBe(true)

      // resource tagged fields
      const resTag = reader.readTaggedFields()
      expect(resTag.ok).toBe(true)

      // validate_only
      const validateOnly = reader.readBoolean()
      expect(validateOnly.ok && validateOnly.value).toBe(false)

      // top-level tagged fields
      const tag = reader.readTaggedFields()
      expect(tag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

describe("buildIncrementalAlterConfigsRequest", () => {
  it("builds a framed v0 request", () => {
    const frame = buildIncrementalAlterConfigsRequest(42, 0, {
      resources: [
        {
          resourceType: ConfigResourceType.Topic,
          resourceName: "t",
          configs: [{ name: "k", configOperation: AlterConfigOp.Set, value: "v" }]
        }
      ]
    })

    const reader = new BinaryReader(frame)
    const size = reader.readInt32()
    expect(size.ok && size.value).toBe(frame.length - 4)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.IncrementalAlterConfigs)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeIncrementalAlterConfigsResponse", () => {
  describe("v0 — per-resource results", () => {
    it("decodes a successful response", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1) // responses
      writer.writeInt16(0) // error_code
      writer.writeString(null) // error_message
      writer.writeInt8(2) // resource_type (Topic)
      writer.writeString("my-topic")

      const reader = new BinaryReader(writer.finish())
      const result = decodeIncrementalAlterConfigsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.responses).toHaveLength(1)

      const resource = result.value.responses[0]
      expect(resource.errorCode).toBe(0)
      expect(resource.errorMessage).toBeNull()
      expect(resource.resourceType).toBe(2)
      expect(resource.resourceName).toBe("my-topic")
    })

    it("decodes a resource-level error", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(100) // throttle
      writer.writeInt32(1)
      writer.writeInt16(36) // INVALID_CONFIG
      writer.writeString("invalid config value")
      writer.writeInt8(2)
      writer.writeString("bad-topic")

      const reader = new BinaryReader(writer.finish())
      const result = decodeIncrementalAlterConfigsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(100)
      const resource = result.value.responses[0]
      expect(resource.errorCode).toBe(36)
      expect(resource.errorMessage).toBe("invalid config value")
    })
  })

  describe("v1 — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // responses (1+1)
      writer.writeInt16(0) // error_code
      writer.writeCompactString(null) // error_message
      writer.writeInt8(2) // resource_type
      writer.writeCompactString("flex-topic")
      writer.writeTaggedFields([]) // resource tagged fields
      writer.writeTaggedFields([]) // top-level tagged fields

      const reader = new BinaryReader(writer.finish())
      const result = decodeIncrementalAlterConfigsResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.responses).toHaveLength(1)
      expect(result.value.responses[0].resourceName).toBe("flex-topic")
    })
  })

  it("decodes multiple resources", () => {
    const writer = new BinaryWriter()

    writer.writeInt32(0) // throttle
    writer.writeInt32(2) // responses
    // resource 1
    writer.writeInt16(0)
    writer.writeString(null)
    writer.writeInt8(2) // Topic
    writer.writeString("topic-a")
    // resource 2
    writer.writeInt16(0)
    writer.writeString(null)
    writer.writeInt8(4) // Broker
    writer.writeString("0")

    const reader = new BinaryReader(writer.finish())
    const result = decodeIncrementalAlterConfigsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.responses).toHaveLength(2)
    expect(result.value.responses[0].resourceName).toBe("topic-a")
    expect(result.value.responses[0].resourceType).toBe(2)
    expect(result.value.responses[1].resourceName).toBe("0")
    expect(result.value.responses[1].resourceType).toBe(4)
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeIncrementalAlterConfigsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v1 flexible response", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // responses (1+1)
      writer.writeInt16(0) // error_code
      writer.writeCompactString(null) // error_message
      writer.writeInt8(2) // resource_type
      // Missing resource_name and tagged fields
      const reader = new BinaryReader(writer.finish())
      const result = decodeIncrementalAlterConfigsResponse(reader, 1)
      expect(result.ok).toBe(false)
    })
  })
})
