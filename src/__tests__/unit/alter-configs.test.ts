import { describe, expect, it } from "vitest"

import {
  type AlterConfigsRequest,
  buildAlterConfigsRequest,
  decodeAlterConfigsResponse,
  encodeAlterConfigsRequest
} from "../../alter-configs"
import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import { ConfigResourceType } from "../../describe-configs"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeAlterConfigsRequest", () => {
  describe("v0 — basic resources + configs + validate_only", () => {
    it("encodes a single topic resource with two configs", () => {
      const writer = new BinaryWriter()
      const request: AlterConfigsRequest = {
        resources: [
          {
            resourceType: ConfigResourceType.Topic,
            resourceName: "my-topic",
            configs: [
              { name: "cleanup.policy", value: "compact" },
              { name: "retention.ms", value: "86400000" }
            ]
          }
        ],
        validateOnly: false
      }
      encodeAlterConfigsRequest(writer, request, 0)
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
      const c1Value = reader.readString()
      expect(c1Value.ok && c1Value.value).toBe("compact")

      // config 2
      const c2Name = reader.readString()
      expect(c2Name.ok && c2Name.value).toBe("retention.ms")
      const c2Value = reader.readString()
      expect(c2Value.ok && c2Value.value).toBe("86400000")

      // validate_only
      const validateOnly = reader.readBoolean()
      expect(validateOnly.ok && validateOnly.value).toBe(false)

      expect(reader.remaining).toBe(0)
    })

    it("encodes validate_only = true", () => {
      const writer = new BinaryWriter()
      const request: AlterConfigsRequest = {
        resources: [],
        validateOnly: true
      }
      encodeAlterConfigsRequest(writer, request, 0)
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
      const request: AlterConfigsRequest = {
        resources: [
          {
            resourceType: ConfigResourceType.Broker,
            resourceName: "0",
            configs: [{ name: "log.retention.hours", value: null }]
          }
        ]
      }
      encodeAlterConfigsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // resources
      reader.readInt8() // resource_type
      reader.readString() // resource_name
      reader.readInt32() // configs

      reader.readString() // name
      const val = reader.readString()
      expect(val.ok && val.value).toBeNull()
    })
  })

  describe("v2+ — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: AlterConfigsRequest = {
        resources: [
          {
            resourceType: ConfigResourceType.Topic,
            resourceName: "flex-topic",
            configs: [{ name: "max.message.bytes", value: "1048576" }]
          }
        ],
        validateOnly: false
      }
      encodeAlterConfigsRequest(writer, request, 2)
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

describe("buildAlterConfigsRequest", () => {
  it("builds a framed v0 request", () => {
    const frame = buildAlterConfigsRequest(42, 0, {
      resources: [
        {
          resourceType: ConfigResourceType.Topic,
          resourceName: "t",
          configs: [{ name: "k", value: "v" }]
        }
      ]
    })

    const reader = new BinaryReader(frame)
    const size = reader.readInt32()
    expect(size.ok && size.value).toBe(frame.length - 4)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.AlterConfigs)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeAlterConfigsResponse", () => {
  describe("v0 — per-resource results", () => {
    it("decodes a successful response", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle_time_ms
      writer.writeInt32(1) // resources
      writer.writeInt16(0) // error_code
      writer.writeString(null) // error_message
      writer.writeInt8(2) // resource_type (Topic)
      writer.writeString("my-topic")

      const reader = new BinaryReader(writer.finish())
      const result = decodeAlterConfigsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.resources).toHaveLength(1)

      const resource = result.value.resources[0]
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
      const result = decodeAlterConfigsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(100)
      const resource = result.value.resources[0]
      expect(resource.errorCode).toBe(36)
      expect(resource.errorMessage).toBe("invalid config value")
    })
  })

  describe("v2+ — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // resources (1+1)
      writer.writeInt16(0) // error_code
      writer.writeCompactString(null) // error_message
      writer.writeInt8(2) // resource_type
      writer.writeCompactString("flex-topic")
      writer.writeTaggedFields([]) // resource tagged fields
      writer.writeTaggedFields([]) // top-level tagged fields

      const reader = new BinaryReader(writer.finish())
      const result = decodeAlterConfigsResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.resources).toHaveLength(1)
      expect(result.value.resources[0].resourceName).toBe("flex-topic")
    })
  })

  it("decodes multiple resources", () => {
    const writer = new BinaryWriter()

    writer.writeInt32(0) // throttle
    writer.writeInt32(2) // resources
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
    const result = decodeAlterConfigsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.resources).toHaveLength(2)
    expect(result.value.resources[0].resourceName).toBe("topic-a")
    expect(result.value.resources[0].resourceType).toBe(2)
    expect(result.value.resources[1].resourceName).toBe("0")
    expect(result.value.resources[1].resourceType).toBe(4)
  })
})
