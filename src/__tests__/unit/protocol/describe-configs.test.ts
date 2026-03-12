import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDescribeConfigsRequest,
  ConfigResourceType,
  decodeDescribeConfigsResponse,
  type DescribeConfigsRequest,
  encodeDescribeConfigsRequest
} from "../../../protocol/describe-configs"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeConfigsRequest", () => {
  describe("v0 — resources with config names", () => {
    it("encodes a single topic resource with all configs", () => {
      const writer = new BinaryWriter()
      const request: DescribeConfigsRequest = {
        resources: [
          {
            resourceType: ConfigResourceType.Topic,
            resourceName: "my-topic",
            configNames: null
          }
        ]
      }
      encodeDescribeConfigsRequest(writer, request, 0)
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

      // config_names (null = all)
      const configLen = reader.readInt32()
      expect(configLen.ok && configLen.value).toBe(-1)

      expect(reader.remaining).toBe(0)
    })

    it("encodes specific config keys", () => {
      const writer = new BinaryWriter()
      const request: DescribeConfigsRequest = {
        resources: [
          {
            resourceType: ConfigResourceType.Topic,
            resourceName: "test-topic",
            configNames: ["cleanup.policy", "retention.ms"]
          }
        ]
      }
      encodeDescribeConfigsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // resources
      reader.readInt8() // type
      reader.readString() // name

      // config_names array
      const configLen = reader.readInt32()
      expect(configLen.ok && configLen.value).toBe(2)

      const c1 = reader.readString()
      expect(c1.ok && c1.value).toBe("cleanup.policy")
      const c2 = reader.readString()
      expect(c2.ok && c2.value).toBe("retention.ms")

      expect(reader.remaining).toBe(0)
    })

    it("encodes broker resource", () => {
      const writer = new BinaryWriter()
      const request: DescribeConfigsRequest = {
        resources: [
          {
            resourceType: ConfigResourceType.Broker,
            resourceName: "0",
            configNames: null
          }
        ]
      }
      encodeDescribeConfigsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // resources
      const resType = reader.readInt8()
      expect(resType.ok && resType.value).toBe(4) // Broker

      const resName = reader.readString()
      expect(resName.ok && resName.value).toBe("0")
    })
  })

  describe("v1+ — adds include_synonyms", () => {
    it("encodes include_synonyms = true", () => {
      const writer = new BinaryWriter()
      const request: DescribeConfigsRequest = {
        resources: [],
        includeSynonyms: true
      }
      encodeDescribeConfigsRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // resources (empty)

      const syn = reader.readBoolean()
      expect(syn.ok && syn.value).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v3+ — adds include_documentation", () => {
    it("encodes include_documentation = true", () => {
      const writer = new BinaryWriter()
      const request: DescribeConfigsRequest = {
        resources: [],
        includeSynonyms: false,
        includeDocumentation: true
      }
      encodeDescribeConfigsRequest(writer, request, 3)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // resources
      reader.readBoolean() // include_synonyms
      const doc = reader.readBoolean()
      expect(doc.ok && doc.value).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v4+ — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: DescribeConfigsRequest = {
        resources: [
          {
            resourceType: ConfigResourceType.Topic,
            resourceName: "flex-topic",
            configNames: ["key"]
          }
        ],
        includeSynonyms: false,
        includeDocumentation: false
      }
      encodeDescribeConfigsRequest(writer, request, 4)
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

      // config_names (compact: 1+1)
      const configLen = reader.readUnsignedVarInt()
      expect(configLen.ok && configLen.value).toBe(2)

      const cName = reader.readCompactString()
      expect(cName.ok && cName.value).toBe("key")

      // resource tagged fields
      const resTag = reader.readTaggedFields()
      expect(resTag.ok).toBe(true)

      // include_synonyms
      const syn = reader.readBoolean()
      expect(syn.ok && syn.value).toBe(false)

      // include_documentation
      const doc = reader.readBoolean()
      expect(doc.ok && doc.value).toBe(false)

      // top-level tagged fields
      const tag = reader.readTaggedFields()
      expect(tag.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

describe("buildDescribeConfigsRequest", () => {
  it("builds a framed v0 request", () => {
    const frame = buildDescribeConfigsRequest(5, 0, {
      resources: [{ resourceType: ConfigResourceType.Topic, resourceName: "t", configNames: null }]
    })

    const reader = new BinaryReader(frame)
    const size = reader.readInt32()
    expect(size.ok && size.value).toBe(frame.length - 4)

    const apiKey = reader.readInt16()
    expect(apiKey.ok && apiKey.value).toBe(ApiKey.DescribeConfigs)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeConfigsResponse", () => {
  describe("v0 — basic config entries", () => {
    it("decodes a single resource with one config", () => {
      const writer = new BinaryWriter()

      // throttle_time_ms
      writer.writeInt32(0)
      // resources (1)
      writer.writeInt32(1)
      // error_code
      writer.writeInt16(0)
      // error_message
      writer.writeString(null)
      // resource_type
      writer.writeInt8(2) // Topic
      // resource_name
      writer.writeString("my-topic")
      // configs (1)
      writer.writeInt32(1)
      // config name
      writer.writeString("cleanup.policy")
      // config value
      writer.writeString("delete")
      // read_only
      writer.writeBoolean(false)
      // is_default
      writer.writeBoolean(true)
      // is_sensitive
      writer.writeBoolean(false)

      const reader = new BinaryReader(writer.finish())
      const result = decodeDescribeConfigsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.resources).toHaveLength(1)

      const resource = result.value.resources[0]
      expect(resource.errorCode).toBe(0)
      expect(resource.resourceType).toBe(2)
      expect(resource.resourceName).toBe("my-topic")
      expect(resource.configs).toHaveLength(1)

      const config = resource.configs[0]
      expect(config.name).toBe("cleanup.policy")
      expect(config.value).toBe("delete")
      expect(config.readOnly).toBe(false)
      expect(config.isDefault).toBe(true)
      expect(config.isSensitive).toBe(false)
      expect(config.configSource).toBe(-1) // not available in v0
      expect(config.synonyms).toHaveLength(0)
    })
  })

  describe("v1+ — adds config_source and synonyms", () => {
    it("decodes config source and synonyms", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle
      writer.writeInt32(1) // resources
      writer.writeInt16(0) // error_code
      writer.writeString(null) // error_message
      writer.writeInt8(2) // Topic
      writer.writeString("syn-topic")
      writer.writeInt32(1) // configs
      // config
      writer.writeString("retention.ms")
      writer.writeString("86400000")
      writer.writeBoolean(false) // read_only
      // v1+: config_source instead of is_default
      writer.writeInt8(1) // DYNAMIC_TOPIC
      writer.writeBoolean(false) // is_sensitive
      // synonyms array (1)
      writer.writeInt32(1)
      writer.writeString("retention.ms")
      writer.writeString("86400000")
      writer.writeInt8(1) // source

      const reader = new BinaryReader(writer.finish())
      const result = decodeDescribeConfigsResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      const config = result.value.resources[0].configs[0]
      expect(config.configSource).toBe(1)
      expect(config.synonyms).toHaveLength(1)
      expect(config.synonyms[0].name).toBe("retention.ms")
      expect(config.synonyms[0].value).toBe("86400000")
      expect(config.synonyms[0].source).toBe(1)
    })
  })

  describe("v3+ — adds documentation", () => {
    it("decodes config documentation", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle
      writer.writeInt32(1) // resources
      writer.writeInt16(0)
      writer.writeString(null)
      writer.writeInt8(2) // Topic
      writer.writeString("doc-topic")
      writer.writeInt32(1) // configs
      writer.writeString("compression.type")
      writer.writeString("producer")
      writer.writeBoolean(false) // read_only
      writer.writeInt8(5) // DEFAULT
      writer.writeBoolean(false) // is_sensitive
      writer.writeInt32(0) // synonyms (empty)
      writer.writeInt8(2) // config_type (STRING)
      writer.writeString("Specify the final compression type for a given topic.")

      const reader = new BinaryReader(writer.finish())
      const result = decodeDescribeConfigsResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      const config = result.value.resources[0].configs[0]
      expect(config.documentation).toBe("Specify the final compression type for a given topic.")
    })
  })

  describe("v4+ — flexible encoding", () => {
    it("decodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()

      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // resources (1+1)
      writer.writeInt16(0) // error_code
      writer.writeCompactString(null) // error_message
      writer.writeInt8(2) // Topic
      writer.writeCompactString("flex-topic")
      writer.writeUnsignedVarInt(2) // configs (1+1)
      // config entry
      writer.writeCompactString("log.retention.hours")
      writer.writeCompactString("168")
      writer.writeBoolean(false) // read_only
      writer.writeInt8(5) // config_source
      writer.writeBoolean(false) // is_sensitive
      // synonyms (compact: 0+1 = 1, empty)
      writer.writeUnsignedVarInt(1)
      // config_type (STRING)
      writer.writeInt8(2)
      // documentation
      writer.writeCompactString("hours to keep a log")
      // config tagged fields
      writer.writeTaggedFields([])
      // resource tagged fields
      writer.writeTaggedFields([])
      // top-level tagged fields
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeDescribeConfigsResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.resources).toHaveLength(1)
      const config = result.value.resources[0].configs[0]
      expect(config.name).toBe("log.retention.hours")
      expect(config.value).toBe("168")
      expect(config.documentation).toBe("hours to keep a log")
    })
  })

  it("decodes resource-level errors", () => {
    const writer = new BinaryWriter()

    writer.writeInt32(0) // throttle
    writer.writeInt32(1) // resources
    writer.writeInt16(29) // CLUSTER_AUTHORIZATION_FAILED
    writer.writeString("not authorised")
    writer.writeInt8(2) // Topic
    writer.writeString("secret-topic")
    writer.writeInt32(0) // configs (empty)

    const reader = new BinaryReader(writer.finish())
    const result = decodeDescribeConfigsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.resources[0].errorCode).toBe(29)
    expect(result.value.resources[0].errorMessage).toBe("not authorised")
    expect(result.value.resources[0].configs).toHaveLength(0)
  })

  it("decodes sensitive config with null value", () => {
    const writer = new BinaryWriter()

    writer.writeInt32(0)
    writer.writeInt32(1)
    writer.writeInt16(0)
    writer.writeString(null)
    writer.writeInt8(2)
    writer.writeString("secure-topic")
    writer.writeInt32(1)
    writer.writeString("ssl.key.password")
    writer.writeString(null) // value hidden
    writer.writeBoolean(false)
    writer.writeBoolean(false) // is_default
    writer.writeBoolean(true) // is_sensitive

    const reader = new BinaryReader(writer.finish())
    const result = decodeDescribeConfigsResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    const config = result.value.resources[0].configs[0]
    expect(config.value).toBeNull()
    expect(config.isSensitive).toBe(true)
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeDescribeConfigsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v4 flexible response", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle
      writer.writeUnsignedVarInt(2) // resources (1+1)
      writer.writeInt16(0) // error_code
      writer.writeCompactString(null) // error_message
      writer.writeInt8(2) // resource_type
      writer.writeCompactString("trunc-topic")
      writer.writeUnsignedVarInt(2) // configs (1+1)
      writer.writeCompactString("key")
      // Missing value and remaining fields
      const reader = new BinaryReader(writer.finish())
      const result = decodeDescribeConfigsResponse(reader, 4)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v1 synonym", () => {
      const writer = new BinaryWriter()
      writer.writeInt32(0) // throttle
      writer.writeInt32(1) // resources
      writer.writeInt16(0) // error_code
      writer.writeString(null) // error_message
      writer.writeInt8(2) // Topic
      writer.writeString("syn-topic")
      writer.writeInt32(1) // configs
      writer.writeString("retention.ms")
      writer.writeString("86400000")
      writer.writeBoolean(false) // read_only
      writer.writeInt8(1) // config_source
      writer.writeBoolean(false) // is_sensitive
      writer.writeInt32(1) // synonyms (1)
      writer.writeString("retention.ms")
      // Missing synonym value and source
      const reader = new BinaryReader(writer.finish())
      const result = decodeDescribeConfigsResponse(reader, 1)
      expect(result.ok).toBe(false)
    })
  })
})
