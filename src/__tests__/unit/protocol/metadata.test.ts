/**
 * Metadata (API key 3) request/response tests.
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_Metadata
 */

import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildMetadataRequest,
  decodeMetadataResponse,
  encodeMetadataRequest,
  type MetadataRequest
} from "../../../protocol/metadata"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeMetadataRequest", () => {
  describe("v0 — topics array only", () => {
    it("encodes empty topics array", () => {
      const writer = new BinaryWriter()
      const request: MetadataRequest = { topics: [] }
      encodeMetadataRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Empty array length
      const lengthResult = reader.readInt32()
      expect(lengthResult.ok).toBe(true)
      expect(lengthResult.ok && lengthResult.value).toBe(0)
      expect(reader.remaining).toBe(0)
    })

    it("encodes multiple topics", () => {
      const writer = new BinaryWriter()
      const request: MetadataRequest = {
        topics: [{ name: "topic-a" }, { name: "topic-b" }]
      }
      encodeMetadataRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Array length
      const lengthResult = reader.readInt32()
      expect(lengthResult.ok).toBe(true)
      expect(lengthResult.ok && lengthResult.value).toBe(2)

      // Topic 1
      const topic1Result = reader.readString()
      expect(topic1Result.ok).toBe(true)
      expect(topic1Result.ok && topic1Result.value).toBe("topic-a")

      // Topic 2
      const topic2Result = reader.readString()
      expect(topic2Result.ok).toBe(true)
      expect(topic2Result.ok && topic2Result.value).toBe("topic-b")

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v1+ — null topics means all topics", () => {
    it("encodes null array for all topics", () => {
      const writer = new BinaryWriter()
      const request: MetadataRequest = { topics: null }
      encodeMetadataRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Null array = -1
      const lengthResult = reader.readInt32()
      expect(lengthResult.ok).toBe(true)
      expect(lengthResult.ok && lengthResult.value).toBe(-1)
      expect(reader.remaining).toBe(0)
    })
  })

  describe("v4+ — adds allow_auto_topic_creation", () => {
    it("encodes allow_auto_topic_creation = true (default)", () => {
      const writer = new BinaryWriter()
      const request: MetadataRequest = { topics: [] }
      encodeMetadataRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Array length
      const lengthResult = reader.readInt32()
      expect(lengthResult.ok).toBe(true)

      // allow_auto_topic_creation
      const autoCreateResult = reader.readBoolean()
      expect(autoCreateResult.ok).toBe(true)
      expect(autoCreateResult.ok && autoCreateResult.value).toBe(true)

      expect(reader.remaining).toBe(0)
    })

    it("encodes allow_auto_topic_creation = false", () => {
      const writer = new BinaryWriter()
      const request: MetadataRequest = {
        topics: [],
        allowAutoTopicCreation: false
      }
      encodeMetadataRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Array length
      reader.readInt32()

      // allow_auto_topic_creation
      const autoCreateResult = reader.readBoolean()
      expect(autoCreateResult.ok).toBe(true)
      expect(autoCreateResult.ok && autoCreateResult.value).toBe(false)
    })
  })

  describe("v8+ — adds authorised operations flags", () => {
    it("encodes include_topic_authorized_operations", () => {
      const writer = new BinaryWriter()
      const request: MetadataRequest = {
        topics: [],
        includeTopicAuthorisedOperations: true
      }
      encodeMetadataRequest(writer, request, 8)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Array length
      reader.readInt32()
      // allow_auto_topic_creation
      reader.readBoolean()
      // include_cluster_authorized_operations
      const clusterOpsResult = reader.readBoolean()
      expect(clusterOpsResult.ok).toBe(true)
      expect(clusterOpsResult.ok && clusterOpsResult.value).toBe(false)
      // include_topic_authorized_operations
      const topicOpsResult = reader.readBoolean()
      expect(topicOpsResult.ok).toBe(true)
      expect(topicOpsResult.ok && topicOpsResult.value).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v9+ — flexible encoding", () => {
    it("encodes with compact array and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: MetadataRequest = {
        topics: [{ name: "test-topic" }]
      }
      encodeMetadataRequest(writer, request, 9)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Compact array: length + 1 = 2
      const lengthResult = reader.readUnsignedVarInt()
      expect(lengthResult.ok).toBe(true)
      expect(lengthResult.ok && lengthResult.value).toBe(2)

      // Topic name (compact string)
      const nameResult = reader.readCompactString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("test-topic")

      // Topic tagged fields
      const topicTagsResult = reader.readTaggedFields()
      expect(topicTagsResult.ok).toBe(true)

      // allow_auto_topic_creation
      const autoCreateResult = reader.readBoolean()
      expect(autoCreateResult.ok).toBe(true)

      // include_cluster_authorized_operations
      reader.readBoolean()

      // include_topic_authorized_operations
      reader.readBoolean()

      // Request tagged fields
      const tagsResult = reader.readTaggedFields()
      expect(tagsResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })

    it("encodes null topics as compact null", () => {
      const writer = new BinaryWriter()
      const request: MetadataRequest = { topics: null }
      encodeMetadataRequest(writer, request, 9)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Compact null = 0
      const lengthResult = reader.readUnsignedVarInt()
      expect(lengthResult.ok).toBe(true)
      expect(lengthResult.ok && lengthResult.value).toBe(0)
    })
  })
})

describe("buildMetadataRequest", () => {
  it("builds a framed v0 request for specific topics", () => {
    const frame = buildMetadataRequest(1, 0, {
      topics: [{ name: "my-topic" }]
    })

    const reader = new BinaryReader(frame)

    // Size prefix
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)
    expect(sizeResult.ok && sizeResult.value).toBe(frame.length - 4)

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.Metadata)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok).toBe(true)
    expect(versionResult.ok && versionResult.value).toBe(0)

    // Correlation ID
    const corrIdResult = reader.readInt32()
    expect(corrIdResult.ok).toBe(true)
    expect(corrIdResult.ok && corrIdResult.value).toBe(1)

    // Client ID (nullable string, v1 header)
    const clientIdResult = reader.readString()
    expect(clientIdResult.ok).toBe(true)

    // Body: topics array
    const arrayLengthResult = reader.readInt32()
    expect(arrayLengthResult.ok).toBe(true)
    expect(arrayLengthResult.ok && arrayLengthResult.value).toBe(1)

    const topicResult = reader.readString()
    expect(topicResult.ok).toBe(true)
    expect(topicResult.ok && topicResult.value).toBe("my-topic")

    expect(reader.remaining).toBe(0)
  })

  it("builds a framed v9 request with client ID", () => {
    const frame = buildMetadataRequest(42, 9, { topics: null }, "test-client")

    const reader = new BinaryReader(frame)

    // Size prefix
    reader.readInt32()

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.Metadata)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok).toBe(true)
    expect(versionResult.ok && versionResult.value).toBe(9)

    // Correlation ID
    const corrIdResult = reader.readInt32()
    expect(corrIdResult.ok).toBe(true)
    expect(corrIdResult.ok && corrIdResult.value).toBe(42)

    // Client ID (nullable string, v2 header per KIP-482)
    const clientIdResult = reader.readString()
    expect(clientIdResult.ok).toBe(true)
    expect(clientIdResult.ok && clientIdResult.value).toBe("test-client")

    // Header tagged fields
    const headerTagsResult = reader.readTaggedFields()
    expect(headerTagsResult.ok).toBe(true)

    // Body: null topics (compact null = 0)
    const topicsLengthResult = reader.readUnsignedVarInt()
    expect(topicsLengthResult.ok).toBe(true)
    expect(topicsLengthResult.ok && topicsLengthResult.value).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeMetadataResponse", () => {
  describe("v0 — brokers and topics", () => {
    it("decodes a minimal response with one broker and one topic", () => {
      const writer = new BinaryWriter()

      // Brokers array (length: 1)
      writer.writeInt32(1)
      // Broker: node_id=1, host="localhost", port=9092
      writer.writeInt32(1)
      writer.writeString("localhost")
      writer.writeInt32(9092)

      // Topics array (length: 1)
      writer.writeInt32(1)
      // Topic: error_code=0, name="test-topic"
      writer.writeInt16(0)
      writer.writeString("test-topic")
      // Partitions (length: 1)
      writer.writeInt32(1)
      // Partition: error_code=0, partition_index=0, leader_id=1
      writer.writeInt16(0)
      writer.writeInt32(0)
      writer.writeInt32(1)
      // replicas (length: 1): [1]
      writer.writeInt32(1)
      writer.writeInt32(1)
      // isr (length: 1): [1]
      writer.writeInt32(1)
      writer.writeInt32(1)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.throttleTimeMs).toBe(0)
        expect(result.value.brokers).toHaveLength(1)
        expect(result.value.brokers[0]).toMatchObject({
          nodeId: 1,
          host: "localhost",
          port: 9092,
          rack: null
        })
        expect(result.value.clusterId).toBeNull()
        expect(result.value.controllerId).toBe(-1)
        expect(result.value.topics).toHaveLength(1)
        expect(result.value.topics[0]).toMatchObject({
          errorCode: 0,
          name: "test-topic",
          isInternal: false
        })
        expect(result.value.topics[0].partitions).toHaveLength(1)
        expect(result.value.topics[0].partitions[0]).toMatchObject({
          errorCode: 0,
          partitionIndex: 0,
          leaderId: 1,
          leaderEpoch: -1,
          replicaNodes: [1],
          isrNodes: [1],
          offlineReplicas: []
        })
      }
    })

    it("decodes a response with multiple brokers", () => {
      const writer = new BinaryWriter()

      // Brokers array (length: 3)
      writer.writeInt32(3)
      // Broker 1
      writer.writeInt32(1)
      writer.writeString("broker1.example.com")
      writer.writeInt32(9092)
      // Broker 2
      writer.writeInt32(2)
      writer.writeString("broker2.example.com")
      writer.writeInt32(9092)
      // Broker 3
      writer.writeInt32(3)
      writer.writeString("broker3.example.com")
      writer.writeInt32(9092)

      // Empty topics array
      writer.writeInt32(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.brokers).toHaveLength(3)
        expect(result.value.brokers.map((b) => b.nodeId)).toEqual([1, 2, 3])
        expect(result.value.topics).toHaveLength(0)
      }
    })
  })

  describe("v1+ — adds controller_id, rack, is_internal", () => {
    it("decodes response with controller and rack", () => {
      const writer = new BinaryWriter()

      // Brokers array (length: 1)
      writer.writeInt32(1)
      // Broker with rack
      writer.writeInt32(1)
      writer.writeString("localhost")
      writer.writeInt32(9092)
      writer.writeString("rack-a")

      // Controller ID
      writer.writeInt32(1)

      // Topics array (length: 1)
      writer.writeInt32(1)
      // Topic with is_internal
      writer.writeInt16(0)
      writer.writeString("__consumer_offsets")
      writer.writeBoolean(true) // is_internal
      // Empty partitions
      writer.writeInt32(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.brokers[0].rack).toBe("rack-a")
        expect(result.value.controllerId).toBe(1)
        expect(result.value.topics[0].isInternal).toBe(true)
      }
    })
  })

  describe("v2+ — adds cluster_id", () => {
    it("decodes response with cluster_id", () => {
      const writer = new BinaryWriter()

      // Empty brokers
      writer.writeInt32(0)

      // Cluster ID
      writer.writeString("test-cluster-id")

      // Controller ID
      writer.writeInt32(1)

      // Empty topics
      writer.writeInt32(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.clusterId).toBe("test-cluster-id")
      }
    })
  })

  describe("v3+ — adds throttle_time_ms", () => {
    it("decodes response with throttle time", () => {
      const writer = new BinaryWriter()

      // Throttle time
      writer.writeInt32(100)

      // Empty brokers
      writer.writeInt32(0)

      // Cluster ID
      writer.writeString("cluster-1")

      // Controller ID
      writer.writeInt32(0)

      // Empty topics
      writer.writeInt32(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 3)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.throttleTimeMs).toBe(100)
      }
    })
  })

  describe("v5+ — adds offline_replicas", () => {
    it("decodes response with offline replicas", () => {
      const writer = new BinaryWriter()

      // Throttle time
      writer.writeInt32(0)

      // Empty brokers
      writer.writeInt32(0)

      // Cluster ID
      writer.writeString("cluster-1")

      // Controller ID
      writer.writeInt32(1)

      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0) // error
      writer.writeString("test")
      writer.writeBoolean(false) // is_internal
      // Partitions (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0) // error
      writer.writeInt32(0) // partition index
      writer.writeInt32(1) // leader
      // replicas
      writer.writeInt32(2)
      writer.writeInt32(1)
      writer.writeInt32(2)
      // isr
      writer.writeInt32(1)
      writer.writeInt32(1)
      // offline replicas
      writer.writeInt32(1)
      writer.writeInt32(2)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 5)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].partitions[0].offlineReplicas).toEqual([2])
      }
    })
  })

  describe("v7+ — adds leader_epoch", () => {
    it("decodes response with leader epoch", () => {
      const writer = new BinaryWriter()

      // Throttle time
      writer.writeInt32(0)

      // Empty brokers
      writer.writeInt32(0)

      // Cluster ID
      writer.writeString("cluster-1")

      // Controller ID
      writer.writeInt32(1)

      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeString("test")
      writer.writeBoolean(false)
      // Partitions (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeInt32(0)
      writer.writeInt32(1)
      writer.writeInt32(5) // leader epoch
      // replicas, isr, offline
      writer.writeInt32(0)
      writer.writeInt32(0)
      writer.writeInt32(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 7)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].partitions[0].leaderEpoch).toBe(5)
      }
    })
  })

  describe("v8+ — adds authorised operations", () => {
    it("decodes response with cluster and topic authorised operations", () => {
      const writer = new BinaryWriter()

      // Throttle time
      writer.writeInt32(0)

      // Empty brokers
      writer.writeInt32(0)

      // Cluster ID
      writer.writeString("cluster-1")

      // Controller ID
      writer.writeInt32(1)

      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeString("test")
      writer.writeBoolean(false)
      // Partitions (length: 0)
      writer.writeInt32(0)
      // Topic authorised operations
      writer.writeInt32(0x0f) // some bitmap

      // Cluster authorised operations
      writer.writeInt32(0xff)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 8)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].topicAuthorisedOperations).toBe(0x0f)
        expect(result.value.clusterAuthorisedOperations).toBe(0xff)
      }
    })
  })

  describe("v9+ — flexible encoding", () => {
    it("decodes response with compact arrays and tagged fields", () => {
      const writer = new BinaryWriter()

      // Throttle time
      writer.writeInt32(0)

      // Brokers (compact array: length + 1 = 2)
      writer.writeUnsignedVarInt(2)
      // Broker
      writer.writeInt32(1)
      writer.writeCompactString("localhost")
      writer.writeInt32(9092)
      writer.writeCompactString("rack-a") // rack (nullable compact)
      writer.writeUnsignedVarInt(0) // broker tagged fields

      // Cluster ID (compact string)
      writer.writeCompactString("cluster-1")

      // Controller ID
      writer.writeInt32(1)

      // Topics (compact array: length + 1 = 2)
      writer.writeUnsignedVarInt(2)
      // Topic
      writer.writeInt16(0) // error
      writer.writeCompactString("test-topic") // name (nullable compact)
      writer.writeBoolean(false) // is_internal
      // Partitions (compact array: length + 1 = 1)
      writer.writeUnsignedVarInt(1)
      // Topic authorised operations
      writer.writeInt32(0)
      // Topic tagged fields
      writer.writeUnsignedVarInt(0)

      // Cluster authorised operations (v9 still has this, removed in v11)
      writer.writeInt32(0)

      // Response tagged fields
      writer.writeUnsignedVarInt(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 9)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.brokers).toHaveLength(1)
        expect(result.value.brokers[0].host).toBe("localhost")
        expect(result.value.brokers[0].rack).toBe("rack-a")
        expect(result.value.topics).toHaveLength(1)
        expect(result.value.topics[0].name).toBe("test-topic")
        expect(result.value.taggedFields).toEqual([])
      }
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated broker data", () => {
      const writer = new BinaryWriter()
      // Brokers array length = 1, but no broker data
      writer.writeInt32(1)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated topic data", () => {
      const writer = new BinaryWriter()
      // Empty brokers
      writer.writeInt32(0)
      // Topics array length = 1, but no topic data
      writer.writeInt32(1)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(false)
    })

    it("decodes topic with error code", () => {
      const writer = new BinaryWriter()

      // Empty brokers
      writer.writeInt32(0)

      // Topics (length: 1)
      writer.writeInt32(1)
      // Topic with UNKNOWN_TOPIC_OR_PARTITION (3)
      writer.writeInt16(3)
      writer.writeString("unknown-topic")
      // Empty partitions
      writer.writeInt32(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].errorCode).toBe(3)
        expect(result.value.topics[0].name).toBe("unknown-topic")
      }
    })

    it("returns failure on truncated throttle time (v3+)", () => {
      const writer = new BinaryWriter()
      // Only 2 bytes instead of 4 for throttle time
      writer.writeInt16(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 3)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated cluster ID (v2+)", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers
      writer.writeInt32(0)
      // Cluster ID length but no string data
      writer.writeInt16(10)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 3)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated controller ID (v1+)", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers
      writer.writeInt32(0)
      // Cluster ID
      writer.writeString("cluster")
      // Only 2 bytes for controller ID instead of 4
      writer.writeInt16(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 3)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated cluster authorized ops (v8–v10)", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers
      writer.writeInt32(0)
      // Cluster ID
      writer.writeString("cluster")
      // Controller ID
      writer.writeInt32(1)
      // Empty topics
      writer.writeInt32(0)
      // Only 2 bytes instead of 4 for cluster ops
      writer.writeInt16(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 8)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated response tagged fields (v9+)", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers (compact)
      writer.writeUnsignedVarInt(1)
      // Cluster ID (compact)
      writer.writeCompactString("cluster")
      // Controller ID
      writer.writeInt32(1)
      // Empty topics (compact)
      writer.writeUnsignedVarInt(1)
      // Cluster ops
      writer.writeInt32(0)
      // Truncated tagged fields - length says 5 but no data
      writer.writeUnsignedVarInt(5)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 9)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated broker host", () => {
      const writer = new BinaryWriter()
      // Brokers (length: 1)
      writer.writeInt32(1)
      // Node ID
      writer.writeInt32(1)
      // Host length but no data
      writer.writeInt16(10)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated broker port", () => {
      const writer = new BinaryWriter()
      // Brokers (length: 1)
      writer.writeInt32(1)
      // Node ID
      writer.writeInt32(1)
      // Host
      writer.writeString("localhost")
      // Only 2 bytes for port
      writer.writeInt16(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated broker rack (v1+)", () => {
      const writer = new BinaryWriter()
      // Brokers (length: 1)
      writer.writeInt32(1)
      // Node ID
      writer.writeInt32(1)
      // Host
      writer.writeString("localhost")
      // Port
      writer.writeInt32(9092)
      // Rack length but no data
      writer.writeInt16(5)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 1)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated broker tagged fields (v9+)", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Brokers (compact: 2 = 1 broker)
      writer.writeUnsignedVarInt(2)
      // Node ID
      writer.writeInt32(1)
      // Host
      writer.writeCompactString("localhost")
      // Port
      writer.writeInt32(9092)
      // Rack
      writer.writeCompactString("rack-a")
      // Tagged fields - length says 3 but no data
      writer.writeUnsignedVarInt(3)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 9)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated partition error code", () => {
      const writer = new BinaryWriter()
      // Empty brokers
      writer.writeInt32(0)
      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0) // topic error
      writer.writeString("test")
      // Partitions (length: 1)
      writer.writeInt32(1)
      // Only 1 byte for error code
      writer.writeInt8(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated partition index", () => {
      const writer = new BinaryWriter()
      // Empty brokers
      writer.writeInt32(0)
      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeString("test")
      // Partitions (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0) // error
      // Only 2 bytes for partition index
      writer.writeInt16(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated leader ID", () => {
      const writer = new BinaryWriter()
      // Empty brokers
      writer.writeInt32(0)
      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeString("test")
      // Partitions (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeInt32(0) // partition index
      // Only 2 bytes for leader
      writer.writeInt16(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated leader epoch (v7+)", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers
      writer.writeInt32(0)
      // Cluster ID
      writer.writeString("cluster")
      // Controller ID
      writer.writeInt32(1)
      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeString("test")
      writer.writeBoolean(false)
      // Partitions (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeInt32(0)
      writer.writeInt32(1) // leader
      // Only 2 bytes for leader epoch
      writer.writeInt16(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 7)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated replicas array", () => {
      const writer = new BinaryWriter()
      // Empty brokers
      writer.writeInt32(0)
      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeString("test")
      // Partitions (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeInt32(0)
      writer.writeInt32(1)
      // Replicas array length = 2 but only 1 value
      writer.writeInt32(2)
      writer.writeInt32(1)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated isr array", () => {
      const writer = new BinaryWriter()
      // Empty brokers
      writer.writeInt32(0)
      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeString("test")
      // Partitions (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeInt32(0)
      writer.writeInt32(1)
      // Replicas
      writer.writeInt32(0)
      // ISR array length = 1 but no data
      writer.writeInt32(1)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 0)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated offline replicas (v5+)", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers
      writer.writeInt32(0)
      // Cluster ID
      writer.writeString("cluster")
      // Controller ID
      writer.writeInt32(1)
      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeString("test")
      writer.writeBoolean(false)
      // Partitions (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeInt32(0)
      writer.writeInt32(1)
      // Replicas
      writer.writeInt32(0)
      // ISR
      writer.writeInt32(0)
      // Offline replicas length = 1 but no data
      writer.writeInt32(1)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 5)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated partition tagged fields (v9+)", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers (compact)
      writer.writeUnsignedVarInt(1)
      // Cluster ID
      writer.writeCompactString("cluster")
      // Controller ID
      writer.writeInt32(1)
      // Topics (compact: 2 = 1 topic)
      writer.writeUnsignedVarInt(2)
      writer.writeInt16(0) // error
      writer.writeCompactString("test")
      writer.writeBoolean(false)
      // Partitions (compact: 2 = 1 partition)
      writer.writeUnsignedVarInt(2)
      writer.writeInt16(0)
      writer.writeInt32(0)
      writer.writeInt32(1)
      writer.writeInt32(0) // leader epoch
      // Replicas (compact)
      writer.writeUnsignedVarInt(1)
      // ISR (compact)
      writer.writeUnsignedVarInt(1)
      // Offline (compact)
      writer.writeUnsignedVarInt(1)
      // Partition tagged fields - length says 5 but no data
      writer.writeUnsignedVarInt(5)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 9)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated topic authorized ops (v8+)", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers
      writer.writeInt32(0)
      // Cluster ID
      writer.writeString("cluster")
      // Controller ID
      writer.writeInt32(1)
      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeString("test")
      writer.writeBoolean(false)
      // Empty partitions
      writer.writeInt32(0)
      // Only 2 bytes for topic ops
      writer.writeInt16(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 8)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated topic tagged fields (v9+)", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers (compact)
      writer.writeUnsignedVarInt(1)
      // Cluster ID
      writer.writeCompactString("cluster")
      // Controller ID
      writer.writeInt32(1)
      // Topics (compact: 2 = 1 topic)
      writer.writeUnsignedVarInt(2)
      writer.writeInt16(0) // error
      writer.writeCompactString("test")
      writer.writeBoolean(false)
      // Empty partitions
      writer.writeUnsignedVarInt(1)
      // Topic ops
      writer.writeInt32(0)
      // Topic tagged fields - length says 3 but no data
      writer.writeUnsignedVarInt(3)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 9)

      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated topic is_internal (v1+)", () => {
      const writer = new BinaryWriter()
      // Empty brokers
      writer.writeInt32(0)
      // Controller ID
      writer.writeInt32(1)
      // Topics (length: 1)
      writer.writeInt32(1)
      writer.writeInt16(0)
      writer.writeString("test")
      // No is_internal byte

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 1)

      expect(result.ok).toBe(false)
    })
  })

  describe("v10+ — topic ID decoding", () => {
    it("decodes response with topic UUID", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers (compact)
      writer.writeUnsignedVarInt(1)
      // Cluster ID
      writer.writeCompactString("cluster")
      // Controller ID
      writer.writeInt32(1)
      // Topics (compact: 2 = 1 topic)
      writer.writeUnsignedVarInt(2)
      writer.writeInt16(0) // error
      writer.writeCompactString("test-topic")
      // Topic UUID (16 bytes) - comes after name in response
      const uuid = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
      writer.writeRawBytes(uuid)
      writer.writeBoolean(false) // is_internal
      // Empty partitions
      writer.writeUnsignedVarInt(1)
      // Topic ops
      writer.writeInt32(0)
      // Topic tagged fields
      writer.writeUnsignedVarInt(0)
      // Cluster authorized ops (v10 still has this, removed in v11)
      writer.writeInt32(0)
      // Response tagged fields
      writer.writeUnsignedVarInt(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 10)

      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value.topics[0].topicId).toEqual(uuid)
      }
    })

    it("returns failure on truncated topic UUID", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers (compact)
      writer.writeUnsignedVarInt(1)
      // Cluster ID
      writer.writeCompactString("cluster")
      // Controller ID
      writer.writeInt32(1)
      // Topics (compact: 2 = 1 topic)
      writer.writeUnsignedVarInt(2)
      writer.writeInt16(0)
      writer.writeCompactString("test")
      // Only 8 bytes of UUID
      writer.writeRawBytes(new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]))

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 10)

      expect(result.ok).toBe(false)
    })
  })

  describe("v11+ — cluster authorized operations removed", () => {
    it("does not read cluster authorized operations in v11", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty brokers (compact)
      writer.writeUnsignedVarInt(1)
      // Cluster ID
      writer.writeCompactString("cluster")
      // Controller ID
      writer.writeInt32(1)
      // Empty topics (compact)
      writer.writeUnsignedVarInt(1)
      // Response tagged fields (no cluster ops before this in v11)
      writer.writeUnsignedVarInt(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeMetadataResponse(reader, 11)

      expect(result.ok).toBe(true)
      if (result.ok) {
        // Default value when not read
        expect(result.value.clusterAuthorisedOperations).toBe(-2147483648)
      }
    })
  })
})

// ---------------------------------------------------------------------------
// Request encoding v10+
// ---------------------------------------------------------------------------

describe("encodeMetadataRequest v10+", () => {
  it("encodes topic with explicit UUID", () => {
    const writer = new BinaryWriter()
    const topicId = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    const request: MetadataRequest = {
      topics: [{ name: "test-topic", topicId }]
    }
    encodeMetadataRequest(writer, request, 10)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // Compact array length
    const lengthResult = reader.readUnsignedVarInt()
    expect(lengthResult.ok).toBe(true)
    expect(lengthResult.ok && lengthResult.value).toBe(2)

    // Topic UUID
    const uuidResult = reader.readRawBytes(16)
    expect(uuidResult.ok).toBe(true)
    expect(uuidResult.ok && uuidResult.value).toEqual(topicId)

    // Topic name
    const nameResult = reader.readCompactString()
    expect(nameResult.ok).toBe(true)
    expect(nameResult.ok && nameResult.value).toBe("test-topic")
  })

  it("encodes topic with null UUID as zeros", () => {
    const writer = new BinaryWriter()
    const request: MetadataRequest = {
      topics: [{ name: "test-topic" }]
    }
    encodeMetadataRequest(writer, request, 10)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // Compact array length
    reader.readUnsignedVarInt()

    // Topic UUID should be 16 zeros
    const uuidResult = reader.readRawBytes(16)
    expect(uuidResult.ok).toBe(true)
    expect(uuidResult.ok && uuidResult.value).toEqual(new Uint8Array(16))
  })
})

// ---------------------------------------------------------------------------
// Flexible encoding error paths (v9+)
// ---------------------------------------------------------------------------

describe("decodeMetadataResponse flexible encoding errors", () => {
  it("returns failure on truncated brokers compact array length", () => {
    const writer = new BinaryWriter()
    // Throttle time
    writer.writeInt32(0)
    // Truncated varint for brokers array (needs continuation but buffer ends)
    writer.writeInt8(0x80) // continuation bit set

    const reader = new BinaryReader(writer.finish())
    const result = decodeMetadataResponse(reader, 9)

    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated topics compact array length", () => {
    const writer = new BinaryWriter()
    // Throttle time
    writer.writeInt32(0)
    // Empty brokers (compact)
    writer.writeUnsignedVarInt(1)
    // Cluster ID
    writer.writeCompactString("cluster")
    // Controller ID
    writer.writeInt32(1)
    // Truncated varint for topics array
    writer.writeInt8(0x80)

    const reader = new BinaryReader(writer.finish())
    const result = decodeMetadataResponse(reader, 9)

    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated partitions compact array length", () => {
    const writer = new BinaryWriter()
    // Throttle time
    writer.writeInt32(0)
    // Empty brokers (compact)
    writer.writeUnsignedVarInt(1)
    // Cluster ID
    writer.writeCompactString("cluster")
    // Controller ID
    writer.writeInt32(1)
    // Topics (compact: 2 = 1 topic)
    writer.writeUnsignedVarInt(2)
    writer.writeInt16(0) // error
    writer.writeCompactString("test")
    writer.writeBoolean(false) // is_internal
    // Truncated varint for partitions array
    writer.writeInt8(0x80)

    const reader = new BinaryReader(writer.finish())
    const result = decodeMetadataResponse(reader, 9)

    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated replicas compact array length", () => {
    const writer = new BinaryWriter()
    // Throttle time
    writer.writeInt32(0)
    // Empty brokers (compact)
    writer.writeUnsignedVarInt(1)
    // Cluster ID
    writer.writeCompactString("cluster")
    // Controller ID
    writer.writeInt32(1)
    // Topics (compact: 2 = 1 topic)
    writer.writeUnsignedVarInt(2)
    writer.writeInt16(0) // error
    writer.writeCompactString("test")
    writer.writeBoolean(false) // is_internal
    // Partitions (compact: 2 = 1 partition)
    writer.writeUnsignedVarInt(2)
    writer.writeInt16(0) // error
    writer.writeInt32(0) // partition index
    writer.writeInt32(1) // leader
    writer.writeInt32(0) // leader epoch
    // Truncated varint for replicas array
    writer.writeInt8(0x80)

    const reader = new BinaryReader(writer.finish())
    const result = decodeMetadataResponse(reader, 9)

    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated replicas compact array values", () => {
    const writer = new BinaryWriter()
    // Throttle time
    writer.writeInt32(0)
    // Empty brokers (compact)
    writer.writeUnsignedVarInt(1)
    // Cluster ID
    writer.writeCompactString("cluster")
    // Controller ID
    writer.writeInt32(1)
    // Topics (compact: 2 = 1 topic)
    writer.writeUnsignedVarInt(2)
    writer.writeInt16(0) // error
    writer.writeCompactString("test")
    writer.writeBoolean(false) // is_internal
    // Partitions (compact: 2 = 1 partition)
    writer.writeUnsignedVarInt(2)
    writer.writeInt16(0) // error
    writer.writeInt32(0) // partition index
    writer.writeInt32(1) // leader
    writer.writeInt32(0) // leader epoch
    // Replicas array says 2 elements but only 1 provided
    writer.writeUnsignedVarInt(3) // length + 1 = 3 means 2 elements
    writer.writeInt32(1)
    // Missing second element

    const reader = new BinaryReader(writer.finish())
    const result = decodeMetadataResponse(reader, 9)

    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated brokers array length (non-flexible)", () => {
    const writer = new BinaryWriter()
    // Only 2 bytes instead of 4 for brokers array length
    writer.writeInt16(1)

    const reader = new BinaryReader(writer.finish())
    const result = decodeMetadataResponse(reader, 0)

    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated topics array length (non-flexible)", () => {
    const writer = new BinaryWriter()
    // Empty brokers
    writer.writeInt32(0)
    // Only 2 bytes instead of 4 for topics array length
    writer.writeInt16(1)

    const reader = new BinaryReader(writer.finish())
    const result = decodeMetadataResponse(reader, 0)

    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated partitions array length (non-flexible)", () => {
    const writer = new BinaryWriter()
    // Empty brokers
    writer.writeInt32(0)
    // Topics (length: 1)
    writer.writeInt32(1)
    writer.writeInt16(0) // error
    writer.writeString("test")
    // Only 2 bytes instead of 4 for partitions array length
    writer.writeInt16(1)

    const reader = new BinaryReader(writer.finish())
    const result = decodeMetadataResponse(reader, 0)

    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated replicas array length (non-flexible)", () => {
    const writer = new BinaryWriter()
    // Empty brokers
    writer.writeInt32(0)
    // Topics (length: 1)
    writer.writeInt32(1)
    writer.writeInt16(0)
    writer.writeString("test")
    // Partitions (length: 1)
    writer.writeInt32(1)
    writer.writeInt16(0)
    writer.writeInt32(0)
    writer.writeInt32(1)
    // Only 2 bytes instead of 4 for replicas array length
    writer.writeInt16(1)

    const reader = new BinaryReader(writer.finish())
    const result = decodeMetadataResponse(reader, 0)

    expect(result.ok).toBe(false)
  })

  it("returns failure on truncated isr compact array length", () => {
    const writer = new BinaryWriter()
    // Throttle time
    writer.writeInt32(0)
    // Empty brokers (compact)
    writer.writeUnsignedVarInt(1)
    // Cluster ID
    writer.writeCompactString("cluster")
    // Controller ID
    writer.writeInt32(1)
    // Topics (compact: 2 = 1 topic)
    writer.writeUnsignedVarInt(2)
    writer.writeInt16(0)
    writer.writeCompactString("test")
    writer.writeBoolean(false)
    // Partitions (compact: 2 = 1 partition)
    writer.writeUnsignedVarInt(2)
    writer.writeInt16(0)
    writer.writeInt32(0)
    writer.writeInt32(1)
    writer.writeInt32(0) // leader epoch
    // Replicas (empty)
    writer.writeUnsignedVarInt(1)
    // Truncated varint for ISR array
    writer.writeInt8(0x80)

    const reader = new BinaryReader(writer.finish())
    const result = decodeMetadataResponse(reader, 9)

    expect(result.ok).toBe(false)
  })
})
