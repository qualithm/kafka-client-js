import { describe, expect, it } from "vitest"

import { ApiKey } from "../../api-keys"
import { BinaryReader } from "../../binary-reader"
import { BinaryWriter } from "../../binary-writer"
import {
  buildFindCoordinatorRequest,
  CoordinatorType,
  decodeFindCoordinatorResponse,
  encodeFindCoordinatorRequest,
  type FindCoordinatorRequest
} from "../../find-coordinator"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeFindCoordinatorRequest", () => {
  describe("v0 — key only", () => {
    it("encodes group key", () => {
      const writer = new BinaryWriter()
      const request: FindCoordinatorRequest = { key: "my-group" }
      encodeFindCoordinatorRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Key (nullable string)
      const keyResult = reader.readString()
      expect(keyResult.ok).toBe(true)
      expect(keyResult.ok && keyResult.value).toBe("my-group")
      expect(reader.remaining).toBe(0)
    })
  })

  describe("v1–v3 — adds key_type", () => {
    it("encodes GROUP type (default)", () => {
      const writer = new BinaryWriter()
      const request: FindCoordinatorRequest = { key: "my-group" }
      encodeFindCoordinatorRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Key
      const keyResult = reader.readString()
      expect(keyResult.ok).toBe(true)
      expect(keyResult.ok && keyResult.value).toBe("my-group")

      // Key type (INT8)
      const typeResult = reader.readInt8()
      expect(typeResult.ok).toBe(true)
      expect(typeResult.ok && typeResult.value).toBe(0)

      expect(reader.remaining).toBe(0)
    })

    it("encodes TRANSACTION type", () => {
      const writer = new BinaryWriter()
      const request: FindCoordinatorRequest = {
        key: "my-txn-id",
        keyType: CoordinatorType.Transaction
      }
      encodeFindCoordinatorRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Key
      const keyResult = reader.readString()
      expect(keyResult.ok).toBe(true)
      expect(keyResult.ok && keyResult.value).toBe("my-txn-id")

      // Key type
      const typeResult = reader.readInt8()
      expect(typeResult.ok).toBe(true)
      expect(typeResult.ok && typeResult.value).toBe(1)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v4+ — flexible encoding with batched lookup", () => {
    it("encodes single key as array", () => {
      const writer = new BinaryWriter()
      const request: FindCoordinatorRequest = { key: "my-group" }
      encodeFindCoordinatorRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Key type (INT8)
      const typeResult = reader.readInt8()
      expect(typeResult.ok).toBe(true)
      expect(typeResult.ok && typeResult.value).toBe(0)

      // Coordinator keys array (compact)
      const lengthResult = reader.readUnsignedVarInt()
      expect(lengthResult.ok).toBe(true)
      expect(lengthResult.ok && lengthResult.value).toBe(2) // length + 1

      // First key
      const keyResult = reader.readCompactString()
      expect(keyResult.ok).toBe(true)
      expect(keyResult.ok && keyResult.value).toBe("my-group")

      // Tagged fields
      const tagResult = reader.readTaggedFields()
      expect(tagResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })

    it("encodes multiple coordinator keys", () => {
      const writer = new BinaryWriter()
      const request: FindCoordinatorRequest = {
        key: "ignored",
        coordinatorKeys: ["group-a", "group-b", "group-c"]
      }
      encodeFindCoordinatorRequest(writer, request, 4)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // Key type
      reader.readInt8()

      // Array length
      const lengthResult = reader.readUnsignedVarInt()
      expect(lengthResult.ok).toBe(true)
      expect(lengthResult.ok && lengthResult.value).toBe(4) // 3 + 1

      // Keys
      const key1 = reader.readCompactString()
      expect(key1.ok && key1.value).toBe("group-a")
      const key2 = reader.readCompactString()
      expect(key2.ok && key2.value).toBe("group-b")
      const key3 = reader.readCompactString()
      expect(key3.ok && key3.value).toBe("group-c")

      // Tagged fields
      const tagResult = reader.readTaggedFields()
      expect(tagResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

describe("buildFindCoordinatorRequest", () => {
  it("builds a framed v0 request", () => {
    const frame = buildFindCoordinatorRequest(1, 0, { key: "test-group" })

    const reader = new BinaryReader(frame)

    // Size prefix
    const sizeResult = reader.readInt32()
    expect(sizeResult.ok).toBe(true)
    expect(sizeResult.ok && sizeResult.value).toBe(frame.length - 4)

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.FindCoordinator)

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
    expect(clientIdResult.ok && clientIdResult.value).toBeNull()

    // Body: key
    const keyResult = reader.readString()
    expect(keyResult.ok).toBe(true)
    expect(keyResult.ok && keyResult.value).toBe("test-group")

    expect(reader.remaining).toBe(0)
  })

  it("builds a framed v4 request with client ID", () => {
    const frame = buildFindCoordinatorRequest(42, 4, { key: "my-group" }, "my-client")

    const reader = new BinaryReader(frame)

    // Size prefix
    reader.readInt32()

    // API key
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.FindCoordinator)

    // API version
    const versionResult = reader.readInt16()
    expect(versionResult.ok && versionResult.value).toBe(4)

    // Correlation ID
    const corrIdResult = reader.readInt32()
    expect(corrIdResult.ok && corrIdResult.value).toBe(42)

    // Client ID (compact string, v2 header)
    const clientIdResult = reader.readCompactString()
    expect(clientIdResult.ok && clientIdResult.value).toBe("my-client")

    // Header tagged fields
    reader.readTaggedFields()

    // Body: key_type
    const typeResult = reader.readInt8()
    expect(typeResult.ok && typeResult.value).toBe(0)

    // Body: coordinator_keys array
    const lengthResult = reader.readUnsignedVarInt()
    expect(lengthResult.ok && lengthResult.value).toBe(2)

    const keyResult = reader.readCompactString()
    expect(keyResult.ok && keyResult.value).toBe("my-group")

    // Body tagged fields
    reader.readTaggedFields()

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeFindCoordinatorResponse", () => {
  describe("v0 — error code, node, host, port", () => {
    it("decodes successful response", () => {
      const writer = new BinaryWriter()
      // Error code: 0 (no error)
      writer.writeInt16(0)
      // Node ID
      writer.writeInt32(1)
      // Host
      writer.writeString("broker-1.example.com")
      // Port
      writer.writeInt32(9092)

      const reader = new BinaryReader(writer.finish())
      const result = decodeFindCoordinatorResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.errorMessage).toBeNull()
      expect(result.value.nodeId).toBe(1)
      expect(result.value.host).toBe("broker-1.example.com")
      expect(result.value.port).toBe(9092)
      expect(result.value.coordinators).toEqual([])
      expect(result.value.taggedFields).toEqual([])
    })

    it("decodes error response", () => {
      const writer = new BinaryWriter()
      // Error code: 15 (COORDINATOR_NOT_AVAILABLE)
      writer.writeInt16(15)
      // Node ID
      writer.writeInt32(-1)
      // Host
      writer.writeString("")
      // Port
      writer.writeInt32(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeFindCoordinatorResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(15)
      expect(result.value.nodeId).toBe(-1)
    })

    it("decodes response with null host as empty string", () => {
      const writer = new BinaryWriter()
      // Error code: 0
      writer.writeInt16(0)
      // Node ID
      writer.writeInt32(1)
      // Host (null)
      writer.writeString(null)
      // Port
      writer.writeInt32(9092)

      const reader = new BinaryReader(writer.finish())
      const result = decodeFindCoordinatorResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.host).toBe("")
    })
  })

  describe("v1–v3 — adds throttle time and error message", () => {
    it("decodes response with throttle time", () => {
      const writer = new BinaryWriter()
      // Throttle time: 100ms
      writer.writeInt32(100)
      // Error code: 0
      writer.writeInt16(0)
      // Error message: null
      writer.writeString(null)
      // Node ID
      writer.writeInt32(2)
      // Host
      writer.writeString("broker-2.local")
      // Port
      writer.writeInt32(9093)

      const reader = new BinaryReader(writer.finish())
      const result = decodeFindCoordinatorResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(100)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.errorMessage).toBeNull()
      expect(result.value.nodeId).toBe(2)
      expect(result.value.host).toBe("broker-2.local")
      expect(result.value.port).toBe(9093)
    })

    it("decodes response with error message", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Error code: 16 (NOT_COORDINATOR)
      writer.writeInt16(16)
      // Error message
      writer.writeString("broker is not the coordinator")
      // Node ID
      writer.writeInt32(-1)
      // Host
      writer.writeString("")
      // Port
      writer.writeInt32(0)

      const reader = new BinaryReader(writer.finish())
      const result = decodeFindCoordinatorResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(16)
      expect(result.value.errorMessage).toBe("broker is not the coordinator")
    })
  })

  describe("v4+ — flexible with coordinators array", () => {
    it("decodes single coordinator", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(50)
      // Coordinators array (compact): length + 1 = 2
      writer.writeUnsignedVarInt(2)
      // Coordinator 1:
      //   key (compact string)
      writer.writeCompactString("my-group")
      //   node_id
      writer.writeInt32(3)
      //   host (compact string)
      writer.writeCompactString("broker-3.kafka.svc")
      //   port
      writer.writeInt32(9094)
      //   error_code
      writer.writeInt16(0)
      //   error_message (compact nullable)
      writer.writeCompactString(null)
      //   tagged fields
      writer.writeTaggedFields([])
      // Top-level tagged fields
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeFindCoordinatorResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(50)
      expect(result.value.coordinators).toHaveLength(1)
      expect(result.value.coordinators[0]).toEqual({
        key: "my-group",
        nodeId: 3,
        host: "broker-3.kafka.svc",
        port: 9094,
        errorCode: 0,
        errorMessage: null,
        taggedFields: []
      })
      // Backward-compatible fields from first coordinator
      expect(result.value.errorCode).toBe(0)
      expect(result.value.nodeId).toBe(3)
      expect(result.value.host).toBe("broker-3.kafka.svc")
      expect(result.value.port).toBe(9094)
    })

    it("decodes multiple coordinators (batched response)", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Coordinators array: 3 coordinators
      writer.writeUnsignedVarInt(4) // length + 1

      // Coordinator 1
      writer.writeCompactString("group-a")
      writer.writeInt32(1)
      writer.writeCompactString("broker-1")
      writer.writeInt32(9092)
      writer.writeInt16(0)
      writer.writeCompactString(null)
      writer.writeTaggedFields([])

      // Coordinator 2
      writer.writeCompactString("group-b")
      writer.writeInt32(2)
      writer.writeCompactString("broker-2")
      writer.writeInt32(9092)
      writer.writeInt16(0)
      writer.writeCompactString(null)
      writer.writeTaggedFields([])

      // Coordinator 3 (with error)
      writer.writeCompactString("group-c")
      writer.writeInt32(-1)
      writer.writeCompactString("")
      writer.writeInt32(0)
      writer.writeInt16(15) // COORDINATOR_NOT_AVAILABLE
      writer.writeCompactString("coordinator not available")
      writer.writeTaggedFields([])

      // Top-level tagged fields
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeFindCoordinatorResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.coordinators).toHaveLength(3)
      expect(result.value.coordinators[0].key).toBe("group-a")
      expect(result.value.coordinators[1].key).toBe("group-b")
      expect(result.value.coordinators[2].key).toBe("group-c")
      expect(result.value.coordinators[2].errorCode).toBe(15)
      expect(result.value.coordinators[2].errorMessage).toBe("coordinator not available")
    })

    it("decodes empty coordinators array", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Empty coordinators array
      writer.writeUnsignedVarInt(1) // length 0 + 1
      // Top-level tagged fields
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeFindCoordinatorResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.coordinators).toHaveLength(0)
      expect(result.value.nodeId).toBe(-1)
      expect(result.value.host).toBe("")
      expect(result.value.port).toBe(0)
    })

    it("decodes null coordinators array as empty", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Null coordinators array (compact: 0 means null)
      writer.writeUnsignedVarInt(0)
      // Top-level tagged fields
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeFindCoordinatorResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.coordinators).toHaveLength(0)
    })

    it("decodes coordinator with null key and host as empty strings", () => {
      const writer = new BinaryWriter()
      // Throttle time
      writer.writeInt32(0)
      // Coordinators array: 1 coordinator
      writer.writeUnsignedVarInt(2)
      // Coordinator with null key and host
      writer.writeCompactString(null) // null key
      writer.writeInt32(1)
      writer.writeCompactString(null) // null host
      writer.writeInt32(9092)
      writer.writeInt16(0)
      writer.writeCompactString(null)
      writer.writeTaggedFields([])
      // Top-level tagged fields
      writer.writeTaggedFields([])

      const reader = new BinaryReader(writer.finish())
      const result = decodeFindCoordinatorResponse(reader, 4)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.coordinators).toHaveLength(1)
      expect(result.value.coordinators[0].key).toBe("")
      expect(result.value.coordinators[0].host).toBe("")
    })
  })

  describe("decode error handling", () => {
    describe("v0 truncated buffers", () => {
      it("fails on truncated error code", () => {
        const reader = new BinaryReader(new Uint8Array([0x00])) // only 1 byte, need 2
        const result = decodeFindCoordinatorResponse(reader, 0)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated node ID", () => {
        const writer = new BinaryWriter()
        writer.writeInt16(0) // error code
        // no node ID
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 0)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated host", () => {
        const writer = new BinaryWriter()
        writer.writeInt16(0) // error code
        writer.writeInt32(1) // node ID
        // no host
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 0)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated port", () => {
        const writer = new BinaryWriter()
        writer.writeInt16(0) // error code
        writer.writeInt32(1) // node ID
        writer.writeString("broker") // host
        // no port
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 0)
        expect(result.ok).toBe(false)
      })
    })

    describe("v1-v3 truncated buffers", () => {
      it("fails on truncated throttle time", () => {
        const reader = new BinaryReader(new Uint8Array([0x00, 0x00])) // only 2 bytes, need 4
        const result = decodeFindCoordinatorResponse(reader, 1)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated error message", () => {
        const writer = new BinaryWriter()
        writer.writeInt32(0) // throttle time
        writer.writeInt16(0) // error code
        // no error message
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 1)
        expect(result.ok).toBe(false)
      })
    })

    describe("v4 truncated buffers", () => {
      it("fails on truncated coordinators array length", () => {
        const writer = new BinaryWriter()
        writer.writeInt32(0) // throttle time
        // no coordinators array
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 4)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated coordinator key", () => {
        const writer = new BinaryWriter()
        writer.writeInt32(0) // throttle time
        writer.writeUnsignedVarInt(2) // 1 coordinator
        // no key
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 4)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated coordinator node ID", () => {
        const writer = new BinaryWriter()
        writer.writeInt32(0) // throttle time
        writer.writeUnsignedVarInt(2) // 1 coordinator
        writer.writeCompactString("group-a") // key
        // no node ID
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 4)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated coordinator host", () => {
        const writer = new BinaryWriter()
        writer.writeInt32(0) // throttle time
        writer.writeUnsignedVarInt(2) // 1 coordinator
        writer.writeCompactString("group-a") // key
        writer.writeInt32(1) // node ID
        // no host
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 4)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated coordinator port", () => {
        const writer = new BinaryWriter()
        writer.writeInt32(0) // throttle time
        writer.writeUnsignedVarInt(2) // 1 coordinator
        writer.writeCompactString("group-a") // key
        writer.writeInt32(1) // node ID
        writer.writeCompactString("broker") // host
        // no port
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 4)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated coordinator error code", () => {
        const writer = new BinaryWriter()
        writer.writeInt32(0) // throttle time
        writer.writeUnsignedVarInt(2) // 1 coordinator
        writer.writeCompactString("group-a") // key
        writer.writeInt32(1) // node ID
        writer.writeCompactString("broker") // host
        writer.writeInt32(9092) // port
        // no error code
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 4)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated coordinator error message", () => {
        const writer = new BinaryWriter()
        writer.writeInt32(0) // throttle time
        writer.writeUnsignedVarInt(2) // 1 coordinator
        writer.writeCompactString("group-a") // key
        writer.writeInt32(1) // node ID
        writer.writeCompactString("broker") // host
        writer.writeInt32(9092) // port
        writer.writeInt16(0) // error code
        // no error message
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 4)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated coordinator tagged fields", () => {
        const writer = new BinaryWriter()
        writer.writeInt32(0) // throttle time
        writer.writeUnsignedVarInt(2) // 1 coordinator
        writer.writeCompactString("group-a") // key
        writer.writeInt32(1) // node ID
        writer.writeCompactString("broker") // host
        writer.writeInt32(9092) // port
        writer.writeInt16(0) // error code
        writer.writeCompactString(null) // error message
        // no tagged fields
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 4)
        expect(result.ok).toBe(false)
      })

      it("fails on truncated top-level tagged fields", () => {
        const writer = new BinaryWriter()
        writer.writeInt32(0) // throttle time
        writer.writeUnsignedVarInt(2) // 1 coordinator
        writer.writeCompactString("group-a") // key
        writer.writeInt32(1) // node ID
        writer.writeCompactString("broker") // host
        writer.writeInt32(9092) // port
        writer.writeInt16(0) // error code
        writer.writeCompactString(null) // error message
        writer.writeTaggedFields([]) // coordinator tagged fields
        // no top-level tagged fields
        const reader = new BinaryReader(writer.finish())
        const result = decodeFindCoordinatorResponse(reader, 4)
        expect(result.ok).toBe(false)
      })
    })
  })
})
