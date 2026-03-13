import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildCreateAclsRequest,
  type CreateAclsRequest,
  decodeCreateAclsResponse,
  encodeCreateAclsRequest
} from "../../../protocol/create-acls"
import {
  AclOperation,
  AclPermissionType,
  AclResourcePatternType,
  AclResourceType
} from "../../../protocol/describe-acls"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeCreateAclsRequest", () => {
  describe("v0 — basic fields", () => {
    it("encodes a single ACL creation", () => {
      const writer = new BinaryWriter()
      const request: CreateAclsRequest = {
        creations: [
          {
            resourceType: AclResourceType.Topic,
            resourceName: "my-topic",
            principal: "User:alice",
            host: "*",
            operation: AclOperation.Read,
            permissionType: AclPermissionType.Allow
          }
        ]
      }
      encodeCreateAclsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // array count (INT32)
      const countResult = reader.readInt32()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(1)

      // resource_type (INT8)
      const typeResult = reader.readInt8()
      expect(typeResult.ok).toBe(true)
      expect(typeResult.ok && typeResult.value).toBe(AclResourceType.Topic)

      // resource_name
      const nameResult = reader.readString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("my-topic")

      // principal
      const principalResult = reader.readString()
      expect(principalResult.ok).toBe(true)
      expect(principalResult.ok && principalResult.value).toBe("User:alice")

      // host
      const hostResult = reader.readString()
      expect(hostResult.ok).toBe(true)
      expect(hostResult.ok && hostResult.value).toBe("*")

      // operation (INT8)
      const opResult = reader.readInt8()
      expect(opResult.ok).toBe(true)
      expect(opResult.ok && opResult.value).toBe(AclOperation.Read)

      // permission_type (INT8)
      const permResult = reader.readInt8()
      expect(permResult.ok).toBe(true)
      expect(permResult.ok && permResult.value).toBe(AclPermissionType.Allow)

      expect(reader.remaining).toBe(0)
    })

    it("encodes empty creations array", () => {
      const writer = new BinaryWriter()
      encodeCreateAclsRequest(writer, { creations: [] }, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const countResult = reader.readInt32()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(0)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v1 — adds resource pattern type", () => {
    it("encodes resource_pattern_type field", () => {
      const writer = new BinaryWriter()
      const request: CreateAclsRequest = {
        creations: [
          {
            resourceType: AclResourceType.Topic,
            resourceName: "prefix-",
            resourcePatternType: AclResourcePatternType.Prefixed,
            principal: "User:bob",
            host: "*",
            operation: AclOperation.Write,
            permissionType: AclPermissionType.Allow
          }
        ]
      }
      encodeCreateAclsRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // array count
      reader.readInt8() // resource_type
      reader.readString() // resource_name

      // resource_pattern_type (INT8)
      const patternResult = reader.readInt8()
      expect(patternResult.ok).toBe(true)
      expect(patternResult.ok && patternResult.value).toBe(AclResourcePatternType.Prefixed)
    })

    it("defaults resource_pattern_type to Literal", () => {
      const writer = new BinaryWriter()
      const request: CreateAclsRequest = {
        creations: [
          {
            resourceType: AclResourceType.Topic,
            resourceName: "test",
            principal: "User:test",
            host: "*",
            operation: AclOperation.Read,
            permissionType: AclPermissionType.Allow
          }
        ]
      }
      encodeCreateAclsRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // array count
      reader.readInt8() // resource_type
      reader.readString() // resource_name

      const patternResult = reader.readInt8()
      expect(patternResult.ok).toBe(true)
      expect(patternResult.ok && patternResult.value).toBe(AclResourcePatternType.Literal)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: CreateAclsRequest = {
        creations: [
          {
            resourceType: AclResourceType.Group,
            resourceName: "flex-group",
            resourcePatternType: AclResourcePatternType.Literal,
            principal: "User:admin",
            host: "*",
            operation: AclOperation.All,
            permissionType: AclPermissionType.Allow
          }
        ]
      }
      encodeCreateAclsRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // compact array (count + 1)
      const countResult = reader.readUnsignedVarInt()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(2) // 1 + 1

      reader.readInt8() // resource_type

      // compact string
      const nameResult = reader.readCompactString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("flex-group")

      reader.readInt8() // resource_pattern_type

      const principalResult = reader.readCompactString()
      expect(principalResult.ok).toBe(true)
      expect(principalResult.ok && principalResult.value).toBe("User:admin")

      const hostResult = reader.readCompactString()
      expect(hostResult.ok).toBe(true)
      expect(hostResult.ok && hostResult.value).toBe("*")

      reader.readInt8() // operation
      reader.readInt8() // permission_type

      // element tagged fields
      const tagResult = reader.readTaggedFields()
      expect(tagResult.ok).toBe(true)

      // top-level tagged fields
      const topTagResult = reader.readTaggedFields()
      expect(topTagResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// buildCreateAclsRequest (framed)
// ---------------------------------------------------------------------------

describe("buildCreateAclsRequest", () => {
  it("builds a framed v0 request", () => {
    const request: CreateAclsRequest = {
      creations: [
        {
          resourceType: AclResourceType.Topic,
          resourceName: "t",
          principal: "User:x",
          host: "*",
          operation: AclOperation.Read,
          permissionType: AclPermissionType.Allow
        }
      ]
    }
    const framed = buildCreateAclsRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.CreateAcls)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeCreateAclsResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes successful creation results", () => {
      const w = new BinaryWriter()
      // throttle_time_ms
      w.writeInt32(0)
      // results array count
      w.writeInt32(2)
      // result 1 — success
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message
      // result 2 — success
      w.writeInt16(0)
      w.writeString(null)

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeCreateAclsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.results).toHaveLength(2)
      expect(result.value.results[0].errorCode).toBe(0)
      expect(result.value.results[0].errorMessage).toBeNull()
      expect(result.value.results[1].errorCode).toBe(0)
    })

    it("decodes error results", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(1) // results count
      w.writeInt16(31) // SECURITY_DISABLED
      w.writeString("ACLs are disabled")

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeCreateAclsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.results[0].errorCode).toBe(31)
      expect(result.value.results[0].errorMessage).toBe("ACLs are disabled")
    })

    it("decodes empty results", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(0) // no results

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeCreateAclsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.results).toHaveLength(0)
    })
  })

  describe("v2 — flexible format", () => {
    it("decodes with compact strings and tagged fields", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      // results compact array (count + 1)
      w.writeUnsignedVarInt(2) // 1 result + 1
      w.writeInt16(0) // error_code
      w.writeCompactString(null) // error_message
      w.writeUnsignedVarInt(0) // result tagged fields
      w.writeUnsignedVarInt(0) // top-level tagged fields

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeCreateAclsResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.results).toHaveLength(1)
      expect(result.value.results[0].errorCode).toBe(0)
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeCreateAclsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v2 input", () => {
      const reader = new BinaryReader(new Uint8Array(3))
      const result = decodeCreateAclsResponse(reader, 2)
      expect(result.ok).toBe(false)
    })
  })
})
