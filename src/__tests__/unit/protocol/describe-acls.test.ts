import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  AclOperation,
  AclPermissionType,
  AclResourcePatternType,
  AclResourceType,
  buildDescribeAclsRequest,
  decodeDescribeAclsResponse,
  type DescribeAclsRequest,
  encodeDescribeAclsRequest
} from "../../../protocol/describe-acls"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeAclsRequest", () => {
  describe("v0 — basic filter fields", () => {
    it("encodes all filter fields", () => {
      const writer = new BinaryWriter()
      const request: DescribeAclsRequest = {
        resourceTypeFilter: AclResourceType.Topic,
        resourceNameFilter: "my-topic",
        principalFilter: "User:alice",
        hostFilter: "*",
        operation: AclOperation.Read,
        permissionType: AclPermissionType.Allow
      }
      encodeDescribeAclsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // resource_type_filter (INT8)
      const typeResult = reader.readInt8()
      expect(typeResult.ok).toBe(true)
      expect(typeResult.ok && typeResult.value).toBe(AclResourceType.Topic)

      // resource_name_filter (nullable string)
      const nameResult = reader.readString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("my-topic")

      // principal_filter (nullable string)
      const principalResult = reader.readString()
      expect(principalResult.ok).toBe(true)
      expect(principalResult.ok && principalResult.value).toBe("User:alice")

      // host_filter (nullable string)
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

    it("encodes null filter fields", () => {
      const writer = new BinaryWriter()
      const request: DescribeAclsRequest = {
        resourceTypeFilter: AclResourceType.Any,
        resourceNameFilter: null,
        principalFilter: null,
        hostFilter: null,
        operation: AclOperation.Any,
        permissionType: AclPermissionType.Any
      }
      encodeDescribeAclsRequest(writer, request, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt8() // resource_type_filter

      // null string is written as -1
      const nameResult = reader.readString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBeNull()

      const principalResult = reader.readString()
      expect(principalResult.ok).toBe(true)
      expect(principalResult.ok && principalResult.value).toBeNull()

      const hostResult = reader.readString()
      expect(hostResult.ok).toBe(true)
      expect(hostResult.ok && hostResult.value).toBeNull()
    })
  })

  describe("v1 — adds pattern type filter", () => {
    it("encodes pattern_type_filter field", () => {
      const writer = new BinaryWriter()
      const request: DescribeAclsRequest = {
        resourceTypeFilter: AclResourceType.Topic,
        resourceNameFilter: "test",
        patternTypeFilter: AclResourcePatternType.Prefixed,
        principalFilter: null,
        hostFilter: null,
        operation: AclOperation.Any,
        permissionType: AclPermissionType.Any
      }
      encodeDescribeAclsRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt8() // resource_type_filter
      reader.readString() // resource_name_filter

      // pattern_type_filter (INT8)
      const patternResult = reader.readInt8()
      expect(patternResult.ok).toBe(true)
      expect(patternResult.ok && patternResult.value).toBe(AclResourcePatternType.Prefixed)
    })

    it("defaults pattern_type_filter to Literal", () => {
      const writer = new BinaryWriter()
      const request: DescribeAclsRequest = {
        resourceTypeFilter: AclResourceType.Topic,
        resourceNameFilter: null,
        principalFilter: null,
        hostFilter: null,
        operation: AclOperation.Any,
        permissionType: AclPermissionType.Any
      }
      encodeDescribeAclsRequest(writer, request, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt8() // resource_type_filter
      reader.readString() // resource_name_filter

      const patternResult = reader.readInt8()
      expect(patternResult.ok).toBe(true)
      expect(patternResult.ok && patternResult.value).toBe(AclResourcePatternType.Literal)
    })
  })

  describe("v2 — flexible encoding", () => {
    it("encodes with compact strings and tagged fields", () => {
      const writer = new BinaryWriter()
      const request: DescribeAclsRequest = {
        resourceTypeFilter: AclResourceType.Topic,
        resourceNameFilter: "flex-topic",
        patternTypeFilter: AclResourcePatternType.Literal,
        principalFilter: "User:bob",
        hostFilter: "*",
        operation: AclOperation.Write,
        permissionType: AclPermissionType.Allow
      }
      encodeDescribeAclsRequest(writer, request, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // resource_type_filter (INT8)
      const typeResult = reader.readInt8()
      expect(typeResult.ok).toBe(true)
      expect(typeResult.ok && typeResult.value).toBe(AclResourceType.Topic)

      // compact string
      const nameResult = reader.readCompactString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("flex-topic")

      // pattern_type_filter
      const patternResult = reader.readInt8()
      expect(patternResult.ok).toBe(true)

      // compact strings
      const principalResult = reader.readCompactString()
      expect(principalResult.ok).toBe(true)
      expect(principalResult.ok && principalResult.value).toBe("User:bob")

      const hostResult = reader.readCompactString()
      expect(hostResult.ok).toBe(true)
      expect(hostResult.ok && hostResult.value).toBe("*")

      reader.readInt8() // operation
      reader.readInt8() // permission_type

      // tagged fields
      const tagResult = reader.readTaggedFields()
      expect(tagResult.ok).toBe(true)

      expect(reader.remaining).toBe(0)
    })
  })
})

// ---------------------------------------------------------------------------
// buildDescribeAclsRequest (framed)
// ---------------------------------------------------------------------------

describe("buildDescribeAclsRequest", () => {
  it("builds a framed v0 request", () => {
    const request: DescribeAclsRequest = {
      resourceTypeFilter: AclResourceType.Any,
      resourceNameFilter: null,
      principalFilter: null,
      hostFilter: null,
      operation: AclOperation.Any,
      permissionType: AclPermissionType.Any
    }
    const framed = buildDescribeAclsRequest(1, 0, request, "test-client")
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.DescribeAcls)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeAclsResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes successful response with resources", () => {
      const w = new BinaryWriter()
      // throttle_time_ms
      w.writeInt32(0)
      // error_code
      w.writeInt16(0)
      // error_message (null)
      w.writeString(null)
      // resources array count
      w.writeInt32(1)
      // resource 1
      w.writeInt8(AclResourceType.Topic) // resource_type
      w.writeString("my-topic") // resource_name
      // acls array count
      w.writeInt32(1)
      // acl 1
      w.writeString("User:alice") // principal
      w.writeString("*") // host
      w.writeInt8(AclOperation.Read) // operation
      w.writeInt8(AclPermissionType.Allow) // permission_type

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeAclsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.errorCode).toBe(0)
      expect(result.value.errorMessage).toBeNull()
      expect(result.value.resources).toHaveLength(1)

      const resource = result.value.resources[0]
      expect(resource.resourceType).toBe(AclResourceType.Topic)
      expect(resource.resourceName).toBe("my-topic")
      expect(resource.patternType).toBe(AclResourcePatternType.Literal) // default
      expect(resource.acls).toHaveLength(1)

      const acl = resource.acls[0]
      expect(acl.principal).toBe("User:alice")
      expect(acl.host).toBe("*")
      expect(acl.operation).toBe(AclOperation.Read)
      expect(acl.permissionType).toBe(AclPermissionType.Allow)
    })

    it("decodes error response", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(31) // SECURITY_DISABLED
      w.writeString("ACLs are disabled")
      w.writeInt32(0) // empty resources

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeAclsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.errorCode).toBe(31)
      expect(result.value.errorMessage).toBe("ACLs are disabled")
      expect(result.value.resources).toHaveLength(0)
    })

    it("decodes empty resources", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // no error
      w.writeString(null)
      w.writeInt32(0) // no resources

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeAclsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.resources).toHaveLength(0)
    })
  })

  describe("v1 — with pattern type", () => {
    it("decodes pattern_type in resources", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message
      w.writeInt32(1) // resources count
      w.writeInt8(AclResourceType.Topic) // resource_type
      w.writeString("prefix-topic") // resource_name
      w.writeInt8(AclResourcePatternType.Prefixed) // pattern_type (v1+)
      w.writeInt32(0) // empty acls

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeAclsResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.resources[0].patternType).toBe(AclResourcePatternType.Prefixed)
    })
  })

  describe("v2 — flexible format", () => {
    it("decodes with compact strings and tagged fields", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeCompactString(null) // error_message
      // resources compact array (count + 1)
      w.writeUnsignedVarInt(2) // 1 resource + 1
      w.writeInt8(AclResourceType.Group) // resource_type
      w.writeCompactString("my-group") // resource_name
      w.writeInt8(AclResourcePatternType.Literal) // pattern_type
      // acls compact array (count + 1)
      w.writeUnsignedVarInt(2) // 1 acl + 1
      w.writeCompactString("User:admin") // principal
      w.writeCompactString("*") // host
      w.writeInt8(AclOperation.All) // operation
      w.writeInt8(AclPermissionType.Allow) // permission_type
      w.writeUnsignedVarInt(0) // acl tagged fields
      w.writeUnsignedVarInt(0) // resource tagged fields
      w.writeUnsignedVarInt(0) // top-level tagged fields

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDescribeAclsResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.resources).toHaveLength(1)
      const resource = result.value.resources[0]
      expect(resource.resourceName).toBe("my-group")
      expect(resource.acls).toHaveLength(1)
      expect(resource.acls[0].principal).toBe("User:admin")
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeDescribeAclsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v2 input", () => {
      const reader = new BinaryReader(new Uint8Array(3))
      const result = decodeDescribeAclsResponse(reader, 2)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated resource entry", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message (v0 — standard string)
      w.writeInt32(1) // resources array (1 entry)
      w.writeInt8(2) // resource_type — but resource_name missing
      const buf = w.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDescribeAclsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated acl entry", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message (v0)
      w.writeInt32(1) // resources array (1 entry)
      w.writeInt8(2) // resource_type
      w.writeString("topic-a") // resource_name
      w.writeInt32(1) // acls array (1 entry)
      // principal missing
      const buf = w.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDescribeAclsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v2 resource entry", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeCompactString(null) // error_message (v2 — compact)
      w.writeUnsignedVarInt(2) // resources compact array (1 entry)
      w.writeInt8(2) // resource_type — but compact resource_name missing
      const buf = w.finish()
      const reader = new BinaryReader(buf)
      const result = decodeDescribeAclsResponse(reader, 2)
      expect(result.ok).toBe(false)
    })
  })
})
