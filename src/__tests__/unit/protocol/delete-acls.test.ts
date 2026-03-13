import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDeleteAclsRequest,
  decodeDeleteAclsResponse,
  type DeleteAclsFilter,
  encodeDeleteAclsRequest
} from "../../../protocol/delete-acls"
import {
  AclOperation,
  AclPermissionType,
  AclResourcePatternType,
  AclResourceType
} from "../../../protocol/describe-acls"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDeleteAclsRequest", () => {
  describe("v0 — basic filter fields", () => {
    it("encodes a single filter", () => {
      const writer = new BinaryWriter()
      const filters: DeleteAclsFilter[] = [
        {
          resourceTypeFilter: AclResourceType.Topic,
          resourceNameFilter: "my-topic",
          principalFilter: "User:alice",
          hostFilter: "*",
          operation: AclOperation.Read,
          permissionType: AclPermissionType.Allow
        }
      ]
      encodeDeleteAclsRequest(writer, { filters }, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // array count (INT32)
      const countResult = reader.readInt32()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(1)

      // resource_type_filter (INT8)
      const typeResult = reader.readInt8()
      expect(typeResult.ok).toBe(true)
      expect(typeResult.ok && typeResult.value).toBe(AclResourceType.Topic)

      // resource_name_filter (nullable string)
      const nameResult = reader.readString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("my-topic")

      // principal_filter
      const principalResult = reader.readString()
      expect(principalResult.ok).toBe(true)
      expect(principalResult.ok && principalResult.value).toBe("User:alice")

      // host_filter
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
      const filters: DeleteAclsFilter[] = [
        {
          resourceTypeFilter: AclResourceType.Any,
          resourceNameFilter: null,
          principalFilter: null,
          hostFilter: null,
          operation: AclOperation.Any,
          permissionType: AclPermissionType.Any
        }
      ]
      encodeDeleteAclsRequest(writer, { filters }, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // array count
      reader.readInt8() // resource_type_filter

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

    it("encodes empty filters array", () => {
      const writer = new BinaryWriter()
      encodeDeleteAclsRequest(writer, { filters: [] }, 0)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      const countResult = reader.readInt32()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(0)

      expect(reader.remaining).toBe(0)
    })
  })

  describe("v1 — adds pattern type filter", () => {
    it("encodes pattern_type_filter field", () => {
      const writer = new BinaryWriter()
      const filters: DeleteAclsFilter[] = [
        {
          resourceTypeFilter: AclResourceType.Topic,
          resourceNameFilter: "prefix-",
          patternTypeFilter: AclResourcePatternType.Prefixed,
          principalFilter: null,
          hostFilter: null,
          operation: AclOperation.Any,
          permissionType: AclPermissionType.Any
        }
      ]
      encodeDeleteAclsRequest(writer, { filters }, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // array count
      reader.readInt8() // resource_type_filter
      reader.readString() // resource_name_filter

      const patternResult = reader.readInt8()
      expect(patternResult.ok).toBe(true)
      expect(patternResult.ok && patternResult.value).toBe(AclResourcePatternType.Prefixed)
    })

    it("defaults pattern_type_filter to Literal", () => {
      const writer = new BinaryWriter()
      const filters: DeleteAclsFilter[] = [
        {
          resourceTypeFilter: AclResourceType.Topic,
          resourceNameFilter: null,
          principalFilter: null,
          hostFilter: null,
          operation: AclOperation.Any,
          permissionType: AclPermissionType.Any
        }
      ]
      encodeDeleteAclsRequest(writer, { filters }, 1)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      reader.readInt32() // array count
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
      const filters: DeleteAclsFilter[] = [
        {
          resourceTypeFilter: AclResourceType.Group,
          resourceNameFilter: "flex-group",
          patternTypeFilter: AclResourcePatternType.Literal,
          principalFilter: "User:admin",
          hostFilter: "*",
          operation: AclOperation.All,
          permissionType: AclPermissionType.Allow
        }
      ]
      encodeDeleteAclsRequest(writer, { filters }, 2)
      const buf = writer.finish()
      const reader = new BinaryReader(buf)

      // compact array (count + 1)
      const countResult = reader.readUnsignedVarInt()
      expect(countResult.ok).toBe(true)
      expect(countResult.ok && countResult.value).toBe(2) // 1 + 1

      reader.readInt8() // resource_type_filter

      const nameResult = reader.readCompactString()
      expect(nameResult.ok).toBe(true)
      expect(nameResult.ok && nameResult.value).toBe("flex-group")

      reader.readInt8() // pattern_type_filter

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
// buildDeleteAclsRequest (framed)
// ---------------------------------------------------------------------------

describe("buildDeleteAclsRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildDeleteAclsRequest(
      1,
      0,
      {
        filters: [
          {
            resourceTypeFilter: AclResourceType.Any,
            resourceNameFilter: null,
            principalFilter: null,
            hostFilter: null,
            operation: AclOperation.Any,
            permissionType: AclPermissionType.Any
          }
        ]
      },
      "test-client"
    )
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.DeleteAcls)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDeleteAclsResponse", () => {
  describe("v0 — non-flexible", () => {
    it("decodes successful filter results with matching ACLs", () => {
      const w = new BinaryWriter()
      // throttle_time_ms
      w.writeInt32(0)
      // filter_results array count
      w.writeInt32(1)
      // filter result 1
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message
      // matching_acls array count
      w.writeInt32(1)
      // matching ACL 1
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message
      w.writeInt8(AclResourceType.Topic) // resource_type
      w.writeString("my-topic") // resource_name
      w.writeString("User:alice") // principal
      w.writeString("*") // host
      w.writeInt8(AclOperation.Read) // operation
      w.writeInt8(AclPermissionType.Allow) // permission_type

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDeleteAclsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.throttleTimeMs).toBe(0)
      expect(result.value.filterResults).toHaveLength(1)

      const filterResult = result.value.filterResults[0]
      expect(filterResult.errorCode).toBe(0)
      expect(filterResult.errorMessage).toBeNull()
      expect(filterResult.matchingAcls).toHaveLength(1)

      const acl = filterResult.matchingAcls[0]
      expect(acl.errorCode).toBe(0)
      expect(acl.resourceType).toBe(AclResourceType.Topic)
      expect(acl.resourceName).toBe("my-topic")
      expect(acl.patternType).toBe(AclResourcePatternType.Literal) // default
      expect(acl.principal).toBe("User:alice")
      expect(acl.host).toBe("*")
      expect(acl.operation).toBe(AclOperation.Read)
      expect(acl.permissionType).toBe(AclPermissionType.Allow)
    })

    it("decodes empty filter results", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(0) // no filter results

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDeleteAclsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.filterResults).toHaveLength(0)
    })

    it("decodes filter with no matching ACLs", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(1) // 1 filter result
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message
      w.writeInt32(0) // no matching ACLs

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDeleteAclsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.filterResults[0].matchingAcls).toHaveLength(0)
    })

    it("decodes filter-level error", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(1) // 1 filter result
      w.writeInt16(31) // SECURITY_DISABLED
      w.writeString("ACLs are disabled")
      w.writeInt32(0) // no matching ACLs

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDeleteAclsResponse(reader, 0)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.filterResults[0].errorCode).toBe(31)
      expect(result.value.filterResults[0].errorMessage).toBe("ACLs are disabled")
    })
  })

  describe("v1 — with pattern type", () => {
    it("decodes pattern_type in matching ACLs", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt32(1) // filter_results count
      w.writeInt16(0) // error_code
      w.writeString(null) // error_message
      w.writeInt32(1) // matching_acls count
      w.writeInt16(0) // acl error_code
      w.writeString(null) // acl error_message
      w.writeInt8(AclResourceType.Topic) // resource_type
      w.writeString("prefix-topic") // resource_name
      w.writeInt8(AclResourcePatternType.Prefixed) // pattern_type (v1+)
      w.writeString("User:bob") // principal
      w.writeString("*") // host
      w.writeInt8(AclOperation.Write) // operation
      w.writeInt8(AclPermissionType.Allow) // permission_type

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDeleteAclsResponse(reader, 1)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      const acl = result.value.filterResults[0].matchingAcls[0]
      expect(acl.patternType).toBe(AclResourcePatternType.Prefixed)
      expect(acl.principal).toBe("User:bob")
    })
  })

  describe("v2 — flexible format", () => {
    it("decodes with compact strings and tagged fields", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      // filter_results compact array (count + 1)
      w.writeUnsignedVarInt(2) // 1 filter result + 1
      w.writeInt16(0) // error_code
      w.writeCompactString(null) // error_message
      // matching_acls compact array (count + 1)
      w.writeUnsignedVarInt(2) // 1 acl + 1
      w.writeInt16(0) // acl error_code
      w.writeCompactString(null) // acl error_message
      w.writeInt8(AclResourceType.Group) // resource_type
      w.writeCompactString("my-group") // resource_name
      w.writeInt8(AclResourcePatternType.Literal) // pattern_type
      w.writeCompactString("User:admin") // principal
      w.writeCompactString("*") // host
      w.writeInt8(AclOperation.All) // operation
      w.writeInt8(AclPermissionType.Allow) // permission_type
      w.writeUnsignedVarInt(0) // acl tagged fields
      w.writeUnsignedVarInt(0) // filter result tagged fields
      w.writeUnsignedVarInt(0) // top-level tagged fields

      const body = w.finish()
      const reader = new BinaryReader(body)
      const result = decodeDeleteAclsResponse(reader, 2)

      expect(result.ok).toBe(true)
      if (!result.ok) {
        return
      }

      expect(result.value.filterResults).toHaveLength(1)
      const acl = result.value.filterResults[0].matchingAcls[0]
      expect(acl.resourceName).toBe("my-group")
      expect(acl.principal).toBe("User:admin")
    })
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(1))
      const result = decodeDeleteAclsResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v2 input", () => {
      const reader = new BinaryReader(new Uint8Array(3))
      const result = decodeDeleteAclsResponse(reader, 2)
      expect(result.ok).toBe(false)
    })
  })
})
