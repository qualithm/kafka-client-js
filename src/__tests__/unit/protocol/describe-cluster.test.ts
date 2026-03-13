import { describe, expect, it } from "vitest"

import { ApiKey } from "../../../codec/api-keys"
import { BinaryReader } from "../../../codec/binary-reader"
import { BinaryWriter } from "../../../codec/binary-writer"
import {
  buildDescribeClusterRequest,
  decodeDescribeClusterResponse,
  type DescribeClusterRequest,
  encodeDescribeClusterRequest
} from "../../../protocol/describe-cluster"

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

describe("encodeDescribeClusterRequest", () => {
  it("encodes v0 request", () => {
    const writer = new BinaryWriter()
    const request: DescribeClusterRequest = {
      includeClusterAuthorizedOperations: true
    }
    encodeDescribeClusterRequest(writer, request, 0)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // boolean
    const boolResult = reader.readBoolean()
    expect(boolResult.ok).toBe(true)
    expect(boolResult.ok && boolResult.value).toBe(true)

    // tagged fields
    const tagResult = reader.readTaggedFields()
    expect(tagResult.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })

  it("encodes v1 request with endpoint_type", () => {
    const writer = new BinaryWriter()
    const request: DescribeClusterRequest = {
      includeClusterAuthorizedOperations: false,
      endpointType: 2
    }
    encodeDescribeClusterRequest(writer, request, 1)
    const buf = writer.finish()
    const reader = new BinaryReader(buf)

    // boolean
    const boolResult = reader.readBoolean()
    expect(boolResult.ok).toBe(true)
    expect(boolResult.ok && boolResult.value).toBe(false)

    // endpoint_type (INT8)
    const endpointResult = reader.readInt8()
    expect(endpointResult.ok).toBe(true)
    expect(endpointResult.ok && endpointResult.value).toBe(2)

    // tagged fields
    const tagResult = reader.readTaggedFields()
    expect(tagResult.ok).toBe(true)

    expect(reader.remaining).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// buildDescribeClusterRequest (framed)
// ---------------------------------------------------------------------------

describe("buildDescribeClusterRequest", () => {
  it("builds a framed v0 request", () => {
    const framed = buildDescribeClusterRequest(
      1,
      0,
      { includeClusterAuthorizedOperations: true },
      "test-client"
    )
    expect(framed).toBeInstanceOf(Uint8Array)

    const reader = new BinaryReader(framed)
    reader.readInt32() // size
    const apiKeyResult = reader.readInt16()
    expect(apiKeyResult.ok).toBe(true)
    expect(apiKeyResult.ok && apiKeyResult.value).toBe(ApiKey.DescribeCluster)
  })
})

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

describe("decodeDescribeClusterResponse", () => {
  it("decodes v0 response with brokers", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeCompactString("cluster-abc") // cluster_id
    w.writeInt32(1) // controller_id
    // brokers array (compact: count + 1)
    w.writeUnsignedVarInt(2) // 1 broker + 1
    // broker 0
    w.writeInt32(1) // broker_id
    w.writeCompactString("broker1.example.com") // host
    w.writeInt32(9092) // port
    w.writeCompactString(null) // rack
    w.writeTaggedFields([]) // broker tagged fields
    // end brokers
    w.writeInt32(-2147483648) // cluster_authorized_operations (not requested)
    w.writeTaggedFields([]) // response tagged fields

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeClusterResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(0)
    expect(result.value.errorCode).toBe(0)
    expect(result.value.errorMessage).toBeNull()
    expect(result.value.clusterId).toBe("cluster-abc")
    expect(result.value.controllerId).toBe(1)
    expect(result.value.brokers).toHaveLength(1)
    expect(result.value.brokers[0].brokerId).toBe(1)
    expect(result.value.brokers[0].host).toBe("broker1.example.com")
    expect(result.value.brokers[0].port).toBe(9092)
    expect(result.value.brokers[0].rack).toBeNull()
    expect(result.value.clusterAuthorizedOperations).toBe(-2147483648)
    expect(result.value.endpointType).toBe(0)
  })

  it("decodes v1 response with endpoint_type", () => {
    const w = new BinaryWriter()
    w.writeInt32(10) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeCompactString("cluster-xyz") // cluster_id
    w.writeInt32(2) // controller_id
    w.writeUnsignedVarInt(1) // 0 brokers + 1
    w.writeInt32(0) // cluster_authorized_operations
    w.writeInt8(1) // endpoint_type
    w.writeTaggedFields([]) // response tagged fields

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeClusterResponse(reader, 1)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.throttleTimeMs).toBe(10)
    expect(result.value.clusterId).toBe("cluster-xyz")
    expect(result.value.controllerId).toBe(2)
    expect(result.value.brokers).toHaveLength(0)
    expect(result.value.endpointType).toBe(1)
  })

  it("decodes error response", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(29) // CLUSTER_AUTHORIZATION_FAILED
    w.writeCompactString("not authorised") // error_message
    w.writeCompactString("") // cluster_id
    w.writeInt32(-1) // controller_id
    w.writeUnsignedVarInt(1) // 0 brokers + 1
    w.writeInt32(-2147483648) // cluster_authorized_operations
    w.writeTaggedFields([]) // response tagged fields

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeClusterResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.errorCode).toBe(29)
    expect(result.value.errorMessage).toBe("not authorised")
  })

  it("decodes response with multiple brokers and rack", () => {
    const w = new BinaryWriter()
    w.writeInt32(0) // throttle_time_ms
    w.writeInt16(0) // error_code
    w.writeCompactString(null) // error_message
    w.writeCompactString("cluster-multi") // cluster_id
    w.writeInt32(1) // controller_id
    // brokers (compact: 3 + 1)
    w.writeUnsignedVarInt(4)
    for (const [id, host, port, rack] of [
      [1, "broker1.local", 9092, "rack-a"],
      [2, "broker2.local", 9093, "rack-b"],
      [3, "broker3.local", 9094, null]
    ] as const) {
      w.writeInt32(id)
      w.writeCompactString(host)
      w.writeInt32(port)
      w.writeCompactString(rack)
      w.writeTaggedFields([])
    }
    w.writeInt32(8) // cluster_authorized_operations
    w.writeTaggedFields([])

    const body = w.finish()
    const reader = new BinaryReader(body)
    const result = decodeDescribeClusterResponse(reader, 0)

    expect(result.ok).toBe(true)
    if (!result.ok) {
      return
    }

    expect(result.value.brokers).toHaveLength(3)
    expect(result.value.brokers[0].rack).toBe("rack-a")
    expect(result.value.brokers[1].brokerId).toBe(2)
    expect(result.value.brokers[1].host).toBe("broker2.local")
    expect(result.value.brokers[1].port).toBe(9093)
    expect(result.value.brokers[1].rack).toBe("rack-b")
    expect(result.value.brokers[2].rack).toBeNull()
    expect(result.value.clusterAuthorizedOperations).toBe(8)
  })

  describe("error handling", () => {
    it("returns failure on truncated input", () => {
      const reader = new BinaryReader(new Uint8Array(2))
      const result = decodeDescribeClusterResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated broker entry", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeCompactString(null) // error_message
      w.writeCompactString("cluster-x") // cluster_id
      w.writeInt32(1) // controller_id
      w.writeUnsignedVarInt(2) // 1 broker + 1
      w.writeInt32(1) // broker_id
      // Missing host, port, rack, tagged fields

      const reader = new BinaryReader(w.finish())
      const result = decodeDescribeClusterResponse(reader, 0)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated v1 endpoint_type", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeCompactString(null) // error_message
      w.writeCompactString("cluster-x") // cluster_id
      w.writeInt32(1) // controller_id
      w.writeUnsignedVarInt(1) // 0 brokers + 1
      w.writeInt32(0) // cluster_authorized_operations
      // Missing endpoint_type for v1

      const reader = new BinaryReader(w.finish())
      const result = decodeDescribeClusterResponse(reader, 1)
      expect(result.ok).toBe(false)
    })

    it("returns failure on truncated cluster_id", () => {
      const w = new BinaryWriter()
      w.writeInt32(0) // throttle_time_ms
      w.writeInt16(0) // error_code
      w.writeCompactString(null) // error_message
      // Missing cluster_id and beyond

      const reader = new BinaryReader(w.finish())
      const result = decodeDescribeClusterResponse(reader, 0)
      expect(result.ok).toBe(false)
    })
  })
})
