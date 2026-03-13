/**
 * DescribeCluster request/response encoding and decoding.
 *
 * The DescribeCluster API (key 60) provides detailed cluster metadata
 * including broker endpoints and authorised operations.
 *
 * **Request versions:**
 * - v0: include_cluster_authorized_operations (BOOLEAN)
 * - v1: adds endpoint_type (INT8)
 * - All versions use flexible encoding
 *
 * **Response versions:**
 * - v0: throttle_time_ms, error_code, error_message, cluster_id, controller_id, brokers[], cluster_authorized_operations
 * - v1: adds endpoint_type
 *
 * @see https://kafka.apache.org/protocol.html#The_Messages_DescribeCluster
 *
 * @packageDocumentation
 */

import { ApiKey } from "../codec/api-keys.js"
import type { BinaryReader, TaggedField } from "../codec/binary-reader.js"
import type { BinaryWriter } from "../codec/binary-writer.js"
import { frameRequest, type RequestHeader } from "../codec/protocol-framing.js"
import { type DecodeResult, decodeSuccess } from "../result.js"

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/**
 * DescribeCluster request payload.
 */
export type DescribeClusterRequest = {
  /** Whether to include cluster authorised operations in the response. */
  readonly includeClusterAuthorizedOperations: boolean
  /** The endpoint type to describe (v1+). 1 = brokers, 2 = controllers. */
  readonly endpointType?: number
  /** Tagged fields. */
  readonly taggedFields?: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/**
 * A broker in the DescribeCluster response.
 */
export type DescribeClusterBroker = {
  /** The broker ID. */
  readonly brokerId: number
  /** The broker hostname. */
  readonly host: string
  /** The broker port. */
  readonly port: number
  /** The rack of the broker, or null. */
  readonly rack: string | null
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

/**
 * DescribeCluster response payload.
 */
export type DescribeClusterResponse = {
  /** Time the request was throttled in milliseconds. */
  readonly throttleTimeMs: number
  /** Error code (0 = no error). */
  readonly errorCode: number
  /** Error message, or null. */
  readonly errorMessage: string | null
  /** The cluster ID. */
  readonly clusterId: string
  /** The controller broker ID. */
  readonly controllerId: number
  /** Brokers in the cluster. */
  readonly brokers: readonly DescribeClusterBroker[]
  /** Bitfield of authorised operations, or -2147483648 if not requested. */
  readonly clusterAuthorizedOperations: number
  /** The endpoint type (v1+). */
  readonly endpointType: number
  /** Tagged fields. */
  readonly taggedFields: readonly TaggedField[]
}

// ---------------------------------------------------------------------------
// Request encoding
// ---------------------------------------------------------------------------

/**
 * Encode a DescribeCluster request body into the given writer.
 */
export function encodeDescribeClusterRequest(
  writer: BinaryWriter,
  request: DescribeClusterRequest,
  apiVersion: number
): void {
  // include_cluster_authorized_operations (BOOLEAN)
  writer.writeBoolean(request.includeClusterAuthorizedOperations)

  // endpoint_type (INT8, v1+)
  if (apiVersion >= 1) {
    writer.writeInt8(request.endpointType ?? 1)
  }

  // tagged fields (all versions are flexible)
  writer.writeTaggedFields(request.taggedFields ?? [])
}

/**
 * Build a complete, framed DescribeCluster request ready to send to a broker.
 */
export function buildDescribeClusterRequest(
  correlationId: number,
  apiVersion: number,
  request: DescribeClusterRequest,
  clientId?: string | null
): Uint8Array {
  const header: RequestHeader = {
    apiKey: ApiKey.DescribeCluster,
    apiVersion,
    correlationId,
    clientId
  }

  return frameRequest(header, (writer) => {
    encodeDescribeClusterRequest(writer, request, apiVersion)
  })
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/**
 * Decode a DescribeCluster response body.
 */
export function decodeDescribeClusterResponse(
  reader: BinaryReader,
  apiVersion: number
): DecodeResult<DescribeClusterResponse> {
  const startOffset = reader.offset

  // throttle_time_ms (INT32)
  const throttleResult = reader.readInt32()
  if (!throttleResult.ok) {
    return throttleResult
  }

  // error_code (INT16)
  const errorCodeResult = reader.readInt16()
  if (!errorCodeResult.ok) {
    return errorCodeResult
  }

  // error_message (COMPACT_NULLABLE_STRING)
  const errorMsgResult = reader.readCompactString()
  if (!errorMsgResult.ok) {
    return errorMsgResult
  }

  // cluster_id (COMPACT_STRING)
  const clusterIdResult = reader.readCompactString()
  if (!clusterIdResult.ok) {
    return clusterIdResult
  }

  // controller_id (INT32)
  const controllerIdResult = reader.readInt32()
  if (!controllerIdResult.ok) {
    return controllerIdResult
  }

  // brokers array
  const brokersResult = decodeBrokersArray(reader)
  if (!brokersResult.ok) {
    return brokersResult
  }

  // cluster_authorized_operations (INT32)
  const authOpsResult = reader.readInt32()
  if (!authOpsResult.ok) {
    return authOpsResult
  }

  // endpoint_type (INT8, v1+)
  let endpointType = 0
  if (apiVersion >= 1) {
    const endpointResult = reader.readInt8()
    if (!endpointResult.ok) {
      return endpointResult
    }
    endpointType = endpointResult.value
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      throttleTimeMs: throttleResult.value,
      errorCode: errorCodeResult.value,
      errorMessage: errorMsgResult.value,
      clusterId: clusterIdResult.value ?? "",
      controllerId: controllerIdResult.value,
      brokers: brokersResult.value,
      clusterAuthorizedOperations: authOpsResult.value,
      endpointType,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}

function decodeBrokersArray(reader: BinaryReader): DecodeResult<DescribeClusterBroker[]> {
  const startOffset = reader.offset

  const countResult = reader.readUnsignedVarInt()
  if (!countResult.ok) {
    return countResult
  }
  const count = countResult.value - 1

  const brokers: DescribeClusterBroker[] = []
  for (let i = 0; i < count; i++) {
    const brokerResult = decodeBrokerEntry(reader)
    if (!brokerResult.ok) {
      return brokerResult
    }
    brokers.push(brokerResult.value)
  }

  return decodeSuccess(brokers, reader.offset - startOffset)
}

function decodeBrokerEntry(reader: BinaryReader): DecodeResult<DescribeClusterBroker> {
  const startOffset = reader.offset

  // broker_id (INT32)
  const brokerIdResult = reader.readInt32()
  if (!brokerIdResult.ok) {
    return brokerIdResult
  }

  // host (COMPACT_STRING)
  const hostResult = reader.readCompactString()
  if (!hostResult.ok) {
    return hostResult
  }

  // port (INT32)
  const portResult = reader.readInt32()
  if (!portResult.ok) {
    return portResult
  }

  // rack (COMPACT_NULLABLE_STRING)
  const rackResult = reader.readCompactString()
  if (!rackResult.ok) {
    return rackResult
  }

  // tagged fields
  const tagResult = reader.readTaggedFields()
  if (!tagResult.ok) {
    return tagResult
  }

  return decodeSuccess(
    {
      brokerId: brokerIdResult.value,
      host: hostResult.value ?? "",
      port: portResult.value,
      rack: rackResult.value,
      taggedFields: tagResult.value
    },
    reader.offset - startOffset
  )
}
