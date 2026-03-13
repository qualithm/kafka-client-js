/**
 * Kafka API keys and version ranges.
 *
 * API keys identify request types in the Kafka binary protocol.
 * Version ranges define the minimum and maximum supported versions
 * for each API, used during version negotiation.
 *
 * @see https://kafka.apache.org/protocol.html#protocol_api_keys
 *
 * @packageDocumentation
 */

/**
 * Kafka protocol API keys.
 *
 * Values match the official Kafka protocol specification.
 */
export const ApiKey = {
  Produce: 0,
  Fetch: 1,
  ListOffsets: 2,
  Metadata: 3,
  LeaderAndIsr: 4,
  StopReplica: 5,
  UpdateMetadata: 6,
  ControlledShutdown: 7,
  OffsetCommit: 8,
  OffsetFetch: 9,
  FindCoordinator: 10,
  JoinGroup: 11,
  Heartbeat: 12,
  LeaveGroup: 13,
  SyncGroup: 14,
  DescribeGroups: 15,
  ListGroups: 16,
  SaslHandshake: 17,
  ApiVersions: 18,
  CreateTopics: 19,
  DeleteTopics: 20,
  DeleteRecords: 21,
  InitProducerId: 22,
  OffsetForLeaderEpoch: 23,
  AddPartitionsToTxn: 24,
  AddOffsetsToTxn: 25,
  EndTxn: 26,
  TxnOffsetCommit: 28,
  DescribeAcls: 29,
  CreateAcls: 30,
  DeleteAcls: 31,
  DescribeConfigs: 32,
  AlterConfigs: 33,
  AlterReplicaLogDirs: 34,
  DescribeLogDirs: 35,
  SaslAuthenticate: 36,
  CreatePartitions: 37,
  CreateDelegationToken: 38,
  RenewDelegationToken: 39,
  ExpireDelegationToken: 40,
  DescribeDelegationToken: 41,
  DeleteGroups: 42,
  ElectLeaders: 43,
  IncrementalAlterConfigs: 44,
  AlterPartitionReassignments: 45,
  ListPartitionReassignments: 46,
  DescribeClientQuotas: 48,
  AlterClientQuotas: 49
} as const

export type ApiKey = (typeof ApiKey)[keyof typeof ApiKey]

/**
 * Supported version range for an API.
 */
export type ApiVersionRange = {
  /** Minimum supported version (inclusive). */
  readonly minVersion: number
  /** Maximum supported version (inclusive). */
  readonly maxVersion: number
}

/**
 * Maps API keys to their header version requirements.
 *
 * - Request header v0-v1: non-flexible (API key, version, correlation ID, client ID)
 * - Request header v2: flexible (adds tagged fields, KIP-482)
 * - Response header v0: non-flexible
 * - Response header v1: flexible (adds tagged fields)
 *
 * The threshold version is the API version at which the API
 * transitions from non-flexible to flexible headers.
 * `null` means the API always uses non-flexible headers.
 */
export type FlexibleVersionThreshold = {
  /** The API version at which flexible headers are first used. `null` if never flexible. */
  readonly flexibleFromVersion: number | null
}

/**
 * Version ranges supported by this client for each API.
 *
 * These are advertised during ApiVersions negotiation. The actual
 * version used per-request is the minimum of the client's max and
 * the broker's max.
 */
export const CLIENT_API_VERSIONS: Partial<Record<ApiKey, ApiVersionRange>> = {
  [ApiKey.ApiVersions]: { minVersion: 0, maxVersion: 3 },
  [ApiKey.Metadata]: { minVersion: 0, maxVersion: 12 },
  [ApiKey.Produce]: { minVersion: 0, maxVersion: 9 },
  [ApiKey.Fetch]: { minVersion: 0, maxVersion: 12 },
  [ApiKey.ListOffsets]: { minVersion: 0, maxVersion: 7 },
  [ApiKey.FindCoordinator]: { minVersion: 0, maxVersion: 4 },
  [ApiKey.JoinGroup]: { minVersion: 0, maxVersion: 9 },
  [ApiKey.SyncGroup]: { minVersion: 0, maxVersion: 5 },
  [ApiKey.Heartbeat]: { minVersion: 0, maxVersion: 4 },
  [ApiKey.LeaveGroup]: { minVersion: 0, maxVersion: 5 },
  [ApiKey.OffsetCommit]: { minVersion: 0, maxVersion: 8 },
  [ApiKey.OffsetFetch]: { minVersion: 0, maxVersion: 7 },
  [ApiKey.SaslHandshake]: { minVersion: 0, maxVersion: 1 },
  [ApiKey.SaslAuthenticate]: { minVersion: 0, maxVersion: 2 },
  [ApiKey.CreateTopics]: { minVersion: 0, maxVersion: 7 },
  [ApiKey.DeleteTopics]: { minVersion: 0, maxVersion: 6 },
  [ApiKey.DeleteRecords]: { minVersion: 0, maxVersion: 2 },
  [ApiKey.CreatePartitions]: { minVersion: 0, maxVersion: 3 },
  [ApiKey.DescribeConfigs]: { minVersion: 0, maxVersion: 4 },
  [ApiKey.AlterConfigs]: { minVersion: 0, maxVersion: 2 },
  [ApiKey.InitProducerId]: { minVersion: 0, maxVersion: 4 },
  [ApiKey.OffsetForLeaderEpoch]: { minVersion: 0, maxVersion: 4 },
  [ApiKey.AddPartitionsToTxn]: { minVersion: 0, maxVersion: 3 },
  [ApiKey.AddOffsetsToTxn]: { minVersion: 0, maxVersion: 3 },
  [ApiKey.EndTxn]: { minVersion: 0, maxVersion: 3 },
  [ApiKey.TxnOffsetCommit]: { minVersion: 0, maxVersion: 3 },
  [ApiKey.DescribeGroups]: { minVersion: 0, maxVersion: 5 },
  [ApiKey.ListGroups]: { minVersion: 0, maxVersion: 4 },
  [ApiKey.DescribeAcls]: { minVersion: 0, maxVersion: 3 },
  [ApiKey.CreateAcls]: { minVersion: 0, maxVersion: 3 },
  [ApiKey.DeleteAcls]: { minVersion: 0, maxVersion: 3 },
  [ApiKey.CreateDelegationToken]: { minVersion: 0, maxVersion: 3 },
  [ApiKey.RenewDelegationToken]: { minVersion: 0, maxVersion: 2 },
  [ApiKey.ExpireDelegationToken]: { minVersion: 0, maxVersion: 2 },
  [ApiKey.DescribeDelegationToken]: { minVersion: 0, maxVersion: 3 },
  [ApiKey.DeleteGroups]: { minVersion: 0, maxVersion: 2 },
  [ApiKey.ElectLeaders]: { minVersion: 0, maxVersion: 2 },
  [ApiKey.IncrementalAlterConfigs]: { minVersion: 0, maxVersion: 1 },
  [ApiKey.AlterPartitionReassignments]: { minVersion: 0, maxVersion: 0 },
  [ApiKey.ListPartitionReassignments]: { minVersion: 0, maxVersion: 0 }
}

/**
 * The API version at which each API transitions to flexible headers (KIP-482).
 * `null` means the API never uses flexible headers within the versions we support.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-482
 */
export const FLEXIBLE_VERSION_THRESHOLDS: Partial<Record<ApiKey, number | null>> = {
  [ApiKey.ApiVersions]: 3,
  [ApiKey.Metadata]: 9,
  [ApiKey.Produce]: 9,
  [ApiKey.Fetch]: 12,
  [ApiKey.ListOffsets]: 6,
  [ApiKey.FindCoordinator]: 3,
  [ApiKey.JoinGroup]: 6,
  [ApiKey.SyncGroup]: 4,
  [ApiKey.Heartbeat]: 4,
  [ApiKey.LeaveGroup]: 4,
  [ApiKey.OffsetCommit]: 8,
  [ApiKey.OffsetFetch]: 6,
  [ApiKey.SaslHandshake]: null,
  [ApiKey.SaslAuthenticate]: 2,
  [ApiKey.CreateTopics]: 5,
  [ApiKey.DeleteTopics]: 4,
  [ApiKey.DeleteRecords]: 2,
  [ApiKey.CreatePartitions]: 2,
  [ApiKey.DescribeConfigs]: 4,
  [ApiKey.AlterConfigs]: 2,
  [ApiKey.InitProducerId]: 2,
  [ApiKey.OffsetForLeaderEpoch]: 4,
  [ApiKey.AddPartitionsToTxn]: 3,
  [ApiKey.AddOffsetsToTxn]: 3,
  [ApiKey.EndTxn]: 3,
  [ApiKey.TxnOffsetCommit]: 3,
  [ApiKey.DescribeGroups]: 5,
  [ApiKey.ListGroups]: 3,
  [ApiKey.DescribeAcls]: 2,
  [ApiKey.CreateAcls]: 2,
  [ApiKey.DeleteAcls]: 2,
  [ApiKey.CreateDelegationToken]: 2,
  [ApiKey.RenewDelegationToken]: 2,
  [ApiKey.ExpireDelegationToken]: 2,
  [ApiKey.DescribeDelegationToken]: 2,
  [ApiKey.DeleteGroups]: 2,
  [ApiKey.ElectLeaders]: 2,
  [ApiKey.IncrementalAlterConfigs]: 1,
  [ApiKey.AlterPartitionReassignments]: 0,
  [ApiKey.ListPartitionReassignments]: 0
}

/**
 * Check whether a given API key + version uses flexible headers.
 */
export function isFlexibleVersion(apiKey: ApiKey, apiVersion: number): boolean {
  const threshold = FLEXIBLE_VERSION_THRESHOLDS[apiKey]
  if (threshold === undefined || threshold === null) {
    return false
  }
  return apiVersion >= threshold
}

/**
 * Negotiate the version to use for a given API key.
 * Returns the highest version supported by both client and broker,
 * or `null` if there is no overlap.
 */
export function negotiateVersion(apiKey: ApiKey, brokerRange: ApiVersionRange): number | null {
  const clientRange = CLIENT_API_VERSIONS[apiKey]
  if (!clientRange) {
    return null
  }

  const min = Math.max(clientRange.minVersion, brokerRange.minVersion)
  const max = Math.min(clientRange.maxVersion, brokerRange.maxVersion)

  if (min > max) {
    return null
  }

  return max
}
