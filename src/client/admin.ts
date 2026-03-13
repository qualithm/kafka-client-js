/**
 * Kafka admin client for topic and cluster management.
 *
 * Provides administrative operations: creating/deleting topics, adding
 * partitions, describing/altering configuration, and listing topics.
 *
 * @packageDocumentation
 */

import { ApiKey, negotiateVersion } from "../codec/api-keys.js"
import { KafkaConnectionError, KafkaError } from "../errors.js"
import type { ConnectionPool } from "../network/broker-pool.js"
import type { KafkaConnection } from "../network/connection.js"
import {
  type AlterClientQuotasEntryResponse,
  type AlterClientQuotasRequest,
  decodeAlterClientQuotasResponse,
  encodeAlterClientQuotasRequest
} from "../protocol/alter-client-quotas.js"
import {
  type AlterConfigsRequest,
  type AlterConfigsResourceResponse,
  decodeAlterConfigsResponse,
  encodeAlterConfigsRequest
} from "../protocol/alter-configs.js"
import {
  type AlterPartitionReassignmentsRequest,
  decodeAlterPartitionReassignmentsResponse,
  encodeAlterPartitionReassignmentsRequest,
  type ReassignableTopicResponse
} from "../protocol/alter-partition-reassignments.js"
import {
  type AlterUserScramCredentialsRequest,
  type AlterUserScramCredentialsResultEntry,
  decodeAlterUserScramCredentialsResponse,
  encodeAlterUserScramCredentialsRequest
} from "../protocol/alter-user-scram-credentials.js"
import { apiVersionsToMap, decodeApiVersionsResponse } from "../protocol/api-versions.js"
import {
  type AclCreationResult,
  type CreateAclsRequest,
  decodeCreateAclsResponse,
  encodeCreateAclsRequest
} from "../protocol/create-acls.js"
import {
  type CreateDelegationTokenRequest,
  type CreateDelegationTokenResponse,
  decodeCreateDelegationTokenResponse,
  encodeCreateDelegationTokenRequest
} from "../protocol/create-delegation-token.js"
import {
  type CreatePartitionsRequest,
  type CreatePartitionsTopicResponse,
  decodeCreatePartitionsResponse,
  encodeCreatePartitionsRequest
} from "../protocol/create-partitions.js"
import {
  type CreateTopicsRequest,
  type CreateTopicsTopicResponse,
  decodeCreateTopicsResponse,
  encodeCreateTopicsRequest
} from "../protocol/create-topics.js"
import {
  decodeDeleteAclsResponse,
  type DeleteAclsFilter,
  type DeleteAclsFilterResult,
  encodeDeleteAclsRequest
} from "../protocol/delete-acls.js"
import {
  decodeDeleteGroupsResponse,
  type DeleteGroupsRequest,
  type DeleteGroupsResult,
  encodeDeleteGroupsRequest
} from "../protocol/delete-groups.js"
import {
  decodeDeleteRecordsResponse,
  type DeleteRecordsRequest,
  type DeleteRecordsTopicResponse,
  encodeDeleteRecordsRequest
} from "../protocol/delete-records.js"
import {
  decodeDeleteTopicsResponse,
  type DeleteTopicsRequest,
  type DeleteTopicsTopicResponse,
  encodeDeleteTopicsRequest
} from "../protocol/delete-topics.js"
import {
  decodeDescribeAclsResponse,
  type DescribeAclsRequest,
  type DescribeAclsResource,
  encodeDescribeAclsRequest
} from "../protocol/describe-acls.js"
import {
  decodeDescribeClientQuotasResponse,
  type DescribeClientQuotasEntry,
  type DescribeClientQuotasRequest,
  encodeDescribeClientQuotasRequest
} from "../protocol/describe-client-quotas.js"
import {
  decodeDescribeConfigsResponse,
  type DescribeConfigsRequest,
  type DescribeConfigsResourceResponse,
  encodeDescribeConfigsRequest
} from "../protocol/describe-configs.js"
import {
  decodeDescribeDelegationTokenResponse,
  type DescribedDelegationToken,
  type DescribeDelegationTokenRequest,
  encodeDescribeDelegationTokenRequest
} from "../protocol/describe-delegation-token.js"
import {
  decodeDescribeGroupsResponse,
  type DescribeGroupsGroup,
  type DescribeGroupsRequest,
  encodeDescribeGroupsRequest
} from "../protocol/describe-groups.js"
import {
  decodeDescribeUserScramCredentialsResponse,
  type DescribeUserScramCredentialsRequest,
  type DescribeUserScramCredentialsResult,
  encodeDescribeUserScramCredentialsRequest
} from "../protocol/describe-user-scram-credentials.js"
import {
  decodeElectLeadersResponse,
  type ElectLeadersRequest,
  type ElectLeadersTopicResponse,
  encodeElectLeadersRequest
} from "../protocol/elect-leaders.js"
import {
  decodeExpireDelegationTokenResponse,
  encodeExpireDelegationTokenRequest,
  type ExpireDelegationTokenRequest,
  type ExpireDelegationTokenResponse
} from "../protocol/expire-delegation-token.js"
import {
  decodeIncrementalAlterConfigsResponse,
  encodeIncrementalAlterConfigsRequest,
  type IncrementalAlterConfigsRequest,
  type IncrementalAlterConfigsResourceResponse
} from "../protocol/incremental-alter-configs.js"
import {
  decodeListGroupsResponse,
  encodeListGroupsRequest,
  type ListGroupsGroup,
  type ListGroupsRequest
} from "../protocol/list-groups.js"
import {
  decodeListPartitionReassignmentsResponse,
  encodeListPartitionReassignmentsRequest,
  type ListPartitionReassignmentsRequest,
  type OngoingTopicReassignment
} from "../protocol/list-partition-reassignments.js"
import {
  decodeMetadataResponse,
  encodeMetadataRequest,
  type MetadataRequest,
  type MetadataTopic
} from "../protocol/metadata.js"
import {
  decodeRenewDelegationTokenResponse,
  encodeRenewDelegationTokenRequest,
  type RenewDelegationTokenRequest,
  type RenewDelegationTokenResponse
} from "../protocol/renew-delegation-token.js"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Retry configuration for the admin client.
 */
export type AdminRetryConfig = {
  /**
   * Maximum number of retry attempts for retriable errors.
   * @default 3
   */
  readonly maxRetries?: number
  /**
   * Initial delay in milliseconds before the first retry.
   * @default 100
   */
  readonly initialRetryMs?: number
  /**
   * Maximum delay in milliseconds between retries.
   * @default 30000
   */
  readonly maxRetryMs?: number
  /**
   * Multiplier for exponential backoff between retries.
   * @default 2
   */
  readonly multiplier?: number
}

/**
 * Options for creating a {@link KafkaAdmin}.
 */
export type AdminOptions = {
  /** The connection pool to use. */
  readonly connectionPool: ConnectionPool
  /** Retry configuration for retriable errors. */
  readonly retry?: AdminRetryConfig
}

/**
 * Topic info returned by {@link KafkaAdmin.listTopics}.
 */
export type TopicInfo = {
  /** Topic name. */
  readonly name: string
  /** Whether this is an internal topic. */
  readonly isInternal: boolean
  /** Number of partitions. */
  readonly partitionCount: number
}

// ---------------------------------------------------------------------------
// KafkaAdmin
// ---------------------------------------------------------------------------

/**
 * Kafka admin client.
 *
 * Performs administrative operations on topics and cluster configuration.
 * Operates through the {@link ConnectionPool} and negotiates API versions
 * automatically.
 *
 * @example
 * ```ts
 * const admin = new KafkaAdmin({ connectionPool: kafka.connectionPool })
 * await admin.createTopics({
 *   topics: [{ name: "my-topic", numPartitions: 3, replicationFactor: 1 }],
 *   timeoutMs: 30000,
 * })
 * const topics = await admin.listTopics()
 * await admin.close()
 * ```
 */
export class KafkaAdmin {
  private readonly pool: ConnectionPool
  private readonly maxRetries: number
  private readonly initialRetryMs: number
  private readonly maxRetryMs: number
  private readonly retryMultiplier: number
  private closed = false

  constructor(options: AdminOptions) {
    this.pool = options.connectionPool
    this.maxRetries = options.retry?.maxRetries ?? 3
    this.initialRetryMs = options.retry?.initialRetryMs ?? 100
    this.maxRetryMs = options.retry?.maxRetryMs ?? 30_000
    this.retryMultiplier = options.retry?.multiplier ?? 2
  }

  /**
   * Create one or more topics.
   *
   * @param request - The CreateTopics request payload.
   * @returns Per-topic results.
   * @throws {KafkaProtocolError} If the broker returns a non-zero error code.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async createTopics(request: CreateTopicsRequest): Promise<readonly CreateTopicsTopicResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.CreateTopics)
      try {
        const responseReader = await conn.send(ApiKey.CreateTopics, apiVersion, (writer) => {
          encodeCreateTopicsRequest(writer, request, apiVersion)
        })

        const result = decodeCreateTopicsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode create topics response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Delete one or more topics.
   *
   * @param request - The DeleteTopics request payload.
   * @returns Per-topic results.
   * @throws {KafkaProtocolError} If the broker returns a non-zero error code.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async deleteTopics(request: DeleteTopicsRequest): Promise<readonly DeleteTopicsTopicResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.DeleteTopics)
      try {
        const responseReader = await conn.send(ApiKey.DeleteTopics, apiVersion, (writer) => {
          encodeDeleteTopicsRequest(writer, request, apiVersion)
        })

        const result = decodeDeleteTopicsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode delete topics response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Delete records from topic partitions up to specified offsets.
   *
   * Records before the specified offset in each partition are deleted
   * (the log start offset is advanced). Use offset -1 to delete up to
   * the high watermark.
   *
   * @param request - The DeleteRecords request payload.
   * @returns Per-topic/partition deletion results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async deleteRecords(
    request: DeleteRecordsRequest
  ): Promise<readonly DeleteRecordsTopicResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.DeleteRecords)
      try {
        const responseReader = await conn.send(ApiKey.DeleteRecords, apiVersion, (writer) => {
          encodeDeleteRecordsRequest(writer, request, apiVersion)
        })

        const result = decodeDeleteRecordsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode delete records response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Create additional partitions for existing topics.
   *
   * @param request - The CreatePartitions request payload.
   * @returns Per-topic results.
   * @throws {KafkaProtocolError} If the broker returns a non-zero error code.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async createPartitions(
    request: CreatePartitionsRequest
  ): Promise<readonly CreatePartitionsTopicResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.CreatePartitions)
      try {
        const responseReader = await conn.send(ApiKey.CreatePartitions, apiVersion, (writer) => {
          encodeCreatePartitionsRequest(writer, request, apiVersion)
        })

        const result = decodeCreatePartitionsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode create partitions response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Describe configuration for specified resources.
   *
   * @param request - The DescribeConfigs request payload.
   * @returns Per-resource config results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async describeConfigs(
    request: DescribeConfigsRequest
  ): Promise<readonly DescribeConfigsResourceResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.DescribeConfigs)
      try {
        const responseReader = await conn.send(ApiKey.DescribeConfigs, apiVersion, (writer) => {
          encodeDescribeConfigsRequest(writer, request, apiVersion)
        })

        const result = decodeDescribeConfigsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode describe configs response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.resources
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Alter configuration for specified resources.
   *
   * This is a non-incremental set: all config keys must be provided.
   * Omitted keys revert to their defaults.
   *
   * @param request - The AlterConfigs request payload.
   * @returns Per-resource results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async alterConfigs(
    request: AlterConfigsRequest
  ): Promise<readonly AlterConfigsResourceResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.AlterConfigs)
      try {
        const responseReader = await conn.send(ApiKey.AlterConfigs, apiVersion, (writer) => {
          encodeAlterConfigsRequest(writer, request, apiVersion)
        })

        const result = decodeAlterConfigsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode alter configs response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.resources
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * List all topics in the cluster.
   *
   * Uses the Metadata API with a null topics list to discover all topics.
   * Internal topics (e.g. `__consumer_offsets`) are included.
   *
   * @returns Array of topic info objects.
   * @throws {KafkaConnectionError} If connection or decoding fails.
   */
  async listTopics(): Promise<readonly TopicInfo[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.Metadata)
      try {
        const metadataRequest: MetadataRequest = { topics: null }
        const responseReader = await conn.send(ApiKey.Metadata, apiVersion, (writer) => {
          encodeMetadataRequest(writer, metadataRequest, apiVersion)
        })

        const result = decodeMetadataResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode metadata response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics.map(toTopicInfo)
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Describe specific topics using the Metadata API.
   *
   * @param topicNames - Topic names to describe.
   * @returns Topic metadata including partitions and leaders.
   * @throws {KafkaConnectionError} If connection or decoding fails.
   */
  async describeTopics(topicNames: readonly string[]): Promise<readonly MetadataTopic[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.Metadata)
      try {
        const metadataRequest: MetadataRequest = {
          topics: topicNames.map((name) => ({ name }))
        }
        const responseReader = await conn.send(ApiKey.Metadata, apiVersion, (writer) => {
          encodeMetadataRequest(writer, metadataRequest, apiVersion)
        })

        const result = decodeMetadataResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode metadata response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Describe one or more consumer groups.
   *
   * Returns detailed metadata about each group including state, protocol,
   * members, and their current assignments.
   *
   * @param request - The DescribeGroups request payload.
   * @returns Per-group descriptions.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async describeGroups(request: DescribeGroupsRequest): Promise<readonly DescribeGroupsGroup[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.DescribeGroups)
      try {
        const responseReader = await conn.send(ApiKey.DescribeGroups, apiVersion, (writer) => {
          encodeDescribeGroupsRequest(writer, request, apiVersion)
        })

        const result = decodeDescribeGroupsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode describe groups response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.groups
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * List all consumer groups known to the broker.
   *
   * @param request - The ListGroups request payload. Pass `{}` for no filter.
   * @returns Listed groups.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async listGroups(request: ListGroupsRequest = {}): Promise<readonly ListGroupsGroup[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.ListGroups)
      try {
        const responseReader = await conn.send(ApiKey.ListGroups, apiVersion, (writer) => {
          encodeListGroupsRequest(writer, request, apiVersion)
        })

        const result = decodeListGroupsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode list groups response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        if (result.value.errorCode !== 0) {
          throw new KafkaConnectionError(
            `list groups request failed with error code ${String(result.value.errorCode)}`,
            { broker: conn.broker }
          )
        }

        return result.value.groups
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Delete one or more consumer groups.
   *
   * Groups must be empty (no active members) to be deleted.
   *
   * @param request - The DeleteGroups request payload.
   * @returns Per-group deletion results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async deleteGroups(request: DeleteGroupsRequest): Promise<readonly DeleteGroupsResult[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.DeleteGroups)
      try {
        const responseReader = await conn.send(ApiKey.DeleteGroups, apiVersion, (writer) => {
          encodeDeleteGroupsRequest(writer, request, apiVersion)
        })

        const result = decodeDeleteGroupsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode delete groups response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.results
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Trigger a preferred or unclean leader election for specified partitions.
   *
   * @param request - The ElectLeaders request payload.
   * @returns Per-topic/partition election results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async electLeaders(request: ElectLeadersRequest): Promise<readonly ElectLeadersTopicResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.ElectLeaders)
      try {
        const responseReader = await conn.send(ApiKey.ElectLeaders, apiVersion, (writer) => {
          encodeElectLeadersRequest(writer, request, apiVersion)
        })

        const result = decodeElectLeadersResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode elect leaders response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.replicaElectionResults
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Incrementally alter configuration for specified resources.
   *
   * Unlike {@link alterConfigs}, this only modifies the specified config
   * entries — unmentioned keys are left unchanged.
   *
   * @param request - The IncrementalAlterConfigs request payload.
   * @returns Per-resource results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async incrementalAlterConfigs(
    request: IncrementalAlterConfigsRequest
  ): Promise<readonly IncrementalAlterConfigsResourceResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(
        ApiKey.IncrementalAlterConfigs
      )
      try {
        const responseReader = await conn.send(
          ApiKey.IncrementalAlterConfigs,
          apiVersion,
          (writer) => {
            encodeIncrementalAlterConfigsRequest(writer, request, apiVersion)
          }
        )

        const result = decodeIncrementalAlterConfigsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode incremental alter configs response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.responses
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Initiate, cancel, or modify partition replica reassignments.
   *
   * @param request - The AlterPartitionReassignments request payload.
   * @returns Per-topic/partition reassignment results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async alterPartitionReassignments(
    request: AlterPartitionReassignmentsRequest
  ): Promise<readonly ReassignableTopicResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(
        ApiKey.AlterPartitionReassignments
      )
      try {
        const responseReader = await conn.send(
          ApiKey.AlterPartitionReassignments,
          apiVersion,
          (writer) => {
            encodeAlterPartitionReassignmentsRequest(writer, request, apiVersion)
          }
        )

        const result = decodeAlterPartitionReassignmentsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode alter partition reassignments response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.responses
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * List ongoing partition replica reassignments.
   *
   * @param request - The ListPartitionReassignments request payload.
   * @returns Per-topic ongoing reassignment state.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async listPartitionReassignments(
    request: ListPartitionReassignmentsRequest
  ): Promise<readonly OngoingTopicReassignment[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(
        ApiKey.ListPartitionReassignments
      )
      try {
        const responseReader = await conn.send(
          ApiKey.ListPartitionReassignments,
          apiVersion,
          (writer) => {
            encodeListPartitionReassignmentsRequest(writer, request, apiVersion)
          }
        )

        const result = decodeListPartitionReassignmentsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode list partition reassignments response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.topics
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Describe ACL bindings matching the specified filter.
   *
   * @param request - The DescribeAcls request payload (filter criteria).
   * @returns ACL resources matching the filter.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async describeAcls(request: DescribeAclsRequest): Promise<readonly DescribeAclsResource[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.DescribeAcls)
      try {
        const responseReader = await conn.send(ApiKey.DescribeAcls, apiVersion, (writer) => {
          encodeDescribeAclsRequest(writer, request, apiVersion)
        })

        const result = decodeDescribeAclsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode describe acls response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.resources
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Create one or more ACL bindings.
   *
   * @param request - The CreateAcls request payload.
   * @returns Per-creation results with error codes.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async createAcls(request: CreateAclsRequest): Promise<readonly AclCreationResult[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.CreateAcls)
      try {
        const responseReader = await conn.send(ApiKey.CreateAcls, apiVersion, (writer) => {
          encodeCreateAclsRequest(writer, request, apiVersion)
        })

        const result = decodeCreateAclsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode create acls response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.results
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Delete ACL bindings matching the specified filters.
   *
   * @param filters - ACL deletion filters. Each filter matches and deletes ACL bindings.
   * @returns Per-filter results with matching deleted ACLs.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async deleteAcls(
    filters: readonly DeleteAclsFilter[]
  ): Promise<readonly DeleteAclsFilterResult[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.DeleteAcls)
      try {
        const responseReader = await conn.send(ApiKey.DeleteAcls, apiVersion, (writer) => {
          encodeDeleteAclsRequest(writer, { filters }, apiVersion)
        })

        const result = decodeDeleteAclsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode delete acls response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.filterResults
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Create a new delegation token.
   *
   * Delegation tokens enable lightweight authentication for Kafka clients
   * without requiring each client to have Kerberos credentials or TLS
   * certificates.
   *
   * @param request - The CreateDelegationToken request payload.
   * @returns The created token details including HMAC and timestamps.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async createDelegationToken(
    request: CreateDelegationTokenRequest
  ): Promise<CreateDelegationTokenResponse> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.CreateDelegationToken)
      try {
        const responseReader = await conn.send(
          ApiKey.CreateDelegationToken,
          apiVersion,
          (writer) => {
            encodeCreateDelegationTokenRequest(writer, request, apiVersion)
          }
        )

        const result = decodeCreateDelegationTokenResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode create delegation token response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Renew an existing delegation token.
   *
   * Extends the expiry time of the token. The caller must be a permitted
   * renewer of the token.
   *
   * @param request - The RenewDelegationToken request payload.
   * @returns The renewed token response including the new expiry timestamp.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async renewDelegationToken(
    request: RenewDelegationTokenRequest
  ): Promise<RenewDelegationTokenResponse> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.RenewDelegationToken)
      try {
        const responseReader = await conn.send(
          ApiKey.RenewDelegationToken,
          apiVersion,
          (writer) => {
            encodeRenewDelegationTokenRequest(writer, request, apiVersion)
          }
        )

        const result = decodeRenewDelegationTokenResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode renew delegation token response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Expire a delegation token.
   *
   * Changes the expiry time of the token. Use a negative
   * `expiryTimePeriodMs` to expire the token immediately.
   *
   * @param request - The ExpireDelegationToken request payload.
   * @returns The expiry response including the final expiry timestamp.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async expireDelegationToken(
    request: ExpireDelegationTokenRequest
  ): Promise<ExpireDelegationTokenResponse> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.ExpireDelegationToken)
      try {
        const responseReader = await conn.send(
          ApiKey.ExpireDelegationToken,
          apiVersion,
          (writer) => {
            encodeExpireDelegationTokenRequest(writer, request, apiVersion)
          }
        )

        const result = decodeExpireDelegationTokenResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode expire delegation token response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Describe delegation tokens.
   *
   * Returns delegation tokens matching the specified owner principals.
   * Pass `null` owners to describe all tokens the requester can see.
   *
   * @param request - The DescribeDelegationToken request payload.
   * @returns The described delegation tokens.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async describeDelegationTokens(
    request: DescribeDelegationTokenRequest
  ): Promise<readonly DescribedDelegationToken[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.DescribeDelegationToken)
      try {
        const responseReader = await conn.send(
          ApiKey.DescribeDelegationToken,
          apiVersion,
          (writer) => {
            encodeDescribeDelegationTokenRequest(writer, request, apiVersion)
          }
        )

        const result = decodeDescribeDelegationTokenResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode describe delegation token response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.tokens
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Describe SCRAM credentials for users.
   *
   * Returns the SCRAM credential information (mechanism and iteration count)
   * for the specified users, or all users if no filter is given.
   *
   * @param request - The DescribeUserScramCredentials request payload.
   * @returns The per-user SCRAM credential results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async describeUserScramCredentials(
    request: DescribeUserScramCredentialsRequest
  ): Promise<readonly DescribeUserScramCredentialsResult[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(
        ApiKey.DescribeUserScramCredentials
      )
      try {
        const responseReader = await conn.send(
          ApiKey.DescribeUserScramCredentials,
          apiVersion,
          (writer) => {
            encodeDescribeUserScramCredentialsRequest(writer, request, apiVersion)
          }
        )

        const result = decodeDescribeUserScramCredentialsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode describe user scram credentials response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.results
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Alter SCRAM credentials for users.
   *
   * Creates, updates, or deletes SCRAM credentials. Use upsertions to create
   * or update credentials and deletions to remove them.
   *
   * @param request - The AlterUserScramCredentials request payload.
   * @returns The per-user alteration results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async alterUserScramCredentials(
    request: AlterUserScramCredentialsRequest
  ): Promise<readonly AlterUserScramCredentialsResultEntry[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(
        ApiKey.AlterUserScramCredentials
      )
      try {
        const responseReader = await conn.send(
          ApiKey.AlterUserScramCredentials,
          apiVersion,
          (writer) => {
            encodeAlterUserScramCredentialsRequest(writer, request, apiVersion)
          }
        )

        const result = decodeAlterUserScramCredentialsResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode alter user scram credentials response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.results
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Describe client quotas matching the given entity filters.
   *
   * Returns quota configuration entries for entities matching all the
   * specified component filters.
   *
   * @param request - The DescribeClientQuotas request payload.
   * @returns The matching quota entries, or an empty array if none match.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async describeClientQuotas(
    request: DescribeClientQuotasRequest
  ): Promise<readonly DescribeClientQuotasEntry[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getAnyBrokerConnection(ApiKey.DescribeClientQuotas)
      try {
        const responseReader = await conn.send(
          ApiKey.DescribeClientQuotas,
          apiVersion,
          (writer) => {
            encodeDescribeClientQuotasRequest(writer, request, apiVersion)
          }
        )

        const result = decodeDescribeClientQuotasResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode describe client quotas response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.entries ?? []
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Alter client quotas for the specified entities.
   *
   * Sets or removes quota configuration keys for the given entity targets.
   *
   * @param request - The AlterClientQuotas request payload.
   * @returns The per-entity alteration results.
   * @throws {KafkaConnectionError} If connection or version negotiation fails.
   */
  async alterClientQuotas(
    request: AlterClientQuotasRequest
  ): Promise<readonly AlterClientQuotasEntryResponse[]> {
    return this.withRetry(async () => {
      const { conn, apiVersion } = await this.getControllerConnection(ApiKey.AlterClientQuotas)
      try {
        const responseReader = await conn.send(ApiKey.AlterClientQuotas, apiVersion, (writer) => {
          encodeAlterClientQuotasRequest(writer, request, apiVersion)
        })

        const result = decodeAlterClientQuotasResponse(responseReader, apiVersion)
        if (!result.ok) {
          throw new KafkaConnectionError(
            `failed to decode alter client quotas response: ${result.error.message}`,
            { broker: conn.broker }
          )
        }

        return result.value.entries
      } finally {
        this.pool.releaseConnection(conn)
      }
    })
  }

  /**
   * Close the admin client.
   *
   * After closing, no more operations can be performed.
   */
  close(): void {
    this.closed = true
  }

  // -------------------------------------------------------------------------
  // Internal — connection management
  // -------------------------------------------------------------------------

  /**
   * Negotiate the API version for a given API key on a connection.
   */
  private async negotiateApiVersion(conn: KafkaConnection, apiKey: ApiKey): Promise<number> {
    const apiVersionsReader = await conn.send(ApiKey.ApiVersions, 0, () => {
      // v0 has an empty body
    })

    const apiVersionsResult = decodeApiVersionsResponse(apiVersionsReader, 0)
    if (!apiVersionsResult.ok) {
      throw new KafkaConnectionError(
        `failed to decode api versions response: ${apiVersionsResult.error.message}`,
        { broker: conn.broker }
      )
    }

    if (apiVersionsResult.value.errorCode !== 0) {
      throw new KafkaConnectionError(
        `api versions request failed with error code ${String(apiVersionsResult.value.errorCode)}`,
        { broker: conn.broker }
      )
    }

    const versionMap = apiVersionsToMap(apiVersionsResult.value)
    const range = versionMap.get(apiKey)
    if (!range) {
      throw new KafkaConnectionError(`broker does not support api key ${String(apiKey)}`, {
        broker: conn.broker,
        retriable: false
      })
    }

    const version = negotiateVersion(apiKey, range)
    if (version === null) {
      throw new KafkaConnectionError(`no compatible api version for key ${String(apiKey)}`, {
        broker: conn.broker,
        retriable: false
      })
    }

    return version
  }

  /**
   * Get a connection to a controller-eligible broker with negotiated API version.
   *
   * Admin write operations (CreateTopics, DeleteTopics, etc.) must be sent to
   * the cluster controller. We use any known broker as the entry point.
   */
  private async getControllerConnection(
    apiKey: ApiKey
  ): Promise<{ conn: KafkaConnection; apiVersion: number }> {
    this.ensureOpen()

    const { brokers } = this.pool
    if (brokers.size === 0) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const firstBroker = brokers.values().next().value
    if (!firstBroker) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const conn = await this.pool.getConnectionByNodeId(firstBroker.nodeId)
    const apiVersion = await this.negotiateApiVersion(conn, apiKey)
    return { conn, apiVersion }
  }

  /**
   * Get a connection to any broker with negotiated API version.
   *
   * Read-only operations (DescribeConfigs, Metadata) can go to any broker.
   */
  private async getAnyBrokerConnection(
    apiKey: ApiKey
  ): Promise<{ conn: KafkaConnection; apiVersion: number }> {
    this.ensureOpen()

    const { brokers } = this.pool
    if (brokers.size === 0) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const firstBroker = brokers.values().next().value
    if (!firstBroker) {
      throw new KafkaConnectionError("no brokers available", { retriable: true })
    }

    const conn = await this.pool.getConnectionByNodeId(firstBroker.nodeId)
    const apiVersion = await this.negotiateApiVersion(conn, apiKey)
    return { conn, apiVersion }
  }

  // -------------------------------------------------------------------------
  // Internal — retry & lifecycle
  // -------------------------------------------------------------------------

  private async withRetry<T>(fn: () => Promise<T>): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      try {
        return await fn()
      } catch (error) {
        const isRetriable = error instanceof KafkaError && error.retriable
        if (!isRetriable || attempt >= this.maxRetries) {
          throw error
        }
        const delay = Math.min(
          this.initialRetryMs * this.retryMultiplier ** attempt,
          this.maxRetryMs
        )
        await sleep(delay)
      }
    }
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new KafkaConnectionError("admin client is closed", { retriable: false })
    }
  }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/**
 * Create a new Kafka admin client.
 *
 * @param options - Admin client options.
 * @returns A new {@link KafkaAdmin} instance.
 */
export function createAdmin(options: AdminOptions): KafkaAdmin {
  return new KafkaAdmin(options)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function toTopicInfo(topic: MetadataTopic): TopicInfo {
  return {
    name: topic.name ?? "",
    isInternal: topic.isInternal,
    partitionCount: topic.partitions.length
  }
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
