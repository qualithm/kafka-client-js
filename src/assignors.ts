/**
 * Partition assignor strategies for consumer group rebalancing.
 *
 * Assignors determine how topic-partitions are distributed among consumer
 * group members. The consumer group leader runs the assignor during the
 * SyncGroup phase.
 *
 * Includes built-in strategies:
 * - **range** (default) — assigns contiguous partition ranges to members per-topic
 * - **roundRobin** — distributes partitions across members in round-robin order
 * - **cooperativeSticky** — maintains previous assignments where possible, supports
 *   incremental cooperative rebalancing
 *
 * @packageDocumentation
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Metadata about a consumer group member for partition assignment.
 */
export type MemberSubscription = {
  /** The member ID assigned by the group coordinator. */
  readonly memberId: string
  /** Topics subscribed by this member. */
  readonly topics: readonly string[]
}

/**
 * Assignment result for a single member.
 */
export type MemberAssignment = {
  /** The member ID. */
  readonly memberId: string
  /** Topic-partition assignments for this member. */
  readonly topicPartitions: readonly { readonly topic: string; readonly partitions: number[] }[]
}

/**
 * Partition assignor strategy.
 *
 * Receives all member subscriptions and known partition counts, and produces
 * a partition assignment for each member.
 */
export type PartitionAssignor = {
  /** Protocol name sent in JoinGroup metadata (e.g. "range", "roundrobin", "cooperative-sticky"). */
  readonly name: string
  /**
   * Assign partitions to members.
   *
   * @param members - Subscriptions for all group members.
   * @param partitionCounts - Known partition counts per topic. May be empty if
   *   partition counts are not yet discovered.
   * @returns Assignment for each member.
   */
  readonly assign: (
    members: readonly MemberSubscription[],
    partitionCounts: ReadonlyMap<string, number>
  ) => readonly MemberAssignment[]
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function getOrCreateTopicMap(
  map: Map<string, Map<string, number[]>>,
  memberId: string
): Map<string, number[]> {
  let topicMap = map.get(memberId)
  if (!topicMap) {
    topicMap = new Map()
    map.set(memberId, topicMap)
  }
  return topicMap
}

function getOrCreatePartitions(topicMap: Map<string, number[]>, topic: string): number[] {
  let partitions = topicMap.get(topic)
  if (!partitions) {
    partitions = []
    topicMap.set(topic, partitions)
  }
  return partitions
}

function collectAllTopics(members: readonly MemberSubscription[]): Set<string> {
  const topics = new Set<string>()
  for (const member of members) {
    for (const topic of member.topics) {
      topics.add(topic)
    }
  }
  return topics
}

function toAssignmentResult(
  result: Map<string, Map<string, number[]>>,
  sorted = false
): readonly MemberAssignment[] {
  return [...result.entries()].map(([memberId, topicMap]) => ({
    memberId,
    topicPartitions: [...topicMap.entries()].map(([topic, partitions]) => ({
      topic,
      partitions: sorted ? partitions.sort((a, b) => a - b) : partitions
    }))
  }))
}

// ---------------------------------------------------------------------------
// Range assignor
// ---------------------------------------------------------------------------

/**
 * Range partition assignor.
 *
 * For each topic, sorts consumers alphabetically by member ID and assigns
 * contiguous partition ranges. This is the default Kafka assignor.
 *
 * @example
 * With 6 partitions and 2 consumers [A, B]:
 * - A gets [0, 1, 2]
 * - B gets [3, 4, 5]
 */
export const rangeAssignor: PartitionAssignor = {
  name: "range",
  assign(
    members: readonly MemberSubscription[],
    partitionCounts: ReadonlyMap<string, number>
  ): readonly MemberAssignment[] {
    const result = new Map<string, Map<string, number[]>>()

    for (const member of members) {
      result.set(member.memberId, new Map())
    }

    const allTopics = collectAllTopics(members)

    for (const topic of allTopics) {
      const numPartitions = partitionCounts.get(topic) ?? 0
      if (numPartitions === 0) {
        continue
      }

      const subscribedMembers = members
        .filter((m) => m.topics.includes(topic))
        .map((m) => m.memberId)
        .sort()

      const numMembers = subscribedMembers.length
      if (numMembers === 0) {
        continue
      }

      const partitionsPerMember = Math.floor(numPartitions / numMembers)
      const remainder = numPartitions % numMembers

      let partitionIndex = 0
      for (const memberId of subscribedMembers) {
        const idx = subscribedMembers.indexOf(memberId)
        const count = partitionsPerMember + (idx < remainder ? 1 : 0)
        const partitions: number[] = []
        for (let j = 0; j < count; j++) {
          partitions.push(partitionIndex++)
        }
        getOrCreateTopicMap(result, memberId).set(topic, partitions)
      }
    }

    return toAssignmentResult(result)
  }
}

// ---------------------------------------------------------------------------
// Round-robin assignor
// ---------------------------------------------------------------------------

/**
 * Round-robin partition assignor.
 *
 * Distributes all partitions across all members in a circular fashion.
 * More even distribution than range when partition counts don't divide evenly.
 */
export const roundRobinAssignor: PartitionAssignor = {
  name: "roundrobin",
  assign(
    members: readonly MemberSubscription[],
    partitionCounts: ReadonlyMap<string, number>
  ): readonly MemberAssignment[] {
    const result = new Map<string, Map<string, number[]>>()

    for (const member of members) {
      result.set(member.memberId, new Map())
    }

    const allTopicPartitions: { topic: string; partition: number }[] = []
    const allTopics = collectAllTopics(members)

    for (const topic of [...allTopics].sort()) {
      const numPartitions = partitionCounts.get(topic) ?? 0
      for (let p = 0; p < numPartitions; p++) {
        allTopicPartitions.push({ topic, partition: p })
      }
    }

    const sortedMemberIds = members.map((m) => m.memberId).sort()
    const memberTopicSets = new Map<string, Set<string>>()
    for (const member of members) {
      memberTopicSets.set(member.memberId, new Set(member.topics))
    }

    let memberIndex = 0
    for (const tp of allTopicPartitions) {
      for (const _ of sortedMemberIds) {
        const candidateId = sortedMemberIds[memberIndex % sortedMemberIds.length]
        memberIndex++
        const candidateTopics = memberTopicSets.get(candidateId)
        if (candidateTopics?.has(tp.topic) === true) {
          const topicMap = getOrCreateTopicMap(result, candidateId)
          getOrCreatePartitions(topicMap, tp.topic).push(tp.partition)
          break
        }
      }
    }

    return toAssignmentResult(result)
  }
}

// ---------------------------------------------------------------------------
// Cooperative sticky assignor
// ---------------------------------------------------------------------------

/**
 * Cooperative sticky partition assignor.
 *
 * Attempts to preserve existing assignments across rebalances while
 * maintaining even distribution. When partitions must move, only the
 * affected partitions are revoked and reassigned (incremental cooperative
 * rebalancing).
 *
 * Previous assignments are tracked across calls to `assign()`. Create a new
 * instance per consumer to maintain state.
 */
export function createCooperativeStickyAssignor(): PartitionAssignor {
  let previousAssignment = new Map<string, Map<string, number[]>>()

  return {
    name: "cooperative-sticky",
    assign(
      members: readonly MemberSubscription[],
      partitionCounts: ReadonlyMap<string, number>
    ): readonly MemberAssignment[] {
      const newAssignment = new Map<string, Map<string, number[]>>()
      for (const member of members) {
        newAssignment.set(member.memberId, new Map())
      }

      const memberTopicSets = new Map<string, Set<string>>()
      for (const member of members) {
        memberTopicSets.set(member.memberId, new Set(member.topics))
      }

      const allTopicPartitions = buildTopicPartitions(members, partitionCounts)
      const assigned = new Set<string>()

      retainStickyAssignments(
        previousAssignment,
        newAssignment,
        memberTopicSets,
        partitionCounts,
        assigned
      )

      const unassigned = collectUnassigned(allTopicPartitions, assigned)

      rebalanceAssignments(
        members,
        newAssignment,
        memberTopicSets,
        unassigned,
        allTopicPartitions.length
      )

      // Save for next rebalance
      previousAssignment = cloneAssignment(newAssignment)

      return toAssignmentResult(newAssignment, true)
    }
  }
}

/** Collect all topic-partition pairs from member subscriptions. */
function buildTopicPartitions(
  members: readonly MemberSubscription[],
  partitionCounts: ReadonlyMap<string, number>
): { topic: string; partition: number }[] {
  const allTopics = collectAllTopics(members)
  const result: { topic: string; partition: number }[] = []
  for (const topic of [...allTopics].sort()) {
    const count = partitionCounts.get(topic) ?? 0
    for (let p = 0; p < count; p++) {
      result.push({ topic, partition: p })
    }
  }
  return result
}

/** Retain valid assignments from previous rebalance. */
function retainStickyAssignments(
  previous: Map<string, Map<string, number[]>>,
  current: Map<string, Map<string, number[]>>,
  memberTopicSets: Map<string, Set<string>>,
  partitionCounts: ReadonlyMap<string, number>,
  assigned: Set<string>
): void {
  for (const [memberId, topicMap] of previous) {
    const subscribedTo = memberTopicSets.get(memberId)
    if (!subscribedTo) {
      continue
    }

    for (const [topic, partitions] of topicMap) {
      if (!subscribedTo.has(topic)) {
        continue
      }
      const maxPartitions = partitionCounts.get(topic) ?? 0
      const validPartitions = partitions.filter((p) => p < maxPartitions)
      if (validPartitions.length > 0) {
        getOrCreateTopicMap(current, memberId).set(topic, [...validPartitions])
        for (const p of validPartitions) {
          assigned.add(`${topic}:${String(p)}`)
        }
      }
    }
  }
}

/** Collect partitions that have not been assigned yet. */
function collectUnassigned(
  allTopicPartitions: { topic: string; partition: number }[],
  assigned: Set<string>
): { topic: string; partition: number }[] {
  const result: { topic: string; partition: number }[] = []
  for (const tp of allTopicPartitions) {
    if (!assigned.has(`${tp.topic}:${String(tp.partition)}`)) {
      result.push(tp)
    }
  }
  return result
}

/** Count total partitions assigned to a member. */
function countMemberAssignments(
  assignment: Map<string, Map<string, number[]>>,
  memberId: string
): number {
  let count = 0
  const topicMap = assignment.get(memberId)
  if (topicMap) {
    for (const partitions of topicMap.values()) {
      count += partitions.length
    }
  }
  return count
}

/** Steal from over-assigned members and distribute unassigned partitions. */
function rebalanceAssignments(
  members: readonly MemberSubscription[],
  newAssignment: Map<string, Map<string, number[]>>,
  memberTopicSets: Map<string, Set<string>>,
  unassigned: { topic: string; partition: number }[],
  totalPartitions: number
): void {
  const numMembers = members.length
  const targetMin = Math.floor(totalPartitions / numMembers)
  const targetExtra = totalPartitions % numMembers

  // Compute per-member targets
  const sortedAsc = members
    .map((m) => m.memberId)
    .sort((a, b) => {
      const diff =
        countMemberAssignments(newAssignment, a) - countMemberAssignments(newAssignment, b)
      return diff !== 0 ? diff : a.localeCompare(b)
    })

  const memberTarget = new Map<string, number>()
  for (let i = 0; i < sortedAsc.length; i++) {
    memberTarget.set(sortedAsc[i], i < targetExtra ? targetMin + 1 : targetMin)
  }

  // Steal excess from over-assigned members (most-loaded first)
  const sortedDesc = members
    .map((m) => m.memberId)
    .sort((a, b) => {
      const diff =
        countMemberAssignments(newAssignment, b) - countMemberAssignments(newAssignment, a)
      return diff !== 0 ? diff : a.localeCompare(b)
    })

  for (const memberId of sortedDesc) {
    const memberMap = getOrCreateTopicMap(newAssignment, memberId)
    let currentCount = countMemberAssignments(newAssignment, memberId)
    const target = memberTarget.get(memberId) ?? targetMin
    if (currentCount <= target) {
      continue
    }

    for (const [topic, partitions] of memberMap) {
      while (partitions.length > 0 && currentCount > target) {
        const removed = partitions.pop()
        if (removed !== undefined) {
          unassigned.push({ topic, partition: removed })
        }
        currentCount--
      }
      if (partitions.length === 0) {
        memberMap.delete(topic)
      }
      if (currentCount <= target) {
        break
      }
    }
  }

  // Distribute unassigned to members with fewest
  for (const tp of unassigned) {
    let bestMember: string | undefined
    let bestCount = Infinity

    for (const memberId of sortedAsc) {
      const topics = memberTopicSets.get(memberId)
      if (topics?.has(tp.topic) !== true) {
        continue
      }
      const count = countMemberAssignments(newAssignment, memberId)
      if (count < bestCount) {
        bestCount = count
        bestMember = memberId
      }
    }

    if (bestMember !== undefined) {
      const topicMap = getOrCreateTopicMap(newAssignment, bestMember)
      getOrCreatePartitions(topicMap, tp.topic).push(tp.partition)
    }
  }
}

/** Deep-clone an assignment map. */
function cloneAssignment(
  assignment: Map<string, Map<string, number[]>>
): Map<string, Map<string, number[]>> {
  const clone = new Map<string, Map<string, number[]>>()
  for (const [memberId, topicMap] of assignment) {
    const copy = new Map<string, number[]>()
    for (const [topic, partitions] of topicMap) {
      copy.set(topic, [...partitions])
    }
    clone.set(memberId, copy)
  }
  return clone
}
