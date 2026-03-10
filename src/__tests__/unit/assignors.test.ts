import { describe, expect, it } from "vitest"

import {
  createCooperativeStickyAssignor,
  type MemberAssignment,
  type MemberSubscription,
  rangeAssignor,
  roundRobinAssignor
} from "../../assignors"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Get partition numbers for a member+topic from assignment list. */
function getPartitions(
  assignments: readonly MemberAssignment[],
  memberId: string,
  topic: string
): number[] {
  const member = assignments.find((a) => a.memberId === memberId)
  if (!member) {
    return []
  }
  const tp = member.topicPartitions.find((t) => t.topic === topic)
  return tp ? [...tp.partitions] : []
}

/** Count total partitions assigned to a member. */
function totalPartitions(assignments: readonly MemberAssignment[], memberId: string): number {
  const member = assignments.find((a) => a.memberId === memberId)
  if (!member) {
    return 0
  }
  return member.topicPartitions.reduce((sum, tp) => sum + tp.partitions.length, 0)
}

// ---------------------------------------------------------------------------
// Range assignor
// ---------------------------------------------------------------------------

describe("rangeAssignor", () => {
  it("has protocol name 'range'", () => {
    expect(rangeAssignor.name).toBe("range")
  })

  it("assigns even partitions across two members", () => {
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t1"] }
    ]
    const partitions = new Map([["t1", 6]])

    const result = rangeAssignor.assign(members, partitions)

    expect(getPartitions(result, "A", "t1")).toEqual([0, 1, 2])
    expect(getPartitions(result, "B", "t1")).toEqual([3, 4, 5])
  })

  it("distributes remainder to earlier members", () => {
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t1"] }
    ]
    const partitions = new Map([["t1", 5]])

    const result = rangeAssignor.assign(members, partitions)

    // A gets 3 (floor + 1 remainder), B gets 2
    expect(getPartitions(result, "A", "t1")).toEqual([0, 1, 2])
    expect(getPartitions(result, "B", "t1")).toEqual([3, 4])
  })

  it("handles single member", () => {
    const members: MemberSubscription[] = [{ memberId: "solo", topics: ["t1"] }]
    const partitions = new Map([["t1", 3]])

    const result = rangeAssignor.assign(members, partitions)

    expect(getPartitions(result, "solo", "t1")).toEqual([0, 1, 2])
  })

  it("handles multiple topics independently", () => {
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1", "t2"] },
      { memberId: "B", topics: ["t1", "t2"] }
    ]
    const partitions = new Map([
      ["t1", 4],
      ["t2", 2]
    ])

    const result = rangeAssignor.assign(members, partitions)

    expect(getPartitions(result, "A", "t1")).toEqual([0, 1])
    expect(getPartitions(result, "B", "t1")).toEqual([2, 3])
    expect(getPartitions(result, "A", "t2")).toEqual([0])
    expect(getPartitions(result, "B", "t2")).toEqual([1])
  })

  it("skips topics with zero partitions", () => {
    const members: MemberSubscription[] = [{ memberId: "A", topics: ["t1"] }]
    const partitions = new Map([["t1", 0]])

    const result = rangeAssignor.assign(members, partitions)

    expect(getPartitions(result, "A", "t1")).toEqual([])
  })

  it("skips topics not in partition counts", () => {
    const members: MemberSubscription[] = [{ memberId: "A", topics: ["missing"] }]
    const partitions = new Map<string, number>()

    const result = rangeAssignor.assign(members, partitions)

    expect(getPartitions(result, "A", "missing")).toEqual([])
  })

  it("only assigns to members subscribed to the topic", () => {
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t2"] }
    ]
    const partitions = new Map([
      ["t1", 3],
      ["t2", 2]
    ])

    const result = rangeAssignor.assign(members, partitions)

    expect(getPartitions(result, "A", "t1")).toEqual([0, 1, 2])
    expect(getPartitions(result, "A", "t2")).toEqual([])
    expect(getPartitions(result, "B", "t2")).toEqual([0, 1])
    expect(getPartitions(result, "B", "t1")).toEqual([])
  })

  it("returns empty assignments for empty members", () => {
    const result = rangeAssignor.assign([], new Map())
    expect(result).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// Round-robin assignor
// ---------------------------------------------------------------------------

describe("roundRobinAssignor", () => {
  it("has protocol name 'roundrobin'", () => {
    expect(roundRobinAssignor.name).toBe("roundrobin")
  })

  it("distributes partitions in round-robin order", () => {
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t1"] }
    ]
    const partitions = new Map([["t1", 4]])

    const result = roundRobinAssignor.assign(members, partitions)

    // Round-robin: p0→A, p1→B, p2→A, p3→B
    expect(getPartitions(result, "A", "t1")).toEqual([0, 2])
    expect(getPartitions(result, "B", "t1")).toEqual([1, 3])
  })

  it("handles odd number of partitions", () => {
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t1"] }
    ]
    const partitions = new Map([["t1", 3]])

    const result = roundRobinAssignor.assign(members, partitions)

    expect(getPartitions(result, "A", "t1")).toEqual([0, 2])
    expect(getPartitions(result, "B", "t1")).toEqual([1])
  })

  it("interleaves across topics sorted alphabetically", () => {
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["alpha", "beta"] },
      { memberId: "B", topics: ["alpha", "beta"] }
    ]
    const partitions = new Map([
      ["alpha", 2],
      ["beta", 2]
    ])

    const result = roundRobinAssignor.assign(members, partitions)

    // alpha:0→A, alpha:1→B, beta:0→A, beta:1→B
    expect(getPartitions(result, "A", "alpha")).toEqual([0])
    expect(getPartitions(result, "B", "alpha")).toEqual([1])
    expect(getPartitions(result, "A", "beta")).toEqual([0])
    expect(getPartitions(result, "B", "beta")).toEqual([1])
  })

  it("skips members not subscribed to a topic", () => {
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t2"] }
    ]
    const partitions = new Map([
      ["t1", 2],
      ["t2", 2]
    ])

    const result = roundRobinAssignor.assign(members, partitions)

    expect(getPartitions(result, "A", "t1")).toEqual([0, 1])
    expect(getPartitions(result, "B", "t2")).toEqual([0, 1])
  })

  it("handles single member", () => {
    const members: MemberSubscription[] = [{ memberId: "solo", topics: ["t1"] }]
    const partitions = new Map([["t1", 3]])

    const result = roundRobinAssignor.assign(members, partitions)

    expect(getPartitions(result, "solo", "t1")).toEqual([0, 1, 2])
  })

  it("returns empty assignments for empty input", () => {
    const result = roundRobinAssignor.assign([], new Map())
    expect(result).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// Cooperative sticky assignor
// ---------------------------------------------------------------------------

describe("createCooperativeStickyAssignor", () => {
  it("has protocol name 'cooperative-sticky'", () => {
    const assignor = createCooperativeStickyAssignor()
    expect(assignor.name).toBe("cooperative-sticky")
  })

  it("assigns all partitions evenly on fresh start", () => {
    const assignor = createCooperativeStickyAssignor()
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t1"] }
    ]
    const partitions = new Map([["t1", 4]])

    const result = assignor.assign(members, partitions)

    expect(totalPartitions(result, "A")).toBe(2)
    expect(totalPartitions(result, "B")).toBe(2)

    // All 4 partitions assigned
    const all = [...getPartitions(result, "A", "t1"), ...getPartitions(result, "B", "t1")].sort(
      (a, b) => a - b
    )
    expect(all).toEqual([0, 1, 2, 3])
  })

  it("preserves assignments across rebalances", () => {
    const assignor = createCooperativeStickyAssignor()
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t1"] }
    ]
    const partitions = new Map([["t1", 4]])

    const first = assignor.assign(members, partitions)
    const aFirst = getPartitions(first, "A", "t1")
    const bFirst = getPartitions(first, "B", "t1")

    // Re-assign with same members — should be identical
    const second = assignor.assign(members, partitions)
    const aSecond = getPartitions(second, "A", "t1")
    const bSecond = getPartitions(second, "B", "t1")

    expect(aSecond.sort()).toEqual(aFirst.sort())
    expect(bSecond.sort()).toEqual(bFirst.sort())
  })

  it("redistributes when a member leaves", () => {
    const assignor = createCooperativeStickyAssignor()
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t1"] },
      { memberId: "C", topics: ["t1"] }
    ]
    const partitions = new Map([["t1", 6]])

    assignor.assign(members, partitions)

    // Member C leaves
    const remaining: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t1"] }
    ]
    const result = assignor.assign(remaining, partitions)

    // All 6 partitions should still be assigned
    const all = [...getPartitions(result, "A", "t1"), ...getPartitions(result, "B", "t1")].sort(
      (a, b) => a - b
    )
    expect(all).toEqual([0, 1, 2, 3, 4, 5])

    // Each gets 3
    expect(totalPartitions(result, "A")).toBe(3)
    expect(totalPartitions(result, "B")).toBe(3)
  })

  it("accommodates a new member joining", () => {
    const assignor = createCooperativeStickyAssignor()
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t1"] }
    ]
    const partitions = new Map([["t1", 6]])

    assignor.assign(members, partitions)

    // Member C joins — sticky assignor steals from over-assigned members
    // to balance within one rebalance (targetMin = 2, targetExtra = 0)
    const expanded: MemberSubscription[] = [
      { memberId: "A", topics: ["t1"] },
      { memberId: "B", topics: ["t1"] },
      { memberId: "C", topics: ["t1"] }
    ]
    const result = assignor.assign(expanded, partitions)

    // All partitions still assigned
    const all = [
      ...getPartitions(result, "A", "t1"),
      ...getPartitions(result, "B", "t1"),
      ...getPartitions(result, "C", "t1")
    ].sort((a, b) => a - b)
    expect(all).toEqual([0, 1, 2, 3, 4, 5])

    // Evenly balanced: 2 each
    expect(totalPartitions(result, "A")).toBe(2)
    expect(totalPartitions(result, "B")).toBe(2)
    expect(totalPartitions(result, "C")).toBe(2)
  })

  it("handles multiple topics", () => {
    const assignor = createCooperativeStickyAssignor()
    const members: MemberSubscription[] = [
      { memberId: "A", topics: ["t1", "t2"] },
      { memberId: "B", topics: ["t1", "t2"] }
    ]
    const partitions = new Map([
      ["t1", 2],
      ["t2", 2]
    ])

    const result = assignor.assign(members, partitions)

    // 4 total partitions across 2 members → 2 each
    expect(totalPartitions(result, "A")).toBe(2)
    expect(totalPartitions(result, "B")).toBe(2)
  })

  it("handles empty partition counts gracefully", () => {
    const assignor = createCooperativeStickyAssignor()
    const members: MemberSubscription[] = [{ memberId: "A", topics: ["t1"] }]
    const partitions = new Map<string, number>()

    const result = assignor.assign(members, partitions)

    expect(totalPartitions(result, "A")).toBe(0)
  })

  it("returns sorted partition numbers", () => {
    const assignor = createCooperativeStickyAssignor()
    const members: MemberSubscription[] = [{ memberId: "A", topics: ["t1"] }]
    const partitions = new Map([["t1", 5]])

    const result = assignor.assign(members, partitions)

    const p = getPartitions(result, "A", "t1")
    expect(p).toEqual([0, 1, 2, 3, 4])
    // Verify sorted
    expect(p).toEqual([...p].sort((a, b) => a - b))
  })
})
