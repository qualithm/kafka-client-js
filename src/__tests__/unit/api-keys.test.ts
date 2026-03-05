import { describe, expect, it } from "vitest"

import {
  ApiKey,
  type ApiVersionRange,
  CLIENT_API_VERSIONS,
  isFlexibleVersion,
  negotiateVersion
} from "../../api-keys"

describe("ApiKey", () => {
  it("has correct values for key APIs", () => {
    expect(ApiKey.Produce).toBe(0)
    expect(ApiKey.Fetch).toBe(1)
    expect(ApiKey.ListOffsets).toBe(2)
    expect(ApiKey.Metadata).toBe(3)
    expect(ApiKey.ApiVersions).toBe(18)
    expect(ApiKey.SaslHandshake).toBe(17)
    expect(ApiKey.SaslAuthenticate).toBe(36)
  })
})

describe("CLIENT_API_VERSIONS", () => {
  it("includes ApiVersions", () => {
    const range = CLIENT_API_VERSIONS[ApiKey.ApiVersions]

    expect(range).toBeDefined()
    expect(range!.minVersion).toBe(0)
    expect(range!.maxVersion).toBe(3)
  })

  it("includes all core APIs", () => {
    const requiredApis = [
      ApiKey.Produce,
      ApiKey.Fetch,
      ApiKey.Metadata,
      ApiKey.ApiVersions,
      ApiKey.FindCoordinator,
      ApiKey.JoinGroup,
      ApiKey.SyncGroup,
      ApiKey.Heartbeat,
      ApiKey.LeaveGroup
    ]

    for (const key of requiredApis) {
      expect(CLIENT_API_VERSIONS[key]).toBeDefined()
    }
  })
})

describe("isFlexibleVersion", () => {
  it("returns false for ApiVersions v0", () => {
    expect(isFlexibleVersion(ApiKey.ApiVersions, 0)).toBe(false)
  })

  it("returns true for ApiVersions v3", () => {
    expect(isFlexibleVersion(ApiKey.ApiVersions, 3)).toBe(true)
  })

  it("returns false for SaslHandshake (never flexible)", () => {
    expect(isFlexibleVersion(ApiKey.SaslHandshake, 0)).toBe(false)
    expect(isFlexibleVersion(ApiKey.SaslHandshake, 1)).toBe(false)
  })

  it("returns true for Metadata v9+", () => {
    expect(isFlexibleVersion(ApiKey.Metadata, 8)).toBe(false)
    expect(isFlexibleVersion(ApiKey.Metadata, 9)).toBe(true)
    expect(isFlexibleVersion(ApiKey.Metadata, 12)).toBe(true)
  })

  it("returns false for unknown API key", () => {
    expect(isFlexibleVersion(255 as ApiKey, 0)).toBe(false)
  })
})

describe("negotiateVersion", () => {
  it("returns highest overlapping version", () => {
    const brokerRange: ApiVersionRange = { minVersion: 0, maxVersion: 3 }
    const version = negotiateVersion(ApiKey.ApiVersions, brokerRange)

    expect(version).toBe(3)
  })

  it("returns broker max when lower than client max", () => {
    const brokerRange: ApiVersionRange = { minVersion: 0, maxVersion: 1 }
    const version = negotiateVersion(ApiKey.ApiVersions, brokerRange)

    expect(version).toBe(1)
  })

  it("returns client max when lower than broker max", () => {
    const brokerRange: ApiVersionRange = { minVersion: 0, maxVersion: 100 }
    const version = negotiateVersion(ApiKey.ApiVersions, brokerRange)

    // Client max for ApiVersions is 3
    expect(version).toBe(3)
  })

  it("returns null when no overlap", () => {
    const brokerRange: ApiVersionRange = { minVersion: 50, maxVersion: 100 }
    const version = negotiateVersion(ApiKey.ApiVersions, brokerRange)

    expect(version).toBeNull()
  })

  it("returns null for unsupported API key", () => {
    const brokerRange: ApiVersionRange = { minVersion: 0, maxVersion: 5 }
    const version = negotiateVersion(255 as ApiKey, brokerRange)

    expect(version).toBeNull()
  })

  it("returns the single overlapping version when ranges barely touch", () => {
    // Client Metadata: 0-12, broker: 12-20 => overlap at 12
    const brokerRange: ApiVersionRange = { minVersion: 12, maxVersion: 20 }
    const version = negotiateVersion(ApiKey.Metadata, brokerRange)

    expect(version).toBe(12)
  })
})
