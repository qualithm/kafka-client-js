#!/usr/bin/env bun
/**
 * Demo script showcasing the @qualithm/kafka-client package.
 *
 * Run with: bun run demo
 */

import { ApiKey, CLIENT_API_VERSIONS, isFlexibleVersion, parseBrokerAddress } from "../src/index.js"

console.log("@qualithm/kafka-client Demo")
console.log("===========================\n")

// Parse broker addresses
const addresses = ["kafka-1:9092", "kafka-2:9093", "localhost"]
for (const addr of addresses) {
  const parsed = parseBrokerAddress(addr)
  console.log(`Broker: ${addr} => ${parsed.host}:${String(parsed.port)}`)
}

// Show supported API versions
console.log("\nSupported API versions:")
for (const [key, range] of Object.entries(CLIENT_API_VERSIONS)) {
  const apiKey = Number(key)
  const name = Object.entries(ApiKey).find(([, v]) => v === apiKey)?.[0] ?? `API ${key}`
  const flexible = isFlexibleVersion(apiKey as ApiKey, range.maxVersion)
  console.log(
    `  ${name}: v${String(range.minVersion)}-v${String(range.maxVersion)}${flexible ? " (flexible)" : ""}`
  )
}
