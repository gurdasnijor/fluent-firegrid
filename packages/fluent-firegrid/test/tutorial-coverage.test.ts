import { describe, expect, it } from "vitest"
import {
  deferredSurface,
  services,
  tutorialTiers,
} from "../examples/tutorial/src/server.ts"

describe("@firegrid/fluent-firegrid tutorial coverage", () => {
  it("fluent-firegrid-keystone.EXAMPLES.2 fluent-firegrid-keystone.EXAMPLES.4 tracks implemented and deferred tutorial tiers", () => {
    expect(tutorialTiers.map((tier) => tier.tier)).toEqual([
      "01-basics",
      "02-spawn",
      "03-timeout",
      "04-retry",
      "05-saga",
      "06-cancel",
      "07-state",
      "08-clients",
      "09-workflows",
      "10-ifaces",
      "11-serdes",
    ])
    expect(services.map((service) => service._kind)).toEqual([
      "service",
      "service",
      "service",
      "object",
      "service",
      "workflow",
      "service",
    ])
    expect(deferredSurface).toHaveLength(5)
    for (const item of deferredSurface) {
      expect(item.missing.length).toBeGreaterThan(12)
    }
  })
})
