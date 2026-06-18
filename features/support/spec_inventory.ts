import { Then } from "@cucumber/cucumber"
import { strict as assert } from "node:assert"

export const defineInventoryStep = (pattern: string): void => {
  Then(pattern, function(requirement: string) {
    assert.notEqual(requirement.trim(), "", "contract inventory scenarios must include requirement text")
  })
}

