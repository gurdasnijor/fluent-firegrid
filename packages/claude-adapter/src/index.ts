/**
 * `@firegrid/claude-adapter` — the Claude Agent SDK adapter (MS-C6, WP D3): a
 * concrete `HarnessAdapter` (per WP D2's `@firegrid/harness-adapter` contract)
 * that lowers Claude Agent SDK transcripts into L1 records (`@firegrid/l1-vocabulary`,
 * interface I2), covering subagent scoping (`parent_tool_use_id`) and usage/cost
 * facts.
 */

export * from "./adapter.ts"
export * from "./events.ts"
export * from "./fixtures.ts"
export * from "./lowering.ts"
