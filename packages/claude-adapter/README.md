# @firegrid/claude-adapter

The **Claude Agent SDK adapter** (managed-sessions SDD §MS-C6, WP D3): a concrete
`HarnessAdapter` (per WP D2's `@firegrid/harness-adapter` contract) that lowers
Claude Agent SDK transcripts into L1 records (`@firegrid/l1-vocabulary`, interface
**I2**). It consumes the ratified contracts as-is — no changes to I2 or the D2
contract.

It covers the two agent-ui defects this lane exists to fix:

- **Subagent scoping** (`parent_tool_use_id`). A Claude assistant/user event whose
  `parent_tool_use_id` is non-null is a subagent's output. Its text, nested tool
  calls, and tool results lower to `tool_call_update` content on the **parent Task
  tool call** — never a top-level `agent_message_chunk` — plus an ignorable
  `firegrid/subagent` attribution record (emitted once per parent). The base
  `foldTurn` therefore attributes subagent work to its parent tool call and never
  interleaves it into top-level turn text, **without reading the extension**.
- **Usage / cost facts.** The SDK `result` message's token usage and
  `total_cost_usd` lower to a `firegrid/usage` extension (ignorable-by-default; a
  usage-aware UI surfaces per-turn cost).

## Shape

- `events.ts` — a faithful, **Effect-free** model of the SDK `SDKMessage` stream
  (`system`/`assistant`/`user`/`result`, `parent_tool_use_id`, `session_id`,
  result `usage` + `total_cost_usd`; content blocks `text`/`thinking`/`tool_use`/
  `tool_result`). The package models the shape rather than depending on
  `@anthropic-ai/claude-agent-sdk`, so the lowering is deterministic and the
  proofs run in CI with no API key.
- `lowering.ts` — `claudeLowering`: the pure `HarnessLowering<ClaudeEvent, …>`.
- `adapter.ts` — `claudeCapabilities`, `recordedClaudeSource`, `makeClaudeAdapter`
  / `claudeAdapterLayer`, and `claudeResumeArtifact` (the Claude session id).

The shipped adapter drives **recorded** transcripts (the proof path) and mediates
no durable tools, so it declares `observe-only`. A gateable live variant — the
real SDK `query()` feeding `ClaudeEvent`s, Firegrid durable tools mediated through
`ToolGate` — is a follow-up at the agent-ui integration (WP E4) and reuses this
same lowering; the live boundary maps a real `SDKMessage` to a `ClaudeEvent`
(a near-identity projection).

## Proofs

`apps/proofs/proofs/claude-adapter.ts`:

- `harness.subagent-scoping` — subagent output folds under its parent tool call
  and never into top-level turn text; `firegrid/subagent` is ignorable (the fold
  is invariant to stripping it).
- `harness.claude.fixture-replay` — the lowering and the D2 `drive` shell
  reconstruct valid, deterministic L1 records from recorded Claude transcripts.
- `harness.claude.usage-facts` — token usage and cost surface as `firegrid/usage`,
  ignorable-by-default.
