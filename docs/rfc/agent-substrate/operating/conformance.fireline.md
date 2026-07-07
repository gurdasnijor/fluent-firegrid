---
type: conformance-profile
status: draft-for-review
authored-by: Codex Architect 4
date: 2026-04-30
peer-review: required
po-signoff: required
related:
  - /rfc/README.md
  - vault/fireline-vnext/sdds/active/runtime-stream-first-cleanup-2026-04-30.md
  - vault/explorations/agent3-conductor-side-cache-projection-audit-2026-04-30.md
  - vault/explorations/agent4-ts-client-schema-replay-audit-2026-04-30.md
  - vault/explorations/agent6-examples-tests-blast-radius-2026-04-30.md
  - vault/explorations/agent7-provider-sandbox-transport-boundary-2026-04-30.md
---

# Fireline Conformance Profile

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

## §1 Purpose

The Stream-First Agent Substrate RFC defines a neutral substrate: durable record
flow, row projections, intent/claim/terminal patterns, adapter boundaries,
durable waits, timers, and conformance levels. This profile is the Fireline
implementation contract layered on top of that neutral RFC.

This document deliberately names Fireline-specific wire rows, permission subject
identity, permission lifecycle, owner-death behavior, adapter reattach behavior,
choreography tool schemas, and phase gates. It is the contract a Fireline product
consumer, including Firepixel, can build against while the RFC remains
language-, runtime-, and protocol-neutral.

Evidence citations are to origin/main `7ece350e`. The profile also records where
the desired Fireline contract differs from current main so cleanup work can close
the gap deliberately rather than by drift.

## §6 Reattach Policies

The RFC names adapter reattach profiles such as no-reattach, load-via-protocol,
supervised reattach, and provider-specific reattach. Fireline maps current
adapters as follows:

| Adapter / provider | Profile | Fireline policy | Evidence / notes |
|---|---|---|---|
| Fake ACP in-memory / tests | No reattach. | Return not-live / failed prompt if the live test proxy is gone. | Mock transport is test-only behind `#[cfg(test)]` in `SessionProxyProvider::Mock` (`crates/fireline-runtime/src/launch/session_proxy.rs:30-39`, `crates/fireline-runtime/src/launch/session_proxy.rs:55-68`). |
| Real ACP endpoint with `session/load` support | Load via protocol. | Reattach by protocol only after the adapter proves the durable `SessionId` is promptable. A session row alone is not sufficient. | Prepared sessions expose `supports_load_session` (`crates/fireline-runtime/src/launch/session_proxy.rs:64-68`), and SessionProxyRegistry uses live proxy ownership for prompt readiness (`crates/fireline-runtime/src/launch/session_proxy.rs:134-154`). |
| Local stdio / local subprocess runtime | No reattach unless an external supervisor keeps the process and exposes a loadable endpoint. | Current local subprocess provider can create/get/list lifecycle state, but process handles are live provider memory, so process-loss recovery is not load-via-protocol today. | Local subprocess lifecycle/readiness is a real provider adapter; detailed execute-path migration remains owned by the cleanup SDD (`crates/fireline-runtime/src/sandbox/providers/local_subprocess.rs:238-327`, `vault/explorations/agent7-provider-sandbox-transport-boundary-2026-04-30.md:33-37`). |
| Docker runtime provider | Supervised reattach only if the container and ACP endpoint are still live and loadable; otherwise reprovision or fail. | Docker provider has lifecycle/readiness and descriptor lookup. | Agent 7 classifies Docker lifecycle/readiness as keep and Docker execute as move-to-adapter (`vault/explorations/agent7-provider-sandbox-transport-boundary-2026-04-30.md:35-37`). |
| Remote Anthropic managed-agent provider | Provider-specific. | Prefer provider-native session lookup and execution semantics. If Anthropic session lookup succeeds and event stream can resume, treat as provider reattach; if the provider reports missing/stopped, fail or reprovision based on launch policy. | Remote Anthropic creates provider sessions, exposes provider endpoints, fetches/list sessions, and executes through provider API polling (`crates/fireline-runtime/src/sandbox/providers/anthropic.rs:451-575`, `vault/explorations/agent7-provider-sandbox-transport-boundary-2026-04-30.md:30-32`). |

### Hosted, Tool-Attach, Telegram, And Webhook Profiles

The tests-as-canon RFC pass identified these product profiles as Fireline-specific
contracts rather than neutral RFC material:

| Profile | Fireline-specific contract | Evidence |
|---|---|---|
| Hosted runtime | Hosted runtime serves ACP, emits state stream rows for prompt/session flow, and keeps ACP available even when Telegram init probing fails. Concurrent attachment returns HTTP conflict, and immediate sequential reattach after disconnect is allowed. | `tests/hosted_runtime.rs:247-310`, `tests/hosted_runtime.rs:318-355`, `tests/hosted_runtime.rs:361-438` |
| Smithery / MCP passthrough | `session/new` passes HTTP MCP server definitions and headers through to the agent; an empty MCP list stays empty; malformed HTTP MCP URLs are preserved until tool use rather than rejected at session creation. | `tests/smithery_mcp_passthrough.rs:291-383`, `tests/smithery_mcp_passthrough.rs:386-430`, `tests/smithery_mcp_passthrough.rs:437-455` |
| Choreography tool descriptors | `fireline_choreography` emits `tool_descriptor` rows for at least `fireline_spawn_agent`, `fireline_spawn_agent_batch`, and `fireline_wait_for`; descriptor values carry only the Anthropic triple and no transport or credential fields. | `tests/managed_agent_tools.rs:38-61`, `tests/managed_agent_tools.rs:96-204` |
| Attach-tool descriptors | `attach_tool` emits exactly one `tool_descriptor` per configured capability, with the key prefix `attach_tool:`. The agent-visible value is transport-agnostic; `transportRef` and `credentialRef` never appear in the descriptor value. Name collisions are first attach wins. | `tests/managed_agent_tools.rs:214-246`, `tests/managed_agent_tools.rs:295-405`, `tests/managed_agent_tools.rs:410-525` |
| Telegram approval subscriber | Component name is `telegram`; scope `tool_calls` matches permission requests, sends an approval prompt, accepts `approve` / `deny` callbacks, writes legacy `permission` completion rows, and preserves trace context. | `crates/fireline-channels/src/telegram_subscriber.rs:24-29`, `crates/fireline-channels/src/telegram_subscriber.rs:686-699`, `crates/fireline-channels/src/telegram_subscriber.rs:818-850`, `crates/fireline-channels/tests/telegram_subscriber.rs:269-324` |
| Webhook subscriber | Component name is `webhook_subscriber`; completion kind is `webhook_delivered`; current row targets include `fireline.webhook.delivered`, cursor, and dead-letter rows. Delivery propagates W3C trace headers and mirrors trace lineage into payload `_meta`. | `crates/fireline-channels/src/webhook_subscriber.rs:20-24`, `crates/fireline-channels/src/webhook_subscriber.rs:430-470`, `crates/fireline-channels/src/webhook_subscriber.rs:702-714`, `crates/fireline-channels/src/webhook_subscriber.rs:1384-1537` |

Webhook and Telegram subscribers use durable cursor, completion, and dead-letter
gates to avoid duplicate external sends and to suppress replay side effects.
Replay may rebuild observation state, but external sends are gated by durable
cursor/completion/dead-letter state. [executable contract:
`crates/fireline-channels/src/webhook_subscriber.rs:1265`;
`crates/fireline-channels/src/webhook_subscriber.rs:1335`;
`crates/fireline-channels/src/webhook_subscriber.rs:1536`;
`crates/fireline-channels/src/webhook_subscriber.rs:1592`;
`crates/fireline-channels/tests/telegram_subscriber_hardening.rs:221`;
`crates/fireline-channels/tests/telegram_subscriber_hardening.rs:306`;
`crates/fireline-channels/tests/telegram_subscriber_hardening.rs:396`]

## §8 Conformance Level Targets

The RFC conformance ladder is neutral. Fireline's target is to become Level 3
for the maintained local runtime path by the end of the stream-first cleanup and
to keep provider/resource work on a Level 4 track.

| Phase / state | Current target level | Acceptance gate |
|---|---|---|
| Current main after stream-first cleanup, mailbox migration, and client docs cleanup (`c21a44ac`) | Level 0: mostly satisfied for durable row append/read and StreamDB projection. Level 1: partial-to-satisfied for launch/prompt claimed-work paths; remaining claimed-work conformance is profile-specific rather than mailbox-blocked. Level 2: mostly satisfied for maintained SessionProxy prompt paths after legacy direct-ACP bypass deletion. Level 3: partial, because approvals/required actions still carry Fireline-specific operator semantics and compatibility references. Level 4: partial for provider/sandbox resources. | Client docs now lead with `@fireline/client` scoped resource helpers and explicit operator capability (`packages/client/README.md`, `packages/client/COOKBOOK.md`); active mailbox class/tool SDDs are retired from `vault/sdds/active`; mailbox is treated as state insert plus `state.changes(...).onInsert` durable wait pattern, not a default class or tool surface. |
| Cleanup Phase 2 | Complete Level 2 for local hosted child-session and sandbox promptable paths. | Spawn, spawn-all, and sandbox execute paths use provider-native execution or `PromptSessionClient`-style session ownership; chunk and terminal prompt rows preserve client-authored request ids (`vault/fireline-vnext/sdds/active/runtime-stream-first-cleanup-2026-04-30.md:224-259`). |
| Cleanup Phase 3 | Complete Level 1 claimed-work semantics across launch and prompt; prepare Level 3 claimed choreographies. | Replay never appends claims or executes work; live eligible unclaimed work appends one claim; owned winning claims execute once; known-dead owners invoke domain policy; duplicate-idempotent launch and prompt dispatch stay green (`vault/fireline-vnext/sdds/active/runtime-stream-first-cleanup-2026-04-30.md:261-287`). |
| Cleanup Phase 4 | Finish Level 0/2 cleanliness and unblock Level 3 permission conformance. | Side caches are either lifecycle-only/test-only or backed by explicit rows; client docs remain stream-first; permission timestamp convention and state schema have documented resolution (`vault/fireline-vnext/sdds/active/runtime-stream-first-cleanup-2026-04-30.md:301-323`). |
| Permission-state migration | Complete Level 3 required-action conformance for Fireline approvals. | Permission projection exposes `pending | approved | denied | timed_out | cancelled | failed`; writers emit direct terminal states and target `perm:` ids; legacy `resolved + outcome` is accepted only as compatibility input. |
| Provider/resource follow-up | Level 4 for each provider that passes reattach/execute conformance. | Provider lifecycle/readiness stays provider-owned; provider execute has provider-native tests or an explicitly declared adapter policy; adapter reattach profile from §6 is tested for each provider (`vault/fireline-vnext/sdds/active/runtime-stream-first-cleanup-2026-04-30.md:453-456`, `vault/explorations/agent7-provider-sandbox-transport-boundary-2026-04-30.md:27-45`). |

Every phase also inherits the cleanup SDD boundary gates: hello-effect cold
start, duplicate-idempotent restart, no prompt rows against dead unowned
sessions, ready launch rows only after live `SessionProxy` ownership, replay
side-effect suppression, prompt request-id preservation, live-only prompt
updates, and cleanup guard enforcement
(`vault/fireline-vnext/sdds/active/runtime-stream-first-cleanup-2026-04-30.md:441-458`).

## §9 Out of Scope

- This profile does not redefine the neutral RFC vocabulary or conformance test
  ladder.
- This profile does not migrate existing code. It records the Fireline contract
  and the current-main gaps that implementation PRs must close.
- This profile does not bless new mailbox classes or coordination layers.
  Mailbox remains a pattern over state writes and durable waits, handled by its
  own migration SDD.
- This profile does not decide whether `@fireline/client` later needs separate
  browser/server export conditions. The TypeScript package surface remains one
  app-facing package.
- This profile does not make `fireline.timer.scheduled` or
  `fireline.timer.fired` current canonical names. Current main uses the
  `wake_timer` entity with inner kinds; any rename requires a migration.
- This profile does not decide whether provider-native execute should preserve
  one-shot semantics or share long-lived session semantics. It only requires
  each provider to declare and test a reattach/execute policy.

## Fireline Executable Evidence From Neutral Requirements

These executable references were moved out of neutral RFC pages so the RFC remains implementation-agnostic. They are Fireline profile evidence for the corresponding neutral requirements.

| Neutral page | Fireline executable evidence |
| --- | --- |
| `coding/conductor-middleware.md` | `tests/managed_agent_tools.rs:248`; `tests/fireline_tool_component_guard.rs:9` |
| `coding/providers-resources-sandboxes.md` | `tests/resource_managed_agent_resources.rs:52`; `tests/managed_agent_primitives_suite.rs:197` |
| `concepts/managed-agent-primitives.md` | `tests/managed_agent_tools.rs:63`; `tests/managed_agent_tools.rs:248`; `crates/fireline-substrate/src/tools/descriptors.rs:239`; `crates/fireline-substrate/src/tools/descriptors.rs:375` |
| `concepts/managed-agent-primitives.md` | `tests/managed_agent_tools.rs:432`; `crates/fireline-substrate/src/tools/publish.rs:228` |
| `internals/durable-state-awaitables-approvals-timers.md` | `crates/fireline-substrate/tests/promises_awakeable_race.rs:16`; `crates/fireline-substrate/tests/promises_awakeable_race.rs:84` |
| `internals/durable-state-awaitables-approvals-timers.md` | `crates/fireline-substrate/tests/promises_ds_dp_composite.rs:29`; `crates/fireline-substrate/tests/promises_ds_dp_composite.rs:145`; `crates/fireline-substrate/tests/promises_ds_dp_composite.rs:316`; `crates/fireline-substrate/tests/promises_ds_dp_composite.rs:402` |
| `internals/durable-state-awaitables-approvals-timers.md` | `tests/observability_agent_plane.rs:621`; `tests/observability_agent_plane.rs:670` |
| `internals/durable-state-awaitables-approvals-timers.md` | `tests/observability_agent_plane.rs:372-419`; `tests/support/managed_agent_suite.rs:1548-1562` |
| `internals/durable-state-awaitables-approvals-timers.md` | `tests/observability_agent_plane.rs:528` |
| `internals/durable-state-awaitables-approvals-timers.md` | `crates/fireline-substrate/tests/promises_wake_timer_cancel.rs:16`; `crates/fireline-substrate/tests/promises_wake_timer_cancel.rs:67` |
| `internals/durable-state-awaitables-approvals-timers.md` | `crates/fireline-substrate/tests/promises_awakeable_timeout.rs:132`; `crates/fireline-substrate/tests/promises_awakeable_timeout.rs:256` |
| `internals/projections-and-channels.md` | `packages/client/test/shape-e.test.ts:142`; `packages/client/test/shape-e.test.ts:313`; `packages/client/test/shape-e.test.ts:350`; `packages/client/test/shape-e.test.ts:395` |
| `internals/session-prompt-adapters.md` | `crates/fireline-substrate/src/promises/state_projector.rs:920` |
| `internals/session-prompt-adapters.md` | `tests/observability_agent_plane.rs:621`; `tests/acp_failure_paths.rs:596` |
| `internals/session-prompt-adapters.md` | `tests/acp_failure_paths.rs:315` |
| `internals/session-prompt-adapters.md` | `tests/acp_stdio_roundtrip.rs:143` |
| `reference/idempotency.md` | `packages/state/test/state-protocol.test.ts:5`; `packages/substrate/test/launch-fold-protection.test.ts:71` |
