# Fireline Record Model Profile

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

> Fireline-specific companion to the neutral Stream-First Agent Substrate RFC. This content was relocated from the Fireline Conformance Profile without changing the implementation contract.
>
> **Frozen / historical (Rust Fireline).** This profile describes the legacy
> Rust `crates/fireline-*` implementation, not the current `fluent-firegrid`
> system; it is retained as reference archaeology. Current implementation
> profiles use the `.fluent.md` suffix — see
> [Implementation Profiles](../README.md#implementation-profiles).

## §2 Canonical Record Names

Fireline's current canonical row names are the names in the StreamDB schema and
protocol envelope metadata. The names below intentionally correct RFC-style or
sketch names such as `fireline.launch.requested`,
`fireline.launch.claimed`, `fireline.prompt.claimed`,
`fireline.prompt.completed`, and `fireline.prompt.failed`: those names are not
present on current main.

| RFC intent | Fireline canonical record type | Payload / row schema | Evidence | Notes |
|---|---|---|---|---|
| Launch request intent | `fireline.launch.request` | `LaunchRequest` | `packages/client/src/protocol/envelopes/launch/v1/messages.ts:23-27`, `packages/client/src/schema.ts:108-110`, `crates/fireline-runtime/src/launch/wire.rs:14` | Not `fireline.launch.requested`. |
| Launch materialized state | `fireline.launch` | `LaunchState` | `packages/client/src/protocol/envelopes/launch/v1/messages.ts:28`, `packages/client/src/protocol/envelopes/launch/v1/messages.ts:105-117`, `packages/client/src/schema.ts:108` | State field carries `accepted`, `running`, `sessionReady`, `stopping`, `failed`, `stopped`. |
| Launch claim | `fireline.launch.claim` | `LaunchClaim` | `packages/client/src/protocol/envelopes/launch/v1/messages.ts:34-38`, `packages/client/src/protocol/envelopes/launch/v1/messages.ts:119-126`, `packages/client/src/schema.ts:110`, `crates/fireline-runtime/src/launch/wire.rs:16` | Not `fireline.launch.claimed`. |
| Launch provisioning progress | `fireline.launch.provision` | `LaunchProvision` | `packages/client/src/protocol/envelopes/launch/v1/messages.ts:39-43`, `packages/client/src/protocol/envelopes/launch/v1/messages.ts:128-138`, `packages/client/src/schema.ts:111` | Substrate-internal launch worker fact. |
| Launch stop intent | `fireline.launch.stop` | `LaunchStop` | `packages/client/src/schema.ts:112`, `crates/fireline-runtime/src/launch/wire.rs:18` | Client stop appends this row family rather than mutating session state directly. |
| Launch terminal/result fact | `fireline.launch.result` | `LaunchResult` | `packages/client/src/protocol/envelopes/launch/v1/messages.ts:29-33`, `packages/client/src/protocol/envelopes/launch/v1/messages.ts:140-148`, `packages/client/src/schema.ts:113` | Carries accepted/running/failed/stopped result. |
| Runtime instance | `fireline.runtime.instance` | `RuntimeInstance` | `packages/client/src/protocol/envelopes/launch/v1/messages.ts:44-48`, `packages/client/src/protocol/envelopes/launch/v1/messages.ts:150-160`, `packages/client/src/schema.ts:114` | `fireline.runtime.*` in product prose currently means this row plus runtime-internal facts such as termination. |
| Runtime terminated signal | `fireline.runtime.terminated` | runtime-internal termination fact | `crates/fireline-runtime/src/launch/prompt_dispatcher.rs:33-35` | Current main has a dispatcher constant, not a protocol envelope schema. |
| Sandbox lifecycle evidence | `fireline.sandbox.lifecycle` | `SandboxLifecycleRow` target contract | `crates/fireline-runtime/src/sandbox/provider_dispatcher.rs:98-102`, `crates/fireline-runtime/src/sandbox/provider_dispatcher.rs:130-152`, `crates/fireline-runtime/src/sandbox_channel.rs:70-81`, `crates/fireline-runtime/src/sandbox_channel.rs:272-355`, `crates/fireline-runtime/src/sandbox_channel.rs:393-408`, `crates/fireline-runtime/src/sandbox_channel.rs:534-541` | Fireline-specific target row family for provider-owned sandbox live-resource lifecycle evidence. See `rfc/coding/providers-resources-sandboxes.fireline.md`. |
| Session row | `fireline.session.v2` | `SessionV2` | `packages/client/src/protocol/envelopes/session/v1/messages.ts:17`, `packages/client/src/protocol/envelopes/session/v1/messages.ts:29-36`, `packages/client/src/schema.ts:83-85` | Canonical session projection row. |
| Prompt request, claim fields, terminal prompt state | `fireline.prompt.request` | `PromptRequest` | `packages/client/src/protocol/envelopes/session/v1/messages.ts:18-22`, `packages/client/src/protocol/envelopes/session/v1/messages.ts:38-56`, `packages/client/src/schema.ts:86`, `crates/fireline-runtime/src/launch/prompt_dispatcher.rs:33-40` | There is no separate `fireline.prompt.claimed`, `.completed`, or `.failed` row on current main. `dispatchAttemptId`, `dispatchedByRuntimeId`, `dispatchedAtMs`, `terminalAtMs`, `state`, and `error` live on this row. |
| Session update chunk | `fireline.chunk.v2` | `ChunkV2` | `packages/client/src/protocol/envelopes/session/v1/messages.ts:23-27`, `packages/client/src/protocol/envelopes/session/v1/messages.ts:58-65`, `packages/client/src/schema.ts:85`, `crates/fireline-runtime/src/launch/session_proxy.rs:19` | Chunk payload imports ACP `zSessionUpdate`. |
| Permission read row | `fireline.permission` | `PermissionRow` | `packages/client/src/protocol/envelopes/permission/v1/messages.ts:78-108`, `packages/client/src/schema.ts:87`, `vault/explorations/agent3-conductor-side-cache-projection-audit-2026-04-30.md:48-49` | This is the StreamDB row family. Writers use direct terminal states and target `perm:` ids; legacy `resolved + outcome` rows are compatibility input. Current protocol metadata also contains request/resolution envelope names below. |
| Permission request fact metadata | `fireline.permission.request` | `PermissionRequest` | `packages/client/src/protocol/envelopes/permission/v1/messages.ts:16-20`, `packages/client/src/protocol/envelopes/permission/v1/messages.ts:27-37` | Q1 decided the row family should be one `fireline.permission` row, not separate StreamDB row types. |
| Permission resolution fact metadata | `fireline.permission.resolved` | `ApprovalResolved` | `packages/client/src/protocol/envelopes/permission/v1/messages.ts:21-25`, `packages/client/src/protocol/envelopes/permission/v1/messages.ts:67-76` | Kept as protocol metadata; current row folding uses `fireline.permission`. |
| Agent suspended | `fireline.agent.suspended` | `AgentSuspended` | `packages/client/src/protocol/envelopes/session/v1/suspend_resume.ts:5-10`, `packages/client/src/protocol/envelopes/session/v1/suspend_resume.ts:28-43`, `crates/fireline-channels/src/choreography/types.rs:12-15` | Choreography tools append this as the durable suspension record. |
| Agent resumed | `fireline.agent.resumed` | `AgentResumed` | `packages/client/src/protocol/envelopes/session/v1/suspend_resume.ts:11-16`, `packages/client/src/protocol/envelopes/session/v1/suspend_resume.ts:45-52`, `crates/fireline-channels/src/choreography/coordinator.rs:106-140` | Choreography completion record. |
| Timer requested / fired | `wake_timer` entity with `kind: "wake_timer_requested"` / `kind: "timer_fired"` | `WakeTimerRequest`, `TimerFired` | `crates/fireline-substrate/src/promises/wake_timer.rs:17-21`, `crates/fireline-substrate/src/promises/wake_timer.rs:48-67`, `crates/fireline-substrate/src/promises/wake_timer.rs:126-145`, `crates/fireline-substrate/src/promises/wake_timer.rs:521-545` | Current main does not define `fireline.timer.scheduled` or `fireline.timer.fired`. If Fireline wants those names, that is a vnext rename/migration, not the current contract. |
| Awakeable waiting / resolved / rejected | internal promise envelope kinds `awakeable_waiting`, `awakeable_resolved`, `awakeable_rejected` | `AwakeableWaiting`, `AwakeableResolved`, `AwakeableRejected` | `crates/fireline-substrate/src/promises/awakeable.rs:17-22`, `crates/fireline-substrate/src/promises/awakeable.rs:29-40`, `crates/fireline-substrate/src/promises/awakeable.rs:88-130`, `crates/fireline-substrate/src/promises/awakeable.rs:178-190` | Promise substrate internals, not public StreamDB rows today. |
| Resource row | `fireline.resource` | `ResourceRow` | `packages/client/src/schema.ts:115`, `vault/explorations/agent7-provider-sandbox-transport-boundary-2026-04-30.md:70-78` | Provider/resource payloads stay Fireline-owned. |
| Filesystem operation row | `fireline.fs.op` | `FsOpRow` | `packages/client/src/schema.ts:116` | Runtime/substrate row family. |
| Runtime stream file row | `fireline.runtime.stream_file` | `RuntimeStreamFileRow` | `packages/client/src/schema.ts:117-121` | Runtime file projection row family. |
| Telegram approval callback | `fireline.telegram.approval.callback` | `TelegramApprovalCallback` | `packages/client/src/schema.ts:90-94` | Channel-specific row family. |
| Webhook delivered / cursor / dead-letter | `fireline.webhook.delivered`, `fireline.webhook.cursor`, `fireline.webhook.dead_letter` | webhook rows | `packages/client/src/schema.ts:95-101` | Channel-specific row families. |
| Telegram cursor / dead-letter | `fireline.telegram.cursor`, `fireline.telegram.dead_letter` | telegram rows | `packages/client/src/schema.ts:102-107` | Channel-specific row families. |

Q1 also records the naming principle: StreamDB rows are the canonical schemas,
timestamps use numeric `...Ms`, identifiers use `...Id`, and permission should be
one row schema rather than separate request/resolution rows
(`vault/explorations/phase-2-wire-name-canonicalization-decision-2026-04-28.md:17-45`,
`vault/explorations/phase-2-wire-name-canonicalization-decision-2026-04-28.md:230-250`).

### Client Collection Registration

`firelineClientState` is Fireline's single StreamDB collection schema. Each
physical row type is registered once with a stable `primaryKey`, and
runtime/session/channel rows materialize from one private client StreamDB handle
rather than multiple per-domain StreamDB instances. [evidence:
`packages/client/src/schema.ts:82-109`; boundary contract:
`packages/client/test/package-boundary.test.ts:42-48`]

### Prompt Row Compatibility

Rust-shaped prompt events may use composite event keys for State Protocol
transport compatibility, but the materialized prompt row's `requestId` remains
the plain canonical request id. Consumers must key prompt request semantics by
the row `requestId`, not by any composite event key used during ingestion.
[executable contract: `packages/client/test/shape-e.test.ts`]

### Test-Enforced Compatibility Names

The implementation tests also enforce several Fireline-specific envelope names
that are not RFC concepts. These names remain part of the current Fireline
profile until the owning migration explicitly removes or renames them.

| Test-enforced name | Meaning | Evidence | Profile status |
|---|---|---|---|
| `tool_descriptor` | Durable state envelope for agent-visible tool descriptors. Value is exactly `{ name, description, inputSchema }`; `headers.operation` is `insert`. | `crates/fireline-substrate/src/tools/descriptors.rs:28-40`, `crates/fireline-substrate/src/tools/descriptors.rs:165-190`, `tests/managed_agent_tools.rs:118-151`, `tests/managed_agent_tools.rs:345-377` | Canonical Fireline descriptor projection. |
| `fireline_choreography:<toolName>` | Provenance key prefix for descriptors emitted by the default choreography component. | `tests/managed_agent_tools.rs:103-135`, `tests/managed_agent_tools.rs:191-204`, `tests/managed_agent_primitives_suite.rs:321-422` | Canonical key prefix. |
| `attach_tool:<toolName>` | Provenance key prefix for descriptors emitted by the `attach_tool` component. Same-name collisions are first attach wins. | `crates/fireline-substrate/src/tools/surface.rs:40-43`, `crates/fireline-substrate/src/tools/surface.rs:120-144`, `tests/managed_agent_tools.rs:295-315`, `tests/managed_agent_tools.rs:335-361`, `tests/managed_agent_tools.rs:410-525` | Canonical key prefix. |
| `runtime_instance` | Legacy hosted-runtime test-visible row name. | `tests/hosted_runtime.rs:299-310` | Compatibility name. The protocol/schema target remains `fireline.runtime.instance` in the canonical row table above. |
| `permission` | Legacy subscriber permission envelope name used by Telegram and durable-subscriber compatibility tests. | `crates/fireline-channels/src/telegram_subscriber.rs:818-850`, `crates/fireline-channels/src/telegram_subscriber.rs:1291-1294`, `crates/fireline-channels/tests/telegram_subscriber.rs:293-324`, `crates/fireline-channels/tests/telegram_subscriber_hardening.rs:531-550` | Compatibility name. The StreamDB row target remains `fireline.permission`. |

`webhook_delivery` was a legacy durable-subscriber fixture name. No surviving
code or test path emits it; the current webhook subscriber writes
`fireline.webhook.delivered` (`crates/fireline-channels/src/webhook_subscriber.rs:20-23`,
`crates/fireline-channels/src/webhook_subscriber.rs:702-714`).

### Field Spelling Profile

The tests-as-canon pass makes these Fireline spellings explicit:

| Field / spelling | Applies to | Evidence | Notes |
|---|---|---|---|
| `createdAtMs` | Permission rows and other current Effect-schema rows. | `packages/client/src/protocol/envelopes/permission/v1/messages.ts:89`, `packages/client/src/protocol/envelopes/session/v1/messages.ts:33`, `packages/client/src/protocol/envelopes/launch/v1/messages.ts:158`, `tests/observability_agent_plane.rs:387-415`, `tests/support/managed_agent_suite.rs:1617-1626` | Public JSON uses camelCase `...Ms`. Rust structs may use `created_at_ms` internally under serde camelCase (`crates/fireline-substrate/src/middleware/governance/approval.rs:633-651`). |
| `resolvedAtMs` | Permission resolution rows. | `packages/client/src/protocol/envelopes/permission/v1/messages.ts:90`, `tests/observability_agent_plane.rs:387-415`, `tests/support/managed_agent_suite.rs:1617-1626` | Do not emit `resolvedAt`, `resolved_at`, or string timestamps on rows. |
| `outcome` | Current permission resolution compatibility shape. | `packages/client/src/protocol/envelopes/permission/v1/messages.ts:86`, `crates/fireline-substrate/src/middleware/governance/approval.rs:633-651`, `tests/observability_agent_plane.rs:387-415`, `tests/managed_agent_harness.rs:489-512` | Current compatibility values are `approved` / `denied`; §4 target folds terminal state into `state`. |
| `headers.operation` | State envelopes, including permission and tool descriptors. | `tests/observability_agent_plane.rs:377-416`, `crates/fireline-substrate/src/tools/descriptors.rs:36-40`, `crates/fireline-substrate/src/tools/descriptors.rs:179-185`, `tests/managed_agent_tools.rs:124-129`, `tests/managed_agent_tools.rs:351-355` | Tool descriptors and approval resolutions use `insert` / `update` according to row family semantics. |
