# Fireline Durable State, Awaitables, Approvals, And Timers Profile

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

> Fireline-specific companion to the neutral Stream-First Agent Substrate RFC. This content was relocated from the Fireline Conformance Profile without changing the implementation contract.

## §3 Permission ID Format

### Legacy Compatibility Format

Fireline permission rows use `permissionId` as the durable primary key. Legacy
streams and compatibility tests may still contain the JSON-array subject key
format:

- Prompt fallback approval: `prompt:["${sessionId}","${requestId}"]`
- Tool-call approval: `tool:["${sessionId}","${toolCallId}"]`

Readers retain compatibility with those ids through private
`@fireline/client` required-action materialization. Compatibility rows are
accepted as input, but target `perm:` rows win when both key forms are present
for the same logical subject (`packages/client/src/required-actions.ts:87-123`).

### Current Target Format

New Fireline writers use these subject ids:

- Tool-call approval: `perm:${sessionId}:tool:${toolCallId}`
- Prompt fallback approval: `perm:${sessionId}:prompt:${requestId}`

The `sessionId`, `toolCallId`, and `requestId` segments are UTF-8 strings encoded
with `encodeURIComponent` / percent decoding over RFC 3986 path-segment
characters. Colons inside a segment must be escaped. ACP JSON-RPC request ids that
are not strings are normalized as compact JSON before percent encoding. The
literal prefix `perm:` is version 1. A future incompatible encoding must use
`perm:v2:` rather than changing the parse rules for existing ids.

The Rust approval gate constructs these ids in `ApprovalSubject::stream_key`
(`crates/fireline-substrate/src/middleware/governance/approval.rs:192-225`).
The TS client mirrors the logical-subject comparison in its private
required-action materializer (`packages/client/src/required-actions.ts:87-123`).
Mixed streams must not contain two live pending rows for the same logical
subject; if both forms are observed, the `perm:` row wins and the legacy row is
ignored for new UI actions.

## §4 Permission State Schema

### Chosen target

Fireline should converge on the RFC-aligned logical state machine:

```ts
type PermissionState =
  | "pending"
  | "approved"
  | "denied"
  | "timed_out"
  | "cancelled"
  | "failed"
```

`pending` is Fireline's row-state spelling for the RFC's non-terminal
`requested` state. The terminal state is carried directly in `state`; the
separate `outcome?: "approved" | "denied"` field is removed from the target
logical model. A terminal row may also carry `reason`, `resolvedBy`,
`resolvedAtMs`, and an optional structured `error` payload for `failed`.

This target is chosen over the current row shape because cancel/timeout cannot be
represented unambiguously by `state: "resolved"` plus `outcome:
"approved" | "denied"`. The RFC required-action model explicitly needs terminal
states `approved`, `denied`, `timed_out`, `cancelled`, and `failed`.

### Current state and compatibility path

Current writers emit direct terminal states (`approved`, `denied`,
`timed_out`, `cancelled`, `failed`) on the `fireline.permission` row family.
The protocol schema still accepts legacy `resolved` and `orphaned` states as
compatibility input (`packages/client/src/protocol/envelopes/permission/v1/messages.ts:78-108`).
The client resolves a required action by updating the same permission row to a
direct terminal state without `outcome`
(`packages/client/src/permissions.ts:70-80`). Rust approval writers do the same
for prompt and tool approvals
(`crates/fireline-substrate/src/middleware/governance/approval.rs:748-795`).

Compatibility path:

1. Keep dual-read normalization at read boundaries:
   - `pending` -> `pending`
   - `resolved + outcome: "approved"` -> `approved`
   - `resolved + outcome: "denied"` -> `denied`
   - `orphaned` -> `failed` with `error.code = "orphaned"`
2. Keep new writers on direct terminal states and target `perm:` ids.
3. Remove `outcome` and legacy states from the protocol input schema after the
   compatibility window.
4. Keep `cancelled` double-l, matching ACP stop-reason spelling already used in
   prompt rows (`packages/client/src/protocol/envelopes/session/v1/messages.ts:43-45`).

### Client Row Contract

`@fireline/client` owns the private StreamDB schema for Fireline rows. It
preserves envelope keys during row normalization, rejects retired prompt
lifecycle states through the protocol schema, and keys permission rows by
`permissionId`. Permission rows may carry optional `reason` text for approval or
failure context. [evidence: `packages/client/src/schema.ts:82-109`;
`packages/client/src/required-actions.ts:42-45`;
`packages/client/src/respond.ts:37-51`; executable contract:
`packages/client/test/shape-e.test.ts:343-370`]

## §5 Dead-Owner Policies

| Domain | Policy | Rationale | Evidence / constraints |
|---|---|---|---|
| Prompt claims | Reattach when the adapter has a verified load/resume path; otherwise fail the prompt row with a typed runtime-not-live error. Do not blind-takeover after a claim could have performed an external side effect. | Prompt side effects are user-visible and may have emitted chunks or tool/permission requests. The safe recovery path is live ownership proof or a durable failure row. | Prompt dispatcher evaluates authored claims, live-owner skips, and known-dead owners through `ClaimEvaluation` (`crates/fireline-runtime/src/launch/prompt_dispatcher.rs:307-339`). The shared claim vocabulary exposes `ExecuteOwned`, `HandleDeadOwner`, and `SkipLiveOwner` (`crates/fireline-substrate/src/active_claim/decision.rs:20-29`). SessionProxy ownership is the promptability check (`crates/fireline-runtime/src/launch/session_proxy.rs:134-154`). |
| Approval waits | Wait durably until approved, denied, timed out, cancelled, or failed. Owner death of the waiting process does not approve or deny. | Approval is a human or policy decision. Process death should not create authorization. Timeouts and cancellations are explicit terminal states under §4. | Current permission rows are durable and multi-writer; readers distinguish pending/resolved state on `fireline.permission` (`vault/explorations/agent3-conductor-side-cache-projection-audit-2026-04-30.md:48-49`). |
| Launch claims | Take over only through the shared claimed-work primitive after replay/live guard and owner-death evaluation. If a prior owner already created a live promptable session and reattach is supported, reattach; otherwise reprovision or fail based on provider capability. | Launch is a provisioning workflow with durable claim rows. Unlike prompts, the correct result may be a fresh provision if no live promptable session exists. | Launch claim/provision rows are explicit (`packages/client/src/protocol/envelopes/launch/v1/messages.ts:119-138`). Phase 3 requires the shared claimed-work primitive to own append-claim, observe-claim, evaluate-owner, execute-owned scheduling, replay suppression, and dead-owner handoff while domain processors own policy (`vault/fireline-vnext/sdds/active/runtime-stream-first-cleanup-2026-04-30.md:261-287`). |
| Choreography suspensions | Treat `fireline.agent.suspended` as a durable wait request. Completion is first valid `fireline.agent.resumed` for the awakeable id; execution retries are subscriber policy, not caller-local state. | Choreography tools already lower to suspended/resumed records. The domain policy is carried by the operation payload and channel driver. | `SuspensionCoordinator` matches `agent_suspended`, checks completion through `fireline.agent.resumed`, and appends `AgentResumed` through the durable subscriber interface (`crates/fireline-channels/src/choreography/coordinator.rs:106-140`). |
