# Fireline Choreography And Combinators Profile

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

## §7 Choreography Tool Schemas

Fireline's current agent-facing tool names are Fireline-prefixed MCP tools:
`fireline_wait_for`, `fireline_sleep`, `fireline_spawn_agent`,
`fireline_spawn_agent_batch`, `fireline_schedule_self`, and `fireline_execute`
(`crates/fireline-channels/src/choreography/mod.rs:474-510`). The canonical
primitive names in product prose are `wait_for`, `sleep`, `spawn`, `spawn_all`,
`schedule_me`, and `execute`.

All current tools append `fireline.agent.suspended` and return a
`SuspensionSentinel { suspended, awakeableId, verb }` to the caller. The
`SuspensionCoordinator` later appends `fireline.agent.resumed` with the operation
result (`crates/fireline-channels/src/choreography/types.rs:598-604`,
`crates/fireline-channels/src/choreography/coordinator.rs:32-40`).

| Primitive | TypeScript input / output | Rust input / output | Record types emitted | Evidence |
|---|---|---|---|---|
| `sleep(durationMs)` | `SleepInput = { durationMs: number; reason?: string }`; immediate output is `SuspensionSentinel`; resumed result is a `WaitOutcome` for a time-elapsed channel. | `SleepInput { duration_ms: u64, reason: Option<String> } -> SuspensionSentinel`, later `AgentResumed.result: WaitOutcome`. | `fireline.agent.suspended` with `operation: wait_for`, `channel: time.elapsed`; later `fireline.agent.resumed`. Wake-timer internals may use `wake_timer` for prompt-bound timers, not public `fireline.timer.*`. | TS schema: `packages/client/src/protocol/envelopes/verb/v1/sleep.ts:4-9`; Rust schema: `crates/fireline-channels/src/choreography/types.rs:551-557`; tool lowering: `crates/fireline-channels/src/choreography/mod.rs:603-632`. |
| `wait_for(trigger, timeoutMs?)` | `WaitForInput = { channel: ChannelTarget; timeoutMs?: number; matchJson?: string }`; output `WaitOutcome = { matched: boolean; eventJson?: string; reason?: "timeout" | "cancelled" | "channelRemoved" }`. `matchJson` and `eventJson` are string-encoded JSON in the TS schema. | `WaitForInput { channel: ChannelTarget, timeout_ms: Option<u64>, match: Option<Value> } -> SuspensionSentinel`, later `WaitOutcome { event: Option<Value> }`. Rust carries parsed JSON `Value` for match/event. | `fireline.agent.suspended` with `operation: wait_for`; later `fireline.agent.resumed`. | TS schema: `packages/client/src/protocol/envelopes/verb/v1/wait_for.ts:35-51`; Rust schema: `crates/fireline-channels/src/choreography/types.rs:132-148`, `crates/fireline-channels/src/choreography/types.rs:511-519`; tool lowering: `crates/fireline-channels/src/choreography/mod.rs:562-598`. |
| `spawn(agent, prompt)` | `SpawnInput = { agent: string; prompt: string; opts?: { sessionKey?: string; timeoutMs?: number; sandboxes: StringMap } }`; `opts` is optional, but when present TS requires `sandboxes`. Final `SpawnResult = { sessionId, finalText, stopReason }`. | `SpawnInput { agent: String, prompt: String, opts: SpawnOptions } -> SuspensionSentinel`, later `SpawnResult`. Rust `SpawnOptions.sandboxes` is optional. | `fireline.agent.suspended` with `operation: spawn`; child path emits session/prompt/chunk rows through the child session implementation; later `fireline.agent.resumed`. | TS schema: `packages/client/src/protocol/envelopes/verb/v1/spawn.ts:4-14`; Rust schema: `crates/fireline-channels/src/choreography/types.rs:160-186`, `crates/fireline-channels/src/choreography/types.rs:559-566`; lowering: `crates/fireline-channels/src/choreography/mod.rs:667-688`. |
| `spawn_all(tasks)` | `SpawnAllInput = { tasks: Array<{ agent; prompt; opts... }> }`; final output is `SpawnResult[]`. | `SpawnAllInput { tasks: Vec<SpawnTask> } -> SuspensionSentinel`, later `Vec<SpawnResult>`. | `fireline.agent.suspended` with `operation: spawn_all`; child sessions emit their own session/prompt/chunk rows; later `fireline.agent.resumed`. | TS schema: `packages/client/src/protocol/envelopes/verb/v1/spawn.ts:16-30`; Rust schema: `crates/fireline-channels/src/choreography/types.rs:171-186`, `crates/fireline-channels/src/choreography/types.rs:568-572`; lowering: `crates/fireline-channels/src/choreography/mod.rs:692-710`. |
| `schedule_me(when, prompt)` | `ScheduleMeInput = { when: string; prompt: string }`; resumed result is `{ scheduledAt: string }`. | `ScheduleMeInput { when: String, prompt: String } -> SuspensionSentinel`, later `AgentResumed.result = { scheduledAt }`. | `fireline.agent.suspended` with `operation: schedule`, `channel: session.prompt`; later `fireline.agent.resumed`. | TS schema: `packages/client/src/protocol/envelopes/verb/v1/schedule.ts:11-14`; Rust schema: `crates/fireline-channels/src/choreography/types.rs:574-579`; lowering/result: `crates/fireline-channels/src/choreography/mod.rs:715-739`, `crates/fireline-channels/src/choreography/coordinator.rs:68-79`. |
| `execute(sandbox, input)` | Fireline MCP input is `{ sandbox: string; input: string }`; result shape is `ExecuteResult = { output: string; exitCode?: number; provisioned?: boolean }`. | `ExecuteInput { sandbox: String, input: String } -> SuspensionSentinel`, later `ExecuteResult`. | `fireline.agent.suspended` with `operation: call`, `channel: sandbox`; later `fireline.agent.resumed`. Provider-native execution may produce provider/resource rows; detailed legacy execute-path migration remains owned by the cleanup SDD. | Rust schemas: `crates/fireline-channels/src/choreography/types.rs:150-158`, `crates/fireline-channels/src/choreography/types.rs:581-586`; tool lowering: `crates/fireline-channels/src/choreography/mod.rs:637-662`; provider execute boundary: `crates/fireline-runtime/src/sandbox/provider_model.rs:10-33`. |
