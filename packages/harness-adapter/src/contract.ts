/**
 * The harness adapter contract (managed-sessions SDD §MS-C6, WP D2) — the
 * reconstruction-model seam between one agent harness and the L1 observation
 * vocabulary (interface I2). Architect-approved surface (PR #99); these types are
 * the ratified contract other lanes/harnesses implement against.
 *
 * The adapter is *an effectful handler that writes only under the Processor's
 * fence, emitting L1 facts as its Append* (see
 * `docs/canon/architecture/fluent/authority-and-actors.md`). It owns **no**
 * authority, durability, wait/timer/child semantics, or projection schema. It
 * consumes `@firegrid/l1-vocabulary` — it lowers harness traffic into
 * `L1StreamRecord`s and never mints a parallel vocabulary.
 *
 * TS zone, Effect shapes per `LLMS.md`. The pure `lower` core is Effect-free;
 * Effect appears only in the I/O shell (`drive`) and the kernel-provided seams.
 */

import type { L1StreamRecord } from "@firegrid/l1-vocabulary"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import type * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"

// ── Declared interception posture (the durability guarantee differs) ──────
// gateable    — can mediate Firegrid durable tools (transactional, replay-served).
// observe-only — cannot mediate any; harness-native effects are only suppressed.
export type InterceptionCapability = "gateable" | "observe-only"

export interface HarnessCapabilities {
  readonly harness: string // stable harness id (matches firegrid/native `harness`)
  readonly interception: InterceptionCapability
  readonly emitsUsage: boolean // lowers firegrid/usage token/cost facts
  readonly emitsSubagents: boolean // lowers subagent scoping (firegrid/subagent + parent tool content)
}

// ── Opaque harness resume state — produced here, stored fenced by B4 (MS-C5) ─
// D2 owns this TS-side shape; B4's F# store is generic over an opaque serialized
// payload plus `(harness, version)` metadata and must not redefine the artifact.
export interface NativeResumeArtifact {
  readonly harness: string
  readonly version: number // harness-owned artifact version
  readonly payload: unknown // opaque; e.g. Claude session id + cursor
}

// ── Pure protocol → L1 lowering (sans-IO; deterministic; Effect-free) ─────
// A fold from recorded harness protocol events to L1 records: no I/O, no clock,
// no entropy. Replaying an event log reproduces identical L1 facts — the
// `harness.fixture-replay` target. Per-harness; D3 supplies Claude's, including
// parent_tool_use_id scoping and usage/cost lowering.
export interface HarnessLowering<Event, State> {
  readonly initial: State
  readonly lower: (
    state: State,
    event: Event
  ) => { readonly state: State; readonly records: ReadonlyArray<L1StreamRecord> }
}

// ── Kernel-provided seams the shell drives against ────────────────────────
// Fenced L1 append. `emit` is the adapter's only write; the kernel appends under
// the turn's fence (I1 DurableLog). A deposed/sealed turn surfaces here and the
// adapter must stop — the reconstruction analog of DurableLog.AppendError.
export class L1Sink extends Context.Service<L1Sink, {
  readonly emit: (record: L1StreamRecord) => Effect.Effect<void, EmitError>
}>()("@firegrid/harness-adapter/L1Sink") {}

// Durable-tool mediation. Provided at a *gateable* adapter's layer construction
// (a dependency of its `Layer`), never in `drive`'s R — so an observe-only
// adapter, whose layer has no `ToolGate`, literally cannot obtain one. L2
// authority is the runtime's (I5), not this contract's.
export class ToolGate extends Context.Service<ToolGate, {
  readonly mediate: (call: MediatedToolCall) => Effect.Effect<CommittedToolResult, GateError>
}>()("@firegrid/harness-adapter/ToolGate") {}

export interface MediatedToolCall {
  readonly toolCallId: string // the L1 tool_call this gates
  readonly name: string
  readonly rawInput: unknown
}
export interface CommittedToolResult {
  readonly toolCallId: string
  readonly output: unknown // durable L2 result fed back to the harness
}

// ── The adapter service (I/O shell) ───────────────────────────────────────
export interface ResumePoint {
  readonly artifact: NativeResumeArtifact // prior harness resume state
  // EXCLUSIVE upper bound (house convention — same boundary class as C1 R3 and
  // D1's fold): the I1 Version below which L1 facts are already durable. Resume
  // emits the suffix FROM `observedThrough` onward (facts at Version >=
  // observedThrough); the fact AT observedThrough is the first not-yet-durable one.
  readonly observedThrough: number
}
export interface DriveInput {
  // The user turn: a NON-EMPTY sequence of `user_message_chunk`s that share one
  // `messageId`. Plural because L1ChunkBody.content is a single content block per
  // chunk (ACP), so a multi-block prompt (text + image + resource) needs several
  // records — a singular record cannot express it.
  readonly prompt: readonly [L1StreamRecord, ...ReadonlyArray<L1StreamRecord>]
  readonly resume?: ResumePoint // present iff continuing an existing turn/session
}
export type L1Terminal =
  | { readonly _tag: "completed" }
  | { readonly _tag: "cancelled" } // a cancel was observed and the turn ended
  | { readonly _tag: "failed"; readonly reason: string }
// This contract fixes only the terminal *representation*. How a cancel reaches a
// running `drive` is B3's MS-C5 surface to define (cancel is a mailbox send, per
// canon) — deliberately not pinned here.
export interface DriveOutcome {
  readonly artifact: NativeResumeArtifact // updated resume state for the kernel to persist (B4-fenced)
  readonly terminal: L1Terminal
}

export class HarnessAdapter extends Context.Service<HarnessAdapter, {
  readonly capabilities: HarnessCapabilities
  // Drive one turn to terminal: lower harness traffic to L1 via `L1Sink.emit` and
  // produce the updated resume artifact. `ToolGate` is deliberately NOT in
  // `drive`'s R — a gateable adapter closes over it at layer construction; an
  // observe-only adapter's layer never depends on it, so it *cannot* mediate. The
  // harness process is a scoped resource.
  readonly drive: (
    input: DriveInput
  ) => Effect.Effect<DriveOutcome, DriveError, L1Sink | Scope.Scope>
}>()("@firegrid/harness-adapter/HarnessAdapter") {}

// ── Typed errors ──────────────────────────────────────────────────────────
export class DriveError extends Data.TaggedError("DriveError")<{
  readonly harness: string
  readonly message: string
  readonly cause?: unknown
}> {}
export class EmitError extends Data.TaggedError("EmitError")<{
  readonly kind: "deposed" | "sealed" | "failed"
  readonly message: string
}> {}
export class GateError extends Data.TaggedError("GateError")<{
  readonly toolCallId: string
  readonly message: string
}> {}
