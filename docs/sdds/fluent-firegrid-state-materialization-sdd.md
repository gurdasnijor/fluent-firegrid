# SDD: Fluent Firegrid State And Materialization

### Table-shaped durable state for virtual objects

|   |   |
| --- | --- |
| Status | Implemented through A-E; complete for current state/materialization layer |
| Date | 2026-06-25 |
| Package | `@firegrid/fluent` plus S2 object-owner support |
| Design lineage | table-shaped fluent object state over S2 object-owner storage |
| Lower runtime | TanStack Workflow over `@firegrid/fluent/s2` |

---

## Decision

Virtual object state uses the old table/materialization authoring shape, not a
string-slot API.

This SDD is complete for the current layer. Future fluent API work should build
above it; it should not add more proof-only object packages or reopen a parallel
durable-function runtime.

The public authoring target is:

```ts
import { object, objectClient, state } from "@firegrid/fluent"
import { primaryKey, Table } from "@firegrid/fluent/state"
import { Option, Schema } from "effect"

class CounterState extends Table<CounterState>("counterState")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number
}) {}

const counter = object({
  name: "counter",
  handlers: {
    *add(amount: number) {
      const st = state(CounterState)
      const current = yield* st.get("v")
      const value = Option.match(current, {
        onNone: () => 0,
        onSome: (row) => row.value
      })
      yield* st.set({ id: "v", value: value + amount })
      return value + amount
    },
    *value() {
      const st = state(CounterState)
      const current = yield* st.get("v")
      return Option.match(current, {
        onNone: () => 0,
        onSome: (row) => row.value
      })
    }
  }
})

yield* objectClient(binding, counter)("user-1").add(5)
yield* objectClient(binding, counter)("user-1").value()
```

`state(Table)` is synchronous and reusable as a value. Its operations are Effects.
The table schema owns the row codec, table name, and primary key.

## Layering

This layer is split deliberately:

1. `@firegrid/fluent` owns the authoring surface:
   `Table`, `primaryKey`, materialization fold types, `state(Table)`, and an
   abstract handler-context state backend.
2. The S2 object-owner runtime owns persistence:
   one owner stream per `(objectName, key)`, ordered actor events, projection,
   fenced draining, and crash recovery.
3. Transport bindings remain outside fluent core.

Fluent core must not import `@firegrid/log`. The S2 owner stream implements the
backend consumed by `state(Table)`.

## Physical Model

Each virtual object key has an invocation owner stream and a state
materialization stream:

```text
obj/{objectName}/{encodedKey}/invocations
obj/{objectName}/{encodedKey}/state
```

The invocation stream is the ordered source of truth for:

- accepted calls
- call start/completion/error
- journaled state reads
- durable `run` results
- signal/awakeable resolution

The state stream is the latest-value-per-`(table, key)` materialization for table
rows. Cold recovery folds both streams: invocation order and call status from
`/invocations`, object table state from `/state`.

## Actor Events

The durable event vocabulary should include at least:

```ts
type ObjectActorEvent =
  | { _tag: "Accepted"; callId: string; method: string; input: unknown; idempotencyKey?: string }
  | { _tag: "Started"; callId: string; runId: string }
  | { _tag: "Completed"; callId: string; output: unknown }
  | { _tag: "Errored"; callId: string; error: unknown }
  | { _tag: "StateChanged"; callId: string; opId: string; table: string; key: string; value?: unknown; op: "set" | "delete" }
  | { _tag: "StateReadJournaled"; callId: string; readId: string; table: string; key: string; value?: unknown }
  | { _tag: "RunJournaled"; callId: string; step: string; value?: unknown; error?: unknown }
  | { _tag: "SignalResolved"; callId: string; name: string; value: unknown }
```

The exact event names can evolve, but these facts are load-bearing.

## State Semantics

`state.get(table, key)` is replay-sensitive and must be journaled per call. A
read-modify-write sequence must replay the same read value after a crash, even if
the object state has advanced meanwhile.

Algorithm:

1. Derive `readId` from the active call id plus a deterministic state-read
   ordinal.
2. If `StateReadJournaled(readId)` exists, decode and return that value.
3. Otherwise read from the current object projection.
4. Append `StateReadJournaled(readId, value)` to the owner stream.
5. Return the journaled value.

`state.set` and `state.delete` append idempotent state-change facts whose
operation identity is derived from the active call and deterministic operation
ordinal/name. Replay must not double-apply a mutation.

## Object Invocation Semantics

`objectClient(binding, objectDef)(key).method(input)` is keyed at the type level.
The final object binding should admit the call into the owner stream, not directly
start an arbitrary workflow run.

Flow:

1. Append `Accepted` to `obj/{objectName}/{key}`.
2. An owner drainer claims the key and runs accepted calls in stream order.
3. The drainer starts/resumes the TanStack workflow run for the head call.
4. The fluent context provides `objectKey` and the object state backend.
5. Completion appends `Completed`; the drainer advances.

Different keys may run concurrently. The same key is serial.

## Acceptance Ladder

Build this in vertical slices:

### A. Fluent Table Materialization

**Status:** Implemented.

**Claim.** Fluent core exposes the durable state authoring shape without an S2
dependency.

**Forces:** `Table`, `primaryKey`, `ChangeMessage`, `MaterializedState`,
`state(Table)`, abstract state backend.

**Proof:** unit tests cover schema-owned primary keys, encode/decode/fold, and
`state(Table)` get/set/delete against a fake backend.

### B. S2 Object State Projection

**Status:** Implemented.

**Claim.** One S2 owner stream can persist and replay table state for one object
key.

**Forces:** owner stream path derivation, actor event codec, projection fold,
state backend implementation.

**Proof:** `add(5)` then fresh process `value()` returns `5` against `s2 lite`.

### C1. Same-Process Same-Key Serialization

**Status:** Implemented.

**Claim.** Concurrent same-key object calls through one runtime binding cannot
lose updates.

**Forces:** `Accepted` admission, owner-stream CAS, unique drain owner ids, one
active call at a time.

**Proof:** two concurrent calls race `add(5)` and `add(7)` for the same key;
both complete and final value is `12` against `s2 lite`.

### C2. Cross-Host Same-Key Serialization

**Status:** Implemented.

**Claim.** Concurrent same-key object calls from different hosts cannot lose
updates.

**Forces:** globally unique generated call ids, `Accepted` admission into one
owner stream, CAS-protected `Started` ownership, owner lease expiry, and one
active call at a time.

**Proof:** two hosts race `add(5)` and `add(7)` for the same key; final value is
`12` and both hosts participated.

### C3. Stale Owner Recovery And Fencing

**Status:** Implemented.

**Claim.** A killed or live-deposed object owner cannot block a key forever or
allow stale writes after takeover.

**Forces:** stale owner recovery, owner handoff, object lease expiry, matching
TanStack runtime lease expiry for the abandoned run, owner-token state write
fencing, and owner-token terminal event fencing.

**Proof:** host A starts a same-key call and is killed before completion; host B
waits for lease expiry, claims ownership, resumes the abandoned run from S2, then
serializes its own same-key call. `store.object-stale-owner` proves
the final state is `12`.

`store.object-live-fencing` keeps host A alive after lease expiry,
lets host B take over and complete the call, then proves host A's late state
write does not affect the final materialized value.

### D. Replay-Safe Reads And Writes

**Status:** Implemented.

**Claim.** Read-modify-write remains stable under crash/replay.

**Forces:** `StateReadJournaled`, idempotent state mutations, replay from owner
stream before handler resume.

**Proof:** crash after state mutation but before call completion; restart does not
double-apply and returns the original result. `store.object-replay-state`
proves a run that read `0`, wrote `5`, and died before completion resumes without
turning the value into `10`; the next serialized `add(7)` observes final state
`12`.

### E. Restate-Like Handles

**Status:** Implemented.

**Claim.** `sendObjectClient` returns durable handles with attach/output
semantics.

**Forces:** idempotency keys, completion lookup by call id, attach/poll APIs.

**Proof:** caller sends with idempotency key, process restarts, another caller
attaches by handle and reads the completed result. `store.object-handles`
proves `sendObjectClient` returns after durable admission, the sender can die,
and another host can attach by calling the same object method with
`{ runId: reference.invocationId }`.

## Guardrails

- Do not replace `state(Table)` with string slots.
- Do not put HTTP in fluent core.
- Do not import `@firegrid/log` into fluent core.
- Do not allow state operations inside `run` actions.
- Do not claim object-state correctness without a real `s2 lite` crash/restart
  proof.
- Do not weaken the TanStack/S2 store proofs while adding object state.

## Completed Hardening

The implemented proof suite covers durable state projection, same-key
serialization, cross-host serialization, killed-owner recovery, live
deposed-owner fencing, replay-safe state reads/writes, and send handles.

State stream messages written by S2 object handlers carry call/owner metadata.
When an object owner changes, projection ignores stale owned state messages and
new state writes verify that the caller still owns the invocation before
appending. Completed/errored invocation events are also fenced by current owner,
so a live but deposed host cannot finish an already-taken-over call.

This is hardening of the implemented S2 object owner, not a new SDD ladder. The
next product layer is the Restate-like fluent authoring/client surface on top of
the completed TanStack/S2 and object-state substrate.
