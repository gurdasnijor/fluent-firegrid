# SDD: effect-s2-flow → Durable Execution

### Architecture reference after the TanStack Workflow pivot

|   |   |
| --- | --- |
| Status | Superseded architecture reference — **do not implement directly** |
| Date | 2026-06-24; revised 2026-06-25 |
| Packages | `effect-s2` (the substrate) · `@firegrid/tanstack-workflow-s2` (S2-backed TanStack Workflow store/runtime adapter) · `@firegrid/fluent-firegrid` / `@firegrid/fluent-firegrid-s2` (higher-level Firegrid conveniences). `effect-s2-flow`, `effect-s2-stream-db`, and `effect-s2-durable` are reference/archived material only. |
| Reference vocabulary | TanStack Workflow; the S2 shared-log KV demo; Restate / `restate-sdk-gen`; Pulsar (vocabulary only) |
| effect-smol | Effect v4 (`Context.Service`, `Layer`, `Stream`, `Queue`, `PubSub`, `Deferred`, `Fiber`, `FiberRef`, `LayerMap`) |

---

## Correction: use TanStack Workflow's runtime seam

`effect-s2-flow` has been carrying too many products:

1. A durable-function execution kernel: checkpointed steps, replay, durable wait/callback, child execution, host recovery.
2. A workflow authoring API: `service`, `object`, `workflow`, generated clients, keyed virtual objects, and application routing.
3. A persistence/runtime backend: event logs, run state, leases, timers, signals, approvals, schedules, and visibility.

That package shape is inviting implementation drift. Agents can add a `sleep` proof, weaken a keyed-state proof, or expand `service/object` vocabulary and still plausibly claim to be "implementing flow."

TanStack Workflow already provides the runtime seam this repo needs:
`WorkflowExecutionStore`. Its bundled in-memory store is the reference
implementation. This repo implements the S2-backed version in
[`tanstack-workflow-s2-store-sdd.md`](./tanstack-workflow-s2-store-sdd.md) and
builds virtual-object state in
[`fluent-firegrid-state-materialization-sdd.md`](./fluent-firegrid-state-materialization-sdd.md).
Do not implement new production work directly from this document.

The vendored TanStack source lives at `repos/tanstack-workflow` and is reference-only, matching the repo's existing `repos/` policy.

## How this build works

The previous draft was a stack of thirteen horizontal layers, each with its own conformance gate. That structure caused the problem it was meant to prevent: **a horizontal layer boundary is a mock boundary.** When the gate for "the orchestrator" is "it folds records correctly," that sentence is *true against a fake substrate* — so a build agent writes the fake, passes the gate, and ships a PR that proves nothing about durability, without even breaking the rules it was given.

The corrected build is now:

1. **TanStack Workflow API/runtime** — lifted into local workspace packages.
2. **S2 store adapter** — implements `WorkflowExecutionStore` with `effect-s2`.
3. **Fluent Firegrid conveniences** — implemented above the TanStack/S2 runtime,
   not inside `effect-s2-flow`.
4. **Components below** — retained as design reference for S2 facts, leases,
   timers, activation, and materialized views. Components are not the
   implementation order.

### The governing rule

Three rules, and they are the point of the whole document:

1. **Every gate is a behavioral property of the real substrate.** Crash (`kill -9`), contention, replay, linearizability — chosen so that passing it against a mock is impossible or meaningless. *There is no honest mock of durability.* If a test passes against a fake substrate, the test is wrong.
2. **No code is built unless it is on the critical path of the current capability's acceptance test.** A component-sliver that isn't load-bearing for the test does not get written. This is how each horizontal concern is *forced to justify itself* — by being necessary to make a real-substrate property pass.
3. **Slices are vertical and thin.** A slice cuts through every component it needs, minimally. We do **not** build a component "fully" before a capability needs it — `effect-s2` itself is sliced to what each capability exercises. There is no "finish the substrate, then start the runtime" phase.
4. **Do not invent workflow APIs inside `effect-s2-flow` or the store adapter.**
   `service`, `object`, `workflow`, generated clients, and virtual-object state
   belong in the fluent layer above TanStack/S2.

The consequence for build agents is mechanical: you cannot satisfy a capability gate with a fake, because the gate runs against `s2 lite` and a fake fails it. The reviewer's check is one glance — *did this PR's test kill the process and survive on real S2?*

## Thesis

A **per-run S2 event stream is the unit of TanStack Workflow replay**; metadata/index streams provide run state, leases, timers, schedules, and visibility. The conditional append is the coordination primitive. TanStack owns the workflow API; this repo owns the S2-backed persistence/runtime substrate.

## Current status

- **`effect-s2` exists** as the production S2 substrate (`S2Client.ts`), sliced to what the runtime needs.
- **The S2-backed TanStack `WorkflowExecutionStore` ladder is implemented.** See [`tanstack-workflow-s2-store-sdd.md`](./tanstack-workflow-s2-store-sdd.md).
- **The fluent virtual-object state/materialization ladder is implemented.** See [`fluent-firegrid-state-materialization-sdd.md`](./fluent-firegrid-state-materialization-sdd.md).
- **`effect-s2-flow` is demoted to architecture/reference material.** Do not expand `service` / `object` / `workflow` / generated-client ergonomics in `effect-s2-flow`.
- **PRs that only add verification proofs are not implementation progress.** A proof is an acceptance gate for a named production `WorkflowExecutionStore` method or adapter behavior.
- **PR #68-style durable sleep work is parked.** Reuse it only as a TanStack
  `scheduleTimer` / `claimDueTimers` / `deliverSignal` proof or delete it.
  Do not weaken existing virtual-object/state proofs.

## Guardrails (what not to build)

- **No second transport.** Runtime code calls `S2Client.stream(...)` / `StreamApi` at the point of I/O. No `effect-s2-flow` transport facade (e.g. `stream/S2Stream.ts`), no duplicate `append`/`read`/`readSession` wrapper. Flow defines domain codecs and types only.
- **No in-repo S2-lite.** No in-memory S2 substitute under `src`, `test-support`, or exports. Validate against the official `s2` CLI running `s2 lite`. Vendored `repos/s2` is reference only — never a build or validation target.
- **No resurrected legacy.** No snapshot-generator infra, no `src/heyapi-client.ts`, no `Channel.ts`, no broad legacy adapters. Do not restore archived packages to have a destination.
- **No hand-maintained client bag.** `effect-s2`'s surface is semantic capabilities over generated protocol + SDK patterns, not a manually mirrored `S2ClientApi`.
- **No component self-certification.** A component does not get its own green checkmark. It is proven only by a capability's real-substrate acceptance test that depends on it.
- **No workflow API invention.** `object(...)`, `state(...)`, keyed placement, object clients, generated Restate-like ergonomics, and custom durable contexts are higher-layer work and blocked during the TanStack store adapter phase.

---

# Implementation Ladder

The runtime implementation ladder lives in
[`tanstack-workflow-s2-store-sdd.md`](./tanstack-workflow-s2-store-sdd.md) and is
implemented:

- A. Event log CAS
- B. Run lifecycle
- C. Leases and stale claims
- D. Timers, signals, and approvals
- E. Schedules

The object-state/materialization ladder lives in
[`fluent-firegrid-state-materialization-sdd.md`](./fluent-firegrid-state-materialization-sdd.md)
and is implemented through send handles.

The older capability notes below are retained only as conceptual mapping for how S2 facts, leases, timers, and materialized views relate to workflow execution. They are not the active work order.

## Conceptual Capability A — Durable step execution

The spine. A function runs, journals its steps, and survives a crash without re-doing completed work.

**Public kernel shape.** A handler is wrapped as a durable function and receives a `DurableContext`; `ctx.step(name, effect)` is the replay boundary.

**Acceptance test (unmockable).** `submit(fn, x)` runs a handler with two `ctx.step` calls. Send `kill -9` between step 1's durable ack and step 2. Restart the process. Assert: step 1 does **not** re-execute (its observable side effect fired exactly once), step 2 runs, the result returns exactly once. All against `s2 lite`.

**Forces (build this, nothing more):**
- `effect-s2`: real `append` (with `matchSeqNum` for the checkpoint CAS) and `read`/`readSession` (fold on restart). Sliced to these verbs.
- The **execution owner**: fold-from-cursor on (re)start, ordered apply-on-ack, and the **atomic journal commit** — `{StepCompleted + checkpoint cursor}` as **one append batch on the execution stream**.
- The **handler runner**: install `DurableContext`; `ctx.step` journal-checks by deterministic name (present ⇒ recorded value, no re-run; absent ⇒ run + guarded-append `StepCompleted`).
- A minimal `DurableContext` and `durableFunction` / `submit` / `attach` / `serveDurableFunctions` entrypoint.

**Defers:** virtual objects, keyed state, service clients, generated clients, timers, callbacks, child execution, append-session throughput, TableView, the View orchestrator.

## Conceptual Capability B — Durable wait and callback suspension

Suspend for time or an external fact with zero resident resources, then resume on a cold process.

**Public kernel shape.** `ctx.wait(name, duration)` and `ctx.createCallback(name)` / `ctx.waitForCallback(name, submitter)` are durable operations on the execution stream.

**Acceptance test (unmockable).** A handler calls `ctx.createCallback("approval")`, emits/sends the callback id from a durable step, and awaits it. The process is torn down after the wait is durably parked. Later, from a different process, the callback is resolved. The owner re-instantiates from cold, folds the real journal, and resumes exactly once. A `ctx.wait("nap", d)` variant kills the host before `fireAt`; on restart the timer still fires once. Against `s2 lite`.

**Forces:**
- Suspend/resume: park on an unresolved await; the resolving fact (`CallbackResolved` / `CallbackRejected` / `TimerFired`) is journaled onto the execution stream; folding it resumes.
- Activation: callback resolution and timer fire wake a cold execution owner.
- Timer driver: journal-authoritative `TimerSet`; driver registration is a reconcilable projection.

**Defers:** virtual-object state, service/object APIs, distributed activation, timer sharding at scale.

## Conceptual Capability C — Child execution

Durable function-to-function calls: parent/child, fan-out, sagas as lower-level execution primitives.

**Public kernel shape.** `ctx.invoke(child, input, { childId })` journals `ChildRequested`, dispatches idempotently, parks, and resumes on `ChildCompleted`.

**Acceptance test (unmockable).** Parent calls child. Force the parent to replay 5x. Assert: the child runs exactly once by `childId` admission, its result routes back to the parent, and a dropped first dispatch is recovered by idempotent re-dispatch. Against `s2 lite`.

**Forces:** child correlation, inline dispatch via the Activator, idempotent re-dispatch on replay, `ChildCompleted` reply routing. Leans entirely on kernel A + B.

**Defers:** generated service clients and Restate-like `serviceClient` / `objectClient` syntax.

## Conceptual Capability D — Local composition helpers

Child contexts, map, parallel, race/select decisions, and cancellation semantics over the kernel primitives. This is convenience over A-C, not a new substrate.

**Acceptance test.** Parent uses child contexts plus parallel branches where each branch performs named durable steps. Replay under forced interleavings and verify journal matching is by deterministic operation identity, not by fiber issue order.

## Deferred Authoring Capability E — Service and workflow declarations

Wrap durable functions in Restate-like `service` / `workflow` declarations, generated or proxy-based clients, and app-facing ingress. This layer uses the kernel; it does not own replay.

## Deferred Authoring Capability F — Virtual objects and durable state

Virtual objects: persistent, consistent per-key state, safe under concurrent owners.

**Acceptance test (unmockable).** (1) `counter.add(5)`; in a fresh process `counter.value()` -> `5`. (2) Two concurrent `add` calls from two would-be owners of the same key -> the fence rejects one; no lost update. (3) Read-your-writes inside a handler: `set` then `get` in the same invocation sees the write. Against `s2 lite`.

**Forces:** per-key owned stream, `state()` over the owned fold, `StateChanged` journaling, a real lease/fence on every owner write, and the exclusive serial drainer.

## Deferred Authoring Capability G — Restate-like clients and generated ergonomics

Typed `serviceClient` / `objectClient` / generated client APIs over kernel child execution and virtual objects.

## Deferred Authoring Capability H — Materialized views & multi-primary reads

The only place the **multi-primary** half of the orchestrator is exercised. Deferred so v1 serves reads through the single owner.

**Acceptance test (when built).** Write on node 1; a strong read on node 2 is linearizable; an eventual read may lag; the Porcupine history check passes.

---

# Components

The menu the capabilities draw from. **None of these has its own gate** — each is proven only by the capability test that depends on it. They are described as target designs; each capability slices them to what its test forces.

## effect-s2 — the substrate

`effect-s2` *is* Layer 0. There is no separate "substrate layer" to build — there is `effect-s2`, and the six things S2 promises, which are its contract.

**Generation.** The protocol layer is generated from S2's OpenAPI spec; only the semantics OpenAPI cannot describe are hand-written. HeyAPI is the parser/plugin orchestrator (it owns OpenAPI normalization and the symbol pipeline); **local Effect-native plugins** emit the artifacts this repo wants.

- Spec: `s2-streamstore/s2-specs` `s2/v1/openapi.json` pinned to `329de93f7b240a4daef9edbeb98ced0699aab7d0`, from the pinned raw URL — not a checked-in copy.
- Generator: `@hey-api/openapi-ts` + local `effect-schema` and `client-effect` plugins → `src/generated/{effect-schema,client-effect}.gen.ts`. `client-effect` emits an Effect `HttpApi` contract and derives the client via `HttpApiClient` — no bespoke request runtime. Generated code is the protocol contract and drift detector; do not post-process it.
- *(Open: the prior decision was HeyAPI-orchestrator + local plugins; our later analysis leaned toward `@effect/openapi-generator` as the backbone, with the dual-content-`read`-200 caveat (`#1978`) and `sessions/` hand-written not generated. Reconcile when Capability A actually exercises the generated client.)*

**The single semantic surface** is `src/S2Client.ts`, grouped around `basins`, `accessTokens`, `locations`, `metrics`, `basin(name)`, `stream(basin, name)`. The stream capability exposes one-shot `append`/`read`/`readSession`/`checkTail`, scoped `appendSession`/`producer`, command-record constructors (`AppendRecord.fence`/`trim`), and integrated `@s2-dev/streamstore` patterns (`u64`, framing/chunking, dedupe headers, serializing append sessions, deserializing read sessions). Downstream depends on these capabilities, not generated internals.

**The contract — six native guarantees** (what "Layer 0" was):

| Guarantee | Semantics | Used for |
| --- | --- | --- |
| Atomic batch | ≤1000 records / 1 MiB, all-or-none, multi-AZ-durable before ack | the durable commit unit (A) |
| `match_seq_num` | optimistic CAS; 412 on mismatch; with retry ⇒ exactly-once | checkpoint CAS / replay-exactly-once (A) |
| `fence` command record | pessimistic, strongly consistent, **cooperative** (a no-token append still lands), ≤36 B, empty clears | ownership / incarnation (B) |
| `check_tail` | current tail seqnum; cheap, storage-class-independent | linearizable-read barrier (E) |
| Append session | pipelined, submission-ordered; a 412 poisons the session | the throughput writer (when needed) |
| Lease via heartbeat / deadline | owner identity by in-stream token; readers confirm via `check_tail` | failover detector (B) |

Tiering: **Express (40 ms ack)** for invocation/inbox streams; **Standard (400 ms)** for cold logs/large views.

**Cooperative fencing (load-bearing).** A `fence` rejects the *wrong* token; a *no-token* write still lands. Therefore **every owner-write carries the current token** or the fence protects nothing. The runner enforces this; not optional. (Forced by Capability B.)

**Command records** (`fence`, `trim`) are records with a single empty-name header, seq-numbered and returned to reads, filterable by `headers.length === 1 && headers[0][0] === ""`. Because they are records, they batch atomically with data — a snapshot + its `trim`, or a fence + data, land together.

**Adopt upstream patterns; do not rebuild:** ordered/backpressured writes → append session / Producer; per-record ordered durability → Producer ticket `ack` (exact seqnum); large messages → `patterns/serialization` framing; shared-stream dedupe → `injectDedupeHeaders`/`DedupeFilter`; exact `u64` → `encodeU64`/`decodeU64`; fencing/CAS → pass `fencingToken` + `matchSeqNum` through **every** append path.

## The Owned orchestrator — the per-key durable engine

One instance per stream (= per key): the Effect translation of the S2 KV demo's loop, owning the materialized state, a command mailbox, a tailing reader, a pending heap, and an ordered writer. **Everything in the durable runtime is a specialization of this engine** with a different record-handler. v1 builds **only the Owned (fenced single-writer) form**; the multi-primary View form is the deferred *Materialized views* component.

**Apply discipline — apply-on-ack, in stream order.** The owner applies its own writes on ack, but **strictly in stream order**: a foreign record can take seq `N` while an own write takes `N+1`; applying `N+1` before `N` would reorder the fold. The rule: an acked own write at `ack.start.seqNum === applied` is **fast-pathed** (apply now → advance `applied`); otherwise it is held **pending-own** and applied when `applied` catches up, and the write's reply `Deferred` (what makes a handler's `set` return) **completes after the ordered local apply, not on raw ack**. So **read-your-writes holds** and the fold never reorders. The owner still tails for **foreign ingress** (signals, timer-fires, child-completions) and applies those in tail order; own records are recognized (incarnation header / pending-own set) and applied exactly once.

**The atomic commit is the only atomicity unit — there is no transaction resource.** S2 has no transaction concept; the only way to make N records atomic is one append batch on one stream. The runner assembles `{own-journal records (StepCompleted/StateChanged) + checkpoint cursor}` and appends them as **one batch on the owner's own stream** — so restart recovers the input cursor from the owner's own journal, no cross-stream read. Over-budget ⇒ a typed `BatchTooLarge` at assembly time (an over-budget atomic group is *unconstructible*; it never silently splits into separately-foldable appends), never a runtime 412. Emits to *separate* downstream streams are not in this batch — they are independent idempotent appends with a dedup key.

```ts
// transaction-free atomic commit: budget-checked single append (NOT a resource/2PC)
const commitAtomic = (stream: string, records: ReadonlyArray<AppendRecord>, opts: { matchSeqNum?: SeqNum; fencingToken?: string }) =>
  records.length > 1000 || sizeOf(records) > MiB
    ? Effect.fail(new BatchTooLarge({ records: records.length }))
    : S2Client.stream(basinFor(stream), stream).pipe(Effect.flatMap((s2) => s2.append(AppendInput.create(records, opts))))
```

**The handler runner.** A handler is spliced into the Owned record path; the runner runs it, buffers its own-journal emits + checkpoint movement, and `commitAtomic`s them. Reads inside the handler are the owner's apply-on-ack state (RYW). Durable execution uses **effectively-once on the owned journal** — positional `match_seq_num` (record K's match is the position after K−1, so a replayed re-append 412s). For an output to a *shared* downstream stream the tail is moved by other producers, so `match_seq_num` gives no uniqueness — use the SDK **dedupe headers** there, or route to a per-`(owner,key)` stream. **External side effects are at-least-once + the provider's idempotency key, always** (the fence protects durable state, not the outside world).

**Inputs are funneled.** A key's logical sources (commands + timer/promise events + child completions) are written into the **one per-key inbox** upstream, so the handler sees a single ordered union and `Match`es. One stream ⇒ one cursor ⇒ a total per-key order — required for deterministic replay. (General multi-input stream processing is Pulsar vocabulary, out of v1.)

**Two write paths, by ownership:**
- `Flow.submit(stream, key, value)` — **unfenced** append (ingress: clients, signals, child requests). Ordered by S2 seqnum. No token.
- `ctx.emit(output, value)` inside a handler — **fenced, guarded** append (the owner is the single writer for its key); carries the fence and a dedup key.

```ts
// runtime/Orchestrator.ts — the Owned loop (apply-on-ack). Own writes apply in the Writer fiber on ack (ordered, §above);
// own records are filtered in the `rec` branch. `pending` strong-reads carry cfg.readDeadline so a stalled reader can't pin clients.
type Cmd<S> =
  | { _tag: "Write";      records: ReadonlyArray<AppendRecord>; reply: Deferred.Deferred<AppendAck, FlowError> }
  | { _tag: "ReadOwned";  project: (s: S, applied: number) => unknown; reply: Deferred.Deferred<unknown> }

const make = Effect.fn("Orchestrator.make")(function*<S>(opts: {
  basin: string; stream: string; initial: S
  reduce: (s: S, r: EventRecord<unknown, unknown>) => S
  fromCursor: number; fencingToken?: string   // present from Capability B onward
}) {
  const command = yield* Queue.bounded<Cmd<S>>(cfg.commandCapacity)        // bounded intake — no unbounded inbox
  const writes  = yield* Queue.bounded<{ records: ReadonlyArray<AppendRecord>; reply: Deferred.Deferred<AppendAck, FlowError> }>(cfg.writeCapacity)
  const changes = yield* PubSub.dropping<EventRecord<unknown, unknown>>(cfg.changesCapacity)  // slow consumer ⇒ drop, never OOM
  const appliedRef = yield* Ref.make(opts.fromCursor)
  let state = opts.initial
  let pending = SortedMap.empty<number, Array<Cmd<S>>>(Order.number)

  const s2 = yield* S2Client.stream(opts.basin, opts.stream)
  const session = yield* s2.appendSession()                                 // unary append+await is fine for A; session adds throughput
  yield* Stream.fromQueue(writes).pipe(                                      // ordered writer fiber, FIFO, carries the fence
    Stream.runForEach(({ records, reply }) =>
      session.submit(AppendInput.create(records, opts.fencingToken ? { fencingToken: opts.fencingToken } : undefined)).pipe(
        Effect.matchCauseEffect({
          onFailure: (c) => Deferred.failCause(reply, Cause.map(c, toFlowError("write"))),
          onSuccess: (ack) => Deferred.succeed(reply, ack) }))),
    Effect.forkScoped)

  yield* Stream.merge(                                                        // the select!: commands ⊕ tailing reader, one consumer
    Stream.fromQueue(command).pipe(Stream.map((c) => ({ k: "cmd" as const, c }))),
    s2.serialization.readSession(decodeRecord, { start: { from: { seqNum: opts.fromCursor } } }).pipe(Stream.map((r) => ({ k: "rec" as const, r }))),
  ).pipe(
    Stream.runForEach((ev) => Effect.gen(function*() {
      if (ev.k === "rec") {
        state = opts.reduce(state, ev.r)
        const applied = ev.r.cursor.seqNum + 1
        yield* Ref.set(appliedRef, applied)
        yield* PubSub.publish(changes, ev.r)                                 // C's suspend/resume subscribes here
        const [ready, rest] = SortedMap.partitionByKey(pending, (k) => k <= applied)
        pending = rest
        for (const [, ws] of ready) for (const w of ws) yield* resolveWaiter(w, state, applied)
      } else if (ev.c._tag === "Write") { yield* Queue.offer(writes, ev.c) }
        else { yield* Deferred.succeed(ev.c.reply, ev.c.project(state, yield* Ref.get(appliedRef))) }
    })),
    Effect.forkScoped)

  return { command, applied: Ref.get(appliedRef), changes: Stream.fromPubSub(changes) }
})
```

**Use the SDK's append primitives — don't hand-roll the writer.** Batching, ordering, and backpressure are exactly what the append session / Producer give (`maxInflightBytes` blocks `submit()`; each ticket carries the exact seqnum). The out-of-order risk above is purely apply-side; the SDK already guarantees durable order = submit order. Capability A can use unary `append` + await; the session is the throughput option.

**Idiom map (tokio KV demo → effect-smol), for whoever ports the loop:** `mpsc` → `Queue` (single consumer = the lock); `oneshot` → `Deferred`; `select!` → `Stream.merge` + one `forkScoped` fiber; `FuturesOrdered` → the ordered writer fiber; `BinaryHeap<Reverse<seq>>` → `SortedMap` drained on apply.

**Event envelope** (the only "streams" vocabulary v1 needs; Source/Sink connector types are deferred):

```ts
interface EventStreamDef<K, A> { name: string; key: Schema.Codec<K, string>; value: Schema.Codec<A, string> }  // physical stream: `${name}/${encode(key)}`
interface EventRecord<K, A>    { stream: string; key: K; value: A; cursor: EventCursor; headers: ReadonlyMap<string, string> }
interface EventCursor          { stream: string; seqNum: SeqNum }   // SeqNum ≙ S2 Rust SeqNum(u64); use SDK encodeU64/decodeU64, NOT the f64 (negligible under per-key sharding — flag, don't gate)
```

## Ownership — lease, fence, activation

**Ownership is the `y-s2` distributed-mutex recipe** (S2's own production design), not bespoke. The lease is the source of truth for single-writer; placement is only contention-reduction.

- **The lease via fencing token.** Token = `"{uuidBase64} {deadlineEpochSec}"` (≤36 B): holder id + deadline. To claim, a contender appends a `fence` record carrying its *known current* token; the loser 412s. **The deadline is auto-expiry** — no separate heartbeat: the holder re-fences with a fresh deadline before expiry; a challenger reading an expired token may claim. Every owner-write carries the current token (cooperative fencing). A stale owner's next tokened write 412s and poisons its session. **Checkpoint co-commits fence-reset + snapshot + `trim` in one atomic batch** — keeping ownership and state consistent; contenders may race a checkpoint but only one commits.
- **Per-key placement is advisory.** Default (`s2Native`): no explicit placement — whichever node gets work for a key attempts the lease and the fence arbitrates. Single-node dynamic instances use `LayerMap.Service` keyed by entity id (instantiate-on-event, evict-on-idle — needed by suspension). Distributed managed placement (cluster `Sharding`) is additive and advisory; the S2 fence stays authoritative.

```ts
const leaseToken = (holder: string, deadlineEpochSec: number) => `${base64(holder)} ${deadlineEpochSec}`  // identity + auto-expiry, ≤36 B
```

**`cluster.Entity` is deliberately not adopted.** It is an `RpcGroup` addressed by id, sharded to a single active runner with idle eviction — but its persistence is **RPC-message-level** (durable mailbox, dedup of in-flight RPCs, `KeepAliveRpc`), **not an event-sourced step journal**. Building on it means two transports + two durability models. The S2 fence + per-key stream already give single-active-writer with the journal as the transport. Cluster `Sharding` may later sit advisorily on top (its mailbox feeding wake signals in), but never foundationally.

**The fence is single-writer *on the stream*, not single-executor *in the world*.** Claiming the fence stops the old owner's next tokened S2 write (412 + poison) — it does **not** stop an in-flight handler mid-external-call. In the window between losing the lease and discovering it on the next write, a deposed owner can still complete an external side effect. This is exactly why external effects are at-least-once + idempotency key, always. Failover is "logically fenced," not "stopped."

**Activation (wake · evict · rehydrate).** Per-key streams are unlimited, but you cannot hold millions of live orchestrators (each is a fiber + reader + session). Placement answers *which node*; activation answers *whether an owner exists*.
- **Wake-on-ingress.** An owner is **demand-instantiated**: because nobody tails a dormant key, the **write path carries the wake** — `Flow.submit(stream, key, …)` to a key with no live owner pokes the **Activator**, which instantiates the Owned orchestrator (claiming the lease) before/as the record lands. This is what a broker / the Restate server does. v1 = single-node Activator over `LayerMap`. **No global tailing of the namespace is ever required** — activation is edge-triggered by the submit.
- **Eviction + rehydration is one knob with failover.** An idle owner (no command/ingress for `maxIdleTime`) checkpoints (snapshot + fence-reset + trim) and tears down. Rehydration on next touch is **cold-fold-from-last-snapshot — the same path and cost as failover.** So snapshot cadence, failover SLA, and rehydration latency are **a single tuning axis**. Durable suspension (below) is just eviction triggered by an *await* instead of *idle*; the resolving fact's `Flow.submit` is the wake.

```ts
interface Activator {
  readonly ensureOwner: (stream: string, key: string) => Effect.Effect<void>   // idempotent: build if absent (claims lease), else no-op
  readonly evictIdle:   (maxIdleTime: Duration) => Effect.Effect<void>          // checkpoint + tear down dormant owners
}  // Flow.submit wraps ensureOwner: an ingress write to a cold key wakes its owner before returning.
```

## Durable facts & waiting

**Durable waiting is fact-driven.** A handler that awaits something unresolved **parks**; the **resolving event is journaled** onto the invocation stream; **folding it resumes** the handler. Three sources of resolving facts (timers, promises/signals, child results) plus the park/resume machinery and the per-invocation seam.

### Timers (`sleep` and delayed delivery)

**The invocation journal is authoritative; the timer-shard record is a reconcilable projection.** This closes the cross-stream gap where a runner writes the intent then crashes before a separate required driver write.

```ts
// invocation stream (AUTHORITATIVE):
TimerSet   = { _tag: "TimerSet";   name: string; fireAt: number }   // durable intent; fireAt journaled here (replay doesn't recompute now()+d)
TimerFired = { _tag: "TimerFired"; name: string }                   // delivered by the driver at fireAt
// TimerProcessor shard (DERIVED, idempotent upsert keyed by timerId = `${invocationStream}/${name}`):
TimerRegistration = { timerId: string; fireAt: number; target: { stream: string; key: string; name: string } }
```

1. `sleep` appends `TimerSet(name, fireAt)` to the invocation stream — the only required write.
2. The owner **ensures** the driver knows it: an idempotent upsert (keyed by `timerId`). A cache rebuildable from the journal, not a second source of truth.
3. **Eviction is gated on the upsert acking** — so an evicted owner's timers are always known. The only crash window is one where the owner hasn't yet evicted, so on restart it replays and **reconciles**: for every `TimerSet` without a matching `TimerFired`/`TimerCanceled`, re-ensure (idempotent).
4. The driver folds registrations into a min-heap by `fireAt`, `Effect.sleep`s to the head, then `Flow.submit(target, TimerFired(name))` (unfenced ingress; wakes via the Activator). `TimerFired` is idempotent (guarded by `timerId`).

**S2 timestamps.** `fireAt` is journaled as the `TimerSet` record's timestamp (so replay reads it, not `now()+d`); recovery seeks the shard by timestamp to bound the scan. **Monotonicity caveat:** S2 forces per-stream timestamp monotonicity, so the schedulable `fireAt` lives in the record **body** and the heap orders by that field. A timer past `fireAt` on replay fires immediately; a canceled timer delivers harmlessly (the fold ignores a `TimerFired` whose await is gone).

### Suspend / resume

A handler awaiting an unresolved durable future **parks** and **resumes** when the resolving fact folds onto the invocation stream. Two modes, one mechanism:

- **Suspended (in-incarnation).** The fiber is alive; the await registers an Effect `Deferred` in a per-invocation `DeferredRegistry` keyed by the await's identity and `yield* Deferred.await`. The apply loop, on folding a resolving fact, completes the `Deferred` → the fiber resumes. Cheap, no re-run.
- **Replay (failover / long wait).** A new owner re-folds; the handler re-runs; each `run(name)` returns its recorded value; an await whose fact is folded returns immediately; an absent one re-parks.

Equivalent because the resolving fact is always journaled. **Durable suspend is the default** — durable-execution waits span days/weeks, so on an unresolved external await (or long `sleep`) the runner tears down and evicts: zero resources while waiting, re-instantiated by replay when the fact arrives. In-memory park is the **bounded-wait optimization only** (short, known-short awaits). The threshold is runner policy; the author sees neither mode.

```ts
type AwaitKey = { _tag: "promise"; name: string } | { _tag: "child"; childId: string } | { _tag: "timer"; name: string }
interface DeferredRegistry {
  readonly await:    <A>(key: AwaitKey, decode: (folded: Option.Option<unknown>) => ResolveOutcome<A>) => Effect.Effect<A, DurableError>  // park (or return now if already folded, in replay)
  readonly complete: (key: AwaitKey, value: unknown) => Effect.Effect<void>   // called by the apply loop on each resolving fact
}
```

The registry lives in `CurrentInvocationScope`; the runner subscribes `complete` to the orchestrator's `changes` stream filtered to resolving facts. Await identity must be deterministic (promise `name`, `childId`) so replay re-registers under the same key. **Serial-drainer interaction:** for an object key, a parked exclusive call queues subsequent exclusive calls (no interleave); a long park durably suspends so the key isn't held in memory.

### Child-call correlation

A parent calling `serviceClient(S).m(x)` / `objectClient(O,k).m(x)` must trigger the child as its own durable invocation, route the result back, and stay journal-safe.

```ts
// PARENT stream:
ChildRequested = { _tag: "ChildRequested"; childId: string; target: InvocationRef; method: string; input: unknown }
ChildCompleted = { _tag: "ChildCompleted"; childId: string; result: Result<unknown> }   // routed back to the parent
Invoke         = { method: string; input: unknown; replyTo?: { stream: string; key: string }; childId?: string; idempotencyKey?: string }
```

`childId` is deterministic (`${parentStream}/${name-or-pos}`). **Correctness is from the child's idempotent admission on `childId`, not from replay suppressing dispatch:**
1. The parent appends `ChildRequested(childId, …)` — records intent so replay reconstructs the same `childId`; does **not** suppress re-dispatch.
2. **Dispatch is inline via the Activator, re-issued idempotently every replay until admission/`ChildCompleted` is observed.** The parent `Flow.submit`s `Invoke{ …, idempotencyKey: childId }` to the child's stream (waking its owner). Re-dispatch is **required** (the first may have been lost on crash) and **harmless** (the child's run-once admission keys on `idempotencyKey = childId` via `insertOrGet`, so N dispatches admit once).
3. The child runs as a normal invocation; on completion it appends `ChildCompleted(childId, result)` to `replyTo` (the parent). Fire-and-forget has no `replyTo`.
4. The parent parks on `{ _tag: "child", childId }`; folding `ChildCompleted` resumes it (or a typed failure). Observing it also stops re-dispatch.

Request-response (`yield* serviceClient(S).m(x)`) parks; fire-and-forget (`.send(x)`) does not.

## CurrentInvocationScope — the seam

The single point where authoring meets flow. The free primitives need **no `Durable.*` ceremony** because they require `CurrentInvocationScope` in their R and the runner discharges it per invocation. The owner-stream model already *is* flow — "one schema-addressed stream per key, ordered log, serial drainer by seq_num, completion from a Completed event, signals as ingress, state as a StateChanged projection" is a fenced handler over a per-key stream, term for term.

```ts
class CurrentInvocationScope extends Context.Service<CurrentInvocationScope, {
  readonly key: string; readonly stream: OwnerStream     // execution id + this execution's fenced writer (the serial drainer)
  readonly steps: StepView                                // journaled StepCompleted facts — the replay boundary for `run`
  readonly tables: TableBindings                          // owned folds — user state()
  readonly deferreds: DeferredRegistry; readonly timers: TimerService; readonly children: ChildEmitter
  readonly mode: "live" | "replay"
}>()("durable/CurrentInvocationScope") {}
```

**The `run`-action boundary is the write-edge.** A `run` action's type **excludes** `CurrentInvocationScope` from its R, so `run(state(Cart).set(…))` is a compile error — the Effect analog of Restate's ctx-less `run` closure. A `run` action is the external-effect edge (at-least-once + idempotency key); it cannot issue durable primitives because the runner does not provide the scope inside it.

## Primitives & authoring

**Durable primitives** are thin lowerings over `CurrentInvocationScope`. `Effect` is the `Operation`; `Fiber` is the `Future`; `Effect.fork` is `spawn`; `Fiber.interrupt` is cancellation. Effect supplies the concurrency engine `restate-sdk-gen` hand-builds — none re-implemented.

| Primitive | Lowering |
| --- | --- |
| `run(action, {name?, retry?})` | check `steps` by name (else position): present ⇒ recorded value / recorded typed failure, no re-run; absent ⇒ run under the `Schedule`, guarded-append `StepCompleted` (effectively-once via fence + `match_seq_num`), fold. Crash-before-append re-runs. |
| `state(Table).get/set/delete` | binding over the execution stream's fold. `set` ⇒ `StateChanged` append + fold; `get` ⇒ owned fold (apply-on-ack ⇒ read-after-ack sees the write). |
| `sleep(name, d)` | `timers.ensure` ⇒ `TimerSet` ⇒ park ⇒ `TimerFired` folds ⇒ resume. |
| `deferred(name)` / `awakeable()` | DurablePromise stream + resolution view; `get` parks if unresolved; `resolve`/`reject` append `Resolved`/`Rejected` (idempotent, first wins). |
| `signal(name, payload)` | unfenced `Flow.submit` to the target (ingress) — folded by its drainer, no resident call required. |
| `serviceClient(S).m` / `objectClient(O,k).m` | `ChildRequested(childId)` + dispatch + park on `childId` ⇒ `ChildCompleted` resumes. `.send` = no park. |
| `all` / `allSettled` | pure `Effect.all`/`forEach` — each constituent `run` journals itself; **no decision entry**. |
| `race` / `select` / `any` | journaled-decision: record a `Decision(winner)` fact so replay re-takes the branch (raw `Effect.race` picks by wall-clock and would diverge). |
| `spawn(op)` | `Effect.fork` + a journal-path `FiberRef` bump (keys inside spawned fibers = `${fiberPath}/${name}`). |
| cancellation | typed `Cancelled` error (recoverable: `catchTag` then yield more durable steps ⇒ journaled cleanup) + `Fiber.interrupt` for in-flight `run` I/O. Effect runs finalizers on interrupt; durable-step finalizers route through the typed-error path. |

**Determinism — keyed journal matching, not issue-position.** Effect's fiber scheduler does not guarantee issue order across concurrent fibers, so flow matches by **deterministic key**: every durable step carries a stable name (`run("reserve")`), the journal is keyed by it, and concurrent steps landing in any order replay correctly. Unnamed steps fall back to position and require deterministic control flow (branch only on input + journaled results — never wall-clock/random/un-journaled reads). Name steps to track identity instead of position.

**Authoring surface** — restate-sdk-gen-shaped: handlers are bare generator methods (`*greet(input) {…}`), the input is the argument (no `Effect.gen` wrapper); the runner adapts the generator to an `Effect` and installs the scope.

```ts
export { service, object, workflow }                                  // declarations → a fenced Owned handler (+ owned state for objects)
export { run, state, sleep, signal, awakeable, deferred }            // free durable primitives
export { all, allSettled, race, select, any, spawn }                 // combinators
export { serviceClient, objectClient }                               // child calls
export { client, sendClient, attach, poll }                          // call surface (hides execution id)
```

| Authoring primitive | Lowering |
| --- | --- |
| `service({name, handlers})` | stateless Owned handler over `${name}.invocation/{execId}` — one stream per execution, fenced |
| `object({name, handlers})` | per-key fenced Owned handler over `${name}.events/{key}` + owned state projection; single consumer = serial drainer by seq_num |
| `workflow` | `object` + run-once admission (`insertOrGet` on run-started, guarded by workflow id) |
| exclusive vs shared handler | fenced single-writer (apply-on-ack) vs a shared read (deferred to Materialized views); `state()` resolves to the owned fold in exclusive |
| `client(s).m(x)` | `Flow.submit(invocationStream, execId, Invoke("m", x))` ⇒ read the roster until terminal ⇒ decode |
| completion (`return`) | atomic-batch commit result to the roster, await ack, drop the execution stream — durable-fact-outlives-destructive-op (roster-ack-before-trim) |

## Materialized views & multi-primary reads (DEFERRED — Capability E)

Shovel-ready design for when a real multi-node read-scaling need appears. **Not built in v1** (reads go through the single owner).

The **View orchestrator** is the multi-primary form of the engine: no fence, no own-writes, **applies purely on tail** (already in order); N replicas run it concurrently over one stream. `getStrong` is `check_tail` + defer.

```ts
interface TableView<K, V> {
  readonly get:       (key: K) => Effect.Effect<Option.Option<V>, FlowError>   // eventual (local apply-prefix)
  readonly getStrong: (key: K) => Effect.Effect<Option.Option<V>, FlowError>   // linearizable: checkTail → defer until applied ≥ tail → read
  readonly changes:   Stream.Stream<readonly [K, V], FlowError>                // future changes (Pulsar forEachAndListen)
}
```

- `getStrong` issues `Effect.request(new CheckTail({stream}), CheckTailResolver)`, then blocks until `applied ≥ tail` (the KV demo's `reflect_applied_state`). `check_tail` reads are **coalesced** into one call per window via a batched `RequestResolver` (cleaner than the demo's bus-stand).
- **Compaction = Checkpoint** (snapshot-start + entries + snapshot-end + `trim`, one atomic batch, durable-before-trim). S2 has no key-compaction — this *is* it; cold start folds from the last snapshot.
- **Memory bound = per-key-stream sharding.** A view over a per-key stream holds one key's history; a shared multi-key view must be cardinality-bounded or snapshot-only. **Never a global cross-key view** — the OOM cliff.
- **Gate (when built):** the Porcupine linearizability check — model the orchestrator + fence as a register `(tail, last_record_hash, fence_token)`, model `match_seq_num`, set an indefinitely-failed append's end-time past all ops (mirror `s2-streamstore/s2-verification`).

```ts
const checkpoint = (stream: string) => Effect.gen(function*() {                 // snapshot at tail + trim, one atomic batch
  const cursor = yield* currentTail(stream); const entries = liveEntries()
  const records = [ snapshotStart(cursor), ...entries.map(asInsert), snapshotEnd(cursor), AppendRecord.trim(cursor) ]
  const s2 = yield* S2Client.stream(basinFor(stream), stream)
  yield* s2.append(AppendInput.create(records, { matchSeqNum: cursor }))        // trim lands iff snapshot does
})
```

---

# Worked Examples

The authoring code, byte-for-byte, with the lowering annotated. Each maps to a capability and its crash-test *is* that capability's gate.

### greeter — Capability A (durable step + retry)

```ts
import { Duration, Effect } from "effect"
import { run, service } from "effect-s2-flow/authoring"

const greeter = service({
  name: "greeter",
  handlers: {
    *greet(req: { name: string }) {
      const greeting = yield* run(Effect.sync(() => `Hello, ${req.name}!`), { retry: { maxAttempts: 3, initialInterval: Duration.millis(100) } })
      return { greeting }
    },
  },
})
// service → stateless Owned handler over greeter.invocation/{execId}; run → journal-check-then-guarded-append StepCompleted → fold (replay: recorded value)
```

### counter — Capability B (virtual object, exclusive write / owner-served read)

```ts
import { object, state } from "effect-s2-flow/authoring"
import { primaryKey, Table } from "effect-s2-flow/table"
import { client } from "effect-s2-flow/invocation"

class CounterState extends Table<CounterState>("counterState")({ id: Schema.String.pipe(primaryKey), value: Schema.Number }) {}

const counter = object({
  name: "counter",
  handlers: {
    *add(amount: number) {                          // exclusive, drained serially; fenced single-writer
      const st = state(CounterState)
      const cur = Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
      yield* st.set({ id: "v", value: cur + amount })   // StateChanged append + fold (apply-on-ack ⇒ RYW)
      return cur + amount
    },
    *value() { return Option.match(yield* state(CounterState).get("v"), { onNone: () => 0, onSome: (r) => r.value }) },  // owner-served read (v1); shared replicas are deferred E
  },
})
yield* client(counter, "user-1").add(5)    // → 5   (counter.events/user-1)
yield* client(counter, "user-2").value()   // → 0   (different key = different stream = isolated)
```

### blockAndWait — Capability C (durable promise + suspend/resume)

```ts
import { workflow, state, deferred } from "effect-s2-flow/authoring"

class WfState extends Table<WfState>("wf")({ id: Schema.String.pipe(primaryKey), input: Schema.String }) {}

const blockAndWait = workflow({
  name: "blockAndWait",
  run: function*(input: string) {
    yield* state(WfState).set({ id: "in", input })
    const value = yield* deferred<string>("done").get      // parks (durable suspend); torn down until resolved
    return value
  },
  handlers: {
    *unblock(output: string) { yield* deferred<string>("done").resolve(output) },   // append Resolved; idempotent, first wins → fold → resume
  },
})
```

### saga — Capability D (child calls + sleep)

```ts
import { service, sleep, serviceClient, all } from "effect-s2-flow/authoring"

const saga = service({
  name: "saga",
  handlers: {
    *placeOrder(req: { orderId: string }) {
      const reserved = yield* serviceClient(inventory).reserve({ orderId: req.orderId })   // ChildRequested → dispatch → park on childId → ChildCompleted resumes
      yield* sleep("settle-delay", Duration.seconds(30))                                    // TimerSet → park → TimerFired
      const [charged, shipped] = yield* all([                                              // fan-out, pure Effect.all (each child journaled by childId)
        serviceClient(billing).charge({ orderId: req.orderId }),
        serviceClient(shipping).schedule({ orderId: req.orderId }),
      ])
      return { reserved, charged, shipped }
    },
  },
})
// sleep survives crash (re-fold fires it); replay re-issues child dispatch idempotently until admission/completion (exactly-once by childId).
```

---

# Reference — quick recap

**The build ladder.** A (durable execution: kill -9 mid-step, one append-batch journal commit, fold-on-restart, `matchSeqNum` CAS) → B (durable state: owned fold + RYW, the **fence** forced by a contention test) → C (durable suspension: park → journaled resolving fact → cold rehydrate; Activator wake-on-ingress; timer driver) → D (orchestration: idempotent child dispatch by `childId`). **E deferred** (multi-primary reads, TableView, `check_tail`, Porcupine). Every gate runs against `s2 lite`; a mock makes it vacuous.

**The engine.** One Owned orchestrator per key: ordered apply-on-ack own writes (fast-path when next, else pending-own, reply after ordered apply ⇒ RYW) + foreign-ingress tail reader. The atomic commit is `{own-journal + checkpoint}` as **one batch on the owner's own stream** — the only atomicity unit, no transaction resource; over-budget ⇒ `BatchTooLarge`. Ownership = `y-s2` lease/fence `"{uuid} {deadline}"` (auto-expiry), authoritative; placement advisory. An owner is demand-instantiated by the **Activator** on ingress; eviction + cold rehydration is one knob with failover + snapshot cadence.

**Resolving facts (what un-parks a handler).** `TimerFired(name)` · `Resolved(name,v)`/`Rejected(name,e)` · `ChildCompleted(childId,r)`. All journaled on the invocation stream; folding any completes a `DeferredRegistry` entry. Durable suspend (tear down + evict) is the default; in-memory park is the bounded-wait optimization.

**Write paths.** Unfenced `Flow.submit` for ingress/signals/child-requests; fenced `ctx.emit` for owned outputs (positional `match_seq_num` dedup on the own journal; SDK dedupe-headers on a shared downstream). External effects: at-least-once + idempotency key, always.

**Determinism.** Named durable steps ⇒ keyed journal matching. Unnamed ⇒ positional (deterministic control flow required). `race`/`select`/`any` ⇒ `Decision(winner)`. `all`/`allSettled` ⇒ pure Effect.

**Effect ≙ restate-sdk-gen.** `Operation`=`Effect` · `Future`=`Fiber` · `spawn`=`Effect.fork` · `interrupt`=`Fiber.interrupt` · `contextLocal`=`FiberRef` · the epoch guard / won flag / stale-waiter pruning = gone (Effect owns fiber lifecycle).

---

# Decisions

One line each; the parenthetical names the authoritative component/capability.

1. **Build is capability-vertical, not layer-horizontal.** The plan is a ladder of platform capabilities; horizontal concerns are forced to justify themselves by being load-bearing for a capability's acceptance test. (Build ladder)
2. **Every gate is a real-substrate property a mock makes vacuous; components do not self-certify.** No fake S2 anywhere; validate against `s2 lite`. (Governing rule)
3. **`effect-s2` is the substrate (= "Layer 0").** Generated protocol (HeyAPI orchestrator + local Effect plugins, pinned spec) + handwritten semantics; the six S2 guarantees are its contract, not a separate layer. (effect-s2)
4. **One Owned orchestrator per key; apply-on-ack in stream order.** Fast-path when next, else pending-own; reply after ordered apply ⇒ RYW. v1 builds only Owned. (Orchestrator)
5. **No transaction resource.** The atomic batch is the only atomicity unit; `{own-journal + checkpoint}` co-located on the owner's own stream; over-budget ⇒ `BatchTooLarge`; downstream is idempotent dedup. Cross-key atomicity deferred. (Orchestrator)
6. **effectively-once = positional `match_seq_num` on the own journal**; SDK dedupe headers on shared downstream; never raw tail-CAS on a shared stream. External effects at-least-once + idempotency key. (Orchestrator)
7. **Capability A runs single-node-unfenced** (`matchSeqNum` CAS gives replay-exactly-once); **the fence is forced by Capability B's contention test.** (A / B)
8. **Ownership = the `y-s2` lease/fence recipe** (`"{uuid} {deadline}"`, auto-expiry); lease authoritative, placement advisory. (Ownership)
9. **`cluster.Entity` not adopted** (RPC + mailbox persistence = redundant second transport); cluster `Sharding` is at most advisory placement later. (Ownership)
10. **Activation = wake-on-ingress via the Activator**; eviction + cold rehydration is one knob with failover and snapshot cadence. (Ownership)
11. **Durable suspend by default** (waits span days/weeks); in-memory park is the bounded-wait optimization. (Suspend/resume)
12. **Timers use S2 record timestamps**; the invocation journal is authoritative, the driver registration a reconcilable projection (idempotent upsert, eviction-gated). (Timers)
13. **Child dispatch is inline + idempotent**, re-issued every replay until admission/`ChildCompleted`; exactly-once from `childId` admission, not replay suppression. (Children)
14. **Determinism = keyed journal matching**; `race`/`select`/`any` journal a `Decision(winner)`; `all`/`allSettled` are pure Effect. (Primitives)
15. **`seqNum` = `SeqNum(u64)` via SDK `encodeU64`/`decodeU64`**; f64 exposure negligible under per-key sharding — flag, don't gate. (Orchestrator/event envelope)
16. **Materialized views / multi-primary reads (E) are deferred**; v1 serves reads through the single owner. Cut from v1 with E: View orchestrator, `check_tail` barrier, TableView, Porcupine. Also cut: Source/Sink (Pulsar IO), the atMost/atLeast/effectively guarantee matrix, first-class multi-input. (Materialized views / Non-Goals)
17. **No schema registry service** — `Schema` is the spine; evolution is decode-time migration. (Non-Goals)
18. **Packaging:** runtime in `effect-s2-flow`; `effect-s2-stream-db`/`effect-s2-durable` archived. (Header / Guardrails)

---

# Non-Goals

- No exactly-once arbitrary external side effects — at-least-once + idempotency key, always.
- No transaction coordinator or `Transaction` resource — the S2 atomic batch (one append, one stream) is the only atomicity unit; cross-stream atomicity is idempotent dedup keys, never 2PC.
- No cross-key atomic transactions in v1.
- No re-implementation of a workflow scheduler — Effect is the scheduler.
- No schema registry service — `Schema` is the spine.
- **Cut from v1** (deferred, not rejected): multi-primary reads / TableView (E); Source/Sink connectors; the atMost/atLeast/effectively-once processing matrix; first-class multi-input stream processing; Shared-subscription competing-consumer dispatch. Key_Shared (per-key streams) is native.
- No broker, worker scheduler, or subscription protocol.

---

# S2 Research Alignment

- **Distributed mutex → ownership.** `y-s2`'s recipe *is* the ownership design: `"{uuid} {deadline}"` token, cooperative fencing, deadline as auto-expiry (no heartbeat), atomic fence-reset + trim on checkpoint. Production-validated; demonstrates placement is optional (the lease arbitrates) — the argument against `cluster.Entity`.
- **`cluster.Entity` → not adopted.** RPC-addressed, single-active-runner with idle eviction and a persisted mailbox, but no event-sourced step journal. Its durability is message-layer; ours is the S2 stream. Concepts worth borrowing; the fence stays authoritative.
- **Timestamping → timers.** Monotonic ms-epoch with timestamp-indexed reads; journal `fireAt` deterministically, bound recovery scans. Forced monotonicity is why `fireAt` is a body field.
- **Linearizability tooling → Capability E.** Porcupine + deterministic simulation, the stream as a register, an indefinite-append-failure's end-time past all ops (`s2-streamstore/s2-verification`).
- **Access control & isolation → higher layer.** Per-key-stream isolation is already the structural boundary; S2 scoped tokens map onto per-namespace/per-key tokens at the invocation boundary — a token-scoping exercise, not an architectural change.

---

# References

- S2 — concurrency control (match_seq_num, fencing): https://s2.dev/docs/concepts/concurrency-control
- S2 — appends (atomic batch, durability, storage classes): https://s2.dev/docs/concepts/appends
- S2 — command records (fence, trim): https://s2.dev/docs/concepts/command-records
- S2 — shared-log KV store (the orchestrator, check_tail, bus-stand): https://s2.dev/blog/kv-store · `s2-streamstore/s2-kv-demo`
- S2 — distributed mutex / leases (ownership recipe): https://s2.dev/blog/durable-yjs-rooms#a-distributed-mutex
- S2 — timestamping: https://s2.dev/blog/timestamping
- S2 — linearizability (Porcupine + DST): https://s2.dev/blog/linearizability · `s2-streamstore/s2-verification`
- S2 — access control & agent isolation: https://s2.dev/blog/access-control · https://s2.dev/blog/distributed-ai-agents#enforcing-isolation-at-the-infrastructure-layer
- S2 — OpenAPI spec (generation source): `s2-streamstore/s2-specs` `s2/v1/openapi.json` @ `329de93f7b240a4daef9edbeb98ced0699aab7d0`
- S2 — Rust `SeqNum(u64)`: `s2-streamstore/s2` › `sdk/src/types.rs`
- effect-smol (Effect v4): https://github.com/Effect-TS/effect-smol/blob/main/LLMS.md
- effect-smol cluster `Entity` (non-adoption verdict): https://github.com/Effect-TS/effect-smol/blob/main/packages/effect/src/unstable/cluster/Entity.ts
- effect-smol `@effect/openapi-generator` (generation backbone candidate): https://github.com/Effect-TS/effect-smol/tree/main/packages/tools/openapi-generator
- Restate — `restate-sdk-gen` (authoring shape, Operation/Future, cancellation): `restatedev/sdk-typescript` › `packages/libs/restate-sdk-gen`
- Restate — service communication (child-call model): https://docs.restate.dev/develop/ts/service-communication
- Prior SDD drafts (superseded by this one): `effect-s2-flow-durable-execution-sdd-clean.md`; archived legacy at `archive/effect-s2-legacy-durable-stream-db` (reference only).
