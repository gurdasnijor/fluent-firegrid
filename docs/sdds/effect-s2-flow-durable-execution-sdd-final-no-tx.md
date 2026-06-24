# SDD: effect-s2-flow → Durable Execution

### A layered build from the S2 substrate up to a Restate-style authoring surface

|   |   |
| --- | --- |
| Status | Draft for implementation |
| Date | 2026-06-24 |
| Packages | `effect-s2` (transport: generated S2 protocol + handwritten semantic capabilities) · `effect-s2-flow` (runtime substrate, Layers 1–7; authoring surface 8–10 lands later). `effect-s2-stream-db` and `effect-s2-durable` are **archived** — reference only, not active targets. |
| Supersedes | `effect-s2-flow-sdd.md` (folded in as Layers 0–6) and the processor portion of the prior stream-db SDD |
| Reference vocabulary | Pulsar (Functions, transactions, IO, TableView, schema); the S2 shared-log KV demo; Restate / `restate-sdk-gen` |
| effect-smol | Effect v4 (`Context.Service`, `Layer`, `Stream`, `Queue`, `PubSub`, `Deferred`, `Fiber`, `FiberRef`, `RequestResolver`, `LayerMap`, cluster entities) |

---

## How to read this document

The design is a stack. Each layer is implementable and testable on its own and depends only on the layers below it. Build in order; each layer has a conformance gate that must pass before the next.

```
Layer -1  effect-s2 fidelity      generated S2 protocol + Effect semantic capabilities
Layer 0   S2 substrate            six native guarantees
Layer 1   Orchestrator            the per-key engine + the concurrency fork          ← runtime core
Layer 2   Streams                 EventStream · Record · Source · Sink
Layer 3   State materialization   TableView (fold · getStrong · compaction)
Layer 4   Processing              Processor (Functions) · guarantees
Layer 5   Atomicity               the batch is the only unit — no coordinator
Layer 6   Ownership               lease · fence · assignment · activation
Layer 7   Durable extensions      TimerService · suspend/resume · child correlation  ← makes flow a DX substrate
Layer 8   CurrentInvocationScope  the seam the Processor runner provides per invocation
Layer 9   Durable primitives      run · state · sleep · signal · awakeable · deferred · combinators
Layer 10  Authoring surface       service · object · workflow · client               ← restate-sdk-gen-shaped
Layer 11  Worked examples         greeter · basics · counter · blockAndWait · checkout
```

Three reading aids live at the end and are **indexes, not re-specifications**: the **Reference** (one-screen recap of the load-bearing rules), the **Decisions** index (one line per decision, pointing to the layer that details it), and **Non-Goals**.

## Thesis

Pulsar centralizes four mechanisms in a broker (dispatch, transaction coordinator, schema registry, compaction); the S2 KV demo shows the materialization/orchestration engine; the baseline `StreamDb` is the locally-serialized CAS writer (the fenced multi-writer generalization is the work here). Compose them and you get a **per-key stream as the unit of everything**, with the conditional append as the coordination primitive and `Schema` as the schema system. Layered up, that substrate bottoms out a Restate-style durable-execution authoring surface **without re-implementing a scheduler** — Effect *is* the scheduler `restate-sdk-gen` hand-builds. `Effect` is the `Operation`, `Fiber` is the `Future`, and the durable layer is a thin set of primitives plus three flow extensions. "Effect all the way down" stays literally true because flow's primitives are themselves Effects.

---

## Current status & next step

Point-in-time; everything below this section is the timeless target design.

- **Layer -1 (effect-s2 fidelity): done enough.** The generation path and the single semantic surface (`S2Client.ts`) are in place. Live semantic tests for `match_seq_num`/`fencingToken` 412 behavior, append-session poisoning, and Producer per-record seqnum ordering are **L1/ownership hardening**, not a prerequisite.
- **Layer 0 (substrate): exposed** through `S2Client.ts`. The pending live tests above are the only gap.
- **Package boundary: resolved.** The runtime substrate lives in `packages/effect-s2-flow`; legacy packages were removed from this checkout.
- **Layer 1 (orchestrator): merged but mis-validated.** `runtime/{ViewOrchestrator,OwnedOrchestrator,CheckTail,FlowError}.ts` exist, but the layer is currently coupled to an **in-repo fake `StreamStore`**. Do not redo the concurrency design or add another transport wrapper to compensate; **remove the fake substrate**.
- **Next PR — Build Plan "Corrective vertical slice 1":** delete the fake `StreamStore`/`InMemoryStreamStore` path and bind both orchestrators to real `effect-s2` stream semantics (`S2Client.stream(...)` / `StreamApi`), validated against the official `s2` CLI running `s2 lite`. Scope is exactly that slice — no TableView/Processor/authoring surface yet. (Full scope and gate: Build Plan, slice 1.)

## Guardrails (what not to rebuild)

These are persistent constraints, not status. They encode decisions about what *not* to build.

- **No second transport.** Runtime code calls `S2Client.stream(...)` / `StreamApi` directly at the point of I/O. Do not add an `effect-s2-flow` transport facade (e.g. `stream/S2Stream.ts`) or any duplicate `append`/`read`/`readSession` API wrapping `effect-s2`. Flow may define domain codecs and types; it must not re-export or fake a parallel stream API.
- **No in-repo S2-lite.** Do not add an in-memory S2 substitute under `src`, `test-support`, or package exports. Validate against the official published `s2` CLI running `s2 lite`. The vendored `repos/s2` is reference material only — never a build or validation target.
- **No resurrected legacy.** Do not reintroduce snapshot-generator infrastructure, `src/heyapi-client.ts`, `Channel.ts`, or broad legacy adapter packages. Do not restore `effect-s2-stream-db` / `effect-s2-durable` just to have a destination — they are archived references.
- **No hand-maintained client bag.** The `effect-s2` public surface is *semantic capabilities* over generated protocol + SDK patterns, not a giant manually-mirrored `S2ClientApi`. Do not post-process generated code.
- **Ship vertical slices, not vocabulary.** Implementation PRs are vertical slices that include the thinnest lower-layer declarations needed to prove behavior. Do not ship vocabulary-only wrappers when the adjacent layer is what proves the direction.

---

# Layer -1 — effect-s2 Fidelity

Before building flow semantics, `effect-s2` must stop being a manually-curated mirror of the S2 client surface — the wrapper is the highest-risk place to accidentally narrow upstream semantics. Append sessions, Producer tickets, fencing, dedupe headers, framing/chunking, and exact `u64` handling are exactly the primitives flow needs, and several are not expressible through a broad hand-written `S2ClientApi` without re-inventing them above it.

**Decision: generate the protocol layer; hand-write only the semantics OpenAPI cannot describe.** Generation is the spine. HeyAPI is the parser/plugin orchestrator (it already owns OpenAPI normalization and the symbol pipeline); **local Effect-native plugins** emit the artifacts this repo actually wants.

- **Spec source:** `s2-streamstore/s2-specs` `s2/v1/openapi.json`, pinned to commit `329de93f7b240a4daef9edbeb98ced0699aab7d0`, fetched from the pinned upstream raw URL — **not** a checked-in copy.
- **Generator:** `@hey-api/openapi-ts` with local `effect-schema` and `client-effect` plugins. `client-effect` emits an Effect **HttpApi** contract and derives the client via **`HttpApiClient`** — it must not emit or depend on a bespoke request runtime.
- **Generated output (realized):** `src/generated/effect-schema.gen.ts` (the schema module) and `src/generated/client-effect.gen.ts` (defines `S2Api` as Effect `HttpApi`/`HttpApiEndpoint` groups — including the `read` endpoint's `text/event-stream` success alternative — derives `S2ProtocolClientApi` via `HttpApiClient.ForApi`, and exposes `make`/`layer`). No plain-TypeScript models, no post-processed adapter.
- **Reference only:** Effect's native OpenAPI generator utilities are a useful design reference; the CLI is not this repo's generation path.

**The generated layer is a protocol binding (and drift detector), not the public architecture.** The single semantic surface is `src/S2Client.ts`, grouped around `basins`, `accessTokens`, `locations`, `metrics`, `basin(name)`, and `stream(basin, name)`. The stream capability exposes one-shot `append`/`read`/`readSession`/`checkTail`, scoped `appendSession` and `producer`, command-record constructors (`AppendRecord.fence` / `trim`), and the integrated upstream `@s2-dev/streamstore` patterns (`u64`, chunking/framing, dedupe headers, serializing append sessions, deserializing read sessions). Downstream packages depend on these capabilities, not on generated internals:

```ts
interface S2StreamClient {
  readonly append:    (input: AppendInput) => Effect.Effect<AppendAck, S2Error>
  readonly read:      ...
  readonly checkTail: Effect.Effect<Tail, S2Error>
}
interface AppendSession {
  readonly submit: (input: AppendInput) => Effect.Effect<AppendTicket, S2Error>
}
interface AppendTicket {
  readonly ack: Effect.Effect<AppendAck, S2Error>   // ticketed submit + ack, not "submit and wait"
}
interface SerializingAppendSession<A> {
  readonly write: (value: A) => Effect.Effect<AppendAck, S2Error>
}
```

**Adopt upstream patterns rather than rebuilding them:**

- Ordered / backpressured writes → S2 append session and Producer API.
- Per-record ordered durability → Producer ticket `ack` with exact seqnum.
- Large logical messages → `patterns/serialization` framing / chunking.
- Shared-stream logical dedupe → `injectDedupeHeaders` / `DedupeFilter`.
- Exact `u64` boundary handling → `encodeU64` / `decodeU64`.
- Fencing and CAS → pass `fencingToken` and `matchSeqNum` through **every** append path (one-shot, append session, Producer, serialized writer).

**Recommended physical shape:**

```
packages/effect-s2/src/
  generated/        generated from s2-specs; no manual edits
  transport/        auth, basin/env config, retry, error normalization
  control-plane/    basins, tokens, metrics, stream CRUD
  streams/          append, read, checkTail, fence/trim command records
  sessions/         append session, ticketed submit, Producer
  patterns/         serialization, framing, dedupe headers, u64 codec
  index.ts          curated exports
```

**Conformance gate L-1.** Generation is reproducible from the pinned spec; generated output typechecks and includes the load-bearing operations (`append`/`read`/`checkTail`), Schema declarations, and typed protocol errors. The semantic surface exposes the native behaviors flow depends on: `matchSeqNum`, `fencingToken`, append-session ticketing/backpressure, Producer per-record seqnums, serialization roundtrip, dedupe headers, and `u64` encode/decode. Live/contract tests for those behaviors harden L1/L6; they are not a prerequisite to start L1, and do not justify rebuilding snapshot infrastructure or a wrapper workaround.

---

# Layer 0 — S2 Substrate

Everything builds on six native S2 guarantees. Layer -1 exposes them faithfully through `effect-s2`; flow must not re-invent them above a narrowed wrapper.

| Guarantee | Semantics | Used for |
| --- | --- | --- |
| **Atomic batch** | ≤1000 records / 1 MiB, all-or-none, multi-AZ-durable before ack | atomic commit (Layer 5) |
| **`match_seq_num`** | optimistic CAS; 412 on mismatch; with retry ⇒ exactly-once | idempotent output, checkpoint CAS |
| **`fence` command record** | pessimistic, strongly consistent, **cooperative** (a no-token append still lands), ≤36 B, empty clears | ownership / incarnation (Layer 6) |
| **`check_tail`** | current tail seqnum; cheap, storage-class-independent | linearizable-read barrier (Layer 3) |
| **Append session** | pipelined, submission-ordered, barrier-on-failure; a 412 poisons the session | the ordered writer (Layer 1) |
| **Lease via heartbeat** | model owner identity by periodic in-stream heartbeats; readers confirm with `check_tail` | failover detector (Layer 6) |

Two tiering knobs: **Express (40 ms ack)** vs **Standard (400 ms ack)** storage classes — invocation/inbox streams use Express; cold logs and large views use Standard.

**Load-bearing caveat (cooperative fencing).** A `fence` rejects writers presenting the *wrong* token; a writer presenting *no* token is allowed. Therefore **every owner-write the runtime issues carries the current fencing token**, or the fence protects nothing. The runner enforces this; it is not optional.

**Command records** (`fence`, `trim`) are records with a single empty-name header, seq-numbered and returned to reads, filterable by `headers.length === 1 && headers[0][0] === ""`. Because they are records, they batch atomically with data — which is how a snapshot and its trim land together (Layer 3), and how a fence co-commits with data.

**Conformance gate L0.** The native guarantees are reachable through the Layer -1 semantic capabilities: `append` (with `fencingToken` and `matchSeqNum`), `read`, `checkTail`, faithful append sessions / Producer tickets, serialized/framed logical messages, dedupe headers, and command records. This layer is the contract the rest of the document targets. (Live 412 / session-poisoning / Producer-ordering tests are L1/ownership hardening, not a blocker to begin the orchestrator.)

---

# Layer 1 — The Orchestrator (runtime core)

One instance per stream (= per key). It is the Effect translation of the S2 KV demo's `orchestrate` loop. It owns the materialized state, a command mailbox, a tailing reader, a pending-cursor heap, and an ordered writer. Everything above is a specialization of this engine with a different record-handler.

### 1.1 The concurrency fork (the single most important decision)

| Model | Apply discipline | Linearizable read | Writers | Used by |
| --- | --- | --- | --- | --- |
| **Fenced single-writer** | apply-on-ack (write → ack → apply own write) | trivial (own state) | one, fenced | Processors (Layer 4), exclusive object handlers |
| **Multi-primary** | apply-on-tail (apply only when the record returns on the tailing read) | `check_tail` + defer | many, concurrent | TableViews (Layer 3), shared object reads |

`StreamDb` today is apply-on-ack and correct *only because* an in-process semaphore guarantees one writer — it is **CAS-guarded + locally serialized** (`Semaphore(1)` + `matchSeqNum`), **not yet fenced**; threading a `fencingToken` through its appends is a required change. The KV demo is apply-on-tail because any replica may write concurrently. **You cannot keep apply-on-ack and add a second writer.** The two compose: the fenced owner is the one writer; the view replicas are the many readers of the same stream. This fork resurfaces at every layer — it *is* Restate's `state()`-exclusive vs `sharedState()`-concurrent distinction (Layer 10).

**These are two concrete implementations, not one parametric loop** — a single loop that both applies-on-ack *and* lets its tail reader apply would **double-apply** own records. Build them separately:

- **`OwnedOrchestrator`** (fenced single-writer). Recovers by folding from the last snapshot, then **applies its own writes on ack — but strictly in stream order.** A foreign record can take seq `N` while the owner's write takes `N+1`; applying `N+1` on ack before the tail reader has applied `N` would reorder the fold. The rule: an acked own write at `ack.start.seqNum === applied` is **fast-pathed** (apply immediately → advance `applied`); otherwise it is held in a **pending-own** set and applied only when `applied` catches up to it, and the write's reply `Deferred` (what makes a handler's `set` return) **completes after the ordered local apply, not on raw ack.** So **read-your-writes holds** (the `set` doesn't resolve until its effect is applied) *and* the fold never reorders. The owner still tails for **foreign ingress** (signals, timer-fires, child-completions from other producers) and applies those in tail order; own records are recognized (incarnation header / pending-own seq set) so they are applied exactly once, by whichever path reaches them first. (`ctx.emit` to a *separate* downstream stream is not own-state and does not feed RYW; Layer 4.)
- **`ViewOrchestrator`** (multi-primary). No fence, no own-writes; **applies purely on tail** (already in order); `getStrong` is `check_tail` + defer. N replicas run it concurrently over the same stream. This is the loop sketched in §1.3.

RYW is therefore an `OwnedOrchestrator` guarantee and a deliberate *non*-guarantee of `ViewOrchestrator` reads (a view is eventually-consistent by construction; `getStrong` is its linearizable escape hatch).

**Use the SDK's append primitives — don't hand-roll the writer.** The ordered-writer fiber, batching, and backpressure are exactly what the S2 SDK's append session and Producer API already provide: a session maintains strict ordering of records across batches, and the Producer gives per-record ordered durability with the correct seqNum on each ticket (`ack.seqNum()`), plus built-in backpressure (`maxInflightBytes`, default 5 MiB, blocks `submit()`). So the `OwnedOrchestrator` writer is a thin wrapper over a Producer / append-session — not a re-implementation. The out-of-order risk above is purely an *apply-side* concern (in-memory fold ordering); the SDK already guarantees durable order = submit order.

### 1.2 Idiom mapping (tokio `main.rs` → effect-smol)

| KV demo (tokio) | effect-smol |
| --- | --- |
| `mpsc::UnboundedSender<OrchestratorCommand>` | `Queue` (single consumer = the lock; replaces `Semaphore`) |
| `oneshot::Sender` | `Deferred` |
| `tokio::select!` over command/ack/record | `Stream.merge` consumed by one `forkScoped` fiber |
| `FuturesOrdered` of append tickets | a dedicated ordered writer fiber draining a `Queue` |
| `BinaryHeap<Reverse<seq>>` (`PendingResponses`) | `SortedMap<number, Waiters>`, drained on apply |
| bus-stand `VecDeque` + `sleep_until` | `Request` + `RequestResolver` (batch window) |

### 1.3 Shape

```ts
// runtime/Orchestrator.ts — the ViewOrchestrator loop (multi-primary, apply-on-tail). The OwnedOrchestrator
// reuses this scaffold but (a) applies own writes in the Writer fiber on ack and (b) filters own records in the
// `rec` branch (see §1.1). `pending` strong-reads carry a deadline (cfg.readDeadline) so a stalled tail reader
// can't pin client requests indefinitely. Surfaced via TableView (L3) and Processor (L4).
type Cmd<S> =
  | { _tag: "Write";        records: ReadonlyArray<AppendRecord>; reply: Deferred.Deferred<AppendAck, FlowError> }
  | { _tag: "ReadStrong";   atTail: number; project: (s: S, applied: number) => unknown; reply: Deferred.Deferred<unknown> }
  | { _tag: "ReadEventual"; project: (s: S, applied: number) => unknown; reply: Deferred.Deferred<unknown> }

interface Orchestrator<S> {
  readonly command: Queue.Enqueue<Cmd<S>>                          // many fibers offer; one fiber consumes
  readonly applied: Effect.Effect<number>                          // current applied-prefix cursor
  readonly changes: Stream.Stream<EventRecord<unknown, unknown>>   // post-apply notifications (drives L7 resume)
}

const make = Effect.fn("Orchestrator.make")(function*<S>(opts: {
  readonly basin: string
  readonly stream: string
  readonly initial: S
  readonly reduce: (state: S, record: EventRecord<unknown, unknown>) => S
  readonly fromCursor: number
  readonly fencingToken?: string   // present ⇒ fenced single-writer (apply-on-ack); absent ⇒ multi-primary
}) {
  const command = yield* Queue.bounded<Cmd<S>>(cfg.commandCapacity)   // bounded: backpressure on intake (no unbounded inbox)
  const writes  = yield* Queue.bounded<{ records: ReadonlyArray<AppendRecord>; reply: Deferred.Deferred<AppendAck, FlowError> }>(cfg.writeCapacity)
  const changes = yield* PubSub.dropping<EventRecord<unknown, unknown>>(cfg.changesCapacity)   // slowest-consumer ⇒ drop, never OOM a hot stream
  const appliedRef = yield* Ref.make(opts.fromCursor)
  let state = opts.initial
  let pending = SortedMap.empty<number, Array<Cmd<S>>>(Order.number)

  // ordered writer fiber (FuturesOrdered analog): submit awaits durable ack, FIFO, carries the fence
  const s2 = yield* S2Client.stream(opts.basin, opts.stream)
  const session = yield* s2.appendSession()
  yield* Stream.fromQueue(writes).pipe(
    Stream.runForEach(({ records, reply }) =>
      session.submit(AppendInput.create(records, opts.fencingToken ? { fencingToken: opts.fencingToken } : undefined)).pipe(
        Effect.matchCauseEffect({
          onFailure: (c) => Deferred.failCause(reply, Cause.map(c, toFlowError("write"))),
          onSuccess: (ack) => Deferred.succeed(reply, ack) }))),
    Effect.forkScoped)

  // the select!: commands ⊕ tailing reader, one consumer fiber, owns `state` and `pending`
  yield* Stream.merge(
    Stream.fromQueue(command).pipe(Stream.map((c) => ({ k: "cmd" as const, c }))),
    s2.serialization.readSession(decodeRecord, { start: { from: { seqNum: opts.fromCursor } } }).pipe(
      Stream.map((r) => ({ k: "rec" as const, r }))),
  ).pipe(
    Stream.runForEach((ev) => Effect.gen(function*() {
      if (ev.k === "rec") {
        state = opts.reduce(state, ev.r)
        const applied = ev.r.cursor.seqNum + 1
        yield* Ref.set(appliedRef, applied)
        yield* PubSub.publish(changes, ev.r)                       // L7 suspend/resume subscribes here
        const [ready, rest] = SortedMap.partitionByKey(pending, (k) => k <= applied)
        pending = rest
        for (const [, waiters] of ready) for (const w of waiters) yield* resolveWaiter(w, state, applied)
      } else {
        const applied = yield* Ref.get(appliedRef)
        switch (ev.c._tag) {
          case "Write":        yield* Queue.offer(writes, ev.c); break
          case "ReadEventual": yield* Deferred.succeed(ev.c.reply, ev.c.project(state, applied)); break
          case "ReadStrong":
            ev.c.atTail <= applied
              ? yield* Deferred.succeed(ev.c.reply, ev.c.project(state, applied))
              : (pending = SortedMap.appendAt(pending, ev.c.atTail, ev.c))
        }
      }
    })),
    Effect.forkScoped)

  return { command, applied: Ref.get(appliedRef), changes: Stream.fromPubSub(changes) }
})
```

`check_tail` batching is a resolver — cleaner than the demo's hand-rolled bus-stand:

```ts
// runtime/CheckTail.ts — coalesce concurrent strong reads into one checkTail per window (the "bus-stand")
class CheckTail extends Request.Class<number, FlowError, { readonly basin: string; readonly stream: string }>()("CheckTail") {}
const CheckTailResolver = RequestResolver.makeBatched((reqs: ReadonlyArray<CheckTail>) =>
  Effect.forEach(Array.groupBy(reqs, (r) => `${r.basin}/${r.stream}`), ([_, group]) =>
    S2Client.stream(group[0].basin, group[0].stream).pipe(
      Effect.flatMap((s2) => s2.checkTail()),
      Effect.flatMap((t) => Effect.forEach(group, (r) => Request.succeed(r, t.tail.seqNum)))))
).pipe(RequestResolver.batchN(256))
```

**Conformance gate L1.** Prove the orchestrators against **real S2 stream semantics** exposed by `effect-s2`, not a hand-rolled in-memory store. `ViewOrchestrator`: eventual read, strong read (`checkTail` barrier), tail folding from `readSession`, pending-read timeout, recovery-from-cursor — all reading real S2 records. `OwnedOrchestrator`: read-your-writes after ordered durable ack/apply, own write applied once, ordered pending-own ack handling, fencing-token propagation/rejection, and the foreign-before-own case (foreign at seq `N`, own ack at `N+1` ⇒ no reorder). Live tests run against the official `s2` CLI running `s2 lite`; the vendored `repos/s2` is reference only, and flow must not create a private S2-lite in package code or exported test support.

---

# Layer 2 — Streams

The data-plane vocabulary, built directly on `effect-s2`'s production stream capability (`stream(basin, name)`) plus `Schema` codecs. There is no `Channel.ts` adapter layer — codec helpers are local to flow: encode/decode records at the boundary, then call `StreamApi.append`/`read`/`readSession` or `serialization.*` directly.

```ts
// stream/EventStream.ts — a declaration. Physical stream per key: `${name}/${encode(key)}`.
interface EventStreamDef<K, A> { readonly name: string; readonly key: Schema.Codec<K, string>; readonly value: Schema.Codec<A, string> }

// stream/Record.ts
interface EventRecord<K, A> { readonly stream: string; readonly key: K; readonly value: A; readonly cursor: EventCursor; readonly headers: ReadonlyMap<string, string> }
interface EventCursor { readonly stream: string; readonly seqNum: SeqNum }   // SeqNum: logically S2 Rust `SeqNum(u64)`
// The @s2-dev/streamstore TS SDK surfaces seqNum as `number` (f64) but ALSO ships `encodeU64`/`decodeU64`
// (in patterns/serialization) for exact u64 handling — use those at the boundary rather than depending on the f64.
// Practically a non-issue under per-key sharding anyway: per-stream record rate puts 2^53 at millennia. Prefer the
// SDK u64 codec; flag upstream; do NOT gate.

// stream/Source.ts — external → stream
interface SourceDef<K, A> { readonly name: string; readonly output: EventStream<K, A>; readonly run: (sink: EventSink<K, A>) => Effect.Effect<void, FlowError, Scope.Scope> }
// stream/Sink.ts — stream → external, with a declared guarantee
interface SinkDef<K, A> { readonly name: string; readonly input: EventStream<K, A>; readonly guarantee: Guarantee; readonly run: (source: EventSource<K, A>, checkpoint: CheckpointStore) => Effect.Effect<void, FlowError, Scope.Scope> }
```

**Two write paths, by ownership** (this distinction recurs everywhere):

- `Flow.submit(stream, key, value)` — **unfenced** append. Multi-producer command/inbox streams (clients submitting work, signals, child requests). Ordered by S2 seqnum, like the demo's multi-primary writes. No token.
- `ctx.emit(output, value)` inside a `Processor` — **fenced, guarded** append. The processor is the single active writer for its key; emits carry the fence and, under `effectivelyOnce`, a dedup key — positional `match_seq_num` when the output is the owner's own stream, an explicit `logicalId` when it is a shared downstream (Layer 4).

Legacy `ChangeMessage` is **not** part of flow. Flow streams carry domain facts in flow's own typed event envelope; flow never imports the old table-changelog protocol.

**Conformance gate L2.** Schema encode/decode errors are typed (`FlowError reason="decode"`); missing and empty streams resolve consistently (a 404 vs a tail-0 416 both mean "nothing to fold"); `matchSeqNum` / `fencingToken` options pass through to `effect-s2` on every guarded owner write.

---

# Layer 3 — State Materialization (TableView)

The orchestrator, fold-only, exposed as a `Schema`-typed service. This is the KV demo's read path generalized over the reducer; it replaces the old stream-db materialization direction (do not restore that package to get a table abstraction).

```ts
// table/TableView.ts
interface TableViewDef<K, V> {
  readonly name: string
  readonly source: EventStream<unknown, unknown> | ReadonlyArray<EventStream<unknown, unknown>>
  readonly key: (record: EventRecord<unknown, unknown>) => K
  readonly reduce: (state: HashMap.HashMap<K, V>, record: EventRecord<unknown, unknown>) => HashMap.HashMap<K, V>
  readonly storageClass?: "express" | "standard"   // default standard for views
}
interface TableView<K, V> {
  readonly get:        (key: K) => Effect.Effect<Option.Option<V>, FlowError>   // eventual (local apply-prefix)
  readonly getStrong:  (key: K) => Effect.Effect<Option.Option<V>, FlowError>   // linearizable: checkTail → defer until applied ≥ tail → read
  readonly refresh:    Effect.Effect<number, FlowError>                         // fold through at least the current tail; return that cursor
  readonly entries:    Effect.Effect<ReadonlyArray<readonly [K, V]>, FlowError>
  readonly changes:            Stream.Stream<readonly [K, V], FlowError>        // future changes only (Pulsar forEachAndListen)
  readonly snapshotAndChanges: Stream.Stream<readonly [K, V], FlowError>        // current map, then follow
}
```

- `getStrong`/`refresh` issue `Effect.request(new CheckTail({ stream }), CheckTailResolver)`, then submit a `ReadStrong` whose `atTail` is the returned cursor; the loop blocks it in `pending` until `applied ≥ atTail`. This is the demo's `reflect_applied_state` discipline, exactly.
- `changes` = `Stream.fromPubSub(orchestrator.changes)` projected to `[key, value]`.
- **Compaction** = `Checkpoint` (snapshot-start + entries + snapshot-end + `trim`, one atomic batch, durable-before-trim). S2 has **no key-compaction** — this snapshot+trim *is* the compaction; cold start folds from the last snapshot (`fromCursor` = snapshot point), so snapshot cadence is a real tuning knob.
- **Memory bound = per-key-stream sharding.** A view over a per-key stream holds one key's history; a view over a shared multi-key stream must be cardinality-bounded or use snapshot-only point lookups. **Never a global cross-key view** — that is the OOM cliff.
- **Multi-primary**: apply-on-tail is mandatory here (this surface does not assume a single writer), which is why N nodes each run the runner and serve reads of the same view concurrently.

```ts
// table/Checkpoint.ts — snapshot the live set at tail + trim before it, one atomic batch
const checkpoint = (stream: string) => Effect.gen(function*() {
  const cursor = yield* currentTail(stream)
  const entries = liveEntries()
  const records = [ snapshotStart(cursor), ...entries.map(asInsert), snapshotEnd(cursor), AppendRecord.trim(cursor) ]
  const s2 = yield* S2Client.stream(basinFor(stream), stream)
  yield* s2.append(AppendInput.create(records, { matchSeqNum: cursor }))  // trim lands iff snapshot does
})
```

**Conformance gate L3.** A linearizable strong-read immediately after a concurrent write reflects it; cold-start fold from a snapshot equals full-replay state; `changes` delivers every applied mutation once.

---

# Layer 4 — Processing (Functions)

A `Processor` is the orchestrator with a handler spliced into the record path and a guarded-output commit. It implements Pulsar's Function model. **Outputs live in the handler's R channel as typed services**: a handler physically cannot emit to an undeclared stream — emit is type-checked against the declaration, no string routing.

```ts
// processor/Processor.ts
interface ProcessorDef<K, In, Out extends Record<string, EventStream<any, any>>, St extends Record<string, TableView<any, any>>, R> {
  readonly name: string
  readonly input: EventStream<K, In>           // the per-key ordered inbox (In may be a Schema.Union)
  readonly outputs: Out                          // become typed emit services in R
  readonly state?: St                            // read-side views available to the handler
  readonly guarantee: Guarantee
  readonly keyAffinity?: (record: EventRecord<K, In>) => K   // default: record.key
  readonly retry?: ProcessorRetryPolicy
  readonly handler: (record: EventRecord<K, In>, ctx: ProcessorContext<Out, St>) =>
    Effect.Effect<void, ProcessorFailure, R | OutputServices<Out>>
}
interface ProcessorContext<Out, St> {
  readonly emit:  <N extends keyof Out>(output: N, value: ValueOf<Out[N]>) => Effect.Effect<void>
  readonly state: { readonly [N in keyof St]: St[N] }   // view reads, pinned to the owner's applied state
  readonly cursor: EventCursor                            // the input record's cursor
  readonly attempt: number
}
```

**Execution (per inbound record).** The runner runs `handler`, buffering its own-journal emits + checkpoint movement, then writes them as **one atomic append batch on the owner's own stream** (Layer 5 — there is no transaction resource, just the atomic batch). Reads inside the handler (`ctx.state.*`) are the owner's **apply-on-ack** state (RYW, §1.1).

**Dedup is positional only where position is deterministic.** `match_seq_num` is a CAS on the *current tail*, **not** a logical append-if-absent on an output id. It yields effectively-once **only** for a stream whose sole writer is this owner *and* whose replay re-issues the identical record sequence — i.e. the owner's **own journal** (the `StepCompleted`/`StateChanged` records of Layer 9): record K's `match_seq_num` is the position after K-1, so a replayed re-append 412s. For an output to a **separate downstream stream shared with other producers**, the tail is moved by those producers and `match_seq_num` gives *no* uniqueness — that path needs an explicit **logical dedup key**, and the S2 SDK ships it: the `patterns/serialization` dedupe headers (`injectDedupeHeaders(writerId, dedupeSeq)` on write + `DedupeFilter` on read) tag each record with a `(writerId, monotonic-seq)` pair consumers drop duplicates by. Use that, or route to a per-`(owner,key)` output stream so the owner is again the sole writer. Each output declares which mode it is in; raw `match_seq_num` is never the shared-stream answer.

**Processing guarantees** — a function of two knobs S2 gives natively: *when the checkpoint commits* and *whether the output is guarded*.

| Guarantee | Loop behavior | Outcome |
| --- | --- | --- |
| `atMostOnce` | commit checkpoint **before** running handler | crash ⇒ record skipped |
| `atLeastOnce` | handler → emit → **then** checkpoint | crash ⇒ reprocess, output may duplicate |
| `effectivelyOnce` | at-least-once **+** guarded emit (deterministic id) | replay 412s ⇒ one durable output per input |

`effectivelyOnce` is exactly-once for *durable outputs* — by positional dedup on the owner's own journal, or a logical dedup key for shared downstream (above); never raw tail-CAS on a shared stream. For *external side effects* it is at-least-once + the provider's idempotency key — always. The guarantee is a `Layer`: checkpoint-vs-emit ordering sets at-most/at-least; the dedup mechanism sets the *once*.

**Multi-source inputs.** Pulsar Functions consume from multiple input topics, so flow supports multi-input too — but the two surfaces differ on ordering, and the difference is load-bearing:

- **Durable-execution default = funnel.** For per-key durable execution (objects/workflows), a key's logical sources (commands + timer/promise events + child completions — Layer 7) are written *into* the one per-key inbox upstream, so the handler sees a single ordered `In = A | B | C` union and `Match`es. One stream ⇒ one cursor ⇒ a total per-key order; required for deterministic replay.
- **General processing = first-class multi-input.** A `Processor` may declare `input: ReadonlyArray<EventStream>` for order-insensitive processing. Per-input order is preserved; cross-input interleaving is non-deterministic (N independent cursors). Permitted only when the handler is commutative across inputs or `effectivelyOnce` makes re-order immaterial — never when replay correctness depends on cross-input order (funnel those). The declaration's cardinality makes the choice explicit at the type level.

**Conformance gate L4.** Replay produces one durable output (`effectivelyOnce`); `atLeastOnce` dupes; `atMostOnce` skips; a handler failure moves no checkpoint; `ctx.emit` to an undeclared output is a type error; a multi-input processor preserves per-input order and a single-input processor sees a total order.

---

# Layer 5 — Atomicity (the batch is the unit; there is no coordinator)

S2 has **no transaction concept**: "batches are the atomic unit; each append writes exactly one batch" (≤1000 records / ≤1 MiB, all-or-none, durable-before-ack). The KV store achieves durability + linearizability + multi-primary using single-batch atomic appends and nothing else — no coordinator, no 2PC, no `Transaction` resource. flow inherits exactly that: **the only way to make N records atomic is to put them in one append batch on one stream.** There is no `Transaction.append/commit/abort` lifecycle and no transaction object; "a transaction" is just the record array a Processor's owned commit already assembles.

```ts
// transaction/AtomicBatch.ts — not a resource, just the budget-enforced atomic append.
// The runner assembles {emits-to-own-journal, checkpoint-record} and appends them as ONE batch.
const commitAtomic = (stream: string, records: ReadonlyArray<AppendRecord>, opts: { matchSeqNum?: SeqNum; fencingToken?: string }) =>
  records.length > 1000 || sizeOf(records) > MiB
    ? Effect.fail(new BatchTooLarge({ records: records.length }))   // typed, at assembly time — not a runtime 412
    : S2Client.stream(basinFor(stream), stream).pipe(
        Effect.flatMap((s2) => s2.append(AppendInput.create(records, opts))))     // atomic: all-or-none
```

**The batch limit is the isolation boundary, not a fan-out cap.** Because reads are linearizable and the atomic unit is the batch, a `TableView` can never fold a *partial* batch — "uncommitted invisible until commit" holds **only** while an atomic group is exactly one batch. A group that would exceed the budget cannot silently split into multiple appends (each atomic alone, not together — a view could then fold a half-applied group); the assembly helper **fails with a typed `BatchTooLarge`** so an over-budget atomic group is *unconstructible*. Exceeding it is then an explicit modeling choice (a saga, or accepting non-atomic idempotent steps), never a silent 412.

**Checkpoint lives on the owner's own stream; downstream outputs are idempotent, never the checkpoint source.** The atomic group is *always* `{own-journal records + checkpoint cursor}` co-located on the **owner's own stream** — so restart recovers the input cursor from the owner's own journal, no cross-stream read. Emits to **separate downstream streams** are not in the checkpoint batch (different stream); they are independent idempotent appends with a dedup key (Layer 4), at-least-once + dedup, decoupled from the checkpoint.

**Cross-key atomicity is out of v1** (the only genuinely open item). The per-key stream is the atomic unit; a transactional-outbox + read-committed path is built only against a real cross-key-atomic case, and carries the isolation question (does a fold see uncommitted records?) — answered then, not now.

**Conformance gate L5.** All-or-none batch under crash; an over-budget group fails at assembly (`BatchTooLarge`), never splits; checkpoint + own-journal emits are one atomic append on the owner's stream; a downstream emit replayed twice is deduped, not double-applied.

---

# Layer 6 — Ownership & Activation

Three of Pulsar's four subscription types are native; all bind to the writer side via one fence/lease primitive both the fenced and multi-primary surfaces consume.

**Ownership is the `y-s2` distributed-mutex recipe** (validated by S2's own production design), not a bespoke scheme. The lease is the source of truth for single-writer; placement (below) is only a contention-reduction optimization.

- **Exclusive / Failover** = a **lease via fencing token**. The token is `"{uuidBase64} {deadlineEpochSec}"` (≤36 B): a unique holder id plus a deadline. To claim, a contender appends a `fence` command record carrying its *known current* token; if another holder won, it 412s. **The deadline is auto-expiry** — no separate heartbeat stream is needed: the holder re-fences with a fresh deadline before expiry to renew, and a challenger that reads an expired token may claim. **Every owner-write carries the current token** (mandatory — fencing is cooperative; a no-token write still lands). A stale owner's next tokened write 412s and poisons its session. The fence value is the incarnation. Checkpoint co-commits **fence-reset + snapshot + `trim` in one atomic batch** (Layer 3) — y-s2's exact move, keeping ownership and state consistent; multiple contenders may race a checkpoint but only one commits.
- **Key_Shared** = per-key stream + placement. Placement decides which node should attempt the lease for a key, to reduce contention — but it is **advisory**, because the S2 fence is authoritative regardless. Two options: (a) **S2-native (default)** — skip explicit placement; whichever node receives work for a key attempts the lease and the fence arbitrates (exactly what y-s2 does). (b) **`LayerMap.Service`** keyed by entity id for single-node dynamic per-key instances (instantiate-on-incoming-event, evict-on-idle — needed by §7.2 durable suspend). Distributed managed placement is a later, additive option (see the cluster note).
- **Shared** (competing consumers, round-robin) = the gap, off the durable-execution critical path.

**`effect/cluster.Entity` is deliberately not adopted.** It gives an addressable id → shard → single-active-runner with `maxIdleTime` eviction and a `CurrentAddress` accessor — superficially attractive for Key_Shared placement. But its protocol is an `RpcGroup` and its persistence is **RPC-message-level**: a durable mailbox / dedup of in-flight requests, `KeepAliveRpc` marked `Persisted`+`Uninterruptible`, resume-chunk-sequencing after restart — **not an event-sourced journal of handler steps**. Building on it means two transports and two durability models (cluster RPC *and* the S2 stream). The S2 fence + per-key stream already deliver single-active-writer with the journal as the transport. If distributed managed placement is later wanted, cluster `Sharding` can sit **advisorily** on top — its `toLayerQueue` mailbox feeding routing/wake signals into the S2 orchestrator, the S2 fence remaining authoritative — but that is additive, never foundational.

```ts
// ownership/Assignment.ts
export const assignment = {
  s2Native: /* default: no placement; the lease/fence arbitrates per key (y-s2 model) */,
  layerMap: /* LayerMap.Service: key → orchestrator instance, one node, evictable (durable-suspend) */,
  // clusterSharding: optional, advisory placement only — S2 fence stays authoritative
}

// ownership/Lease.ts — the y-s2 token: identity + auto-expiry deadline, ≤36 B
const leaseToken = (holder: string, deadlineEpochSec: number) => `${base64(holder)} ${deadlineEpochSec}`
```

**The fence is single-writer *on the stream*, not single-executor *in the world*.** Claiming the fence stops the old owner's next tokened S2 write (412 + session poison) — it does *not* stop an in-flight handler that is mid-external-call. In the window between losing the lease and discovering the fence on its next write, a deposed owner can still complete an external side effect (the duplicate email). This is exactly why external effects are **at-least-once + idempotency key, always** (Layer 4): the fence protects durable state, the idempotency key protects the outside world. Failover narration should say "logically fenced," not "stopped."

**Conformance gate L6.** Failover resumes from the last checkpoint; a stale fenced writer is rejected and its session poisons; a lease auto-expires at its deadline and a challenger claims; fence-reset + snapshot + trim commit atomically; under `layerMap`, an evicted key re-instantiates on the next incoming record. Validate with the **linearizability checker** (Build Plan).

### 6.x Activation & lifecycle (wake · evict · rehydrate)

Per-key physical streams are unlimited in S2, but **you cannot hold millions of live orchestrators** — each is a fiber + tailing reader + append session. Placement answers *which node* owns a key; it does *not* answer *when an owner exists*. Two gaps live here and are answered together: (a) child dispatch (§7.3) and durable suspend (§7.2) both assume "someone notices an ingress write to a key," but if the owner is evicted, nobody is tailing; (b) idle eviction + rehydration cost is unspecified.

**Wake-on-ingress (the activation trigger).** An owner is **demand-instantiated**. Because no one tails a dormant key's stream, the **write path carries the wake**: `Flow.submit(stream, key, …)` to a key with no live owner pokes the **Activator**, which instantiates the `OwnedOrchestrator` for that key (claiming the lease, §6) before/as the record lands. This is what a message broker / the Restate server does — route an inbound message to a (possibly cold) virtual object. v1 = single-node `Activator` over `LayerMap` (submit checks the map; miss ⇒ build). Distributed = a placement service, or cluster `Sharding` advisorily, with the S2 fence still authoritative. **No global tailing of "the namespace" is ever required** — activation is edge-triggered by the submit, not by observing all streams.

**Eviction + rehydration is one coupled knob with failover.** An idle owner (no command and no foreign ingress for `maxIdleTime`) checkpoints (snapshot + fence-reset + trim, §3) and tears down. Rehydration on the next touch is **cold-fold-from-last-snapshot — the same path and cost as failover recovery.** Therefore snapshot cadence, failover SLA, and idle-eviction-rehydration latency are **a single tuning axis**, not three: a denser snapshot makes failover *and* rehydration cheaper at higher steady write cost. Durable-execution access is sparse and bursty per key, so this is load-bearing. The durable-suspend default (§7.2) is just eviction triggered by an *await* rather than by *idle*; the wake that resumes it is the resolving fact's `Flow.submit` hitting the Activator, identical to any other ingress.

```ts
// ownership/Activator.ts — demand instantiation; the submit path is the only trigger
interface Activator {
  readonly ensureOwner: (stream: string, key: string) => Effect.Effect<void>   // idempotent: build if absent (claims lease), else no-op
  readonly evictIdle:   (maxIdleTime: Duration) => Effect.Effect<void>          // checkpoint + tear down dormant owners
}
// Flow.submit wraps ensureOwner: an ingress write to a cold key wakes its owner before returning.
```

**Conformance gate L6.x.** A submit to a cold key instantiates its owner and the record is processed; an owner idle past `maxIdleTime` checkpoints and tears down; a durably-suspended invocation is re-instantiated by the resolving fact's submit and resumes by replay; rehydration latency equals failover-recovery latency for the same snapshot cadence.

---

# Layer 7 — Durable-Execution Extensions

flow as specced (Layers 0–6) is a stream processor. Three additions turn it into a durable-execution substrate. None touch the orchestrator core — they are additions at the Processor-runner and host level. (The archived `effect-s2-durable` package is prior-art reference; the active path is to rebuild these semantics on flow after L1–L7 are in place.)

The shared shape: durable waiting is **fact-driven**. A handler that awaits something unresolved parks; the resolving event is journaled onto the invocation stream; folding it resumes the handler. The three extensions are three sources of resolving facts (timers, promises/signals, child results) plus the runner machinery that parks and resumes.

## 7.1 TimerService

**Purpose.** Back `sleep(name, duration)` and any delayed delivery. flow is event-driven; a timer needs a durable mechanism to append a `TimerFired` fact at a future wall-clock time.

**Event model — the invocation journal is authoritative; the timer-shard record is a derived projection** (rebuildable from the journal), not an independent source of truth:

```ts
// on the invocation stream (the journal) — AUTHORITATIVE:
TimerSet   = { _tag: "TimerSet";   name: string; fireAt: number }    // the durable intent; fireAt journaled here
TimerFired = { _tag: "TimerFired"; name: string }                    // delivered by the TimerProcessor at fireAt
// in the TimerProcessor shard — DERIVED (idempotent upsert keyed by timerId; reconcilable from TimerSet facts):
TimerRegistration = { timerId: string; fireAt: number; target: { stream: string; key: string; name: string } }
```

`timerId` is deterministic: `${invocationStream}/${name}`. `name` defaults to journal position (same determinism rule as `run`, Layer 9).

**Mechanism.** Making the driver record *derived from invocation facts and idempotently re-ensured* closes the cross-stream gap where a runner writes `TimerSet` and then crashes before a separate required timer write — leaving a pending sleep with no driver record. The driver registration is never an independent fact that can desync:

1. `sleep` appends `TimerSet(name, fireAt)` to the **invocation stream** — the durable intent, the only required write. `fireAt` is journaled here (so replay doesn't recompute `now()+d`).
2. The owner then **ensures** the driver knows the timer: an idempotent upsert (keyed by `timerId`) to the `TimerProcessor` shard. This is a cache the owner can always rebuild from its own journal, not a second source of truth.
3. **Eviction is gated on registration being durable.** The durable-suspend path (§7.2) tears the owner down only *after* the driver upsert acks — so an evicted owner's timers are always known to the driver. The only crash window (between the `TimerSet` and the upsert) is one in which the owner has *not yet evicted*, so on restart it replays and **reconciliation** runs: for every `TimerSet` in the journal without a matching `TimerFired`/`TimerCanceled`, re-ensure the upsert (idempotent). No timer is ever lost, and none depends on a write that could vanish independently of the journal.
4. The `TimerProcessor` folds its registrations into a min-heap by `fireAt`, `Effect.sleep`s to the head, then `Flow.submit(target, TimerFired(name))` to the invocation stream (an unfenced ingress that wakes the owner via the Activator, §6.x). `TimerFired` is idempotent (guarded by `timerId`), so a re-fire after failover delivers once.

```ts
// runtime/TimerService.ts — used by the runner, not the author
interface TimerService {
  readonly ensure:    (timerId: string, target: TimerTarget, fireAt: number) => Effect.Effect<void, FlowError>  // idempotent upsert; reconciled from TimerSet facts
  readonly reconcile: (journal: ReadonlyArray<TimerSet>, fired: ReadonlySet<string>) => Effect.Effect<void>     // re-ensure unfired timers on (re)activation
  readonly cancel:    (timerId: string) => Effect.Effect<void, FlowError>
}
```

**S2 timestamps.** Best-effort wall-clock; granularity bounded by the driver's wake resolution — S2 has no native delayed delivery, so the driver fiber is required. S2 record timestamps (ms since epoch, GA) are used two ways: (1) `fireAt` is journaled as the `TimerSet` record's timestamp (client-specified via `timestamping.mode`), so replay reads the recorded `fireAt` rather than recomputing `now()+d` — the determinism guarantee for `sleep`; (2) recovery seeks the timer shard by timestamp (`read --timestamp` / `--ago`) to bound the re-fold scan. **Monotonicity caveat:** S2 forces per-stream timestamp monotonicity, so a later-appended timer with an earlier `fireAt` would be clamped forward — therefore the schedulable `fireAt` lives in the record **body**, and the heap orders by that field. A timer that should already have fired (replay/failover past `fireAt`) fires immediately. A canceled timer (handler completed/interrupted before `fireAt`) delivers harmlessly — the fold ignores a `TimerFired` whose await is gone.

**Conformance gate L7.1.** A `sleep` survives host kill before `fireAt` (re-fold fires it); `TimerFired` is idempotent across failover; a canceled timer does not resume a completed invocation.

## 7.2 Runner suspend/resume hook

**Purpose.** A handler that awaits an unresolved durable future — a `deferred`/`awakeable`/`signal` not yet resolved, a child result not yet arrived, a timer not yet fired — must **park** (not busy-wait, not fail) and **resume** when the resolving fact folds onto the invocation stream. This is the heart of durable execution and the unifier of its two execution modes.

**Two execution modes, one mechanism.**

- **Suspended (in-incarnation).** The handler fiber is alive. The await creates an Effect `Deferred`, registers it in a per-invocation `DeferredRegistry` keyed by the await's identity, and `yield* Deferred.await`. The orchestrator's apply loop, on folding a resolving fact, looks the key up and completes the `Deferred` → the fiber resumes at the await. Cheap, fast, no re-run.
- **Replay (failover / long wait).** A new owner re-folds the stream; the handler re-runs from the top; each `run(name)` returns its recorded value (Layer 9); when it reaches an await whose resolving fact is already folded, the await returns immediately; an await whose fact is absent re-parks (transition back to suspended mode in the new incarnation).

These are equivalent because **the resolving fact is always journaled** — parked-in-memory or torn-down-and-replayed, it resumes at the same await with the same value. **Durable suspend is the default.** Durable-execution waits are human-in-the-loop and routinely span days or weeks, so on an unresolved external await (or a long `sleep`) the runner tears the handler down and evicts the orchestrator (via `layerMap`/placement, Layer 6): zero resources consumed while waiting (the Restate FaaS-suspension property), re-instantiated by replay when the resolving fact arrives. In-memory park is the **bounded-wait optimization only** — reserved for short, known-short awaits (a child call expected back in ms) where tear-down + replay would cost more than it saves. The threshold is a runner policy (per-await-kind, or a max in-memory-park duration); the author sees neither mode.

```ts
// runtime/Suspension.ts
type AwaitKey =
  | { _tag: "promise"; name: string }
  | { _tag: "child";   childId: string }
  | { _tag: "timer";   name: string }

interface DeferredRegistry {
  // park (or, in replay mode, return immediately if the resolving fact is already folded)
  readonly await:    <A>(key: AwaitKey, decode: (folded: Option.Option<unknown>) => ResolveOutcome<A>) => Effect.Effect<A, DurableError>
  // called by the orchestrator apply loop on each resolving fact
  readonly complete: (key: AwaitKey, value: unknown) => Effect.Effect<void>
}
```

The registry lives in `CurrentInvocationScope` (Layer 8). The runner subscribes `registry.complete` to the orchestrator's `changes` stream, filtered to resolving fact types. The await's identity must be deterministic (promise `name`, `childId`) so replay re-registers under the same key.

**Serial-drainer interaction (virtual objects).** For an object key, exclusive calls are drained serially. If the resident exclusive call parks, subsequent exclusive calls queue (the drainer does not interleave them) — matching Restate's one-exclusive-at-a-time semantics. A long park therefore durably suspends (tear down + evict) so the key is not held in memory; the next exclusive call, or the resolving fact, re-instantiates the owner. Shared reads (`getStrong`) are unaffected — they run against the multi-primary view concurrently.

**Journal safety.** The await itself is not a journal entry; the resolving fact is. On replay the fold reconstructs which awaits are resolved. Parking and resuming perform no observable writes — only the resolving fact (written by the timer service, a signal, a child completion, or an external resolve) is journaled.

**Conformance gate L7.2.** A handler parked on a `deferred` resumes when resolved (in-memory); the same handler, host-killed and restarted, replays to the await and continues if the fact arrived, else re-parks; a durably-suspended (torn-down) handler re-instantiates on the resolving fact and continues; a parked exclusive object call queues subsequent exclusive calls and does not interleave them.

## 7.3 Child-call correlation

**Purpose.** A parent handler calling `serviceClient(S).m(x)` or `objectClient(O, k).m(x)` must (1) trigger the child as its own durable invocation, (2) route the child's terminal result back, (3) stay journal-safe (the call is a durable fact; replay does not re-trigger).

```ts
// on the PARENT stream (the journal):
ChildRequested = { _tag: "ChildRequested"; childId: string; target: InvocationRef; method: string; input: unknown }
ChildCompleted = { _tag: "ChildCompleted"; childId: string; result: Result<unknown> }   // routed back to the parent
// the Invoke envelope the child receives carries a reply address:
Invoke = { method: string; input: unknown; replyTo?: { stream: string; key: string }; childId?: string; idempotencyKey?: string }
```

`childId` is deterministic: `${parentStream}/${name-or-journalPosition}` — same determinism rule as `run`. Replay reconstructs the same `childId`; **correctness comes from the child's idempotent admission on `childId`, not from replay suppressing dispatch.**

**Mechanism.**

1. The parent handler appends `ChildRequested(childId, target, method, encode(input))` to its own stream. This fact records the parent's *intent* (so replay reconstructs the same `childId`) — it does **not** suppress re-dispatch.
2. **Dispatch (v1: inline via the Activator; no namespace observer), re-issued idempotently on every replay until the parent observes either the child's admission ack or `ChildCompleted`.** Per-key physical streams mean there is no single "namespace" topic to tail, so dispatch is inline: the parent runner `Flow.submit`s to the child's stream `Invoke{ method, input, replyTo, childId, idempotencyKey: childId }`, waking the child's owner via the Activator (§6.x). Re-dispatch is **safe and required** — the first dispatch may have been lost (crash between journal and submit), so the parent keeps dispatching until it sees the child accepted; and re-dispatch is **harmless** because the child's run-once admission keys on `idempotencyKey = childId` (`insertOrGet`), so N dispatches admit the child **once**. (Deferred scale-out: a sharded dispatch outbox `flow.dispatch/{shard}` a router tails — centralized routing/flow-control once the activation layer exists. Not v1.)
3. The child runs as a normal invocation. On completion it appends `ChildCompleted(childId, result)` to `replyTo` (the parent stream) — reply address from the `Invoke` envelope. (Fire-and-forget children have no `replyTo` and append nothing back.)
4. The parent's suspend/resume hook (§7.2) parks on `{ _tag: "child", childId }`; when `ChildCompleted(childId, result)` folds onto the parent stream, the parent resumes with the result (or a typed failure if the child failed terminally). Observing `ChildCompleted` (or the admission ack) is also what lets the parent stop re-dispatching.

**Two flavors.** Request-response (`yield* serviceClient(S).m(x)`) parks on `childId`. Fire-and-forget (`serviceClient(S).m.send(x)`) appends `ChildRequested` with no `replyTo` and does not park.

**API** is the authoring-side client (Layer 9/10): `serviceClient(S)` / `objectClient(O, key)` return typed clients whose methods, inside a handler, append the request and park (or send).

**Conformance gate L7.3.** Parent calls child and gets the result; the child runs **exactly once across arbitrarily many parent replays/re-dispatches** (childId admission, not replay suppression); a dropped first dispatch is recovered by re-dispatch; a terminal child failure surfaces as a typed error at the parent's await; fire-and-forget runs without the parent parking.

---

# Layer 8 — CurrentInvocationScope (the seam)

The single point where the durable-execution authoring layer meets flow. The ergonomic surface (Layer 10) needs **no `Durable.*` ceremony** because the free primitives (`run`/`state`/`sleep`/`signal`/`awakeable`/`deferred`) require `CurrentInvocationScope` in their R, and the Processor runner discharges it per invocation.

The swap is clean because the owner-stream model already *is* flow: "one schema-addressed S2 stream per object key, ordered ActorEvent log, serial drainer by seq_num, completion from a Completed event, signals as ingress appends, state as a StateChanged projection, single in-process owner per execution" — that is a flow fenced `Processor` over a per-key `EventStream` with a `TableView` projection, term for term. flow is the extraction of that machinery.

```ts
// the per-invocation service the flow Processor runner provides. The free primitives are static accessors over it.
class CurrentInvocationScope extends Context.Service<CurrentInvocationScope, {
  readonly key:       string             // object/workflow key (execution id for services)
  readonly stream:    OwnerStream         // this execution's fenced writer (the orchestrator's single consumer = the serial drainer)
  readonly steps:     StepView            // journaled StepCompleted facts — the replay boundary for `run`
  readonly tables:    TableBindings        // flow TableViews folded over the same stream — user `state()`
  readonly deferreds: DeferredRegistry     // park/resume (§7.2) for signal · awakeable · deferred · child
  readonly timers:    TimerService         // sleep (§7.1)
  readonly children:  ChildEmitter         // serviceClient · objectClient (§7.3)
  readonly mode:      "live" | "replay"    // suspended-incarnation vs failover-replay (§7.2)
}>()("durable/CurrentInvocationScope") {}
```

**The `run`-action boundary is the flow write-edge.** A `run` action's type excludes `CurrentInvocationScope` from its R, so `run(state(Cart).set(…))` is a compile error at the `run` call — the Effect analog of Restate's ctx-less `run` closure. A `run` action is the external-effect edge (at-least-once + idempotency key); it cannot issue durable primitives because the runner does not provide the scope inside it.

**Conformance gate L8.** The free primitives resolve the scope inside a handler and fail to typecheck inside a `run` action; the same handler code runs unchanged whether the runner is in `live` or `replay` mode.

---

# Layer 9 — Durable Primitives

Thin primitives over `CurrentInvocationScope`. `Effect` is the `Operation` (lazy, multi-run); `Fiber` is the `Future` (eager, memoized); `Effect.fork` is `spawn`; `Fiber.interrupt` is targeted cancellation. Effect supplies the entire concurrency engine `restate-sdk-gen` hand-builds — the fiber tree, `wake`/`advance`, the epoch guard, `onMainExit` abandon/join, `contextLocal` (→ `FiberRef`). None is re-implemented.

| Primitive | Lowering |
| --- | --- |
| `run(action, { name?, retry? })` | check `steps` by name (else journal position): present ⇒ return recorded value / replay recorded typed failure, no re-run; absent ⇒ run under the retry `Schedule`, guarded-append `StepCompleted(key, encode(result))` (effectively-once via owner fence + `match_seq_num`), fold. Crash-before-append re-runs (at-least-once). |
| `state(Table).get/set/delete` | binding over the execution stream's fold for that table. `set`⇒`StateChanged` append + fold; `get`⇒read the owned fold (apply-on-ack ⇒ read-after-ack sees the write); `delete`⇒delete change. |
| `sleep(name, d)` | `timers.ensure` ⇒ `TimerSet` (ack) ⇒ park ⇒ `TimerFired` folds ⇒ resume (§7.1). |
| `deferred(name)` / `awakeable()` | `DurablePromise` stream + resolution view; `get` folds (parks via `deferreds` if unresolved); `resolve`/`reject` appends `Resolved`/`Rejected` (idempotent, first wins). |
| `signal(name, payload)` | unfenced `Flow.submit` to the target stream (ingress append) — folded by the target's drainer, no resident call required. |
| `serviceClient(S).m` / `objectClient(O,k).m` | `ChildRequested(childId)` + dispatch + park on `childId` ⇒ `ChildCompleted` resumes (§7.3). `.send` = request without park. |
| `all(fs)` / `allSettled(fs)` | pure `Effect.all` / `Effect.forEach` — each constituent `run` journals itself; no decision entry; outcome is input-order + journaled constituents. |
| `race(fs)` / `select(branches)` / `any(fs)` | journaled-decision combinators: raw `Effect.race` picks a winner by wall-clock, so replay could diverge — these record a `Decision(winner)` fact so replay re-takes the branch. |
| `spawn(op)` | `Effect.fork` + a journal-path `FiberRef` bump (deterministic step keys inside spawned fibers: key = `${fiberPath}/${name}`, `fiberPath` = parent path + spawn index). |
| cancellation | a typed `Cancelled` error in the Effect error channel (recoverable: `catchTag("Cancelled", …)` then yield more durable steps ⇒ journaled cleanup ⇒ recover) + `Fiber.interrupt` for in-flight `run` I/O abort. Effect runs finalizers on interrupt; finalizers performing durable steps route through the typed-error path. |

**Determinism — keyed journal matching, not issue-position.** Restate matches journal entries positionally, needing a deterministic *issue order* its custom scheduler guarantees. Effect's fiber scheduler does not guarantee issue order across concurrent fibers, so flow matches by **deterministic key**: every durable step carries a stable name (`run("reserve")`), the journal is keyed by it, and concurrent steps landing in any stream order replay correctly. Unnamed steps fall back to journal position and therefore require deterministic control flow (branch only on input + already-journaled results, never wall-clock/random/un-journaled reads) — name the steps to track identity instead of position.

**Conformance gate L9.** `run` returns recorded values on replay and never re-runs; positional keys replay under deterministic control flow; `race`/`select` re-take the journaled branch; `Effect.all` needs no decision entry; cancellation is recoverable and journaled as terminal (`CancelledError`, code 409, not retried).

---

# Layer 10 — Authoring Surface

restate-sdk-gen-shaped: group handlers as **bare generator methods** (`*greet(input) { … }`) — the input is the argument, no `handlerRequest`, no `Effect.gen` wrapper — and call through a typed client that hides the execution id and the submit/attach dance. Inside, `yield* run(...)` etc. stay typed. The runner adapts the generator to an `Effect` and installs `CurrentInvocationScope`.

```ts
export const Flow = { /* Layers 2–6 facade: eventStream, tableView, processor, source, sink, submit, atomicBatch, runners, assignment, Guarantee */ }

// effect-s2-flow/authoring — no Durable.* namespace; free primitives over CurrentInvocationScope
export { service, object, workflow }                       // declarations → flow Processor (+ TableView for objects)
export { run, state, sharedState, sleep, signal, awakeable, deferred }  // free durable primitives
export { all, allSettled, race, select, any, spawn }       // combinators (all/allSettled = pure Effect; race/select/any journaled)
export { serviceClient, objectClient }                     // child calls
export { client, sendClient, attach, poll }                // call surface (hides execution id)
```

| Authoring primitive | flow lowering |
| --- | --- |
| `service({ name, handlers })` | stateless `Processor` over `${name}.invocation/{execId}` — one stream per execution, fenced single-writer |
| `object({ name, handlers })` | per-key fenced `Processor` over `${name}.events/{key}` (the ActorEvent log) + `ObjectStateView` (the StateChanged projection); the orchestrator's single consumer is the serial drainer by seq_num |
| `workflow` | `object` + run-once admission (`insertOrGet` on the run-started event, guarded by workflow id) |
| exclusive vs shared handler | fenced single-writer (apply-on-ack) vs `TableView.getStrong` (apply-on-tail) — the Layer 1 fork; `state()` resolves to the owned fold in exclusive, the view in shared |
| `*method(input)` | the Processor's handler; runner installs `CurrentInvocationScope` and adapts the generator to an `Effect` |
| `client(s).m(x)` | `Flow.submit(invocationStream, execId, Invoke("m", x))` (unfenced ingress) ⇒ `getStrong` the roster view until terminal ⇒ decode |
| `sendClient` / `attach(id)` / `poll(id)` | submit-only ⇒ execId / `getStrong` roster until terminal / view `get` (non-blocking); `idempotencyKey` pins the id via guarded-append admission |
| completion (`return`) | atomic-batch commit result to the roster view, await ack, drop the execution stream, mark `resultAcked` — durable-fact-outlives-destructive-op (snapshot/roster-ack-before-trim) |

The roster/result-outlives-stream invariant is flow's Layer 3 `Checkpoint` discipline (durable-before-trim) applied to invocation completion.

---

# Layer 11 — Worked Examples

The cited surfaces, byte-for-byte the authoring code, with the lowering annotated. Only `CurrentInvocationScope`'s implementation moved (from the owner-stream engine to a flow Processor runner); "Effect all the way down" stays literally true.

### 11.1 greeter — service + durable step + retry

```ts
import { Duration, Effect } from "effect"
import { run, service } from "effect-s2-flow/authoring"
import { client } from "effect-s2-flow/invocation"

const greeter = service({
  name: "greeter",
  handlers: {
    *greet(req: { name: string }) {
      const greeting = yield* run(Effect.sync(() => `Hello, ${req.name}!`), {
        retry: { maxAttempts: 3, initialInterval: Duration.millis(100) },
      })
      return { greeting }
    },
  },
})
// service(greeter)         → stateless Processor over greeter.invocation/{execId}, fenced owner
// run(action, { retry })   → steps miss → run under Schedule → guarded-append StepCompleted(pos, …) → fold (replay: recorded value)
// return { greeting }      → roster commit + ack → drop stream → resultAcked
// client(greeter).greet(x) → submit Invoke("greet", x) → getStrong roster until terminal
```

### 11.2 basics — combinators (pure Effect vs journaled)

```ts
import { service, run, all, race, select } from "effect-s2-flow/authoring"

const basics = service({
  name: "basics",
  handlers: {
    *sequential() {                          // two journaled steps, source order
      const a = yield* run(() => fetchA(), { name: "a" })
      const b = yield* run(() => fetchB(), { name: "b" })
      return `${a}-${b}`
    },
    *parallel() {                            // pure Effect.all: each step journaled by name, NO decision entry
      const [a, b] = yield* all([run(() => fetchA(), { name: "a" }), run(() => fetchB(), { name: "b" })])
      return `${a}+${b}`
    },
    *whicheverFirst() {                      // race: winner recorded as Decision(winner) so replay re-takes it
      return yield* race([run(() => fetchFast(), { name: "primary" }), run(() => fetchSlow(), { name: "secondary" })])
    },
    *knowingWhichWon() {                     // select = race + tag
      const r = yield* select({ fast: run(() => fetchFast(), { name: "fast" }), slow: run(() => fetchSlow(), { name: "slow" }) })
      return r._tag === "fast" ? `fast-won: ${r.value}` : `slow-won: ${r.value}`
    },
  },
})
// stateless Processor; each run(name) = journal-check-then-guarded-append StepCompleted(name, …).
// all → two independent journaled steps; race/select → additionally append Decision(winner).
```

### 11.3 counter — virtual object, exclusive write / shared read

```ts
import { object, state, sharedState } from "effect-s2-flow/authoring"
import { primaryKey, Table } from "effect-s2-flow/table"
import { client } from "effect-s2-flow/invocation"

class CounterState extends Table<CounterState>("counterState")({ id: Schema.String.pipe(primaryKey), value: Schema.Number }) {}

const counter = object({
  name: "counter",
  handlers: {
    *add(amount: number) {                   // exclusive: Accepted(add) drained serially by the orchestrator
      const st = state(CounterState)
      const cur = Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
      yield* st.set({ id: "v", value: cur + amount })   // StateChanged(counterState) append + fold
      return cur + amount                    // → Completed event; client decodes from it
    },
    *value() {                               // shared read: ObjectStateView.getStrong(key), concurrent while no writer holds the key
      const st = sharedState(CounterState)
      return Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
    },
  },
})
// object(counter)           → per-key fenced Processor over counter.events/{key} + ObjectStateView projection
yield* client(counter, "user-1").add(5)    // → 5   (counter.events/user-1)
yield* client(counter, "user-2").value()   // → 0   (different key = different stream = isolated)
```

### 11.4 checkout — durable state + the run-action boundary

```ts
import { service, state, run } from "effect-s2-flow/authoring"
import { primaryKey, Table } from "effect-s2-flow/table"

class Cart extends Table<Cart>("cart")({ cartId: Schema.String.pipe(primaryKey), items: Schema.Array(Schema.String) }) {}

const checkout = service({
  name: "checkout",
  handlers: {
    *go(_req: { user: string }) {
      const cart = state(Cart)                            // names the `cart` fold over THIS execution's stream — synchronous
      yield* cart.set({ cartId: "c1", items: ["apple"] }) // StateChanged(cart) append + fold
      const current = yield* cart.get("c1")               // owned fold (apply-on-ack) — read-after-ack sees the write
      // yield* run(cart.set(…))  ← COMPILE ERROR: a run action's R excludes CurrentInvocationScope (the flow write-edge)
      return current
    },
  },
})
```

### 11.5 blockAndWait — durable promise + suspend/resume (exercises §7.2)

```ts
import { workflow, state, sharedState, deferred } from "effect-s2-flow/authoring"

class WfState extends Table<WfState>("wf")({ id: Schema.String.pipe(primaryKey), input: Schema.String }) {}

const blockAndWait = workflow({
  name: "blockAndWait",
  run: function*(input: string) {
    yield* state(WfState).set({ id: "in", input })
    const done = deferred<string>("done")
    const value = yield* done.get                 // parks the run fiber until resolved (§7.2)
    return value
  },
  handlers: {
    *unblock(output: string) {                    // shared: anyone with the workflow id can resolve
      yield* deferred<string>("done").resolve(output)   // append Resolved("done", output); idempotent, first wins
    },
    *getInput() { return Option.getOrNull(yield* sharedState(WfState).get("in")) },
  },
})
// workflow = object + run-once admission (run-started guarded by workflow id).
// deferred("done") → DurablePromise stream keyed (wfId,"done") + PromiseResolutionView.
// done.get unresolved → Processor parks the run fiber; `unblock` appends Resolved → fold → resume.
```

### 11.6 orchestration — child calls + sleep (exercises §7.1 and §7.3)

```ts
import { service, run, sleep, serviceClient, all } from "effect-s2-flow/authoring"

const saga = service({
  name: "saga",
  handlers: {
    *placeOrder(req: { orderId: string }) {
      // child call: ChildRequested(childId) → dispatch → park on childId → ChildCompleted resumes (7.3)
      const reserved = yield* serviceClient(inventory).reserve({ orderId: req.orderId })
      yield* sleep("settle-delay", Duration.seconds(30))   // TimerSet → park → TimerFired (7.1)
      // fan-out children, pure Effect.all (each child journaled by its childId)
      const [charged, shipped] = yield* all([
        serviceClient(billing).charge({ orderId: req.orderId }),
        serviceClient(shipping).schedule({ orderId: req.orderId }),
      ])
      return { reserved, charged, shipped }
    },
  },
})
// each serviceClient(_).m(x) → guarded-append ChildRequested(childId=`saga.invocation/{exec}/{pos}`) → dispatch (idempotent by childId)
// → child runs as its own invocation → emits ChildCompleted(childId) to saga's stream → parent resumes.
// sleep survives crash (re-fold fires it); replay re-issues child dispatch idempotently until admission/completion.
```

---

# Build Plan (vertical slices, in order)

Each step is gated by validation signal, not by one PR per conceptual layer. Implementation PRs are vertical slices that include the thinnest set of lower-layer declarations needed to prove useful behavior. (Status of each slice lives in **Current Status**, above.)

0. **effect-s2 fidelity** — generate from the pinned `s2-specs` spec via HeyAPI + local Effect plugins; hand-write only the semantics OpenAPI can't represent. Gate: Layer -1.
1. **Corrective vertical slice 1 — orchestrators over real S2.** Remove the fake `StreamStore`/`InMemoryStreamStore` path and exported test-support; bind `ViewOrchestrator`/`OwnedOrchestrator` to `S2Client.stream(...)` / `StreamApi` (append, `readSession`, `checkTail`, append-session/Producer for ordered owned writes, fencing propagation). Gate: Layer 1 — over the official `s2` CLI running `s2 lite`. No `S2Stream.ts` facade, no duplicate stream API, no in-repo S2-lite, no vendored server as the validation target.
2. **TableView read model.** Add only the Layer 2 declarations/codecs needed for Layer 3, then `TableView` fold/read over the real-S2-backed `ViewOrchestrator`. Gate: Layer 2 + Layer 3.
3. **Processor commit path.** `Processor`, typed `ctx.emit`, guarantee layers, and the Layer 5 atomic-batch helper together (the checkpoint/output behavior is the validation signal). Gate: Layer 4 + Layer 5.
4. **Ownership, activation, failover.** Lease/fence token, owner claim/renew, activation on ingress, idle eviction, cold rehydration. Gate: Layer 6 + Layer 6.x.
5. **Durable waits.** `TimerService`, `DeferredRegistry`, apply-loop completion, durable suspend/resume. Gate: Layer 7.1 + Layer 7.2.
6. **Child calls.** `ChildRequested`, inline dispatch via the Activator, admission by `childId`, `ChildCompleted` reply routing. Gate: Layer 7.3.
7. **CurrentInvocationScope + durable primitives.** The runner-provided scope, then `run`/`state`/`sleep`/`signal`/`awakeable`/`deferred` + combinators. Gate: Layer 8 + Layer 9.
8. **Authoring surface + examples.** `service`/`object`/`workflow`/`client` and the worked examples, including crash-restart-without-duplicate-output for the saga and a read-after-emit-on-own-state case. Gate: Layer 10 + Layer 11.

**Linearizability checker (gates the orchestrator slices 1–2, not optional).** Replaying the demo's example trace won't catch a concurrency violation, and linearizability is the headline claim. Generate interleaved `submit`/`read-strong`/`read-eventual` histories and check the strong path is linearizable (eventual no worse than sequentially consistent) with a Porcupine/Knossos-style checker — model the orchestrator + fence as a register `(tail, last_record_hash, fence_token)`, model `match_seq_num`, and set an indefinitely-failed append's end-time after all other ops (the Jepsen gotcha that `match_seq_num`-retry resolves; mirror `s2-streamstore/s2-verification`). This is the gate that *earns* the Layer 1 claim.

---

# Reference — Primitive & Lowering Quick-Reference

**Streams & ownership.** Per-key physical stream `${name}/${encode(key)}`. Cursor `seqNum` is `SeqNum(u64)` (SDK `encodeU64`/`decodeU64`; the f64 `number` is negligible under per-key sharding — flag, don't gate). `OwnedOrchestrator` ⇒ **ordered** apply-on-ack own writes (fast-path only when next; else pending-own; reply after ordered apply ⇒ RYW) + foreign-ingress tail reader; `ViewOrchestrator` ⇒ apply-on-tail + `check_tail`. Ownership = `y-s2` lease/fence token `"{uuid} {deadline}"` (deadline = auto-expiry), authoritative; placement advisory; an owner is demand-instantiated by the **Activator** on ingress. Unfenced `Flow.submit` for ingress/signals/child-requests; fenced `ctx.emit` for processor outputs (positional dedup on own journal, SDK dedupe-headers on shared downstream).

**The resolving facts (what un-parks a handler).** `TimerFired(name)` (§7.1) · `Resolved(name,value)` / `Rejected(name,err)` for promises & signals · `ChildCompleted(childId,result)` (§7.3). All journaled on the invocation stream; folding any of them completes a `DeferredRegistry` entry.

**Determinism.** Named durable steps ⇒ keyed journal matching (concurrency-safe). Unnamed ⇒ positional (requires deterministic control flow). `race`/`select`/`any` ⇒ `Decision(winner)` fact. `all`/`allSettled` ⇒ pure Effect, no decision entry. Spawned-fiber step keys ⇒ `${fiberPath}/${name}` via `FiberRef`.

**Effect ≙ restate-sdk-gen.** `Operation`=`Effect` · `Future`=`Fiber` · `spawn`=`Effect.fork` · `task.interrupt`=`Fiber.interrupt` · `onMainExit:"abandon"`=fork-in-handler-scope · `onMainExit:"join"`=`Fiber.join` · `contextLocal`=`FiberRef` · the epoch guard / `won` flag / stale-waiter pruning = gone (Effect's runtime owns fiber lifecycle). Cancellation = typed `Cancelled` error (recoverable) + `Fiber.interrupt` for I/O.

**Transport.** The control/data-plane protocol is generated from `s2-specs` via HeyAPI + local Effect plugins (not hand-mirrored, not post-processed). Ordered + backpressured writes ⇒ SDK append session / Producer (`maxInflightBytes` backpressure, per-record ordered ack with exact seqNum). Downstream idempotency ⇒ SDK dedupe headers. Large (>1 MiB) messages ⇒ SDK chunking + framing (a framed message spans multiple records and is *not* atomic if it exceeds a batch). Exact seqnums ⇒ SDK `encodeU64`/`decodeU64`. The `effect-s2` public surface is semantic capabilities over those primitives, not a hand-maintained client bag.

---

# Decisions

One line each; the layer named in parentheses is authoritative.

1. **effect-s2 = generated protocol + handwritten semantics.** HeyAPI orchestrates generation from the pinned `s2-specs` spec; local `effect-schema`/`client-effect` plugins emit Effect Schema + an HttpApi contract + an `HttpApiClient`-derived client; the handwritten layer is only what OpenAPI can't encode. (L-1)
2. **Concurrency fork.** Fenced single-writer (apply-on-ack) vs multi-primary (apply-on-tail) are two concrete orchestrators, not one loop. (L1)
3. **Owned writes apply in stream order.** Fast-path on ack only when next; else pending-own; reply after ordered apply ⇒ RYW, fold never reorders. The SDK append-session/Producer is the ordered writer. (L1)
4. **`seqNum` = `SeqNum(u64)`** via SDK `encodeU64`/`decodeU64`; f64 exposure negligible under per-key sharding — flag upstream, don't gate. (L2)
5. **No transaction resource.** The S2 atomic batch is the only atomicity unit; over-budget fails as `BatchTooLarge`; checkpoint co-locates on the owner's own stream; downstream is idempotent dedup. Cross-key atomicity stays deferred (the one open item). (L5)
6. **`effectivelyOnce` dedup.** Positional `match_seq_num` only on the owner's own journal; SDK dedupe headers on shared downstream; never raw tail-CAS on a shared stream. (L4)
7. **Multi-input.** Funnel-into-one-inbox is the durable-execution default (deterministic order); declared multi-input is allowed for order-insensitive/commutative processing. (L4)
8. **Ownership = the `y-s2` lease/fence recipe.** `"{uuid} {deadline}"` token, deadline as auto-expiry; the lease is authoritative, placement is advisory. (L6)
9. **`cluster.Entity` not adopted.** RPC + mailbox persistence is a redundant second transport; cluster `Sharding` is at most advisory placement later. (L6)
10. **Activation = wake-on-ingress via the Activator.** Eviction + cold rehydration is one tuning knob with failover SLA and snapshot cadence. (L6.x)
11. **Durable suspend by default.** Waits span days/weeks ⇒ tear-down + evict + replay-on-resolution; in-memory park is the bounded-wait optimization only. (L7.2)
12. **Timers use S2 record timestamps.** The invocation journal is authoritative; the driver registration is a reconcilable projection (idempotent upsert, re-ensured on activation, eviction-gated). (L7.1)
13. **Child dispatch is inline + idempotent.** Re-issued every replay until admission/`ChildCompleted`; exactly-once from `childId` admission, not replay suppression. (L7.3)
14. **Determinism = keyed journal matching.** Named steps key the journal; `race`/`select`/`any` journal a `Decision(winner)`; `all`/`allSettled` are pure Effect. (L9)
15. **Packaging.** The runtime substrate lives in `effect-s2-flow`; `effect-s2-stream-db`/`effect-s2-durable` are archived. The layering is firm; this package cut is explicit. (Header / Guardrails)
16. **No schema registry service.** `Schema` is the spine; evolution is decode-time migration; a registry-as-stream is a later option. (Non-Goals)

---

# Non-Goals

- No exactly-once arbitrary external side effects — at-least-once + idempotency key, always.
- No transaction coordinator or `Transaction` resource — the S2 atomic batch (one append, one stream) is the only atomicity unit; cross-stream atomicity is idempotent dedup keys, never 2PC.
- No cross-stream atomic transactions in v1 — the per-key stream is the atomic unit.
- No Shared-subscription competing-consumer dispatch — Key_Shared (per-key streams) is native; Shared is not.
- No schema registry service — `Schema` is the spine.
- No re-implementation of a workflow scheduler — Effect is the scheduler; the durable layer is primitives + three flow extensions.
- No broker, worker scheduler, or subscription protocol.

---

# S2 Research Alignment

The design intentionally tracks S2's own published research:

- **Distributed mutex → Layer 6 ownership.** S2's `y-s2` recipe *is* the ownership design: lease via a `"{uuid} {deadline}"` token, cooperative fencing, deadline as auto-expiry (no separate heartbeat), atomic fence-reset + trim on checkpoint. A production-validated pattern; adopting it verbatim removes the bespoke heartbeat machinery and demonstrates explicit placement is optional (the lease arbitrates) — the core argument against `cluster.Entity`.
- **`cluster.Entity` → not adopted.** An `RpcGroup` protocol addressed by id, sharded to a single active runner with idle eviction and a persisted mailbox — but no event-sourced step journal. Its durability is at the message layer; ours is the S2 stream. The placement/eviction *concepts* are worth borrowing; the S2 fence stays the authoritative single-writer guarantee.
- **Timestamping → TimerService.** Monotonic ms-epoch with timestamp-indexed reads and a `timestamping.mode` knob: journal `fireAt` deterministically and bound recovery scans. Forced monotonicity is why `fireAt` is a body field for out-of-order schedules.
- **Linearizability tooling → the validation spike.** Porcupine + deterministic simulation, modeling the stream as a register and extending an indefinite-append-failure's end-time past all ops. The build plan adopts the same against the orchestrator + fence (`s2-streamstore/s2-verification` as reference).
- **Access control & isolation → higher layer.** Per-key-stream isolation is already the structural boundary; S2 scoped access tokens and agent isolation map onto issuing per-namespace/per-key scoped tokens at the invocation boundary, dovetailing with the existing biscuit-scoping / `agent.pw` approach. A deployment/security concern above the runtime — a token-scoping exercise, not an architectural change.

---

# References

- S2 — concurrency control (match_seq_num, fencing): https://s2.dev/docs/concepts/concurrency-control
- S2 — appends (atomic batch, durability, storage classes): https://s2.dev/docs/concepts/appends
- S2 — command records (fence, trim): https://s2.dev/docs/concepts/command-records
- S2 — shared-log KV store (orchestrator, check_tail, bus-stand): https://s2.dev/blog/kv-store · `s2-streamstore/s2-kv-demo`
- S2 — distributed mutex / leases (ownership recipe): https://s2.dev/blog/durable-yjs-rooms#a-distributed-mutex
- S2 — timestamping (TimerService): https://s2.dev/blog/timestamping
- S2 — linearizability testing (Porcupine + DST): https://s2.dev/blog/linearizability · `s2-streamstore/s2-verification`
- S2 — access control & agent isolation: https://s2.dev/blog/access-control · https://s2.dev/blog/distributed-ai-agents#enforcing-isolation-at-the-infrastructure-layer
- S2 — OpenAPI spec (generation source): `s2-streamstore/s2-specs` `s2/v1/openapi.json` @ `329de93f7b240a4daef9edbeb98ced0699aab7d0`
- S2 — Rust `SeqNum(u64)`: `s2-streamstore/s2` › `sdk/src/types.rs`
- Pulsar — Functions / processing guarantees: https://pulsar.apache.org/docs/next/functions-concepts/
- Pulsar — transactions / IO / TableView / schema: https://pulsar.apache.org/docs/next/txn-how/
- effect-smol (Effect v4): https://github.com/Effect-TS/effect-smol/blob/main/LLMS.md
- effect-smol cluster `Entity` (non-adoption verdict): https://github.com/Effect-TS/effect-smol/blob/main/packages/effect/src/unstable/cluster/Entity.ts
- Restate — `restate-sdk-gen` (authoring shape, Operation/Future, cancellation): `restatedev/sdk-typescript` › `packages/libs/restate-sdk-gen`
- Restate — service communication (child-call model): https://docs.restate.dev/develop/ts/service-communication
- Companion SDDs: `effect-s2-flow-sdd.md` (folded in here); archived legacy at `archive/effect-s2-legacy-durable-stream-db` (reference only).