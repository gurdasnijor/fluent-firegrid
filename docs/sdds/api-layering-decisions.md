# API Layering — Decision Log

Doc-Class: sdd (companion)
Status: record — rationale, rejected alternatives, and audit history for
[`api-layering-sdd.md`](./api-layering-sdd.md). Read that document for the
end state; read this one only when you want to know *why* or want to
re-litigate (don't).
Date: 2026-07-07
Owner: Firegrid Architecture

## Ergonomics decisions (sources: evaluation-SDD DF.FSharp lessons; mikhailshilkov/DurableFunctions.FSharp; jet/equinox; restatedev restate-sdk-gen DESIGN/guide/README)

1. **Serialization is derived; codecs are opt-in.** Wire formats derive from
   `'in`/`'out` at compile time via inline derivation (Thoth-`Auto`-style —
   Fable-safe; what is banned is *runtime* reflection serialization). Users
   never see a codec. `defineWith` pins explicit codecs for
   evolution-sensitive types; golden wire fixtures in the corpus make silent
   format drift go red.
2. **Two-segment names (`service/handler`) from day one.** Names land in
   journals and instance addresses — the one durable commitment of the
   authoring surface. Freezing the scheme keeps future grouping sugar a
   no-op on persisted data.
3. **Flat registration list** via a single `reg` function. The `service {}`
   custom-ops CE is deferred to service-RPC parity work; the builder-pipe
   host is rejected as dominated (worse ergonomics, same erasure wrapper).
4. **`workflow { }` — settled by the lineage itself.** restate-sdk-gen
   exists because raw async+ctx "gets awkward as concurrency patterns
   multiply"; its generator DSL (yield* as the only bind, factory-per-run =
   our Delay/Run) is a CE in JavaScript clothing. The restricted bind is
   the product: a stray `Async.Sleep`/`DateTime.Now` is a compile error,
   not a replay corruption discovered weeks later. Name aligned with the
   kernel's journaled vocabulary (`WorkflowStarted`); `durable { }` is a
   deprecated alias. Raw async+ctx rejected as the model sdk-gen escaped.
5. **Dot-method descriptors** (`reserve.Call`, `approved.Await`,
   `checkout.Start`, `run.Result`): discovery is dot-completion; module
   functions remain underneath as the stable core.
6. **Cancellation lifted from sdk-gen verbatim**: observed at bind
   boundaries, never mid-statement; recoverable (catch the
   durable-cancelled failure, perform more journaled compensation, return
   any value); uncaught → typed `Cancelled` terminal. Steps receive
   cancellation via `Async`'s native `CancellationToken`.
7. **Tagged `select`** over kernel `WhenAny` returning a caller DU
   (`match!`-consumed); `Await`-with-timeout returns `Result` for the
   binary case. `and!` = applicative fan-out (teaching test in T1); it is
   concurrency vocabulary, not quickstart material.
8. **Log attach is an async sequence** (`AsyncSeq` semantics; a JS async
   iterable under the future emission).
9. Retry policies as DUs; eternal orchestrations as returned
   `ContinueAsNew` values (DurableFunctions.FSharp shapes).
10. **Deferred v2 kernel extension**: sdk-gen's `Operation`/`Future` split —
    `spawn` with held memoized futures, per-routine `interrupt`,
    `any`/`allSettled`, abandon/join exit policies. Our lazy `Workflow<'a>`
    programs are already the `Operation` half; copy the proven design when
    a use case demands it.
11. **Entities are a Decider, not an actor framework** (evaluated:
    Fable.Actor, MailboxProcessor, Akka/Akkling). In-memory actors isolate
    at the process; entities isolate at the stream — actor guarantees end
    at the crash/host boundary where ours begin (engine history:
    in-process serialization ⇒ lost updates across a crash). Replay
    requires `state = fold(events)`: receive loops hold closure state
    (unrecoverable); reducer actors transition state without an event log.
    Every durable actor system converges on decide/evolve (Akka
    Persistence, Orleans JournaledGrain, Equinox).
    **S2-under-the-platform-seam variant** (e.g. Fable.Actor `Platform.fs`)
    evaluated: the seam abstracts transport between live processes and has
    no slots for the five things durability consists of — closure state,
    live-Pid identity, selective receive (incompatible with a durable FIFO
    without persisted skip sets; why even BEAM/OTP has no durable actors),
    ref-correlated replies, provenance-less sends. Threading them through
    rebuilds `Processor`/`Mailbox`/`Host`: the kernel *is* the S2 actor
    platform, contract narrowed to programs recovery can honor. Sanctioned
    L4 utility: durable-*delivery* actors (at-least-once, stateless or
    idempotent consumers) over `client.Logs` + a cursor. Sanctioned reuse:
    in-memory actor libs as invisible L1-shell plumbing and for ephemeral
    concurrency inside applications.

12. **The Restate triad, adopted with one deliberate difference.**
    Studied restatedev docs (services / virtual objects / workflows).
    Adopted: **Service** as a named construct (stateless durable
    request-response; semantically a workflow with an auto id — named
    because "start here" semantics deserve a name); **entities are full
    virtual objects**: `Decide` receives the key (`ctx.key`) and returns a
    *reply* committed atomically with the events (Equinox
    decide-with-result), `entity.Call` = exclusive request-response,
    `entity.Send` = fire-and-forget, `entity.State grade` = shared handlers
    with the consistency grade explicit (Restate's shared handlers leave it
    implicit). The single-writer constraint is enforced below the API —
    durable inbox admission + epoch fencing at commit — which holds across
    host kills and split-brain (stronger than partition-ownership).
    **Deliberate difference:** Restate fuses effectful multi-step logic
    into exclusive object handlers; we keep `Decide` pure and route
    multi-step keyed logic through a workflow the entity starts — compose,
    don't fuse (replay-safety of entity state stays by-construction).
    Recorded as a candidate future sugar if composition proves noisy.

## Stress-test findings (Restate tutorial; choreography-agent mapping)

Adopted surface additions: `Step.declare`/`Workflow.declare` +
`Worker.implement` (contract/impl split); `Client.cancel`/`run.Cancel`
(generalizes the session kernel's proven durable cancel); `Client.send` +
`sendAfter` (one-way + delayed, provenance-deduped); `Workflow.callChild`
(send-intent + `ChildTerminal` wake). Adopted corpus laws: saga compensation
across a mid-compensation host kill; step-timeout race over `WhenAny`.
Not adopted: held-promise style (see decision 10); Restate shared read
handlers (replaced by graded projection reads); HTTP ingress at the
platform layer (an application/transport concern — the `serveFluentS2`
precedent).

## The 2026-07-07 audit (why the layering SDD exists)

The managed-sessions wave shipped a proven kernel with no consumer layer.
Verified findings: the emitted seam exported almost nothing (the session
kernel was unreachable from TS); exports returned Fable `Async`, not
Promises, despite "Promise-first" claims; the P4 facade package was an
empty stub; the complete DF-style engine (`Durable<'a>` free monad,
`durable { }` CE, `Workflow.*`, `Stepper` journaled replay — kernel-proven)
was unexported and unnamed in any document; package names leaked ledger
jargon (`l1-vocabulary`); the adapter contract carried a hard
`effect@4.0.0-beta.87` dependency in exported types, contradicting the
ratified Promise-first canon (drift entered via a lanes-doc summary line
"Effect shapes per LLMS.md", then was enforced by review). Root cause:
validation gates accepted *assertions* ("rides the seam, by construction")
instead of artifacts, and no contract defined each layer's outward surface.
The layering SDD is that contract; the red/green loop is its enforcement.

Superseded during drafting (recorded so the drafts aren't re-proposed):
a TS-first `@firegrid/sessions` facade (twice removed: first recast as a
reference app under zero-domain-nouns, then re-sequenced behind the F#
library under F#-first); a builder-pipe host; stringly `Workflow.call`;
per-definition explicit codecs; `and!`-with-unit in the quickstart; an
"interim raw-fence" reading of the wave's C1 surface.
