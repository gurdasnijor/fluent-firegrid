# SDD: The Firegrid Platform — API Layers & Target Surfaces

Doc-Class: sdd
Status: draft — architect-authored; **pending human ratification** (nothing
in the execution plan dispatches until ratified)
Date: 2026-07-07
Owner: Firegrid Architecture
Companions: [`fsharp-fable-effsharp-evaluation-sdd.md`](./fsharp-fable-effsharp-evaluation-sdd.md)
(F#-zone internal style), [`managed-sessions-agent-ui-sdd.md`](./managed-sessions-agent-ui-sdd.md)
(kernel capability semantics — now an application-history document)

## The product

**Firegrid is a durable-execution platform over S2 streams.** Semantics
follow the Durable Functions model (Burckhardt et al., OOPSLA 2021):
orchestrations as deterministic programs replayed over journaled histories;
addressable single-writer **entities**; at-least-once **activities** with
journal-served results; durable timers and signals. Hosting follows the
Apache Pulsar Functions spirit: functions attached to streams, run by
namespace-scoped host workers. Being stream-native, the platform also offers
what those systems don't: **sealed durable logs with byte-faithful attach**
(replay prefix → live tail → terminal) and **checkpointed projections**.

**The platform ships zero domain nouns.** There is no first-class "session",
"agent", or "turn" — exactly as Restate has no "session". Agent sessions are
the platform's first *application*, and their vocabulary belongs to that
application.

## End state

```
 L4  APPLICATIONS & ADAPTERS — their own vocabulary, zero platform nouns
       agent-sessions reference app · agent-ui · harness adapters
        │
 L3  @firegrid/durable — THE platform facade (the only consumer package)
       plain TS, Promise-first: activities · orchestrations · entities
       · timers/signals · log attach · projections
        │
 L2  emitted seam (Fable → Exports.fs) — private; serves only L3
        │
 L1  platform kernel (F#, domain-free, sans-IO, proven)
       Durable<'a> + durable { } CE · Workflow.* · Stepper replay
       · Processor/Mailbox entities · activities · timers · wakes
       · Authority · DurableLog · Checkpoint · StateReads
        │
 L0  S2 streams
```

| Layer | What lives there | Audience | Rule |
| --- | --- | --- | --- |
| L4 | Applications (reference app, agent-ui) and harness adapters | App developers, end users | Own vocabulary; consume L3 only |
| L3 | `@firegrid/durable` | Any TS developer | Plain TS; the platform's one public API |
| L2 | Fable emission + `Exports.fs` | L3 only | Private, mechanical, demand-driven; importing `dist/` is off-contract |
| L1 | F# platform kernel | Platform developers, proofs | Domain-free; never imports applications |
| L0 | S2 | L1 only | Never leaks upward |

## Target surfaces

### L3 — `@firegrid/durable`

Promise-first; `AsyncIterable` for streams; every kernel DU error surfaces as
a tagged union (`{ _tag: "Deposed" } | …`, never thrown strings); zero S2 or
codec assembly required of the consumer; **no runtime dependencies**.
Indicative shape (T1's corpus refines and freezes it):

```ts
import { DurableClient, host } from "@firegrid/durable"

// Host side (Pulsar-style): register functions, run a namespace worker.
host({ basin, namespace: "prod" })
  .activity("charge", async (input) => { … })
  .orchestration("checkout", async (ctx, input) => {
    const paid = await ctx.call("charge", input)          // journaled; replay-served
    const [a, b] = await ctx.all([…])                     // fan-out/fan-in
    const winner = await ctx.any([ctx.timer(deadline), ctx.signal("approved")])
    return result
  })
  .entity("counter", { init, ops: { add: (s, n) => s + n } })
  .run()

// Client side.
const client = await DurableClient.connect({ basin })
await client.start("checkout", input, { id: "order-42" })
await client.signal("order-42", "approved", payload)
const status = await client.status("order-42")

// Stream-native primitives (beyond the DF trio):
const log = client.logs.open(["invoices", "2026-07"])      // sealed durable log
for await (const ev of log.attach()) { … }                  // prefix → tail → terminal
const view = await client.projections.read(myFold, { read: "eventual" }) // lag = data
```

The F#-native authoring surface is the existing `durable { }` computation
expression (`Workflow.call/all/waitForSignal/sleepUntil/any`) — the TS `ctx`
vocabulary mirrors it one-to-one.

### L4 — the agent-sessions reference application

Application code in the platform's vocabulary: session = keyed entity, cancel
= a command, turn output = a sealed log (attach is the platform primitive),
history = a checkpointed projection. It lives app-side (`examples/`, later
agent-ui's home), and its scenario corpus **is the platform's integration
acceptance** — anywhere it must reach around `@firegrid/durable`, that is a
platform gap and becomes a platform work packet.

### L2 — the seam

Mechanical Fable pass-throughs (`Async` acceptable; L3 converts), grown only
as greening the corpora demands: orchestration drive/replay + host/registry,
entity admission, activity plumbing, generic sealed-log create/append/seal/
attach, checkpoint + read primitives. Application modules are never seam
mandates. Wake plumbing is not exported.

## Doctrine (binding on ratification)

1. **Zero domain nouns** in L0–L3 — names, exports, subjects, docs.
2. **Plain TS at L3/L4 exported surfaces**: Promise, AsyncIterable, tagged
   unions. No `effect` (or any framework) in exported types or hard deps of
   platform/contract packages; an optional `/effect` subpath wrapper may
   *wrap* the plain facade. Pre-platform Effect packages are exempt until
   retired.
3. **Names in consumer vocabulary**; spec coordinates (interface ids,
   milestone ids, WP ids) never appear in package names or exports.
4. **Placement**: platform never imports applications; domain modules never
   live in platform namespaces.
5. **Contracts are artifacts**: a target surface = package skeleton (bodies
   `throw NotImplemented`) + **frozen red corpus** (consumer tests that fail)
   + short prose companion. Prose-only or "by construction" surface claims
   are rejected at review.
6. **Kernel work is not wave-complete** until reachable through L3 or
   explicitly deferred as its own ledger row.

## Execution: the top-down red/green loop

1. The architect authors each surface package + red corpus; **the human
   ratifies the corpus before merge** (the contract is read as runnable
   code). Red tests are frozen at ratification — editing one is an architect
   gate.
2. A manifest (`targets.json`: test → work packet → `red`/`green`) drives a
   target suite beside the blocking suite, strict in both directions: CI
   fails on a green regression **or** on a red test passing without an
   explicit promotion commit (manifest + ledger flip together).
3. Workers take green-making work packets via the coordinator. Merge
   authority: reds flipped green, **zero edits to test bodies**, manifest and
   ledger flipped in the same PR.
4. Everything below L3 is built demand-driven by greening — no speculative
   seam exports, no speculative kernel surface.

| WP | Deliverable | Gate |
| --- | --- | --- |
| T0 | Ratchet: manifest runner + strict target suite in CI | None (mechanical) |
| T1 | `@firegrid/durable` skeleton + red corpus + platform prose companion. Corpus: replay determinism across a host kill (activities not re-executed); fan-out/fan-in; `any` races; signal to a parked orchestration across restart; durable timer across restart; entity op serialization; typed activity failure; deterministic `currentTime`; status/result query; log attach (prefix/tail/terminal, byte-faithful); projection read with observable lag | **Human ratifies** |
| T2 | Reference-app red corpus against `@firegrid/durable`: start turn / cancel from an unprivileged second client / duplicate-cancel idempotence / single-writer with typed rejection / deposed-writer rejection / attach semantics / history with cause and lag | **Human ratifies** |
| T3 | Harness-adapter contract as plain TS + red fixture-replay conformance corpus | **Human ratifies** |

T1 before T2 by design: the first application consumes the platform, it does
not bypass it. This costs the agent-ui integration path one design cycle and
that trade is deliberate.

## Getting there from today (the only section where legacy names appear)

| Today on `main` | Disposition |
| --- | --- |
| `src/Firegrid.Store` Foundation modules (`Durable/*`, `Authority`, `DurableLog`, `Checkpoint`, `StateReads`, wake path) | **Platform kernel (L1). Keep** — proven this wave; gains seam + facade exposure as T1 greens. |
| `SessionLifecycle.fs`, `Turn.fs`, `SessionHistory.fs` (inside `Firegrid.Store`) | **Application code predating the platform surface.** Move out of platform namespaces; re-express over the facade as T2 greening demands; never platform API. |
| `Exports.fs` (StateReads + legacy exports) | Grows into the platform seam demand-driven; legacy `ObjectState`/`WorkflowLog` exports retire with the TanStack path. |
| `@firegrid/l1-vocabulary` | Rename → `@firegrid/session-events` (L4 app-ecosystem package; content unchanged). |
| `@firegrid/harness-adapter`, `@firegrid/claude-adapter` | Keep names; exported contract de-Effected via T3; descriptions rewritten consumer-first. |
| `@firegrid/fluent` + `@firegrid/runtime` (TanStack) | Frozen. Retire when `@firegrid/durable` reaches parity (checklist in T1's prose companion). |
| `@firegrid/log` (empty stub) | Delete. |
| In-flight kernel WPs (A4 impl, C2, B4) | Finish as-is — L1 altitude, unaffected; they make greening cheaper. |
| Managed-sessions ledger + wave process | Continues for in-flight work only; all new surface work flows through T-rows. |

## Appendix: the 2026-07-07 audit (why this SDD exists)

The managed-sessions wave shipped a proven kernel with no consumer layer.
Verified findings: the seam exported almost nothing (the session kernel was
unreachable from TS); exports returned Fable `Async`, not Promises, despite
"Promise-first" surface claims; the P4 facade package was an empty stub; the
complete DF-style engine (`Durable<'a>` free monad, `durable { }` CE,
`Workflow.*`, `Stepper` journaled replay — kernel-proven) was unexported and
unnamed in any document; package names leaked ledger jargon; and the adapter
contract carried a hard `effect@4.0.0-beta.87` dependency in its exported
types, contradicting the ratified Promise-first canon — the drift entered
through a lanes-doc summary line ("Effect shapes per LLMS.md") and was then
enforced by review. Root cause: validation gates accepted *assertions*
("rides the seam, by construction") instead of artifacts, and no contract
defined each layer's outward surface. This SDD is that contract; the
red/green loop is its enforcement.
