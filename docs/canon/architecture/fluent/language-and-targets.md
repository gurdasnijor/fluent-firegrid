# Language Zones and Build Targets

Doc-Class: canon
Status: active
Date: 2026-07-06
Owner: Firegrid Architecture
Substrate: S2

Decision record for the F#/Fable commitment. Ends the split-brain state between
the F# `src/` tree and the Effect-TS `packages/` tree by assigning every module
a home by rule, not by history.

## The Two-Zone Rule

**F# owns everything that writes S2.** The substrate client, folds,
checkpoint/trim, turn logs, lifecycle authority, wake routing, timers — the
durable core. Platform capability work (managed-sessions lanes A/B/C) is F#.

**TypeScript owns everything that touches the TS ecosystem.** The proof
harness (`apps/proofs`), harness adapters (Claude Agent SDK, ACP — their SDKs
are TS-native), the protocol client SDK, and consumers such as agent-ui.
Adapter and consumer work (lanes D/E) is TS.

A module that seems to need both zones is a design smell: split it at the
protocol or the seam below.

## The One-Seam Law

Exactly two ways to cross between zones:

1. **The kernel protocol** — ingress/attach/control surfaces, per the
   runtime-fronted posture in [`architecture.md`](./architecture.md). Processes
   (adapters, consumers, the proof harness) prefer this edge.
2. **One Fable-emitted package seam** — the Fable→JS build of the kernel,
   published under the existing substrate package names with hand-curated
   `.d.ts`. Library-level access (custom TS hosts, law-level proofs) uses this
   edge only.

No other deep imports across zones. TS proofs driving an F# kernel are
structurally black-box, which mechanically enforces the proofs-consume-only-
public-exports rule in `AGENTS.md`.

## The Sans-IO Core Rule

Kernel *semantics* are pure F# modules: `fold : State -> Record -> State`,
`plan : State -> Intent list`, codecs, version math, predicate evaluation.
No async, no HTTP, no ambient clock or randomness — time and entropy enter as
data. I/O lives in per-target shells.

This is what makes the multi-target story cheap:

| Target | Core | Shell | Status |
| --- | --- | --- | --- |
| JS / Node | Fable→JS | Node S2 client; kernel host + TS bindings | Active (this repo) |
| Rust data plane | Fable→Rust core crate | Hand-written host over the native `s2-sdk` crate (seed: eff-firegrid PR #63 `firegrid-host`) | Deferred until a perf requirement writes its SDD |
| dotnet | Native | FsCheck property tests / fuzzing of pure semantics | Opportunistic |

Fable's Rust backend handles the pure-functional F# subset and little more —
the sans-IO rule keeps the core inside that subset by construction. Do not open
a Rust lane ahead of a demanding consumer; the option is kept warm by this rule
plus golden vectors, at near-zero cost.

## Cross-Target Conformance

- **Golden vectors** for every codec (step records, turn chunks, checkpoint
  records): byte-level fixtures checked by all targets, so differently-built
  hosts can serve the same S2 streams in a mixed fleet.
- **The TS proof suite** drives any host build through the same public
  protocol; proofs are target-blind, so the conformance suite gates every
  plane for free.

## Dispositions

| Asset | Disposition |
| --- | --- |
| eff-firegrid `src/S2` (later client rev) | Port here; supersedes `src/Firegrid.Log/S2` scaffold |
| eff-firegrid `Foundation` (`SubjectHistory`/`StateView`/`KvStore`) + `Foundation/Durable` kernel + F# proofs | Port here as the native-kernel seed (lanes A/B starting points) |
| eff-firegrid repo | Frozen donor after ports |
| `packages/fluent` authoring surface + TanStack lowering | Frozen; TS-authored durable procedures wait for a real consumer (re-status finish-line SDD) |
| `packages/core`, `packages/runtime` | Absorbed/removed as the F# kernel lands, per drift policy |
| `packages/trace`, `apps/proofs` | TS, unchanged |

## Consequences for Agents

- Target Surfaces (gate G6) for F#-zone lanes are F# signatures (modules,
  DU-typed errors); TS-zone lanes keep Effect shapes per `LLMS.md`.
- The known risk is agent fluency in F#: mitigate with the ported eff-firegrid
  code as the exemplar corpus and strict G6 review. The F# compiler is the
  stricter reviewer; lean on it.
