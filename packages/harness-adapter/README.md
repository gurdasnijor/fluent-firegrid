# @firegrid/harness-adapter

The **harness adapter contract** (managed-sessions SDD §MS-C6, WP D2): the
reconstruction-model seam between one agent harness and the L1 observation
vocabulary (interface **I2**, `@firegrid/l1-vocabulary`). Architect-approved
surface (PR #99).

The adapter is *an effectful handler that writes only under the Processor's
fence, emitting L1 facts as its Append*
([authority-and-actors](../../docs/canon/architecture/fluent/authority-and-actors.md)).
It owns **no** authority, durability, wait/timer/child semantics, or projection
schema. It lowers harness traffic into `L1StreamRecord`s and never mints a
parallel vocabulary.

## Shape

TS zone, Effect shapes per `LLMS.md`. The pure `lower` core is Effect-free; Effect
appears only in the I/O shell (`drive`) and the kernel-provided seams.

- `contract.ts` — the ratified types and service seams: `HarnessLowering`,
  `HarnessAdapter`, `L1Sink` (the adapter's only write — a fenced append),
  `ToolGate` (durable-tool mediation, provided at a *gateable* adapter's layer
  construction only), `NativeResumeArtifact`, `DriveInput`/`DriveOutcome`,
  `HarnessCapabilities`, and the tagged errors.
- `replay.ts` — `replay(lowering, events)`: the pure, deterministic lowering fold,
  the `harness.fixture-replay` target.
- `reconstruction.ts` — `makeReconstructionAdapter` / `reconstructionAdapterLayer`:
  the generic `drive` shell. Validates the prompt, runs a `HarnessSource`, lowers
  its events, and emits the suffix from `observedThrough` onward (exclusive upper
  bound) under the fence. Resume suppresses the already-durable prefix.
- `reference.ts` — `referenceLowering` / `recordedTranscriptSource`: an ACP-native
  pass-through lowering (an ACP event already *is* an `L1StreamRecord`).
  Deliberately **not** the Claude adapter — WP D3 supplies Claude's non-trivial
  lowering (`parent_tool_use_id` scoping, usage/cost facts) against this same
  contract. The reference is `observe-only`, so its layer never depends on
  `ToolGate`.

## Interception posture is type-enforced

`ToolGate` is not in `drive`'s requirements. A `gateable` adapter closes over it at
layer construction; an `observe-only` adapter's layer never depends on it, so it
*cannot* obtain one — the "observe-only cannot mediate" rule is a type-level
property, not prose.

## Proof

`apps/proofs/proofs/harness-fixture-replay.ts`:

- `harness.fixture-replay` — for every D1 seed fixture, the pure `replay` and the
  `drive` shell both reconstruct an L1 record sequence identical to the fixture
  and an identical `foldTurn` state; replay is deterministic across runs; a
  mutated transcript is detected as divergent.
- `harness.resume-suppression` — driving with a `ResumePoint` at an interior
  `observedThrough` emits exactly the suffix at Version ≥ `observedThrough`,
  re-emitting nothing before it, and prefix + suffix reconstruct the full fold.
  (The side-effect-non-re-execution half of resume needs a live gateable harness
  and is proven in D3.)
