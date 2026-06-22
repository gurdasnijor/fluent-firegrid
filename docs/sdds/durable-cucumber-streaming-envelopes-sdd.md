# SDD: Streaming Envelope Output for durable-cucumber

## 1. Purpose

Evolve the `@firegrid/durable-cucumber` runner (PR #42) from **batched**
`Envelope[]` output to a **streaming** model: envelopes are appended to the
durable run stream *as they are produced*, and consumers (CLI, formatters, the
CCK gate) **tail them live** rather than waiting for the run to finish.

This salvages the one idea from the (now-closed) #43 not captured elsewhere —
the *streaming runner* — and grafts it onto #42's existing code with minimal
surface change.

## 2. Current State (the batching point)

The runner is **already streaming internally** and batches in exactly one place:

- `runner-core.ts` — `runFeatures(sources, host, exec): Stream<Envelope, E, R>`
  already emits envelopes incrementally in canonical order
  (`framing → scenarios → testRunFinished`).
- `runner.ts` — `makeRunner.run` is the only batching boundary:

  ```ts
  const envelopes = yield* Stream.runCollect(runFeatures(...)).pipe(Effect.map(Array.from))
  yield* run("publish-envelopes", RunEnvelopes.open(runId).appendBatch(envelopes))
  return { success }
  ```

- `streams.ts` — `RunEnvelopes = EventStream("cucumber-effect/run-envelopes")(...)`
  is already a durable append-only S2 stream keyed by run id. The NDJSON
  protocol output *is* this stream.

So the producer-side change is: drain `runFeatures` into incremental appends
instead of one terminal `appendBatch`. The consumer-side change is: read by
tailing `RunEnvelopes` from a sequence number rather than reading the final
batch.

## 3. Load-Bearing Decisions

1. **The durable run stream is the source of truth, live.** `RunEnvelopes` is
   already the canonical output; streaming just means it is written
   incrementally and read before the run completes. No new output channel, no
   process-local PubSub or queue (per the closed #43 §8.1 and the stream-db
   subscription model in `effect-s2-stream-db-relational-ivm-sdd.md` §4.5).
2. **Each incremental append is its own journaled durable step.** Today one
   `run("publish-envelopes", …)` is journaled once, so replay re-reads the ack
   and never double-appends. Incremental appends must preserve that: each append
   is a `run("append-<seq>", …)` keyed by a deterministic sequence, so a
   coordinator replay re-reads acks instead of re-appending. **Idempotent append
   by sequence is the central correctness concern**, not the streaming itself.
3. **Append order is canonical, never wall-clock.** `runFeatures` already fixes
   the order; appends must follow the stream order it produces, regardless of
   how scenarios are scheduled (CCK serial vs. parallel). Concurrency in the
   executor must not reorder the durable stream.
4. **`testRunFinished` is the completion fact.** The coordinator RPC return
   (`{ success }`) stops being how consumers learn the run ended; the terminal
   `testRunFinished` envelope in the stream is. Consumers detect completion by
   the terminal envelope, so a late/replaying coordinator and a live consumer
   agree.
5. **Append granularity is per scenario-attempt, plus run framing.** The natural
   durable unit is the scenario-attempt slice (the scenario `object` already
   journals its step outcomes; its envelope slice is one more journaled fact at
   its boundary), bracketed by the run-framing envelopes
   (`testRunStarted` + `testCase` declarations) up front and `testRunFinished`
   at the end. Per-envelope appends are correct but needlessly fine-grained.

## 4. Target Flow

```text
runFeatures emits:  [framing…] → [scenarioA envelopes] → [scenarioB envelopes] → testRunFinished
                          │              │                       │                     │
coordinator appends:  run("append-0")  run("append-1")      run("append-2")     run("append-3")
                          │              │                       │                     │
RunEnvelopes (S2):    seq 0…k ────────── seq … ──────────────── seq … ───────────── seq N (terminal)
                          │
consumer tails:       reads seq 0,1,2,… live; stops at testRunFinished
```

Producer (`runner.ts`): replace `runCollect` + terminal `appendBatch` with a
streamed fold that appends each chunk under a deterministic, replay-stable
`run("append-<seq>", …)` step. `Stream.runFoldEffect` (or `Stream.mapAccum` over
a chunked `runFeatures`) carries the sequence counter; chunk boundaries follow
Decision 5.

Consumer: a `tail(runId, fromSeq)` read over `RunEnvelopes` that yields
envelopes as appended and terminates on `testRunFinished`. This depends on the
stream-db read/tail (change-stream) primitive tracked in
`effect-s2-stream-db-relational-ivm-sdd.md` (§4.5) and the
`streamdb-engine-followups` memo (`TableFacade.changes` / EventStream
read-from-seq). If that primitive is not yet available, the consumer falls back
to polling `RunEnvelopes` reads from the last seen seq.

## 5. Open Decision: who appends

- **(A) Coordinator appends (smaller diff).** The runner service drains
  `runFeatures` and owns every append. Canonical ordering is trivial (one
  writer); the diff is essentially the `runner.ts` fold above. Scenarios stay as
  they are.
- **(B) Scenario-attempt objects append their own slice (more live, more work).**
  Each scenario `object` appends its envelope slice to the run stream at its
  `end`; the coordinator writes only framing + terminal envelopes. More naturally
  live and distributed (#43 file 1's Run/Scenario-Attempt architecture), but
  crosses object boundaries (a scenario object writing the run's stream) and
  needs a cross-writer ordering rule (e.g. reserve per-scenario seq ranges from
  the coordinator's canonical order).

Recommendation: **ship (A) first** — it is a near-mechanical change to one
handler and delivers live consumer tailing immediately; revisit (B) only if
per-scenario write distribution becomes necessary.

## 6. Changes vs PR #42

| File | Change |
| --- | --- |
| `runner.ts` | Replace `runCollect` + terminal `appendBatch` with a streamed fold that appends each chunk under `run("append-<seq>", …)`; derive `success` from the terminal envelope rather than a collected array. |
| `runner-core.ts` | Unchanged shape (already a `Stream`); may expose chunk boundaries aligned to Decision 5 (per-scenario grouping) if not already. |
| `streams.ts` / consumers | Add `tail(runId, fromSeq)` over `RunEnvelopes`; the CCK gate and formatters read via tail and stop on `testRunFinished`. |
| `scenario.ts` | Unchanged for option (A); gains slice-append at `end` only if option (B) is chosen. |
| Engine (`step-host`, `step-exec`, CCK gate) | Unchanged. |

## 7. Validation

- The CCK gate (`minimal`, `attachments`) must still pass when fed by the **tail**
  path, not the batch path — proving consumers see the identical canonical
  envelope sequence live.
- A replay/recovery test: kill the coordinator mid-stream; on recovery, the
  idempotent `run("append-<seq>", …)` steps re-read acks and the durable stream
  contains each envelope exactly once (Decision 2).

## 8. Relationship to Other SDDs

- **Output streaming (this SDD)** and **authoring surface**
  (`durable-cucumber-authoring-surface-sdd.md`) are orthogonal: this changes how
  envelopes leave the runner; that changes how steps/state are authored. Both
  graft onto the same #42 engine independently.
- Depends on the stream read/tail primitive from
  `effect-s2-stream-db-relational-ivm-sdd.md` (#44).

## 9. References

- PR #42 — `Add durable Cucumber runner (coordinator + worker)`.
- Closed #43 — `cucumber-durable-table-stream-sdd.md` (streaming runner,
  Producer/Consumer Data Flow, Subscriptions) — the design this SDD salvages.
- `docs/sdds/effect-s2-stream-db-relational-ivm-sdd.md` — stream read/tail
  subscription model.
