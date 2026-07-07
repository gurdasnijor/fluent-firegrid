import { foldTurn, l1Fixtures, type L1StreamRecord } from "@firegrid/l1-vocabulary"
import {
  HarnessAdapter,
  L1Sink,
  reconstructionAdapterLayer,
  recordedTranscriptSource,
  referenceArtifact,
  referenceLowering,
  replay
} from "@firegrid/harness-adapter"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"

import { proof } from "../src/Proof.ts"

class HarnessProofError extends Data.TaggedError("HarnessProofError")<{
  readonly issues: ReadonlyArray<string>
}> {}

const json = (value: unknown): string => JSON.stringify(value)
const sameJson = (a: unknown, b: unknown): boolean => json(a) === json(b)

// A minimal valid prompt (one user_message_chunk). The reference transcript is
// self-contained, so the shell does not re-emit the prompt; it is here to satisfy
// the ratified non-empty `DriveInput.prompt` contract.
const prompt = [
  { sessionUpdate: "user_message_chunk", messageId: "prompt", content: { type: "text", text: "go" } }
] as [L1StreamRecord]

/** Drive one recorded transcript through the reconstruction shell, capturing every emitted L1 record. */
const driveCapture = Effect.fn("driveCapture")(function*(
  records: ReadonlyArray<L1StreamRecord>,
  observedThrough?: number
) {
  const captured = yield* Ref.make<ReadonlyArray<L1StreamRecord>>([])
  const sinkLayer = Layer.succeed(L1Sink, {
    emit: (record: L1StreamRecord) => Ref.update(captured, (records) => [...records, record])
  })
  const adapterLayer = reconstructionAdapterLayer(recordedTranscriptSource(records))
  const input = observedThrough === undefined
    ? { prompt }
    : { prompt, resume: { artifact: referenceArtifact(records.length), observedThrough } }
  const outcome = yield* Effect.gen(function*() {
    const adapter = yield* HarnessAdapter
    return yield* adapter.drive(input)
  }).pipe(Effect.scoped, Effect.provide(adapterLayer), Effect.provide(sinkLayer))
  return { captured: yield* Ref.get(captured), outcome }
})

/**
 * `harness.fixture-replay` — for every D1 seed fixture, the pure `replay` and the
 * `drive` shell each reconstruct an L1 record sequence identical to the fixture
 * and an identical `foldTurn` state (adapter determinism); replay is stable across
 * runs; and a mutated transcript is detected as divergent (the assertion has teeth).
 */
export const harnessFixtureReplayProof = proof("harness.fixture-replay")
  .describedAs(
    "Proves the reconstruction adapter deterministically reconstructs identical L1 records + folded state from a recorded transcript, over the D1 fixture corpus."
  )
  .spec(({ property }) =>
    property("harness.fixture-replay")
      .workload(() =>
        Effect.gen(function*() {
          const issues: Array<string> = []
          const fail = (message: string): void => {
            issues.push(message)
          }
          if (l1Fixtures.length === 0) fail("fixture corpus is empty")

          for (const fixture of l1Fixtures) {
            const pure = replay(referenceLowering, fixture.records)
            if (!sameJson(pure, fixture.records)) {
              fail(`${fixture.name}: pure replay diverged from the recorded transcript`)
            }
            if (!sameJson(replay(referenceLowering, fixture.records), pure)) {
              fail(`${fixture.name}: pure replay is nondeterministic`)
            }
            if (!sameJson(foldTurn(pure), foldTurn(fixture.records))) {
              fail(`${fixture.name}: replayed fold state diverged`)
            }

            const { captured, outcome } = yield* driveCapture(fixture.records)
            if (!sameJson(captured, fixture.records)) {
              fail(`${fixture.name}: drive emitted a record sequence != the transcript`)
            }
            if (!sameJson(foldTurn(captured), foldTurn(fixture.records))) {
              fail(`${fixture.name}: drive folded state diverged`)
            }
            if (outcome.terminal._tag !== "completed") {
              fail(`${fixture.name}: unexpected terminal ${outcome.terminal._tag}`)
            }
            if (outcome.artifact.harness !== "reference-acp") {
              fail(`${fixture.name}: unexpected resume artifact harness ${outcome.artifact.harness}`)
            }
          }

          // Divergence must be detected: mutate the first record and require inequality.
          const sample = l1Fixtures[0]
          if (sample !== undefined && sample.records.length > 0) {
            const mutated = sample.records.map((record, index) =>
              index === 0 ? { ...record, content: { type: "text", text: "MUTATED" } } : record
            )
            if (sameJson(replay(referenceLowering, mutated), sample.records)) {
              fail("a mutated transcript was not detected as divergent")
            }
          }

          if (issues.length > 0) {
            return yield* new HarnessProofError({ issues })
          }
          return { ok: true } as const
        })
      )
      .verify(({ expect }) => [expect.workloadResult({ ok: true })])
  )

/**
 * `harness.resume-suppression` — driving with a `ResumePoint` at an interior
 * `observedThrough` emits exactly the suffix at Version >= `observedThrough`
 * (exclusive upper bound): nothing before it is re-emitted, `observedThrough = 0`
 * emits the whole turn, and `observedThrough = length` emits nothing. The
 * side-effect-non-re-execution half needs a live gateable harness and is D3's.
 */
export const harnessResumeSuppressionProof = proof("harness.resume-suppression")
  .describedAs(
    "Proves the reconstruction adapter's fact-level resume-suppression: resume emits only the suffix from the exclusive-upper-bound observedThrough, re-emitting no already-durable fact."
  )
  .spec(({ property }) =>
    property("harness.resume-suppression")
      .workload(() =>
        Effect.gen(function*() {
          const issues: Array<string> = []
          const fail = (message: string): void => {
            issues.push(message)
          }

          for (const fixture of l1Fixtures) {
            const n = fixture.records.length
            const full = yield* driveCapture(fixture.records, 0)
            if (!sameJson(full.captured, fixture.records)) {
              fail(`${fixture.name}: observedThrough=0 did not emit the whole turn`)
            }
            const empty = yield* driveCapture(fixture.records, n)
            if (empty.captured.length !== 0) {
              fail(`${fixture.name}: observedThrough=length re-emitted ${empty.captured.length} durable facts`)
            }
            for (let k = 1; k < n; k++) {
              const { captured } = yield* driveCapture(fixture.records, k)
              const expected = fixture.records.slice(k)
              if (!sameJson(captured, expected)) {
                fail(`${fixture.name}: observedThrough=${k} emitted the wrong suffix`)
              }
              // Prefix + resumed suffix reconstruct the full turn (no fact lost or doubled).
              if (!sameJson([...fixture.records.slice(0, k), ...captured], fixture.records)) {
                fail(`${fixture.name}: observedThrough=${k} prefix+suffix != full turn`)
              }
            }
          }

          if (issues.length > 0) {
            return yield* new HarnessProofError({ issues })
          }
          return { ok: true } as const
        })
      )
      .verify(({ expect }) => [expect.workloadResult({ ok: true })])
  )
