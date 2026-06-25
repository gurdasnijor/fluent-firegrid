import * as NodeServices from "@effect/platform-node/NodeServices"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Argument from "effect/unstable/cli/Argument"
import * as Command from "effect/unstable/cli/Command"
import * as Flag from "effect/unstable/cli/Flag"

import { type CompletedProof, type Proof, runProof } from "./Proof.ts"
import { layer as TraceRuntimeLayer } from "./TraceRuntime.ts"
import { VerificationError } from "./VerificationError.ts"

const version = "0.0.0"

const outputFlag = Flag.choice("output", ["text", "json"]).pipe(
  Flag.withDescription("Output format"),
  Flag.withDefault("text")
)

const verification = Command.make("verification").pipe(
  Command.withDescription("Run Firegrid verification proofs"),
  Command.withSharedFlags({
    output: outputFlag
  })
)

const proof = Command.make("proof").pipe(
  Command.withDescription("List and run verification proofs")
)

const selectedProofs = (
  proofs: ReadonlyArray<Proof<any>>,
  name: string
): Effect.Effect<ReadonlyArray<Proof<any>>, VerificationError> => {
  if (name === "all") return Effect.succeed(proofs)
  const found = proofs.find((proof) => proof.name === name)
  if (found !== undefined) return Effect.succeed([found])
  return new VerificationError({ message: `unknown proof ${name}` })
}

const completedProofJson = (completed: CompletedProof): unknown => ({
  proof: completed.proof.name,
  trialId: completed.trial.trialId,
  result: completed.trial.result._tag,
  report: completed.trial.report?.path
})

const printCompletedProof = Effect.fn("verification.cli.printCompletedProof")(function*(
  completed: CompletedProof
) {
  const root = yield* verification
  if (root.output === "json") {
    yield* Console.log(JSON.stringify(completedProofJson(completed), undefined, 2))
    return
  }
  yield* Console.log(`proof ${completed.proof.name}: passed`)
  yield* Console.log(`trial ${completed.trial.trialId}`)
  if (completed.trial.report?.path !== undefined) {
    yield* Console.log(`report ${completed.trial.report.path}`)
  }
})

const makeListCommand = (proofs: ReadonlyArray<Proof<any>>) =>
  Command.make(
    "list",
    {},
    Effect.fn("verification.cli.proof.list")(function*() {
      const root = yield* verification
      if (root.output === "json") {
        yield* Console.log(JSON.stringify(
          proofs.map((proof) => ({
            name: proof.name,
            description: proof.description
          })),
          undefined,
          2
        ))
        return
      }
      yield* Effect.forEach(
        proofs,
        (proof) => Console.log(`${proof.name} - ${proof.description}`),
        { discard: true }
      )
    })
  ).pipe(
    Command.withDescription("List available verification proofs")
  )

const makeRunCommand = (proofs: ReadonlyArray<Proof<any>>) =>
  Command.make(
    "run",
    {
      name: Argument.string("name").pipe(
        Argument.withDescription("Proof name, or all"),
        Argument.withDefault("all")
      ),
      reportDir: Flag.optional(Flag.directory("report-dir")).pipe(
        Flag.withDescription("Directory for JSON proof reports")
      ),
      trialId: Flag.optional(Flag.string("trial-id")).pipe(
        Flag.withDescription("Trial id for a single proof run")
      )
    },
    Effect.fn("verification.cli.proof.run")(function*({ name, reportDir, trialId }) {
      const selected = yield* selectedProofs(proofs, name)
      const reportPath = Option.getOrUndefined(reportDir)
      const requestedTrialId = Option.getOrUndefined(trialId)
      if (requestedTrialId !== undefined && selected.length !== 1) {
        return yield* new VerificationError({ message: "--trial-id can only be used with one named proof" })
      }
      const completed = yield* Effect.forEach(
        selected,
        (proof) =>
          runProof(proof, {
            ...(reportPath === undefined ? {} : { reportDir: reportPath }),
            ...(requestedTrialId === undefined ? {} : { trialId: requestedTrialId })
          }),
        { concurrency: 1 }
      )
      yield* Effect.forEach(completed, printCompletedProof, { discard: true })
    })
  ).pipe(
    Command.withDescription("Run one proof or all proofs")
  )

const makeCommand = (proofs: ReadonlyArray<Proof<any>>) =>
  verification.pipe(
    Command.withSubcommands([
      proof.pipe(Command.withSubcommands([makeListCommand(proofs), makeRunCommand(proofs)]))
    ])
  )

export const runCli = (proofs: ReadonlyArray<Proof<any>>) =>
  Command.run(makeCommand(proofs), { version }).pipe(
    Effect.provide(Layer.mergeAll(
      NodeServices.layer,
      TraceRuntimeLayer({ serviceName: "firegrid-verification-cli" })
    ))
  )
