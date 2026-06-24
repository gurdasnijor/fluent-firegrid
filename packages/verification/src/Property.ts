import { ChdbClient } from "@firegrid/observability"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Random from "effect/Random"

import { runTraceProof, type TraceProof } from "./TraceProof.ts"
import { VerificationError } from "./VerificationError.ts"

export interface HostDescriptor {
  readonly name: string
  readonly value: unknown
}

export const hostDescriptor = (name: string, value: unknown): HostDescriptor => ({ name, value })

export const TrialId = Context.Reference<string | undefined>("@firegrid/verification/Property/TrialId", {
  defaultValue: () => undefined
})

export interface Faults {
  readonly killHost: (name: string) => Effect.Effect<void, VerificationError>
  readonly restartHost: (name: string) => Effect.Effect<void, VerificationError>
  readonly killHostAfterSpan: (
    name: string,
    match: {
      readonly span: string
      readonly attributes?: Record<string, string>
    }
  ) => Effect.Effect<void, VerificationError>
}

export interface WaitForSpanOptions {
  readonly attributes?: Record<string, string>
  readonly attempts?: number
  readonly interval?: Duration.Input
}

export class VerificationRuntime extends Context.Service<VerificationRuntime, {
  readonly flush: Effect.Effect<void, VerificationError>
  readonly waitForSpan: (span: string, options?: WaitForSpanOptions) => Effect.Effect<void, VerificationError>
}>()("@firegrid/verification/Property/VerificationRuntime") {
  static readonly layer = Layer.succeed(
    this,
    {
      flush: Effect.fail(new VerificationError({ message: "VerificationRuntime.flush is not implemented" })),
      waitForSpan: Effect.fn("VerificationRuntime.waitForSpan")(function*(span: string) {
        return yield* new VerificationError({
          message: `VerificationRuntime.waitForSpan(${span}) is not implemented`
        })
      })
    }
  )
}

export interface WorkloadContext {
  readonly faults: Faults
  readonly runtime: VerificationRuntime["Service"]
  readonly operation: typeof operation
}

export interface CompletedTrial<A> {
  readonly trialId: string
  readonly result: Exit.Exit<A, unknown>
  readonly chdb: ChdbClient["Service"]
  readonly reportDir?: string
}

export interface Check<A> {
  readonly name: string
  readonly run: (trial: CompletedTrial<A>) => Effect.Effect<void, VerificationError>
}

export interface S2LiteSpec {
  readonly persistence: "local-root"
}

export interface PropertySpec<A> {
  readonly name: string
  readonly s2Lite?: S2LiteSpec
  readonly hosts: ReadonlyMap<string, HostDescriptor>
  readonly workload: (context: WorkloadContext) => Effect.Effect<A, unknown>
  readonly checks: ReadonlyArray<Check<A>>
}

class PropertyBuilder<A> {
  constructor(
    private readonly spec: {
      readonly name: string
      readonly s2Lite?: S2LiteSpec
      readonly hosts: ReadonlyMap<string, HostDescriptor>
      readonly workload?: (context: WorkloadContext) => Effect.Effect<A, unknown>
    }
  ) {}

  s2Lite(spec: S2LiteSpec): PropertyBuilder<A> {
    return new PropertyBuilder({ ...this.spec, s2Lite: spec })
  }

  host(name: string, host: unknown): PropertyBuilder<A> {
    return this.hosts({ [name]: host })
  }

  hosts(hosts: Record<string, unknown>): PropertyBuilder<A> {
    return new PropertyBuilder({
      ...this.spec,
      hosts: new Map([
        ...this.spec.hosts,
        ...Object.entries(hosts).map(([name, value]) => [name, hostDescriptor(name, value)] as const)
      ])
    })
  }

  workload<B>(workload: (context: WorkloadContext) => Effect.Effect<B, unknown>): PropertyBuilder<B> {
    return new PropertyBuilder({
      ...this.spec,
      workload
    })
  }

  verify(...checks: ReadonlyArray<Check<A> | TraceProof>): PropertySpec<A> {
    if (this.spec.workload === undefined) {
      throw new VerificationError({ message: `property ${this.spec.name} is missing a workload` })
    }
    return {
      name: this.spec.name,
      hosts: this.spec.hosts,
      workload: this.spec.workload,
      checks: checks.map(asCheck),
      ...(this.spec.s2Lite === undefined ? {} : { s2Lite: this.spec.s2Lite })
    }
  }
}

export const property = (name: string): PropertyBuilder<never> =>
  new PropertyBuilder({
    name,
    hosts: new Map()
  })

const asCheck = <A>(check: Check<A> | TraceProof): Check<A> => {
  if ("run" in check) return check
  return {
    name: check.name,
    run: (trial) =>
      runTraceProof(check, trial.trialId).pipe(
        Effect.provideService(ChdbClient, trial.chdb)
      )
  }
}

export const expectWorkloadResult = <A>(expected: A): Check<A> => ({
  name: "expectWorkloadResult",
  run: (trial) =>
    Effect.gen(function*() {
      if (Exit.isFailure(trial.result)) {
        return yield* new VerificationError({
          message: "workload failed",
          cause: trial.result.cause
        })
      }
      const actual = trial.result.value
      if (!Equal.equals(actual, expected)) {
        return yield* new VerificationError({
          message: `workload result mismatch: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`
        })
      }
    })
})

const stringifyJson = (value: unknown): string => {
  try {
    return JSON.stringify(value) ?? "null"
  } catch {
    return String(value)
  }
}

export const operation = Effect.fnUntraced(function*<A, E, R>(
  name: string,
  input: unknown,
  effect: Effect.Effect<A, E, R>,
  options: {
    readonly clientId?: string | number
    readonly operationId?: string | number
    readonly key?: string
  } = {}
) {
  const generatedId = yield* Random.nextInt
  const operationId = options.operationId ?? `${name}-${generatedId}`
  const clientId = options.clientId ?? "default"
  const withStatus = effect.pipe(
    Effect.tap((output) =>
      Effect.annotateCurrentSpan({
        "firegrid.operation.output.json": stringifyJson(output),
        "firegrid.operation.status": "ok"
      })
    ),
    Effect.tapError((cause) =>
      Effect.annotateCurrentSpan({
        "firegrid.operation.failure.kind": typeof cause === "object" && cause !== null && "_tag" in cause
          ? String(cause._tag)
          : "unknown",
        "firegrid.operation.status": "error"
      })
    )
  )
  return yield* withStatus.pipe(
    Effect.withSpan("verification.operation", {
      attributes: {
        "firegrid.client.id": String(clientId),
        "firegrid.operation.id": String(operationId),
        "firegrid.operation.input.json": stringifyJson(input),
        "firegrid.operation.key": options.key ?? "",
        "firegrid.operation.name": name,
        "firegrid.operation.status": "running"
      }
    })
  )
})

const defaultFaults: Faults = {
  killHost: (name) => new VerificationError({ message: `fault killHost(${name}) is not implemented` }),
  restartHost: (name) => new VerificationError({ message: `fault restartHost(${name}) is not implemented` }),
  killHostAfterSpan: (name) => new VerificationError({ message: `fault killHostAfterSpan(${name}) is not implemented` })
}

export interface RunPropertyOptions {
  readonly trialId?: string
  readonly faults?: Faults
  readonly reportDir?: string
}

const trialIdFromName = (name: string): string => name.replace(/[^A-Za-z0-9_.-]/g, "-")

export const runProperty = Effect.fn("runProperty")(function*<A>(
  spec: PropertySpec<A>,
  options: RunPropertyOptions = {}
) {
  const chdb = yield* ChdbClient
  const runtime = yield* VerificationRuntime
  const trialId = options.trialId ?? trialIdFromName(spec.name)
  const result = yield* Effect.exit(
    spec.workload({
      faults: options.faults ?? defaultFaults,
      operation,
      runtime
    }).pipe(
      Effect.provideService(TrialId, trialId),
      Effect.annotateSpans({
        "firegrid.property.name": spec.name,
        "firegrid.trial.id": trialId
      }),
      Effect.withSpan("verification.trial", {
        attributes: {
          "firegrid.property.name": spec.name,
          "firegrid.trial.id": trialId
        }
      })
    )
  )
  yield* runtime.flush
  const completed: CompletedTrial<A> = {
    trialId,
    result,
    chdb,
    ...(options.reportDir === undefined ? {} : { reportDir: options.reportDir })
  }
  yield* Effect.forEach(spec.checks, (check) => check.run(completed), { discard: true })
  return completed
})
