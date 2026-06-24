import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer"
import { ChdbClient } from "@firegrid/observability"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Exit from "effect/Exit"
import { FileSystem } from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Random from "effect/Random"

import { makeProcessHostFaults, startProcessHosts } from "./ProcessHost.ts"
import { spanSummary, writeTrialReport, type WrittenTrialReport } from "./Report.ts"
import { makeS2Runtime, type S2Runtime } from "./S2Runtime.ts"
import { type S2LiteConfig, S2LiteSupervisor } from "./S2LiteSupervisor.ts"
import { runTraceProof, traceOperation, type TraceOperationMatch, type TraceProof, traceSql } from "./TraceProof.ts"
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

export interface Hosts {
  readonly kill: (name: string) => Effect.Effect<void, VerificationError>
  readonly restart: (name: string) => Effect.Effect<void, VerificationError>
  readonly killAfterSpan: (
    name: string,
    match: {
      readonly span: string
      readonly attributes?: Record<string, string>
    }
  ) => Effect.Effect<void, VerificationError>
}

const hostsFromFaults = (faults: Faults): Hosts => ({
  kill: faults.killHost,
  restart: faults.restartHost,
  killAfterSpan: faults.killHostAfterSpan
})

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
  readonly s2: S2Runtime
  readonly hosts: Hosts
  readonly faults: Faults
  readonly runtime: VerificationRuntime["Service"]
  readonly operation: typeof operation
  readonly s2Endpoint?: string
}

export interface CompletedTrial<A> {
  readonly trialId: string
  readonly result: Exit.Exit<A, unknown>
  readonly chdb: ChdbClient["Service"]
  readonly reportDir?: string
  readonly report?: WrittenTrialReport
}

export interface Check<A> {
  readonly name: string
  readonly run: (trial: CompletedTrial<A>) => Effect.Effect<void, VerificationError>
}

export interface Verifiers<A> {
  readonly expect: {
    readonly workloadResult: (expected: unknown) => Check<A>
  }
  readonly traceOperation: (name: string, match: TraceOperationMatch) => TraceProof
  readonly traceSql: (name: string, sql: string) => TraceProof
}

type Verification<A> = Check<A> | TraceProof
type VerificationCollection<A> = ReadonlyArray<Verification<A>> | Record<string, Verification<A>>
type VerificationFactory<A> = (verifiers: Verifiers<A>) => VerificationCollection<A>

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

  verify(factory: VerificationFactory<A>): PropertySpec<A>
  verify(collection: VerificationCollection<A>): PropertySpec<A>
  verify(...checks: ReadonlyArray<Verification<A>>): PropertySpec<A>
  verify(
    first?: Verification<A> | VerificationCollection<A> | VerificationFactory<A>,
    ...rest: ReadonlyArray<Verification<A>>
  ): PropertySpec<A> {
    if (this.spec.workload === undefined) {
      throw new VerificationError({ message: `property ${this.spec.name} is missing a workload` })
    }
    const checks = verificationEntries(first, rest, verifiers<A>())
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

const verifiers = <A>(): Verifiers<A> => ({
  expect: {
    workloadResult: expectWorkloadResult<A>
  },
  traceOperation,
  traceSql
})

const isVerification = <A>(value: unknown): value is Verification<A> =>
  typeof value === "object"
  && value !== null
  && "name" in value
  && ("run" in value || "sql" in value)

const verificationEntries = <A>(
  first: Verification<A> | VerificationCollection<A> | VerificationFactory<A> | undefined,
  rest: ReadonlyArray<Verification<A>>,
  helpers: Verifiers<A>
): ReadonlyArray<Verification<A>> => {
  if (first === undefined) return rest
  if (typeof first === "function") return collectionEntries(first(helpers))
  if (rest.length > 0) return [first as Verification<A>, ...rest]
  if (isVerification<A>(first)) return [first]
  return collectionEntries(first)
}

const collectionEntries = <A>(collection: VerificationCollection<A>): ReadonlyArray<Verification<A>> =>
  Array.isArray(collection) ? collection : Object.values(collection)

export const expectWorkloadResult = <A = unknown>(expected: unknown): Check<A> => ({
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
  readonly s2Lite?: Partial<S2LiteConfig>
}

const trialIdFromName = (name: string): string => name.replace(/[^A-Za-z0-9_.-]/g, "-")

const allocatePort = Effect.fn("runProperty.allocatePort")(function*() {
  return yield* Effect.scoped(
    Effect.gen(function*() {
      const server = yield* NodeSocketServer.make({ host: "127.0.0.1", port: 0 }).pipe(
        Effect.mapError((cause) => new VerificationError({ message: "failed to allocate a local port", cause }))
      )
      if (server.address._tag !== "TcpAddress") {
        return yield* new VerificationError({ message: "allocated a non-TCP socket for s2 lite" })
      }
      return server.address.port
    })
  )
})

const makeLocalRoot = Effect.fn("runProperty.makeLocalRoot")(function*() {
  const fs = yield* FileSystem
  return yield* fs.makeTempDirectoryScoped({ prefix: "firegrid-verification-s2lite-" }).pipe(
    Effect.mapError((cause) => new VerificationError({ message: "failed to create s2 lite local root", cause }))
  )
})

const resolveS2LiteConfig = Effect.fn("runProperty.resolveS2LiteConfig")(function*(
  options: Partial<S2LiteConfig> | undefined
) {
  const port = options?.port ?? (yield* allocatePort())
  const localRoot = options?.localRoot ?? (yield* makeLocalRoot().pipe(Effect.provide(NodeFileSystem.layer)))
  return {
    ...options,
    port,
    localRoot
  } satisfies S2LiteConfig
})

const startS2Lite = Effect.fn("runProperty.startS2Lite")(function*(
  spec: S2LiteSpec | undefined,
  options: Partial<S2LiteConfig> | undefined
) {
  if (spec === undefined) return undefined
  const config = yield* resolveS2LiteConfig(options)
  return yield* Effect.gen(function*() {
    const supervisor = yield* S2LiteSupervisor
    yield* supervisor.start
    yield* Effect.addFinalizer(() => supervisor.kill.pipe(Effect.ignore))
    return yield* supervisor.endpoint
  }).pipe(
    Effect.provide(S2LiteSupervisor.layer(config)),
    Effect.mapError((cause) => new VerificationError({ message: "failed to start s2 lite", cause }))
  )
})

export const runProperty = Effect.fn("runProperty")(function*<A>(
  spec: PropertySpec<A>,
  options: RunPropertyOptions = {}
) {
  const chdb = yield* ChdbClient
  const runtime = yield* VerificationRuntime
  const trialId = options.trialId ?? trialIdFromName(spec.name)
  return yield* Effect.scoped(
    Effect.gen(function*() {
      const s2Endpoint = yield* startS2Lite(spec.s2Lite, options.s2Lite)
      const supervisedHosts = yield* startProcessHosts(spec.hosts.values(), {
        trialId,
        ...(s2Endpoint === undefined ? {} : { s2Endpoint })
      })
      const faults = options.faults ?? (
        supervisedHosts.size === 0 ? defaultFaults : makeProcessHostFaults(supervisedHosts, runtime)
      )
      const hosts = hostsFromFaults(faults)
      const s2 = makeS2Runtime(s2Endpoint)
      const result = yield* Effect.exit(
        spec.workload({
          faults,
          hosts,
          operation,
          runtime,
          s2,
          ...(s2Endpoint === undefined ? {} : { s2Endpoint })
        })
      )
      yield* runtime.flush
      const completed: CompletedTrial<A> = {
        trialId,
        result,
        chdb,
        ...(options.reportDir === undefined ? {} : { reportDir: options.reportDir })
      }
      yield* Effect.forEach(spec.checks, (check) =>
        Effect.gen(function*() {
          const checkExit = yield* Effect.exit(check.run(completed))
          if (Exit.isSuccess(checkExit)) return
          if (options.reportDir === undefined) {
            return yield* new VerificationError({
              message: `check ${check.name} failed`,
              cause: checkExit.cause
            })
          }
          const report = yield* writeTrialReport({
            trialId,
            checks: spec.checks.length,
            status: "failed",
            reportDir: options.reportDir,
            failedCheck: check.name,
            failure: String(checkExit.cause)
          }).pipe(Effect.provideService(ChdbClient, chdb))
          const observed = yield* spanSummary(trialId).pipe(Effect.provideService(ChdbClient, chdb))
          return yield* new VerificationError({
            message: `check ${check.name} failed${
              report.path === undefined ? "" : `; report written to ${report.path}`
            }\n\nObserved spans:\n${observed}`,
            cause: checkExit.cause
          })
        }), { discard: true })
      if (options.reportDir === undefined) return completed
      const report = yield* writeTrialReport({
        trialId,
        checks: spec.checks.length,
        status: "passed",
        reportDir: options.reportDir
      }).pipe(Effect.provideService(ChdbClient, chdb))
      return { ...completed, report }
    })
  ).pipe(
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
})
