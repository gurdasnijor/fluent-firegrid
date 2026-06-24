import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as NodeServices from "@effect/platform-node/NodeServices"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Scope from "effect/Scope"
import * as ChildProcess from "effect/unstable/process/ChildProcess"
import * as HttpClient from "effect/unstable/http/HttpClient"

import type { Faults, VerificationRuntime } from "./Property.ts"
import { VerificationError } from "./VerificationError.ts"

const ProcessHostTypeId = Symbol.for("@firegrid/verification/ProcessHost")

export interface ProcessHostLaunchContext {
  readonly trialId: string
  readonly hostId: string
  readonly hostEnv?: Record<string, string>
  readonly s2Endpoint?: string
}

export interface ProcessHostConfig {
  readonly command: string | ((context: ProcessHostLaunchContext) => string)
  readonly args?: ReadonlyArray<string> | ((context: ProcessHostLaunchContext) => ReadonlyArray<string>)
  readonly cwd?: string
  readonly env?:
    | Record<string, string | undefined>
    | ((
      context: ProcessHostLaunchContext
    ) => Record<string, string | undefined>)
  readonly extendEnv?: boolean
  readonly stdout?: "ignore" | "inherit"
  readonly stderr?: "ignore" | "inherit"
  readonly readiness?: {
    readonly url: string | ((context: ProcessHostLaunchContext) => string)
    readonly attempts?: number
    readonly interval?: Duration.Input
  }
}

export interface ProcessHostDescriptor {
  readonly [ProcessHostTypeId]: true
  readonly config: ProcessHostConfig
}

export interface HostDescriptorLike {
  readonly name: string
  readonly value: unknown
}

interface ManagedProcessHost {
  readonly name: string
  readonly start: Effect.Effect<void, VerificationError>
  readonly stop: Effect.Effect<void, VerificationError>
  readonly kill: Effect.Effect<void, VerificationError>
  readonly restart: Effect.Effect<void, VerificationError>
}

export const processHost = (config: ProcessHostConfig): ProcessHostDescriptor => ({
  [ProcessHostTypeId]: true,
  config
})

export const isProcessHost = (value: unknown): value is ProcessHostDescriptor =>
  typeof value === "object"
  && value !== null
  && ProcessHostTypeId in value
  && (value as ProcessHostDescriptor)[ProcessHostTypeId] === true

const resolve = <A>(
  value: A | ((context: ProcessHostLaunchContext) => A) | undefined,
  context: ProcessHostLaunchContext,
  fallback: A
): A => typeof value === "function" ? (value as (context: ProcessHostLaunchContext) => A)(context) : value ?? fallback

const mergeResourceAttributes = (
  existing: string | undefined,
  attributes: Record<string, string>
): string => {
  const appended = Object.entries(attributes).map(([key, value]) => `${key}=${value}`).join(",")
  return existing === undefined || existing === "" ? appended : `${existing},${appended}`
}

const processEnv = (
  config: ProcessHostConfig,
  context: ProcessHostLaunchContext
): Record<string, string | undefined> => {
  const configured = resolve(config.env, context, {})
  return {
    ...configured,
    ...context.hostEnv,
    FIREGRID_HOST_ID: context.hostId,
    FIREGRID_TRIAL_ID: context.trialId,
    ...(context.s2Endpoint === undefined ? {} : { S2_ENDPOINT: context.s2Endpoint }),
    OTEL_RESOURCE_ATTRIBUTES: mergeResourceAttributes(configured.OTEL_RESOURCE_ATTRIBUTES, {
      "firegrid.host.id": context.hostId,
      "firegrid.trial.id": context.trialId
    })
  }
}

const waitUntilReady = Effect.fn("ProcessHost.waitUntilReady")(function*(
  name: string,
  config: ProcessHostConfig,
  context: ProcessHostLaunchContext
) {
  if (config.readiness === undefined) return
  const attempts = config.readiness.attempts ?? 120
  const interval = config.readiness.interval ?? "50 millis"
  const url = resolve(config.readiness.url, context, "")

  const loop = (remaining: number): Effect.Effect<void, VerificationError, HttpClient.HttpClient> =>
    Effect.gen(function*() {
      const ready = yield* Effect.exit(HttpClient.get(url))
      if (ready._tag === "Success") return
      if (remaining <= 0) {
        return yield* new VerificationError({
          message: `host ${name} did not become ready at ${url}`,
          cause: ready.cause
        })
      }
      yield* Effect.sleep(interval)
      return yield* loop(remaining - 1)
    })

  return yield* loop(attempts).pipe(Effect.provide(NodeHttpClient.layerFetch))
})

const makeManagedHost = Effect.fn("ProcessHost.makeManagedHost")(function*(
  name: string,
  descriptor: ProcessHostDescriptor,
  context: Omit<ProcessHostLaunchContext, "hostId">
) {
  const state = yield* Ref.make<
    Option.Option<{
      readonly stop: Effect.Effect<void>
      readonly kill: Effect.Effect<void>
    }>
  >(Option.none())
  const scope = yield* Scope.Scope
  const launchContext: ProcessHostLaunchContext = { ...context, hostId: name }
  const config = descriptor.config

  const release = Effect.fn("ProcessHost.release")(function*(mode: "stop" | "kill") {
    const current = yield* Ref.get(state)
    if (Option.isNone(current)) return
    const finalizer = mode === "kill" ? current.value.kill : current.value.stop
    yield* finalizer
    yield* Ref.set(state, Option.none())
  })

  const start = Effect.fn("ProcessHost.start")(function*() {
    const current = yield* Ref.get(state)
    if (Option.isSome(current)) return

    const command = resolve(config.command, launchContext, "")
    const args = resolve(config.args, launchContext, [])
    const proc = yield* ChildProcess.make(command, args, {
      cwd: config.cwd,
      env: processEnv(config, launchContext),
      extendEnv: config.extendEnv ?? true,
      stdout: config.stdout ?? "ignore",
      stderr: config.stderr ?? "ignore"
    }).pipe(
      Effect.provide(NodeServices.layer),
      Effect.provideService(Scope.Scope, scope),
      Effect.mapError((cause) => new VerificationError({ message: `failed to start host ${name}`, cause }))
    )

    const handle = {
      stop: proc.kill({ killSignal: "SIGTERM", forceKillAfter: "5 seconds" }).pipe(Effect.ignore),
      kill: proc.kill({ killSignal: "SIGKILL" }).pipe(Effect.ignore)
    }
    yield* waitUntilReady(name, config, launchContext).pipe(
      Effect.catch((cause) => handle.kill.pipe(Effect.andThen(Effect.fail(cause))))
    )
    yield* Ref.set(state, Option.some(handle))
    yield* Scope.addFinalizer(scope, release("stop"))
  })

  const startWithSpan = start().pipe(
    Effect.withSpan("verification.host.start", {
      attributes: {
        "firegrid.host.id": name
      }
    })
  )

  const managed: ManagedProcessHost = {
    name,
    start: startWithSpan,
    stop: release("stop").pipe(
      Effect.withSpan("verification.host.stop", {
        attributes: { "firegrid.host.id": name, "verification.signal": "SIGTERM" }
      })
    ),
    kill: release("kill").pipe(
      Effect.withSpan("verification.host.kill", {
        attributes: { "firegrid.host.id": name, "verification.signal": "SIGKILL" }
      })
    ),
    restart: release("stop").pipe(
      Effect.andThen(startWithSpan),
      Effect.withSpan("verification.host.restart", {
        attributes: { "firegrid.host.id": name }
      })
    )
  }

  return managed
})

const requireManagedHost = (
  hosts: ReadonlyMap<string, ManagedProcessHost>,
  name: string
): Effect.Effect<ManagedProcessHost, VerificationError> => {
  const host = hosts.get(name)
  return host === undefined
    ? Effect.fail(new VerificationError({ message: `host ${name} is not supervised by the verification runner` }))
    : Effect.succeed(host)
}

export const makeProcessHostFaults = (
  hosts: ReadonlyMap<string, ManagedProcessHost>,
  runtime: VerificationRuntime["Service"]
): Faults => ({
  killHost: (name) => requireManagedHost(hosts, name).pipe(Effect.flatMap((host) => host.kill)),
  restartHost: (name) => requireManagedHost(hosts, name).pipe(Effect.flatMap((host) => host.restart)),
  killHostAfterSpan: (name, match) =>
    runtime.waitForSpan(match.span, match.attributes === undefined ? {} : { attributes: match.attributes }).pipe(
      Effect.andThen(requireManagedHost(hosts, name).pipe(Effect.flatMap((host) => host.kill)))
    )
})

export const startProcessHosts = Effect.fn("ProcessHost.startProcessHosts")(function*(
  hosts: Iterable<HostDescriptorLike>,
  context: Omit<ProcessHostLaunchContext, "hostId">
) {
  const managed = new Map<string, ManagedProcessHost>()
  yield* Effect.forEach(Array.from(hosts), (host) =>
    Effect.gen(function*() {
      if (!isProcessHost(host.value)) return
      const process = yield* makeManagedHost(host.name, host.value, context)
      yield* process.start
      managed.set(host.name, process)
    }), { discard: true })
  return managed
})
