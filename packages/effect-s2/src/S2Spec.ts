import { Effect, Option } from "effect"
import type { BasinConfig, StreamConfig } from "./internal/sdk.ts"
import { S2Client } from "./S2Client.ts"
import { type S2ClientError, S2NotFound } from "./S2Error.ts"

/**
 * `S2Spec` — an Effect-native declarative reconciler for S2 basins and
 * explicitly-named streams: the SDK analog of `s2 apply`, for bootstrap/runtime
 * provisioning where the CLI isn't available.
 *
 * It is a thin fold over existing `S2Client` operations — `getBasinConfig` /
 * `getStreamConfig` to diff, `ensureBasin` / `ensureStream` to create, and
 * `reconfigureBasin` / `reconfigureStream` to update — introducing no new
 * transport or protocol. It covers the *coarse, named* resources (basins +
 * singleton streams); dynamic per-key/per-execution streams are created at
 * runtime by `effect-s2-stream-db` `open`, not declared here.
 *
 * Design: docs/sdds/s2-resource-provisioning-sdd.md §1.
 */

/** A singleton, explicitly-named stream within a basin. */
export interface StreamSpec {
  readonly name: string
  readonly config?: StreamConfig
}

/** A basin and its singleton streams. */
export interface BasinSpec {
  readonly name: string
  readonly config?: BasinConfig
  readonly streams?: ReadonlyArray<StreamSpec>
}

/** A declarative spec: basins + explicitly-named streams (no tokens or ACLs). */
export interface S2Spec {
  readonly basins: ReadonlyArray<BasinSpec>
}

/** Reconcile action for one resource: create (`+`), reconfigure (`~`), no-change (`=`). */
export type Change = "create" | "reconfigure" | "noop"

/** The planned action for one declared resource. */
export interface ResourcePlan {
  readonly resource: "basin" | "stream"
  readonly name: string
  readonly basin: string
  readonly change: Change
}

/** The diff a `plan`/`apply` produces: one entry per declared resource. */
export interface S2Plan {
  readonly changes: ReadonlyArray<ResourcePlan>
}

/** The `+`/`~`/`=` marker for a change (the `s2 apply` diff vocabulary). */
export const changeMarker = (change: Change): "+" | "~" | "=" =>
  change === "create" ? "+" : change === "reconfigure" ? "~" : "="

// Canonical JSON (object keys sorted, recursively) so structural config
// comparison is insensitive to key order.
const sortDeep = (value: unknown): unknown =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([a], [b]) => (a < b ? -1 : 1))
        .map(([key, val]) => [key, sortDeep(val)]),
    )
    : value

const canonical = (value: unknown): string => JSON.stringify(sortDeep(value))

// Partial reconcile: only the fields PRESENT in the spec must match the current
// config; unspecified fields are left as-is, so they never force a reconfigure.
const partialMatches = (spec: object, current: object): boolean => {
  const cur = current as Record<string, unknown>
  return Object.entries(spec).every(([key, value]) => canonical(value) === canonical(cur[key]))
}

const changeOf = (config: object | undefined, current: Option.Option<object>): Change =>
  Option.match(current, {
    onNone: () => "create",
    onSome: (cur) => (config === undefined || partialMatches(config, cur) ? "noop" : "reconfigure"),
  })

// A config read as an Option: None when the resource doesn't exist yet.
const optionalConfig = <A>(
  read: Effect.Effect<A, S2ClientError, S2Client>,
): Effect.Effect<Option.Option<A>, S2ClientError, S2Client> =>
  read.pipe(
    Effect.map(Option.some),
    Effect.catch((cause) => (cause instanceof S2NotFound ? Effect.succeedNone : Effect.fail(cause))),
  )

const currentBasinConfig = (basin: string): Effect.Effect<Option.Option<BasinConfig>, S2ClientError, S2Client> =>
  optionalConfig(S2Client.getBasinConfig({ basin }))

const currentStreamConfig = (
  basin: string,
  stream: string,
): Effect.Effect<Option.Option<StreamConfig>, S2ClientError, S2Client> =>
  optionalConfig(S2Client.getStreamConfig({ stream }, { basinName: basin }))

const planBasin = (basin: BasinSpec): Effect.Effect<ReadonlyArray<ResourcePlan>, S2ClientError, S2Client> =>
  Effect.gen(function*() {
    const basinCurrent = yield* currentBasinConfig(basin.name)
    const basinPlan: ResourcePlan = {
      resource: "basin",
      name: basin.name,
      basin: basin.name,
      change: changeOf(basin.config, basinCurrent),
    }
    const streamPlans = yield* Effect.forEach(basin.streams ?? [], (stream) =>
      currentStreamConfig(basin.name, stream.name).pipe(
        Effect.map((current): ResourcePlan => ({
          resource: "stream",
          name: stream.name,
          basin: basin.name,
          change: changeOf(stream.config, current),
        })),
      ))
    return [basinPlan, ...streamPlans]
  })

const planChanges = (spec: S2Spec): Effect.Effect<S2Plan, S2ClientError, S2Client> =>
  Effect.forEach(spec.basins, planBasin).pipe(
    Effect.map((perBasin): S2Plan => ({ changes: perBasin.flat() })),
  )

const changeFor = (plan: S2Plan, resource: "basin" | "stream", name: string, basin: string): Change =>
  plan.changes.find((entry) => entry.resource === resource && entry.name === name && entry.basin === basin)?.change
    ?? "noop"

// One resource's reconcile: create → ensure (create-if-absent) then reconfigure
// the present fields so the spec config lands regardless of ensure's create-config
// semantics; reconfigure → drift on an existing resource; noop → untouched.
const reconcileResource = <C extends object>(
  change: Change,
  config: C | undefined,
  ensure: (config: C | undefined) => Effect.Effect<unknown, S2ClientError, S2Client>,
  reconfigure: (config: C) => Effect.Effect<unknown, S2ClientError, S2Client>,
): Effect.Effect<void, S2ClientError, S2Client> =>
  Effect.gen(function*() {
    if (change === "create") {
      yield* ensure(config)
    }
    if ((change === "create" || change === "reconfigure") && config !== undefined) {
      yield* reconfigure(config)
    }
  })

const applyBasin = (basin: BasinSpec, plan: S2Plan): Effect.Effect<void, S2ClientError, S2Client> =>
  Effect.gen(function*() {
    yield* reconcileResource(
      changeFor(plan, "basin", basin.name, basin.name),
      basin.config,
      (config) => S2Client.ensureBasin({ basin: basin.name, ...(config === undefined ? {} : { config }) }),
      (config) => S2Client.reconfigureBasin({ basin: basin.name, ...config }),
    )
    yield* Effect.forEach(basin.streams ?? [], (stream) =>
      reconcileResource(
        changeFor(plan, "stream", stream.name, basin.name),
        stream.config,
        (config) =>
          S2Client.ensureStream(
            { stream: stream.name, ...(config === undefined ? {} : { config }) },
            { basinName: basin.name },
          ),
        (config) => S2Client.reconfigureStream({ stream: stream.name, ...config }, { basinName: basin.name }),
      ), { discard: true })
  })

/**
 * Compute the create/reconfigure/no-change diff for `spec` WITHOUT mutating —
 * a dry run over `getBasinConfig` / `getStreamConfig`.
 */
export const plan = (spec: S2Spec): Effect.Effect<S2Plan, S2ClientError, S2Client> =>
  planChanges(spec).pipe(Effect.withSpan("S2.spec.plan"))

/**
 * Reconcile `spec`: `ensureBasin`/`ensureStream` for create, `reconfigureBasin`/
 * `reconfigureStream` (present fields only) for drift; no-change resources are
 * untouched. Idempotent — re-applying a converged spec is a no-op. Returns the
 * diff computed BEFORE any mutation.
 */
export const apply = (spec: S2Spec): Effect.Effect<S2Plan, S2ClientError, S2Client> =>
  Effect.gen(function*() {
    const planned = yield* planChanges(spec)
    yield* Effect.forEach(spec.basins, (basin) => applyBasin(basin, planned), { discard: true })
    return planned
  }).pipe(Effect.withSpan("S2.spec.apply"))
