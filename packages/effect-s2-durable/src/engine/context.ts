import type { AnyTable, RowOf, TableFacade } from "effect-s2-stream-db"
import * as Context from "effect/Context"
import type * as Deferred from "effect/Deferred"
import type * as Effect from "effect/Effect"
import type * as Exit from "effect/Exit"
import type * as Fiber from "effect/Fiber"
import type * as HashMap from "effect/HashMap"
import * as Option from "effect/Option"
import type * as Ref from "effect/Ref"
import type { Handler } from "../authoring/types.ts"
import type { DurableExecutionError } from "../errors.ts"
import type { ActorSnapshot } from "../object/machine/index.ts"
import type { ObjectStateBackend } from "../object/owner-driver.ts"
import type { WorkflowDb } from "../storage/service-tables.ts"

/** The opened per-execution db (success type of `WorkflowDb.open`). */
export type WfDb = Effect.Success<ReturnType<typeof WorkflowDb.open>>

/**
 * The durable record store a service `state(Table)` binding writes to: the active
 * execution's own stream.
 */
type StateStore = { readonly table: <Tbl extends AnyTable>(table: Tbl) => TableFacade<RowOf<Tbl>> }

export interface ServiceInvocation {
  readonly kind: "service"
  readonly executionId: string
  readonly handlerName: string
  readonly db: WfDb
  readonly stateDb: StateStore
  readonly inputEncoded: unknown
  readonly runSeq: Ref.Ref<number>
  readonly readSeq: Ref.Ref<number>
  readonly awakeSeq: Ref.Ref<number>
  readonly callSeq: Ref.Ref<number>
}

export interface ObjectInvocation {
  readonly kind: "object"
  readonly callId: string
  readonly method: string
  readonly inputEncoded: unknown
  readonly state: ObjectStateBackend
  readonly runSeq: Ref.Ref<number>
  readonly awakeSeq: Ref.Ref<number>
  readonly callSeq: Ref.Ref<number>
}

export interface SharedObjectInvocation {
  readonly kind: "shared"
  readonly object: string
  readonly key: string
  readonly method: string
  readonly inputEncoded: unknown
  readonly snapshot: ActorSnapshot
}

export type Invocation = ServiceInvocation | ObjectInvocation | SharedObjectInvocation

/**
 * The active-invocation slot: ambient handler-local state with a default, so a
 * Context.Reference is the right Effect primitive.
 */
export const ActiveInvocation = Context.Reference<Option.Option<Invocation>>(
  "effect-s2-durable/engine/ActiveInvocation",
  { defaultValue: () => Option.none() }
)

/** An in-process owned service execution: live fiber, result waiter, and db. */
export interface RunningEntry {
  readonly fiber: Fiber.Fiber<unknown, unknown>
  readonly deferred: Deferred.Deferred<Exit.Exit<unknown, unknown>, DurableExecutionError>
  readonly invocation: ServiceInvocation
}

/** A `run` step's terminal outcome. */
export interface StepRecord {
  readonly success: boolean
  readonly value?: unknown
  readonly error?: unknown
}

/** A durable timer fact. */
export interface TimerRecord {
  readonly deadlineMs: number
  readonly status: "pending" | "fired"
}

export type RegisteredHandler = Handler<unknown, unknown, never, never>

export interface ObjectHandlerSeed {
  readonly object: string
  readonly method: string
  readonly handler: RegisteredHandler
}

export type RunningMap = HashMap.HashMap<string, RunningEntry>
