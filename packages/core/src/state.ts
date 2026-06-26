import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"

import type { FluentFiregridError } from "./error.ts"
export {
  cel,
  type CelExpressionBuilder,
  type CelExpressionInput,
  type CelExpressionNode,
  type CelFactory,
  type CelFieldExpression,
  type CelLiteral,
  type CelStatePredicate,
  evaluateStatePredicate,
  type StatePredicate,
  type StatePredicateContext,
  validateStatePredicate
} from "./statePredicate.ts"
import type { StatePredicate } from "./statePredicate.ts"

export interface StateWaitBackendOptions {
  readonly environmentVersion?: string
  readonly name: string
  readonly signalName: string
  readonly timeoutAt?: number
  readonly timeoutMs?: number
  readonly waitId?: string
}

export interface StateIndexWaitBackendOptions extends StateWaitBackendOptions {
  readonly index: ReadonlyArray<string>
  readonly indexKey: string
  readonly vars: Readonly<Record<string, unknown>>
}

export interface ObjectStateBackend {
  readonly get: (
    table: string,
    key: string,
    options?: { readonly readId?: string }
  ) => Effect.Effect<Option.Option<unknown>, FluentFiregridError>
  readonly set: (
    table: string,
    key: string,
    value: unknown,
    options?: { readonly opId?: string }
  ) => Effect.Effect<void, FluentFiregridError>
  readonly delete: (
    table: string,
    key: string,
    options?: { readonly opId?: string }
  ) => Effect.Effect<void, FluentFiregridError>
  readonly waitFor?: (
    table: string,
    key: string,
    predicate: StatePredicate,
    options: StateWaitBackendOptions
  ) => Effect.Effect<Option.Option<unknown>, FluentFiregridError>
  readonly waitForIndex?: (
    table: string,
    predicate: StatePredicate,
    options: StateIndexWaitBackendOptions
  ) => Effect.Effect<Option.Option<unknown>, FluentFiregridError>
}

export type StateOperation = "insert" | "update" | "delete"
export type StateControl = "snapshot-start" | "snapshot-end" | "reset"

export interface StateChangeMessage {
  readonly type: string
  readonly key: string
  readonly value?: unknown
  readonly old_value?: unknown
  readonly headers: {
    readonly operation: StateOperation
    readonly callId?: string
    readonly ownerId?: string
    readonly txid?: string
  }
}

export interface StateControlMessage {
  readonly headers: {
    readonly control: StateControl
    readonly offset?: string
  }
}

export interface StateReadJournaledMessage {
  readonly type: string
  readonly key: string
  readonly value?: unknown
  readonly headers: {
    readonly callId?: string
    readonly ownerId?: string
    readonly present: boolean
    readonly read: "journaled"
    readonly readId: string
  }
}

export type StateMessage = StateChangeMessage | StateControlMessage | StateReadJournaledMessage

export namespace ChangeMessage {
  export const Operation = Schema.Literals(["insert", "update", "delete"])
  export type Operation = typeof Operation.Type

  export const Control = Schema.Literals(["snapshot-start", "snapshot-end", "reset"])
  export type Control = typeof Control.Type

  export const ChangeMessage = Schema.Struct({
    type: Schema.String,
    key: Schema.String,
    value: Schema.optional(Schema.Unknown),
    old_value: Schema.optional(Schema.Unknown),
    headers: Schema.Struct({
      operation: Operation,
      callId: Schema.optional(Schema.String),
      ownerId: Schema.optional(Schema.String),
      txid: Schema.optional(Schema.String)
    })
  })
  export type ChangeMessage = typeof ChangeMessage.Type

  export const ControlMessage = Schema.Struct({
    headers: Schema.Struct({
      control: Control,
      offset: Schema.optional(Schema.String)
    })
  })
  export type ControlMessage = typeof ControlMessage.Type

  export const ReadJournaledMessage = Schema.Struct({
    type: Schema.String,
    key: Schema.String,
    value: Schema.optional(Schema.Unknown),
    headers: Schema.Struct({
      callId: Schema.optional(Schema.String),
      ownerId: Schema.optional(Schema.String),
      present: Schema.Boolean,
      read: Schema.Literal("journaled"),
      readId: Schema.String
    })
  })
  export type ReadJournaledMessage = typeof ReadJournaledMessage.Type

  export const Message = Schema.Union([ChangeMessage, ControlMessage, ReadJournaledMessage])
  export type Message = typeof Message.Type

  export const isChange = (message: Message): message is ChangeMessage => "operation" in message.headers

  export const isControl = (message: Message): message is ControlMessage => "control" in message.headers

  export const isReadJournaled = (message: Message): message is ReadJournaledMessage => "read" in message.headers

  const Json = Schema.UnknownFromJsonString

  export const encode = (message: Message): Effect.Effect<string, Schema.SchemaError> =>
    Schema.encodeEffect(Message)(message).pipe(Effect.flatMap(Schema.encodeEffect(Json)))

  export const decode = (body: string): Effect.Effect<Message, Schema.SchemaError> =>
    Schema.decodeEffect(Json)(body).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Message)))
}

export class MaterializedState {
  private readonly byType = new Map<string, Map<string, unknown>>()

  static empty(): MaterializedState {
    return new MaterializedState()
  }

  apply(message: ChangeMessage.Message): void {
    if (ChangeMessage.isReadJournaled(message)) {
      return
    }
    if (ChangeMessage.isControl(message)) {
      if (message.headers.control !== "snapshot-end") this.byType.clear()
      return
    }
    const collection = this.collectionFor(message.type)
    if (message.headers.operation === "delete") {
      collection.delete(message.key)
    } else {
      collection.set(message.key, message.value)
    }
  }

  get(type: string, key: string): Option.Option<unknown> {
    return Option.fromNullishOr(this.byType.get(type)?.get(key))
  }

  values(type: string): ReadonlyArray<unknown> {
    const collection = this.byType.get(type)
    return collection === undefined ? [] : Array.from(collection.values())
  }

  entries(): ReadonlyArray<{ readonly type: string; readonly key: string; readonly value: unknown }> {
    return Array.from(this.byType.entries()).flatMap(([type, collection]) =>
      Array.from(collection.entries()).map(([key, value]) => ({ type, key, value }))
    )
  }

  private collectionFor(type: string): Map<string, unknown> {
    const existing = this.byType.get(type)
    if (existing !== undefined) {
      return existing
    }
    const created = new Map<string, unknown>()
    this.byType.set(type, created)
    return created
  }
}

const stableJson = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`
  if (typeof value === "object" && value !== null) {
    return `{${
      Object.entries(value as Record<string, unknown>).sort(([left], [right]) => left.localeCompare(right))
        .map(([key, nested]) => `${JSON.stringify(key)}:${stableJson(nested)}`).join(",")
    }}`
  }
  return JSON.stringify(value)
}

export const stateIndexKey = (
  index: ReadonlyArray<string>,
  vars: Readonly<Record<string, unknown>>
): string => index.map((field) => `${field}=${stableJson(vars[field])}`).join("&")
