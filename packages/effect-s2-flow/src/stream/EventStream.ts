import type * as Schema from "effect/Schema"

export interface EventStreamDef<K, A> {
  readonly name: string
  readonly key: Schema.Codec<K, string>
  readonly value: Schema.Codec<A, string>
}

export const make = <K, A>(
  name: string,
  options: {
    readonly key: Schema.Codec<K, string>
    readonly value: Schema.Codec<A, string>
  }
): EventStreamDef<K, A> => ({
  name,
  key: options.key,
  value: options.value
})

export const streamName = <K, A>(definition: EventStreamDef<K, A>, key: string): string => `${definition.name}/${key}`
