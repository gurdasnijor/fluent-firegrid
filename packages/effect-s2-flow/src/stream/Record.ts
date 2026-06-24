export interface EventCursor {
  readonly stream: string
  readonly seqNum: number
}

export interface EventRecord<K, A> {
  readonly stream: string
  readonly key: K
  readonly value: A
  readonly cursor: EventCursor
  readonly headers: ReadonlyMap<string, string>
}
