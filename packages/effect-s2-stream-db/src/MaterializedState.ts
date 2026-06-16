import { Option } from "effect"
import type { Message } from "./ChangeMessage.ts"
import { isChange } from "./ChangeMessage.ts"

/**
 * The State-Protocol fold: latest-value-per-`(type, key)`.
 *
 * This is the single materialization used by both the live apply-on-ack path
 * and cold replay — handed the same decoded `Message`, it cannot diverge
 * between the two (the directionality invariant, SDD §A4).
 *
 * Owner-process, in-memory, single-writer. Not thread-safe by design.
 */
export class MaterializedState {
  private readonly byType = new Map<string, Map<string, unknown>>()

  static empty(): MaterializedState {
    return new MaterializedState()
  }

  /** Apply one decoded message in stream order. */
  apply(message: Message): void {
    if (!isChange(message)) {
      // `snapshot-start` begins a fresh full-state dump; `reset` clears.
      // `snapshot-end` is a boundary marker with no effect on the fold.
      if (message.headers.control !== "snapshot-end") {
        this.byType.clear()
      }
      return
    }
    const collection = this.collectionFor(message.type)
    if (message.headers.operation === "delete") {
      collection.delete(message.key)
    } else {
      collection.set(message.key, message.value)
    }
  }

  /** Latest value at `(type, key)`, if present. */
  get(type: string, key: string): Option.Option<unknown> {
    return Option.fromNullishOr(this.byType.get(type)?.get(key))
  }

  /** All live values of a type, in insertion order. */
  values(type: string): ReadonlyArray<unknown> {
    const collection = this.byType.get(type)
    return collection === undefined ? [] : Array.from(collection.values())
  }

  /** All live `(type, key, value)` entries — the snapshot source for compaction. */
  entries(): ReadonlyArray<{ readonly type: string; readonly key: string; readonly value: unknown }> {
    return Array.from(this.byType.entries()).flatMap(([type, collection]) =>
      Array.from(collection.entries()).map(([key, value]) => ({ type, key, value })),
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
