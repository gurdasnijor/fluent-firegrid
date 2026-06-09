/**
 * Append decision matrix (effect-server PRODUCERS.1-8, STORE.3/6).
 *
 * Exercises the STM `MemoryStore` append decision directly — including the two
 * build-addenda corrections: PRODUCERS.7 idempotent close retry and PRODUCERS.8
 * (F1) epoch advance with non-zero seq.
 */
import { it } from "@effect/vitest"
import { expect } from "vitest"
import { Effect, Option } from "effect"
import * as MemoryStore from "../src/MemoryStore.ts"
import * as Store from "../src/Store.ts"
import type * as Protocol from "../src/Protocol.ts"

const enc = new TextEncoder()

const create = (path: string, contentType = "text/plain") =>
  Store.Store.pipe(
    Effect.flatMap((s) =>
      s.createStream({
        path,
        contentType,
        entityBody: new Uint8Array(),
        close: false,
      }),
    ),
  )

const append = (over: Partial<Protocol.AppendRequest> & { path: string }) =>
  Store.Store.pipe(
    Effect.flatMap((s) =>
      s.append({
        contentType: "text/plain",
        entityBody: enc.encode("x"),
        close: false,
        streamSeq: Option.none(),
        idempotentProducer: Option.none(),
        ...over,
      }),
    ),
  )

const producer = (id: string, epoch: number, seq: number) =>
  Option.some({ id, epoch, seq })

const run = <A, E>(effect: Effect.Effect<A, E, Store.Store>) =>
  effect.pipe(Effect.provide(MemoryStore.layer))

it.effect("plain append -> PlainAccepted (204)", () =>
  run(
    Effect.gen(function* () {
      yield* create("a")
      const r = yield* append({ path: "a" })
      expect(r.append._tag).toBe("PlainAccepted")
    }),
  ),
)

it.effect(
  "producer append -> ProducerAccepted (200), then re-send -> ProducerDuplicate (204)",
  () =>
    run(
      Effect.gen(function* () {
        yield* create("p")
        const first = yield* append({
          path: "p",
          idempotentProducer: producer("w", 0, 0),
        })
        expect(first.append._tag).toBe("ProducerAccepted")
        const dup = yield* append({
          path: "p",
          idempotentProducer: producer("w", 0, 0),
        })
        expect(dup.append._tag).toBe("ProducerDuplicate")
      }),
    ),
)

it.effect("producer sequence gap -> ProducerGap(expected)", () =>
  run(
    Effect.gen(function* () {
      yield* create("g")
      yield* append({ path: "g", idempotentProducer: producer("w", 0, 0) })
      const gap = yield* append({
        path: "g",
        idempotentProducer: producer("w", 0, 2),
      })
      expect(gap.append).toMatchObject({
        _tag: "ProducerGap",
        expectedSeq: 1,
        receivedSeq: 2,
      })
    }),
  ),
)

it.effect("stale producer epoch -> ProducerFenced(currentEpoch)", () =>
  run(
    Effect.gen(function* () {
      yield* create("f")
      yield* append({ path: "f", idempotentProducer: producer("w", 1, 0) })
      const fenced = yield* append({
        path: "f",
        idempotentProducer: producer("w", 0, 0),
      })
      expect(fenced.append).toMatchObject({
        _tag: "ProducerFenced",
        currentEpoch: 1,
      })
    }),
  ),
)

it.effect(
  "PRODUCERS.8 / F1: epoch advance with non-zero seq -> ProducerGap(expected: 0)",
  () =>
    run(
      Effect.gen(function* () {
        yield* create("f1")
        // brand-new producer presenting epoch 1 with a non-zero seq
        const gap = yield* append({
          path: "f1",
          idempotentProducer: producer("w", 1, 5),
        })
        expect(gap.append).toMatchObject({
          _tag: "ProducerGap",
          expectedSeq: 0,
          receivedSeq: 5,
        })
      }),
    ),
)

it.effect(
  "PRODUCERS.7: retried close by same producer -> ProducerDuplicate(closed) (204), NOT ClosedConflict",
  () =>
    run(
      Effect.gen(function* () {
        yield* create("c")
        const closed = yield* append({
          path: "c",
          idempotentProducer: producer("w", 0, 0),
          close: true,
        })
        expect(closed.append).toMatchObject({
          _tag: "ProducerAccepted",
          closed: true,
        })
        const retry = yield* append({
          path: "c",
          idempotentProducer: producer("w", 0, 0),
          close: true,
        })
        expect(retry.append._tag).toBe("ProducerDuplicate")
        expect(retry.append).toMatchObject({ closed: true })
      }),
    ),
)

it.effect(
  "a different writer appending to a closed stream -> ClosedConflict (409)",
  () =>
    run(
      Effect.gen(function* () {
        yield* create("cc")
        yield* append({
          path: "cc",
          idempotentProducer: producer("w1", 0, 0),
          close: true,
        })
        const conflict = yield* append({
          path: "cc",
          entityBody: enc.encode("y"),
        })
        expect(conflict.append._tag).toBe("ClosedConflict")
      }),
    ),
)

it.effect("content-type mismatch -> ContentTypeMismatch (409)", () =>
  run(
    Effect.gen(function* () {
      yield* create("ct", "text/plain")
      const mismatch = yield* append({
        path: "ct",
        contentType: "application/json",
      })
      expect(mismatch.append._tag).toBe("ContentTypeMismatch")
    }),
  ),
)

it.effect("append then catch-up read returns the appended bytes", () =>
  run(
    Effect.gen(function* () {
      yield* create("r")
      yield* append({ path: "r", entityBody: enc.encode("hello") })
      const chunk = yield* Store.Store.pipe(
        Effect.flatMap((s) => s.read("r", "-1")),
      )
      expect(new TextDecoder().decode(chunk.entityBody)).toBe("hello")
      expect(chunk.upToDate).toBe(true)
    }),
  ),
)
