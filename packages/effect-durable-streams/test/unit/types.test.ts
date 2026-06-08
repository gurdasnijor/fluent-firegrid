/**
 * Public-surface lockdown via expect-type. The assertions are type-only —
 * each `it` runs a typechecker pass; nothing is invoked at runtime. The
 * test bodies define functions that TypeScript checks but vitest never
 * calls.
 */
import type { HttpClient } from "@effect/platform"
import { expectTypeOf } from "expect-type"
import type { Effect, Schema, Scope, Sink, Stream } from "effect"
import { describe, it } from "vitest"
import { DurableStream } from "../../src/index.ts"

declare const TestSchema: Schema.Schema<{ n: number }, { n: number }>
declare const url: string

describe("public surface (type-level)", () => {
  it("read returns Stream<A, ReadError, HttpClient>", () => {
    const _check = (): void => {
      const result = DurableStream.read({ endpoint: { url }, schema: TestSchema })
      expectTypeOf(result).toEqualTypeOf<
        Stream.Stream<{ n: number }, DurableStream.ReadError, HttpClient.HttpClient>
      >()
    }
    void _check
  })

  it("collect returns Effect<ReadonlyArray<A>, ReadError, HttpClient>", () => {
    const _check = (): void => {
      const result = DurableStream.collect({ endpoint: { url }, schema: TestSchema })
      expectTypeOf(result).toEqualTypeOf<
        Effect.Effect<
          ReadonlyArray<{ n: number }>,
          DurableStream.ReadError,
          HttpClient.HttpClient
        >
      >()
    }
    void _check
  })

  it("snapshotThenFollow returns Effect<SnapshotResult, ReadError, HttpClient>", () => {
    const _check = (): void => {
      const result = DurableStream.snapshotThenFollow({
        endpoint: { url },
        schema: TestSchema,
      })
      expectTypeOf(result).toEqualTypeOf<
        Effect.Effect<
          DurableStream.SnapshotResult<{ n: number }>,
          DurableStream.ReadError,
          HttpClient.HttpClient
        >
      >()
    }
    void _check
  })

  it("append returns Effect<{offset}, WriteError, HttpClient>", () => {
    const _check = (): void => {
      const result = DurableStream.append({
        endpoint: { url },
        schema: TestSchema,
        event: { n: 1 },
      })
      expectTypeOf(result).toEqualTypeOf<
        Effect.Effect<
          { readonly offset: DurableStream.Offset },
          DurableStream.WriteError,
          HttpClient.HttpClient
        >
      >()
    }
    void _check
  })

  it("producer returns Effect<Producer<A>, TransportError, HttpClient | Scope>", () => {
    const _check = (): void => {
      const result = DurableStream.producer({
        endpoint: { url },
        schema: TestSchema,
        producerId: "p",
      })
      expectTypeOf(result).toEqualTypeOf<
        Effect.Effect<
          DurableStream.Producer<{ n: number }>,
          DurableStream.TransportError,
          HttpClient.HttpClient | Scope.Scope
        >
      >()
    }
    void _check
  })

  it("Producer<A> extends Sink<void, A, never, ProducerFailure, never>", () => {
    expectTypeOf<DurableStream.Producer<{ n: number }>>().toMatchTypeOf<
      Sink.Sink<void, { n: number }, never, DurableStream.ProducerFailure, never>
    >()
  })

  it("HeadResult shape includes etag + cacheControl", () => {
    expectTypeOf<DurableStream.HeadResult>().toMatchTypeOf<{
      readonly offset: DurableStream.Offset
      readonly contentType: string | undefined
      readonly streamClosed: boolean
      readonly ttlSeconds: number | undefined
      readonly expiresAt: string | undefined
      readonly etag: string | undefined
      readonly cacheControl: string | undefined
    }>()
  })

  it("Bound carries the same typed surface", () => {
    const _check = (): void => {
      const b = DurableStream.define({ endpoint: { url }, schema: TestSchema })
      expectTypeOf(b.read()).toEqualTypeOf<
        Stream.Stream<{ n: number }, DurableStream.ReadError, HttpClient.HttpClient>
      >()
      expectTypeOf(b.head).toEqualTypeOf<
        Effect.Effect<
          DurableStream.HeadResult,
          DurableStream.TransportError | DurableStream.NotFound | DurableStream.Gone,
          HttpClient.HttpClient
        >
      >()
    }
    void _check
  })
})
