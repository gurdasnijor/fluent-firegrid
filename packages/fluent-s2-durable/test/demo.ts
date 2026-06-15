import { type Duration, Effect } from "effect"
import type { Handler } from "../src/index.ts"

/**
 * The §8 demo workflow and its external (idempotent) side-effecting services.
 *
 * `chargeCard` models a real external charge: `calls` counts every invocation
 * (so we can see replay re-runs), while `charged` only advances for a *new*
 * idempotency key. The exactly-once invariant the ACs assert is `charged === 1`
 * — it holds whether the journal write landed (replay short-circuits, so the
 * effect never re-runs) or didn't (the effect re-runs but the key dedupes).
 */
export interface OrderInput {
  readonly orderId: string
  readonly amount: number
}

export interface Receipt {
  readonly status: "fulfilled" | "rejected"
  readonly chargeId?: string
}

export interface ChargeBook {
  charged: number
  fulfilled: number
  calls: number
  fulfillCalls: number
  readonly chargedKeys: Set<string>
  readonly fulfilledKeys: Set<string>
}

export const makeChargeBook = (): ChargeBook => ({
  charged: 0,
  fulfilled: 0,
  calls: 0,
  fulfillCalls: 0,
  chargedKeys: new Set(),
  fulfilledKeys: new Set(),
})

const chargeCard = (book: ChargeBook, key: string, amount: number): Effect.Effect<string> =>
  Effect.sync(() => {
    book.calls += 1
    if (!book.chargedKeys.has(key)) {
      book.chargedKeys.add(key)
      book.charged += 1
    }
    return `charge:${key}:${amount}`
  })

const fulfill = (book: ChargeBook, chargeId: string): Effect.Effect<string> =>
  Effect.sync(() => {
    book.fulfillCalls += 1
    if (!book.fulfilledKeys.has(chargeId)) {
      book.fulfilledKeys.add(chargeId)
      book.fulfilled += 1
    }
    return `fulfilled:${chargeId}`
  })

/** The order workflow. `cooloff` is parameterized so tests can use a short sleep. */
export const makeOrderHandler = (
  book: ChargeBook,
  cooloff: Duration.Duration,
): Handler<OrderInput, Receipt> =>
  (ctx, input) =>
    Effect.gen(function* () {
      const charge = yield* ctx.run("charge", chargeCard(book, input.orderId, input.amount))
      yield* ctx.sleep("cooloff", cooloff)
      const approved = yield* ctx.awakeable<boolean>("approval")
      if (!approved) return { status: "rejected" } satisfies Receipt
      const receipt = yield* ctx.run("fulfill", fulfill(book, charge))
      return { status: "fulfilled", chargeId: receipt } satisfies Receipt
    })
