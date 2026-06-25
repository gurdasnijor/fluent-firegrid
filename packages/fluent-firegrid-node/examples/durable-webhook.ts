import { genericSend, object, objectSendClient, run, service, state } from "@firegrid/fluent-firegrid"
import { primaryKey, Table } from "@firegrid/fluent-firegrid/state"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"

import type { FluentS2NodeServerOptions, FluentWebhookRoutes } from "../src/index.ts"

export interface StripeInvoiceEvent {
  readonly id: string
  readonly type: "invoice.payment_failed" | "invoice.payment_succeeded"
  readonly data: {
    readonly object: {
      readonly id: string
    }
  }
}

class InvoiceEvents extends Table<InvoiceEvents>("invoiceEvents")({
  id: Schema.String.pipe(primaryKey),
  attempts: Schema.Number,
  invoiceId: Schema.String,
  status: Schema.String
}) {}

const invoiceEvents = state(InvoiceEvents)

export const paymentTracker = object({
  name: "payment-tracker",
  handlers: {
    *onPaymentFailed(event: StripeInvoiceEvent) {
      const existing = yield* invoiceEvents.get(event.id)
      const attempts = Option.match(existing, {
        onNone: () => 1,
        onSome: (row) => row.attempts + 1
      })
      yield* invoiceEvents.set({
        attempts,
        id: event.id,
        invoiceId: event.data.object.id,
        status: "retrying"
      })

      yield* genericSend({
        delay: { minutes: 5 },
        handler: "retryCapture",
        idempotencyKey: `invoice:${event.data.object.id}:retry:${attempts}`,
        input: { attempt: attempts, eventId: event.id, invoiceId: event.data.object.id },
        key: event.data.object.id,
        kind: "object",
        name: "payment-tracker"
      })

      return { attempts, status: "retrying" }
    },

    *onPaymentSucceeded(event: StripeInvoiceEvent) {
      yield* invoiceEvents.set({
        attempts: 0,
        id: event.id,
        invoiceId: event.data.object.id,
        status: "paid"
      })
      return { status: "paid" }
    },

    *retryCapture(input: { readonly attempt: number; readonly eventId: string; readonly invoiceId: string }) {
      yield* run(() => enqueuePaymentRetry(input), { name: "enqueue-payment-retry" })
      return { queued: true }
    }
  }
})

export const stripeWebhook = service({
  name: "stripe-webhook",
  handlers: {
    *onEvent(event: StripeInvoiceEvent) {
      yield* run(() => verifyStripeEvent(event), { name: "verify-stripe-event" })

      if (event.type === "invoice.payment_failed") {
        yield* objectSendClient(paymentTracker, event.data.object.id).onPaymentFailed(event, {
          idempotencyKey: `tracker:${event.id}:failed`
        })
      }
      if (event.type === "invoice.payment_succeeded") {
        yield* objectSendClient(paymentTracker, event.data.object.id).onPaymentSucceeded(event, {
          idempotencyKey: `tracker:${event.id}:succeeded`
        })
      }

      return { accepted: true, invoiceId: event.data.object.id }
    }
  }
})

export const webhookDefinitions = [stripeWebhook, paymentTracker] as const

export const stripeWebhookRoutes = {
  "/webhooks/stripe": {
    definition: stripeWebhook,
    handler: "onEvent",
    idempotencyKey: (request) => request.headers.get("stripe-event-id") ?? undefined,
    verify: (request, body) => verifyStripeSignature(request.headers, body)
  }
} satisfies FluentWebhookRoutes

export const billingWebhookServerOptions = (
  s2Endpoint: string
): FluentS2NodeServerOptions => ({
  definitions: webhookDefinitions,
  namespace: "billing",
  port: 8080,
  s2Endpoint,
  webhooks: stripeWebhookRoutes
})

export const verifyStripeEvent = (event: StripeInvoiceEvent): boolean => event.id.length > 0

export const verifyStripeSignature = (headers: Headers, body: Uint8Array): boolean =>
  headers.get("stripe-signature") === "test-signature" && body.length > 0

export const enqueuePaymentRetry = (_input: {
  readonly attempt: number
  readonly eventId: string
  readonly invoiceId: string
}): void => {}
