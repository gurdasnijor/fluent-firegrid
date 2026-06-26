import {
  all,
  celFor,
  genericSend,
  object,
  objectKey,
  objectSendClient,
  run,
  schemas,
  service,
  state,
  workflow,
  workflowSendClient
} from "@firegrid/fluent"
import { primaryKey, Table } from "@firegrid/fluent/state"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"

import type { FluentS2NodeServerOptions } from "../src/index.ts"

export interface AddCartItemInput {
  readonly customerId: string
  readonly quantity: number
  readonly sku: string
}

export interface CheckoutCartInput extends AddCartItemInput {
  readonly paymentToken: string
  readonly requestId: string
  readonly shippingAddress: string
}

export interface CheckoutOrderInput extends CheckoutCartInput {
  readonly cartId: string
}

export interface OrderShipmentInput {
  readonly orderId: string
  readonly shippingAddress: string
}

class CartRows extends Table<CartRows>("commerceCarts")({
  cartId: Schema.String.pipe(primaryKey),
  customerId: Schema.String,
  quantity: Schema.Number,
  sku: Schema.String,
  status: Schema.String
}) {}

class OrderRows extends Table<OrderRows>("commerceOrders")({
  chargeId: Schema.String,
  customerId: Schema.String,
  orderId: Schema.String.pipe(primaryKey),
  reservationId: Schema.String,
  shipmentId: Schema.String,
  status: Schema.String
}) {}

const carts = state(CartRows)
const orders = state(OrderRows)
const orderStatus = celFor(OrderRows)

export const orderAggregate = object({
  name: "order",
  handlers: {
    *create(input: CheckoutOrderInput & { readonly orderId: string }) {
      yield* orders.set({
        chargeId: "",
        customerId: input.customerId,
        orderId: input.orderId,
        reservationId: "",
        shipmentId: "",
        status: "created"
      })
      return { orderId: input.orderId, status: "created" }
    },

    *markPaid(input: { readonly chargeId: string; readonly orderId: string; readonly reservationId: string }) {
      const existing = yield* orders.get(input.orderId)
      const row = Option.getOrElse(existing, () => ({
        chargeId: "",
        customerId: "",
        orderId: input.orderId,
        reservationId: "",
        shipmentId: "",
        status: "created"
      }))
      yield* orders.set({
        ...row,
        chargeId: input.chargeId,
        reservationId: input.reservationId,
        status: "paid"
      })
      return { orderId: input.orderId, status: "paid" }
    },

    *markReadyToShip(input: { readonly labelId: string; readonly orderId: string }) {
      const existing = yield* orders.get(input.orderId)
      const row = Option.getOrElse(existing, () => ({
        chargeId: "",
        customerId: "",
        orderId: input.orderId,
        reservationId: "",
        shipmentId: "",
        status: "paid"
      }))
      yield* orders.set({
        ...row,
        shipmentId: input.labelId,
        status: "ready-to-ship"
      })
      return { orderId: input.orderId, status: "ready-to-ship" }
    },

    *markShipped(input: { readonly orderId: string; readonly trackingId: string }) {
      const existing = yield* orders.get(input.orderId)
      const row = Option.getOrElse(existing, () => ({
        chargeId: "",
        customerId: "",
        orderId: input.orderId,
        reservationId: "",
        shipmentId: "",
        status: "ready-to-ship"
      }))
      yield* orders.set({
        ...row,
        shipmentId: input.trackingId,
        status: "shipped"
      })
      return { orderId: input.orderId, status: "shipped", trackingId: input.trackingId }
    },

    *waitUntilShipped(input: { readonly timeoutMs: number }) {
      const orderId = yield* objectKey
      const shipped = yield* orders.waitFor(orderId, {
        name: "order-shipped",
        timeoutMs: input.timeoutMs,
        when: orderStatus.expr((t) => t.row.status.eq("shipped"))
      })
      return shipped
    }
  }
})

export const fulfillmentService = service({
  name: "fulfillment",
  handlers: {
    *requestShipment(input: OrderShipmentInput) {
      const label = yield* run(() => purchaseShippingLabel(input), { name: "purchase-shipping-label" })
      yield* objectSendClient(orderAggregate, input.orderId).markReadyToShip({
        labelId: label.labelId,
        orderId: input.orderId
      }, {
        idempotencyKey: `order:${input.orderId}:ready-to-ship`
      })
      yield* genericSend({
        delay: { minutes: 1 },
        handler: "ship",
        idempotencyKey: `order:${input.orderId}:ship`,
        input: { orderId: input.orderId, shippingAddress: input.shippingAddress },
        kind: "service",
        name: "fulfillment"
      })
      return { labelId: label.labelId, queued: true }
    },

    *ship(input: OrderShipmentInput) {
      const shipment = yield* run(() => shipOrder(input), { name: "ship-order" })
      yield* objectSendClient(orderAggregate, input.orderId).markShipped({
        orderId: input.orderId,
        trackingId: shipment.trackingId
      }, {
        idempotencyKey: `order:${input.orderId}:shipped`
      })
      return shipment
    }
  }
})

export const checkoutWorkflow = workflow({
  name: "checkout",
  handlers: {
    *submit(input: CheckoutOrderInput) {
      const orderId = yield* run(() => createOrderId(input), { name: "create-order-id" })
      yield* objectSendClient(orderAggregate, orderId).create({ ...input, orderId }, {
        idempotencyKey: `order:${orderId}:create`
      })

      const [reservation, charge] = yield* all([
        run(() => reserveInventory(input), { name: "reserve-inventory" }),
        run(() => authorizePayment(input), { name: "authorize-payment" })
      ])

      yield* objectSendClient(orderAggregate, orderId).markPaid({
        chargeId: charge.chargeId,
        orderId,
        reservationId: reservation.reservationId
      }, {
        idempotencyKey: `order:${orderId}:paid`
      })
      yield* genericSend({
        handler: "requestShipment",
        idempotencyKey: `order:${orderId}:fulfillment`,
        input: { orderId, shippingAddress: input.shippingAddress },
        kind: "service",
        name: "fulfillment"
      })

      return {
        chargeId: charge.chargeId,
        orderId,
        reservationId: reservation.reservationId,
        status: "accepted"
      }
    }
  },
  descriptors: {
    submit: schemas({
      input: Schema.Struct({
        cartId: Schema.String,
        customerId: Schema.String,
        paymentToken: Schema.String,
        quantity: Schema.Number,
        requestId: Schema.String,
        shippingAddress: Schema.String,
        sku: Schema.String
      }),
      output: Schema.Struct({
        chargeId: Schema.String,
        orderId: Schema.String,
        reservationId: Schema.String,
        status: Schema.String
      })
    })
  }
})

export const shoppingCart = object({
  name: "shopping-cart",
  handlers: {
    *addItem(input: AddCartItemInput) {
      const cartId = yield* objectKey
      yield* carts.set({
        cartId,
        customerId: input.customerId,
        quantity: input.quantity,
        sku: input.sku,
        status: "open"
      })
      return { cartId, status: "open" }
    },

    *checkout(input: CheckoutCartInput) {
      const cartId = yield* objectKey
      const existing = yield* carts.get(cartId)
      const cart = Option.getOrElse(existing, () => ({
        cartId,
        customerId: input.customerId,
        quantity: input.quantity,
        sku: input.sku,
        status: "open"
      }))
      yield* carts.set({ ...cart, status: "checkout-started" })
      const handle = yield* workflowSendClient(checkoutWorkflow).submit({
        cartId,
        customerId: cart.customerId,
        paymentToken: input.paymentToken,
        quantity: cart.quantity,
        requestId: input.requestId,
        shippingAddress: input.shippingAddress,
        sku: cart.sku
      }, {
        idempotencyKey: `checkout:${cartId}:${input.requestId}`
      })
      return { cartId, checkoutRunId: handle.invocationId, status: "checkout-started" }
    }
  }
})

export const orderingDefinitions = [
  shoppingCart,
  checkoutWorkflow,
  orderAggregate,
  fulfillmentService
] as const

export const orderingServerOptions = (
  s2Endpoint: string
): FluentS2NodeServerOptions => ({
  definitions: orderingDefinitions,
  namespace: "ordering",
  port: 8080,
  s2Endpoint
})

export const createOrderId = (input: CheckoutOrderInput): string => `order-${input.requestId}`

export const reserveInventory = (input: CheckoutOrderInput): { readonly reservationId: string } => ({
  reservationId: `reservation-${input.sku}-${input.requestId}`
})

export const authorizePayment = (input: CheckoutOrderInput): { readonly chargeId: string } => ({
  chargeId: `charge-${input.paymentToken}-${input.requestId}`
})

export const purchaseShippingLabel = (input: OrderShipmentInput): { readonly labelId: string } => ({
  labelId: `label-${input.orderId}`
})

export const shipOrder = (input: OrderShipmentInput): { readonly trackingId: string } => ({
  trackingId: `tracking-${input.orderId}`
})
