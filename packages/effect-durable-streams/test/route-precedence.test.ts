/**
 * Reserved route precedence + slash-path round-trip (effect-server HTTP.7,
 * HTTP_API.8, CONFORMANCE.9) — the red probe required before advertising the
 * control plane.
 *
 *   (a) a slash-containing stream path round-trips through the stream routes
 *       without truncation;
 *   (b) an undefined `/v1/stream/__ds/*` path is rejected (never creates a user
 *       stream named `__ds/...`).
 */
import { afterAll, beforeAll, expect, test } from "vitest"
import { startServer } from "./support/start-server.ts"
import type { Running } from "./support/start-server.ts"

let server: Running

beforeAll(async () => {
  server = await startServer()
})
afterAll(async () => {
  await server.close()
})

test("slash-containing stream path round-trips through stream routes untruncated", async () => {
  const path = "rooms/general/messages"
  const url = `${server.baseUrl}/v1/stream/${path}`

  const created = await fetch(url, {
    method: "PUT",
    headers: { "Content-Type": "text/plain" },
  })
  expect(created.status).toBeLessThan(300)

  const appended = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "text/plain" },
    body: "hello",
  })
  expect(appended.status).toBeLessThan(300)

  const read = await fetch(`${url}?offset=-1`)
  expect(read.status).toBe(200)
  expect(await read.text()).toBe("hello")
})

test("undefined /v1/stream/__ds/* is rejected and never creates a user stream", async () => {
  const reserved = `${server.baseUrl}/v1/stream/__ds/subscriptions/x`

  const put = await fetch(reserved, {
    method: "PUT",
    headers: { "Content-Type": "text/plain" },
  })
  expect(put.status).toBe(404)

  // It must not have been created as a stream named `__ds/subscriptions/x`.
  const head = await fetch(reserved, { method: "HEAD" })
  expect(head.status).toBe(404)
})
