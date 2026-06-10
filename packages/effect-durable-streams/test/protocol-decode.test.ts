/**
 * HTTP decode path over a real server (effect-server HTTP.5, PRODUCERS): a
 * well-formed producer append round-trips to 200 with producer response
 * headers, and a malformed producer integer header is a 400 — exercising the
 * Schema-based strict-integer decode (UintFromString).
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

const streamUrl = (p: string) => `${server.baseUrl}/v1/stream/${p}`

test("well-formed producer append -> 200 with producer response headers", async () => {
  const url = streamUrl(`prod-${Date.now()}`)
  await fetch(url, { method: "PUT", headers: { "Content-Type": "text/plain" } })

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "text/plain",
      "Producer-Id": "w",
      "Producer-Epoch": "0",
      "Producer-Seq": "0",
    },
    body: "hi",
  })
  expect(res.status).toBe(200)
  expect(res.headers.get("producer-epoch")).toBe("0")
  expect(res.headers.get("producer-seq")).toBe("0")
})

test("malformed producer integer header -> 400 (strict-uint Schema rejects)", async () => {
  const url = streamUrl(`prod-bad-${Date.now()}`)
  await fetch(url, { method: "PUT", headers: { "Content-Type": "text/plain" } })

  // Note: leading/trailing whitespace is stripped by the HTTP transport before
  // the decode sees it, so " 1" is not a meaningful over-the-wire malformed case.
  for (const epoch of ["1e3", "-1", "0x1", "1.5", "abc"]) {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "text/plain",
        "Producer-Id": "w",
        "Producer-Epoch": epoch,
        "Producer-Seq": "0",
      },
      body: "hi",
    })
    expect(res.status, `epoch=${JSON.stringify(epoch)}`).toBe(400)
  }
})
