import { afterEach, describe, expect, it } from "vitest"
import { startTestServer } from "./test-server"
import type { TestServerHandle } from "./test-server"

interface AcquireResponse {
  readonly epoch: number
  readonly token: string
}

interface WakeEvent {
  readonly type: string
  readonly stream?: string
  readonly consumer?: string
  readonly worker?: string
  readonly epoch?: number
}

const jsonHeaders = { "content-type": "application/json" }

const expectOk = async (response: Response): Promise<void> => {
  if (response.ok) return
  throw new Error(
    `Expected success but received ${response.status}: ${await response.text()}`,
  )
}

const readJson = async <A>(response: Response): Promise<A> => {
  await expectOk(response)
  return (await response.json()) as A
}

describe("tf-k94k package-pinned Durable Streams consumer substrate witness", () => {
  let server: TestServerHandle | undefined

  afterEach(async () => {
    await server?.stop()
    server = undefined
  })

  it("writes claimed wake events through the pinned real @durable-streams/server package", async () => {
    server = await startTestServer()

    const id = crypto.randomUUID()
    const stream = `/v1/stream/tf-k94k-events-${id}`
    const wakeStream = `/v1/stream/tf-k94k-wake-${id}`
    const consumer = `tf-k94k-consumer-${id}`
    const worker = `tf-k94k-worker-${id}`

    await expectOk(
      await fetch(`${server.url}${stream}`, {
        method: "PUT",
        headers: jsonHeaders,
        body: JSON.stringify({ kind: "events" }),
      }),
    )
    await expectOk(
      await fetch(`${server.url}${wakeStream}`, {
        method: "PUT",
        headers: jsonHeaders,
        body: JSON.stringify({ kind: "wake" }),
      }),
    )

    await expectOk(
      await fetch(`${server.url}/consumers`, {
        method: "POST",
        headers: jsonHeaders,
        body: JSON.stringify({
          consumer_id: consumer,
          streams: [stream],
        }),
      }),
    )
    await expectOk(
      await fetch(`${server.url}/consumers/${consumer}/wake`, {
        method: "PUT",
        headers: jsonHeaders,
        body: JSON.stringify({
          type: "pull-wake",
          wake_stream: wakeStream,
        }),
      }),
    )

    await expectOk(
      await fetch(`${server.url}${stream}`, {
        method: "POST",
        headers: jsonHeaders,
        body: JSON.stringify({ kind: "work" }),
      }),
    )

    const acquired = await readJson<AcquireResponse>(
      await fetch(`${server.url}/consumers/${consumer}/acquire`, {
        method: "POST",
        headers: jsonHeaders,
        body: JSON.stringify({ worker }),
      }),
    )

    expect(acquired.epoch).toBeGreaterThanOrEqual(1)
    expect(acquired.token).not.toHaveLength(0)

    const wakeEvents = await readJson<Array<WakeEvent>>(
      await fetch(`${server.url}${wakeStream}?offset=-1`),
    )
    expect(wakeEvents).toContainEqual(
      expect.objectContaining({
        type: "wake",
        stream,
        consumer,
      }),
    )
    expect(wakeEvents).toContainEqual(
      expect.objectContaining({
        type: "claimed",
        stream,
        worker,
        epoch: acquired.epoch,
      }),
    )
  })
})
