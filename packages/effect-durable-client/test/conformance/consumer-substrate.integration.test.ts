import { afterEach, describe, expect, it } from "vitest"
import { startTestServer } from "./test-server"
import type { TestServerHandle } from "./test-server"

interface AcquireResponse {
  readonly generation: number
  readonly token: string
}

interface WakeEvent {
  readonly type: string
  readonly stream?: string
  readonly subscription_id?: string
  readonly generation?: number
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

describe("tf-k94k package-pinned Durable Streams subscription substrate witness", () => {
  let server: TestServerHandle | undefined

  afterEach(async () => {
    await server?.stop()
    server = undefined
  })

  it("writes wake events through the pinned real @durable-streams/server package", async () => {
    server = await startTestServer()

    const id = crypto.randomUUID()
    const streamName = `tf-k94k-events-${id}`
    const wakeStreamName = `tf-k94k-wake-${id}`
    const stream = `/v1/stream/${streamName}`
    const wakeStream = `/v1/stream/${wakeStreamName}`
    const consumer = `tf-k94k-consumer-${id}`
    const worker = `tf-k94k-worker-${id}`
    const subscriptionRoute = `/v1/stream/__ds/subscriptions/${encodeURIComponent(consumer)}`

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
      await fetch(`${server.url}${subscriptionRoute}`, {
        method: "PUT",
        headers: jsonHeaders,
        body: JSON.stringify({
          type: "pull-wake",
          streams: [streamName],
          wake_stream: wakeStreamName,
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
      await fetch(`${server.url}${subscriptionRoute}/claim`, {
        method: "POST",
        headers: jsonHeaders,
        body: JSON.stringify({ worker }),
      }),
    )

    expect(acquired.generation).toBeGreaterThanOrEqual(1)
    expect(acquired.token).not.toHaveLength(0)

    const wakeEvents = await readJson<Array<WakeEvent>>(
      await fetch(`${server.url}${wakeStream}?offset=-1`),
    )
    expect(wakeEvents).toContainEqual(
      expect.objectContaining({
        type: "wake",
        stream: streamName,
        subscription_id: consumer,
        generation: acquired.generation,
      }),
    )
  })
})
