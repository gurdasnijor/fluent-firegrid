import { Effect } from "effect"
import { afterEach, describe, expect, it } from "vitest"
import { DurableStream, stream as readStream } from "@durable-streams/client"
import { decodeStreamPath } from "@firegrid/fluent-stream-log"
import * as InMemoryStreamLog from "@firegrid/fluent-stream-log-inmemory"
import {
  makeClient,
  makeHttpChannel,
  makeServer,
  startHttpServer,
  type StartedHttpServer,
} from "@firegrid/fluent-durable-streams-spike"

const openServers: StartedHttpServer[] = []

const start = async () => {
  const log = await Effect.runPromise(InMemoryStreamLog.make())
  const started = await startHttpServer(makeServer(log))
  openServers.push(started)
  return started
}

afterEach(async () => {
  const servers = openServers.splice(0)
  await Promise.all(servers.map((server) => server.close()))
})

describe("HTTP DurableStreamsChannel", () => {
  it("lets the same client run over an HTTP channel", async () => {
    const started = await start()

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* decodeStreamPath("http/orders")
        const client = makeClient(makeHttpChannel({ baseUrl: started.url }))
        const stream = client.stream(path, "application/json")

        const create = yield* stream.create({ body: [] })
        const append = yield* stream.append([{ id: 1 }, { id: 2 }])
        const read = yield* stream.readJson("-1")
        const head = yield* stream.head()

        return { append, create, head, read }
      }),
    )

    expect(result.create).toMatchObject({ _tag: "Created" })
    expect(result.append).toMatchObject({ _tag: "Noop" })
    expect(result.read).toMatchObject({
      _tag: "ReadJson",
      items: [{ id: 1 }, { id: 2 }],
      upToDate: true,
      closed: false,
    })
    expect(result.head).toMatchObject({
      _tag: "Head",
      metadata: { contentType: "application/json", closed: false },
    })
  })

  it("accepts direct HTTP operations in the shape used by conformance clients", async () => {
    const started = await start()
    const url = `${started.url}/conformance-client`

    const create = await fetch(url, {
      method: "PUT",
      headers: { "content-type": "text/plain" },
    })
    const append = await fetch(url, {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: "hello",
    })
    const head = await fetch(url, { method: "HEAD" })
    const read = await fetch(`${url}?offset=-1`)
    const remove = await fetch(url, { method: "DELETE" })

    expect(create.status).toBe(201)
    expect(create.headers.get("stream-next-offset")).toBeTruthy()
    expect(append.status).toBe(204)
    expect(append.headers.get("stream-next-offset")).toBeTruthy()
    expect(head.status).toBe(200)
    expect(head.headers.get("content-type")).toBe("text/plain")
    expect(read.status).toBe(200)
    expect(await read.text()).toBe("hello")
    expect(remove.status).toBe(204)
  })

  it("can be driven by the reference TypeScript client used by conformance", async () => {
    const started = await start()
    const url = `${started.url}/v1/stream/reference-client`

    const stream = await DurableStream.create({ url, contentType: "text/plain" })
    await stream.append("hello from reference client")

    const head = await stream.head()
    const read = await readStream({ url, live: false })

    expect(head).toMatchObject({
      exists: true,
      contentType: "text/plain",
      streamClosed: false,
    })
    expect(new TextDecoder().decode(await read.body())).toBe("hello from reference client")
    expect(read.upToDate).toBe(true)
  })

  it("round-trips producer headers over HTTP", async () => {
    const started = await start()
    const url = `${started.url}/conformance-producer`

    await fetch(url, {
      method: "PUT",
      headers: { "content-type": "text/plain" },
    })
    const first = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "text/plain",
        "producer-id": "writer",
        "producer-epoch": "0",
        "producer-seq": "0",
      },
      body: "first",
    })
    const duplicate = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "text/plain",
        "producer-id": "writer",
        "producer-epoch": "0",
        "producer-seq": "0",
      },
      body: "first retry",
    })
    const gap = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "text/plain",
        "producer-id": "writer",
        "producer-epoch": "0",
        "producer-seq": "2",
      },
      body: "gap",
    })

    expect(first.status).toBe(204)
    expect(duplicate.status).toBe(200)
    expect(duplicate.headers.get("producer-seq")).toBe("0")
    expect(gap.status).toBe(409)
    expect(gap.headers.get("producer-expected-seq")).toBe("1")
    expect(gap.headers.get("producer-received-seq")).toBe("2")
  })
})
