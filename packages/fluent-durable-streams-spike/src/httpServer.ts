/* eslint-disable effect/no-runPromise */
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http"
import { Effect } from "effect"
import type { DurableStreamsServer } from "./model.ts"
import {
  appendStatus,
  isProblem,
  pathFromUrl,
  problemBody,
  problemStatus,
  PRODUCER_EPOCH,
  PRODUCER_EXPECTED_SEQ,
  PRODUCER_ID,
  PRODUCER_RECEIVED_SEQ,
  PRODUCER_SEQ,
  STREAM_CLOSED,
  STREAM_NEXT_OFFSET,
  STREAM_UP_TO_DATE,
  textEncoder,
} from "./httpShared.ts"

export interface StartedHttpServer {
  readonly url: string
  readonly close: () => Promise<void>
}

const requestBody = (request: IncomingMessage): Promise<Uint8Array> =>
  new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    request.on("data", (chunk: Buffer) => chunks.push(chunk))
    request.on("error", reject)
    request.on("end", () => resolve(new Uint8Array(Buffer.concat(chunks))))
  })

const send = (
  response: ServerResponse,
  status: number,
  headers: Record<string, string> = {},
  body?: string | Uint8Array,
) => {
  response.writeHead(status, headers)
  response.end(body)
}

const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect)

const headerString = (
  value: string | string[] | undefined,
  fallback: string,
): string => Array.isArray(value) ? value[0] ?? fallback : value ?? fallback

export const startHttpServer = async (
  durableStreams: DurableStreamsServer,
  options?: { readonly port?: number },
): Promise<StartedHttpServer> => {
  const handle = async (request: IncomingMessage, response: ServerResponse) => {
    try {
      const url = new URL(request.url ?? "/", "http://127.0.0.1")
      const path = await run(pathFromUrl(url))
      const contentType = headerString(request.headers["content-type"], "application/octet-stream")

      switch (request.method) {
        case "PUT": {
          const body = await requestBody(request)
          const outcome = await run(durableStreams.create({
            path,
            contentType,
            body,
            closed: request.headers[STREAM_CLOSED] === "true",
          }))
          if (isProblem(outcome)) {
            send(response, problemStatus(outcome), { "content-type": "application/json" }, problemBody(outcome))
            return
          }
          send(response, outcome._tag === "Created" ? 201 : 200, {
            [STREAM_NEXT_OFFSET]: outcome.metadata.tailOffset,
            ...(outcome.metadata.closed && { [STREAM_CLOSED]: "true" }),
          })
          return
        }

        case "POST": {
          const body = await requestBody(request)
          const producerId = request.headers[PRODUCER_ID]
          const producerEpoch = request.headers[PRODUCER_EPOCH]
          const producerSeq = request.headers[PRODUCER_SEQ]
          const outcome = await run(durableStreams.append({
            path,
            contentType,
            body,
            close: request.headers[STREAM_CLOSED] === "true",
            ...(typeof producerId === "string" &&
              typeof producerEpoch === "string" &&
              typeof producerSeq === "string" && {
              producer: {
                producerId,
                epoch: Number(producerEpoch),
                seq: Number(producerSeq),
              },
            }),
          }))
          const status = appendStatus(outcome)
          const headers: Record<string, string> = {}
          switch (outcome._tag) {
            case "Appended":
            case "Noop":
            case "Duplicate":
              headers[STREAM_NEXT_OFFSET] = outcome.metadata.tailOffset
              if (outcome.metadata.closed) {
                headers[STREAM_CLOSED] = "true"
              }
              if (outcome._tag === "Duplicate") {
                headers[PRODUCER_EPOCH] = producerEpoch?.toString() ?? "0"
                headers[PRODUCER_SEQ] = producerSeq?.toString() ?? "0"
              }
              break
            case "AlreadyClosed":
            case "WriteToClosed":
              headers[STREAM_NEXT_OFFSET] = outcome.finalOffset
              headers[STREAM_CLOSED] = "true"
              break
            case "Fenced":
              headers[PRODUCER_EPOCH] = String(outcome.currentEpoch)
              break
            case "SequenceGap":
              headers[PRODUCER_EXPECTED_SEQ] = String(outcome.expectedSeq)
              headers[PRODUCER_RECEIVED_SEQ] = String(outcome.receivedSeq)
              break
          }
          send(
            response,
            status,
            isProblem(outcome) ? { ...headers, "content-type": "application/json" } : headers,
            isProblem(outcome) ? problemBody(outcome) : undefined,
          )
          return
        }

        case "HEAD": {
          const outcome = await run(durableStreams.head(path))
          if (isProblem(outcome)) {
            send(response, problemStatus(outcome))
            return
          }
          send(response, 200, {
            "content-type": outcome.metadata.contentType,
            [STREAM_NEXT_OFFSET]: outcome.metadata.tailOffset,
            ...(outcome.metadata.closed && { [STREAM_CLOSED]: "true" }),
          })
          return
        }

        case "GET": {
          const offset = url.searchParams.get("offset") ?? undefined
          const outcome = await run(durableStreams.read({ path, ...(offset !== undefined && { offset: offset as never }) }))
          if (isProblem(outcome)) {
            send(response, problemStatus(outcome), { "content-type": "application/json" }, problemBody(outcome))
            return
          }
          if (outcome._tag !== "Read") {
            send(response, 500, { "content-type": "text/plain" }, "unexpected read outcome")
            return
          }
          const contentType = outcome.records[0]?.contentType ?? "application/octet-stream"
          const body = contentType.split(";")[0]?.trim().toLowerCase() === "application/json"
            ? textEncoder.encode(`[${outcome.records.map((record) => new TextDecoder().decode(record.bytes)).join(",")}]`)
            : Buffer.concat(outcome.records.map((record) => Buffer.from(record.bytes)))
          send(response, body.length === 0 ? 204 : 200, {
            "content-type": contentType,
            [STREAM_NEXT_OFFSET]: outcome.nextOffset,
            ...(outcome.upToDate && { [STREAM_UP_TO_DATE]: "true" }),
            ...(outcome.closed && { [STREAM_CLOSED]: "true" }),
          }, body)
          return
        }

        case "DELETE": {
          const outcome = await run(durableStreams.delete(path))
          send(response, outcome._tag === "Deleted" ? 204 : problemStatus(outcome), {})
          return
        }

        default:
          send(response, 405)
      }
    } catch (error) {
      send(response, 500, { "content-type": "text/plain" }, error instanceof Error ? error.message : String(error))
    }
  }

  const server: Server = createServer((request, response) => {
    void handle(request, response)
  })

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(options?.port ?? 0, "127.0.0.1", () => resolve())
  })

  const address = server.address()
  if (address === null || typeof address === "string") {
    throw new Error("failed to start HTTP server")
  }

  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => error === undefined ? resolve() : reject(error))
      }),
  }
}
