import { FiregridConfig } from "../../config.ts"
import { Effect } from "effect"

const STREAM_CLOSED_HEADER = "stream-closed"
const STREAM_NEXT_OFFSET = "stream-next-offset"
const STREAM_UP_TO_DATE = "stream-up-to-date"
const OFFSET_BEGIN = "-1"

interface ProbeResponse {
  readonly status: number
  readonly nextOffset: string | undefined
  readonly upToDate: boolean
  readonly streamClosed: boolean
  readonly text: string
}

const readProbe = async (response: Response): Promise<ProbeResponse> => ({
  status: response.status,
  nextOffset: response.headers.get(STREAM_NEXT_OFFSET) ?? undefined,
  upToDate: response.headers.get(STREAM_UP_TO_DATE) !== null,
  streamClosed: response.headers.get(STREAM_CLOSED_HEADER) === "true",
  text: await response.text(),
})

export const streamClosureSubstrateDriver = Effect.gen(function*() {
  const config = yield* FiregridConfig
  if (config.durableStreamsBaseUrl === undefined) {
    return yield* Effect.fail(
      new Error("stream-closure-substrate requires durableStreamsBaseUrl"),
    )
  }
  const baseUrl = config.durableStreamsBaseUrl
  const streamPath = `closure-substrate/${config.namespace ?? "firelab"}/turn`

  const put = () =>
    Effect.tryPromise(() =>
      fetch(`${baseUrl}/v1/stream/${streamPath}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      }).then(readProbe))
  const append = (marker: string, close: boolean) =>
    Effect.tryPromise(() =>
      fetch(`${baseUrl}/v1/stream/${streamPath}`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          // Atomic append-and-close: the terminal body and the close are one
          // request, so a crash cannot leave "result appended, stream open".
          ...(close ? { [STREAM_CLOSED_HEADER]: "true" } : {}),
        },
        body: JSON.stringify({ marker }),
      }).then(readProbe))
  const readFromBegin = (spanName: string, label: string) =>
    Effect.tryPromise(() =>
      fetch(`${baseUrl}/v1/stream/${streamPath}?offset=${OFFSET_BEGIN}`, {
        method: "GET",
      }).then(readProbe)).pipe(
      Effect.tap(probe =>
        Effect.annotateCurrentSpan({
          "firegrid.closure_substrate.read_label": label,
          "firegrid.closure_substrate.read_status": String(probe.status),
          "firegrid.closure_substrate.read_next_offset": probe.nextOffset ?? "<none>",
          // The headline distinction: up_to_date can be true while stream_closed
          // is false — caught up is NOT terminal.
          "firegrid.closure_substrate.read_up_to_date": probe.upToDate,
          "firegrid.closure_substrate.read_stream_closed": probe.streamClosed,
          "firegrid.closure_substrate.read_has_terminal": probe.text.includes("TURN_RESULT"),
        })),
      Effect.withSpan(spanName, { kind: "client" }),
    )

  yield* put().pipe(
    Effect.withSpan("firelab.closure_substrate.create", { kind: "client" }),
  )
  yield* append("STEP_A", false).pipe(
    Effect.withSpan("firelab.closure_substrate.append", {
      attributes: { "firegrid.closure_substrate.marker": "STEP_A" },
    }),
  )
  yield* append("STEP_B", false).pipe(
    Effect.withSpan("firelab.closure_substrate.append", {
      attributes: { "firegrid.closure_substrate.marker": "STEP_B" },
    }),
  )

  // 1. Caught-up-but-open read: up_to_date true, stream_closed false. A
  //    re-driver that stopped here would WRONGLY conclude the turn is done.
  yield* readFromBegin(
    "firelab.closure_substrate.read_open_caught_up",
    "open_caught_up",
  )

  // 2. Terminal result via atomic append-and-close (one request).
  yield* append("TURN_RESULT", true).pipe(
    Effect.tap(probe =>
      Effect.annotateCurrentSpan({
        "firegrid.closure_substrate.terminal_append_status": String(probe.status),
      })),
    Effect.withSpan("firelab.closure_substrate.append_and_close", {
      kind: "client",
      attributes: { "firegrid.closure_substrate.marker": "TURN_RESULT" },
    }),
  )

  // 3. Post-close read: stream_closed true is the terminal signal.
  yield* readFromBegin(
    "firelab.closure_substrate.read_after_close",
    "after_close",
  )

  // 4. A closed stream rejects further appends (observe the rejection status).
  yield* append("AFTER_CLOSE", false).pipe(
    Effect.tap(probe =>
      Effect.annotateCurrentSpan({
        "firegrid.closure_substrate.post_close_append_status": String(probe.status),
        "firegrid.closure_substrate.post_close_append_body": probe.text.slice(0, 120),
      })),
    Effect.withSpan("firelab.closure_substrate.append_after_close", {
      kind: "client",
    }),
  )
}).pipe(
  Effect.withSpan("firelab.closure_substrate.driver", {
    kind: "client",
    attributes: { "firegrid.closure_substrate.hostless_sim": true },
  }),
)
