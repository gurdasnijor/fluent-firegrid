import { FiregridConfig } from "../../config.ts"
import { Effect } from "effect"

// Durable Streams wire constants the wrapper does not yet expose as typed fork
// helpers (effect-durable-streams/src/Writer.ts `create` has no fork option).
// The embedded @durable-streams/server reads these PUT headers to create a fork;
// the SDD's "send the fork headers directly at first, then promote that to the
// wrapper" is exactly what this probe does.
const STREAM_FORKED_FROM = "stream-forked-from"
const STREAM_FORK_OFFSET = "stream-fork-offset"
const STREAM_NEXT_OFFSET = "stream-next-offset"
const STREAM_UP_TO_DATE = "stream-up-to-date"
const STREAM_CLOSED = "stream-closed"
const OFFSET_BEGIN = "-1"

interface ProbeResponse {
  readonly status: number
  readonly nextOffset: string | undefined
  readonly upToDate: boolean
  readonly streamClosed: boolean
  readonly text: string
}

// Raw-wire probe (matches the established firelab substrate-probe idiom in
// restate-primitive-compat): captures status, the offset/up-to-date/closed
// response headers, and the raw body text. We assert on unique marker substrings
// rather than parse framed items, so the evidence is robust to message framing.
const readProbe = async (response: Response): Promise<ProbeResponse> => ({
  status: response.status,
  nextOffset: response.headers.get(STREAM_NEXT_OFFSET) ?? undefined,
  upToDate: response.headers.get(STREAM_UP_TO_DATE) !== null,
  streamClosed: response.headers.get(STREAM_CLOSED) === "true",
  text: await response.text(),
})

export const forkChildSessionSubstrateDriver = Effect.gen(function*() {
  const config = yield* FiregridConfig
  if (config.durableStreamsBaseUrl === undefined) {
    return yield* Effect.fail(
      new Error("fork-child-session-substrate requires durableStreamsBaseUrl"),
    )
  }
  const baseUrl = config.durableStreamsBaseUrl
  const suffix = `fork-substrate/${config.namespace ?? "firelab"}`
  const parentPath = `${suffix}/parent`
  const childPath = `${suffix}/child`

  const put = (path: string, headers: Record<string, string>) =>
    Effect.tryPromise(() =>
      fetch(`${baseUrl}/v1/stream/${path}`, {
        method: "PUT",
        headers: { "content-type": "application/json", ...headers },
      }).then(readProbe))
  const append = (path: string, marker: string) =>
    Effect.tryPromise(() =>
      fetch(`${baseUrl}/v1/stream/${path}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ marker }),
      }).then(readProbe))

  // Read from BEGIN and fold the wire facts + per-marker presence onto THIS
  // read's span (Effect.tap runs inside the span scope). The flags are
  // OBSERVATIONS for the finding to interpret — the sim declares no pass/fail
  // (firelab methodology: the trace is the deliverable, the verdict human).
  const readAndObserve = (
    spanName: string,
    label: string,
    path: string,
    markers: Record<string, string>,
  ) =>
    Effect.tryPromise(() =>
      fetch(`${baseUrl}/v1/stream/${path}?offset=${OFFSET_BEGIN}`, { method: "GET" })
        .then(readProbe)).pipe(
      Effect.tap(probe =>
        Effect.annotateCurrentSpan({
          "firegrid.fork_substrate.read_label": label,
          "firegrid.fork_substrate.read_status": String(probe.status),
          "firegrid.fork_substrate.read_next_offset": probe.nextOffset ?? "<none>",
          "firegrid.fork_substrate.read_up_to_date": probe.upToDate,
          "firegrid.fork_substrate.read_stream_closed": probe.streamClosed,
          ...Object.fromEntries(
            Object.entries(markers).map(([key, marker]) => [
              `firegrid.fork_substrate.${label}.${key}`,
              probe.text.includes(marker),
            ]),
          ),
        })),
      Effect.withSpan(spanName, { kind: "client" }),
    )

  // 1. Parent stream + two inherited events (A, B).
  yield* put(parentPath, {}).pipe(
    Effect.tap(probe =>
      Effect.annotateCurrentSpan({
        "firegrid.fork_substrate.parent_path": parentPath,
        "firegrid.fork_substrate.parent_create_status": String(probe.status),
        "firegrid.fork_substrate.parent_create_body": probe.text.slice(0, 200),
      })),
    Effect.withSpan("firelab.fork_substrate.parent.create", { kind: "client" }),
  )
  yield* append(parentPath, "PARENT_A").pipe(
    Effect.withSpan("firelab.fork_substrate.parent.append", {
      attributes: { "firegrid.fork_substrate.marker": "PARENT_A" },
    }),
  )
  const afterB = yield* append(parentPath, "PARENT_B").pipe(
    Effect.withSpan("firelab.fork_substrate.parent.append", {
      attributes: { "firegrid.fork_substrate.marker": "PARENT_B" },
    }),
  )
  // The parent's next-offset after B is the fork point: "inherit up to here".
  const forkOffset = afterB.nextOffset

  // 2. Append C to the parent AFTER capturing the fork offset. C lands past the
  //    fork point, so a correct fork must NOT inherit it.
  yield* append(parentPath, "PARENT_C").pipe(
    Effect.withSpan("firelab.fork_substrate.parent.append_post_fork", {
      attributes: { "firegrid.fork_substrate.marker": "PARENT_C" },
    }),
  )

  // 3. Fork the child at the captured offset. NOTE: the server keys streams by
  //    full request pathname (url.pathname), so Stream-Forked-From must carry the
  //    full `/v1/stream/<path>`, not the bare stream id — a footgun for the
  //    future typed fork helper / fluent-runtime Store.ts.
  const forkedFromValue = `/v1/stream/${parentPath}`
  const childCreate = yield* put(childPath, {
    [STREAM_FORKED_FROM]: forkedFromValue,
    ...(forkOffset === undefined ? {} : { [STREAM_FORK_OFFSET]: forkOffset }),
  }).pipe(
    Effect.tap(probe =>
      Effect.annotateCurrentSpan({
        "firegrid.fork_substrate.child_path": childPath,
        "firegrid.fork_substrate.forked_from": forkedFromValue,
        "firegrid.fork_substrate.fork_offset": forkOffset ?? "<unavailable>",
        "firegrid.fork_substrate.fork_create_status": String(probe.status),
        "firegrid.fork_substrate.fork_create_body": probe.text.slice(0, 200),
      })),
    Effect.withSpan("firelab.fork_substrate.child.fork", { kind: "client" }),
  )
  yield* Effect.annotateCurrentSpan(
    "firegrid.fork_substrate.fork_create_status",
    String(childCreate.status),
  )

  // 4. Read the freshly forked child: observe which parent markers were
  //    inherited (boundary semantics — recorded, not assumed).
  yield* readAndObserve(
    "firelab.fork_substrate.child.read_inherited",
    "child_inherited",
    childPath,
    {
      parent_a: "PARENT_A",
      parent_b: "PARENT_B",
      parent_c_leaked_past_fork: "PARENT_C",
    },
  )

  // 5. Diverge: append D to the child only.
  yield* append(childPath, "CHILD_D").pipe(
    Effect.withSpan("firelab.fork_substrate.child.append", {
      attributes: { "firegrid.fork_substrate.marker": "CHILD_D" },
    }),
  )

  // 6. Re-read child (history + own divergent event) and re-read parent
  //    (must be unaffected by the child's append).
  yield* readAndObserve(
    "firelab.fork_substrate.child.read_after_diverge",
    "child_after_diverge",
    childPath,
    { parent_a: "PARENT_A", parent_b: "PARENT_B", child_d: "CHILD_D" },
  )
  yield* readAndObserve(
    "firelab.fork_substrate.parent.read_after_diverge",
    "parent_after_diverge",
    parentPath,
    {
      parent_a: "PARENT_A",
      parent_b: "PARENT_B",
      parent_c: "PARENT_C",
      child_d_leaked_into_parent: "CHILD_D",
    },
  )
}).pipe(
  Effect.withSpan("firelab.fork_substrate.driver", {
    kind: "client",
    attributes: { "firegrid.fork_substrate.hostless_sim": true },
  }),
)
