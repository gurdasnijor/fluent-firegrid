import { FiregridConfig } from "../../config.ts"
import { Effect } from "effect"

const PRODUCER_ID = "producer-id"
const PRODUCER_EPOCH = "producer-epoch"
const PRODUCER_SEQ = "producer-seq"
const PRODUCER_EXPECTED_SEQ = "producer-expected-seq"
const PRODUCER_RECEIVED_SEQ = "producer-received-seq"
const STREAM_NEXT_OFFSET = "stream-next-offset"
const OFFSET_BEGIN = "-1"

interface ProbeResponse {
  readonly status: number
  readonly nextOffset: string | undefined
  readonly expectedSeq: string | undefined
  readonly receivedSeq: string | undefined
  readonly text: string
}

const readProbe = async (response: Response): Promise<ProbeResponse> => ({
  status: response.status,
  nextOffset: response.headers.get(STREAM_NEXT_OFFSET) ?? undefined,
  expectedSeq: response.headers.get(PRODUCER_EXPECTED_SEQ) ?? undefined,
  receivedSeq: response.headers.get(PRODUCER_RECEIVED_SEQ) ?? undefined,
  text: await response.text(),
})

const occurrences = (text: string, marker: string): number =>
  text.split(marker).length - 1

export const idempotentProducerSubstrateDriver = Effect.gen(function*() {
  const config = yield* FiregridConfig
  if (config.durableStreamsBaseUrl === undefined) {
    return yield* Effect.fail(
      new Error("idempotent-producer-substrate requires durableStreamsBaseUrl"),
    )
  }
  const baseUrl = config.durableStreamsBaseUrl
  const streamPath = `producer-substrate/${config.namespace ?? "firelab"}/journal`
  const producerId = "firelab-producer-1"

  const put = () =>
    Effect.tryPromise(() =>
      fetch(`${baseUrl}/v1/stream/${streamPath}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      }).then(readProbe))

  // A producer append carries (id, epoch, seq). The server returns 200
  // (appended), 204 (duplicate — same id/seq already stored), 403 (stale epoch
  // — zombie fencing), or 409 (sequence gap). We OBSERVE the status + the
  // server's expected/received seq rather than assume the seq convention.
  const producerAppend = (
    spanName: string,
    label: string,
    epoch: number,
    seq: number,
    marker: string,
  ) =>
    Effect.tryPromise(() =>
      fetch(`${baseUrl}/v1/stream/${streamPath}`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          [PRODUCER_ID]: producerId,
          [PRODUCER_EPOCH]: String(epoch),
          [PRODUCER_SEQ]: String(seq),
        },
        body: JSON.stringify({ marker }),
      }).then(readProbe)).pipe(
      Effect.tap(probe =>
        Effect.annotateCurrentSpan({
          "firegrid.producer_substrate.label": label,
          "firegrid.producer_substrate.sent_epoch": epoch,
          "firegrid.producer_substrate.sent_seq": seq,
          "firegrid.producer_substrate.status": String(probe.status),
          "firegrid.producer_substrate.expected_seq": probe.expectedSeq ?? "<none>",
          "firegrid.producer_substrate.received_seq": probe.receivedSeq ?? "<none>",
        })),
      Effect.withSpan(spanName, {
        kind: "client",
        attributes: { "firegrid.producer_substrate.marker": marker },
      }),
    )

  const readCounts = (spanName: string, label: string, markers: ReadonlyArray<string>) =>
    Effect.tryPromise(() =>
      fetch(`${baseUrl}/v1/stream/${streamPath}?offset=${OFFSET_BEGIN}`, {
        method: "GET",
      }).then(readProbe)).pipe(
      Effect.tap(probe =>
        Effect.annotateCurrentSpan({
          "firegrid.producer_substrate.read_label": label,
          "firegrid.producer_substrate.read_status": String(probe.status),
          ...Object.fromEntries(
            markers.map(marker => [
              `firegrid.producer_substrate.${label}.count.${marker}`,
              occurrences(probe.text, marker),
            ]),
          ),
        })),
      Effect.withSpan(spanName, { kind: "client" }),
    )

  yield* put().pipe(
    Effect.withSpan("firelab.producer_substrate.create", { kind: "client" }),
  )

  // Producer seq is 0-based (the server reports expected_seq=0 for the first
  // append). The first side-effecting step is seq 0.
  // 1. First write of a side-effecting step.
  yield* producerAppend(
    "firelab.producer_substrate.first_write",
    "first_write",
    1,
    0,
    "SIDE_EFFECT_1",
  )
  // 2. Restart re-drives the SAME (id, epoch, seq): retry safety must dedup.
  yield* producerAppend(
    "firelab.producer_substrate.replay_same_seq",
    "replay_same_seq",
    1,
    0,
    "SIDE_EFFECT_1",
  )
  // 3. The side effect must appear exactly once (read-back proves no double-write).
  yield* readCounts(
    "firelab.producer_substrate.read_after_replay",
    "after_replay",
    ["SIDE_EFFECT_1"],
  )
  // 4. A zombie writer at a lower epoch must be fenced.
  yield* producerAppend(
    "firelab.producer_substrate.stale_epoch",
    "stale_epoch",
    0,
    1,
    "SIDE_EFFECT_STALE",
  )
  // 5. The live writer advances to the next seq.
  yield* producerAppend(
    "firelab.producer_substrate.advance",
    "advance",
    1,
    1,
    "SIDE_EFFECT_2",
  )
  // 6. A sequence gap must be rejected.
  yield* producerAppend(
    "firelab.producer_substrate.seq_gap",
    "seq_gap",
    1,
    9,
    "SIDE_EFFECT_GAP",
  )
  // 7. Final read: 1×SIDE_EFFECT_1, 1×SIDE_EFFECT_2, 0×stale, 0×gap.
  yield* readCounts(
    "firelab.producer_substrate.read_final",
    "final",
    ["SIDE_EFFECT_1", "SIDE_EFFECT_2", "SIDE_EFFECT_STALE", "SIDE_EFFECT_GAP"],
  )
}).pipe(
  Effect.withSpan("firelab.producer_substrate.driver", {
    kind: "client",
    attributes: { "firegrid.producer_substrate.hostless_sim": true },
  }),
)
