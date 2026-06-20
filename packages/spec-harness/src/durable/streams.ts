import type { Envelope } from "@cucumber/messages"
import { Schema } from "effect"
import { EventStream } from "effect-s2-stream-db"

/**
 * The canonical Cucumber Messages output as a durable append-only S2 event
 * stream, one per run (keyed by run id). The runner appends envelopes as facts;
 * consumers (the CLI, formatters, the CCK gate) read/tail them back — the NDJSON
 * protocol output is just this stream. No projection table is needed for it
 * (per the durable-table SDD §8.1); envelopes are opaque JSON at the boundary.
 */
export const RunEnvelopes = EventStream("cucumber-effect/run-envelopes")(Schema.Unknown, Schema.String)

/** Narrow a stream record value back to a Cucumber `Envelope`. */
export const asEnvelope = (value: unknown): Envelope => value as Envelope
