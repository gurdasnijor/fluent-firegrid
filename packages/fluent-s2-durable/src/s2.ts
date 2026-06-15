import type { AppendRecord, ReadRecord } from "@s2-dev/streamstore"
import { Context, type Effect, type Stream } from "effect"
import type { AppendCondFailed, S2Error } from "./errors.ts"

/**
 * §5.1 — the `S2` service: a thin *Effect* layer over the S2 TS SDK. It exists
 * for three reasons the raw SDK can't provide: it turns the Promise/throwing API
 * into typed Effects (`AppendCondFailed`/`S2Error`, `Stream`, `Layer`), it is the
 * dependency-injection seam the runtime depends on, and it is what the §9 fault
 * harness decorates. The *data* it carries is the SDK's own — `AppendRecord` (a
 * batch may include `AppendRecord.fence`/`.trim` commands) and
 * `ReadRecord<"bytes">` — so there is no redundant re-modeling. This is the
 * "Bifrost" seam; S2 is the substrate.
 */
export interface S2Service {
  readonly append: (
    stream: string,
    records: ReadonlyArray<AppendRecord>,
    opts?: { readonly fencingToken?: string; readonly matchSeqNum?: number },
  ) => Effect.Effect<{ readonly tail: number }, AppendCondFailed | S2Error>

  /** Read session as a Stream; `from` is a seq_num, `follow` keeps it open at the tail. */
  readonly read: (
    stream: string,
    from: number,
    opts?: { readonly follow?: boolean },
  ) => Stream.Stream<ReadRecord<"bytes">, S2Error>

  readonly checkTail: (stream: string) => Effect.Effect<number, S2Error>
}

export class S2 extends Context.Service<S2, S2Service>()(
  "@firegrid/fluent-s2-durable/S2",
) {}
