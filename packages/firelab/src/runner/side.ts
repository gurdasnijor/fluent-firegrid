import { Effect } from "effect"

type FiregridSide = "driver" | "host" | "codec" | "subprocess" | "sdk"

// Wraps the effect in a labeled span AND propagates a `firegrid.side`
// attribute to every descendant span via `Effect.annotateSpans`. Wrapping
// alone (the old shape) only stamped the wrapper; children kept emitting
// without the side attribute, which is what made `firegrid.side` useless as
// a trace filter dimension. The propagating annotation is the Effect-native
// equivalent of "OTel baggage + custom SpanProcessor onStart"; we don't need
// either.
export const annotateSide = (side: FiregridSide) =>
  <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    self.pipe(
      Effect.withSpan(`firegrid.side.${side}`, { kind: "internal" }),
      Effect.annotateSpans("firegrid.side", side),
    )
