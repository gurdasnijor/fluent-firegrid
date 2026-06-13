import { S2 } from "@s2-dev/streamstore"
import { Context, Effect, Layer } from "effect"
import type { S2ProfileError } from "./errors.ts"
import { tryS2 } from "./errors.ts"

export type { S2ProfileError } from "./errors.ts"

export interface S2ProfileOptions {
  readonly accessToken: string
  readonly basin: string
  readonly endpoints?: {
    readonly account?: string
    readonly basin?: string
  }
  readonly streamPrefix?: string
}

export interface S2ProfileService {
  readonly s2: S2
  readonly basinName: string
  readonly basin: ReturnType<S2["basin"]>
}

export class S2ProfileConfig extends Context.Service<S2ProfileConfig, S2ProfileOptions>()(
  "@firegrid/fluent-durable-streams/S2ProfileConfig",
) {}

export class S2Profile extends Context.Service<S2Profile, S2ProfileService>()(
  "@firegrid/fluent-durable-streams/S2Profile",
) {}

export const makeS2Profile: Effect.Effect<S2ProfileService, S2ProfileError, S2ProfileConfig> =
  Effect.gen(function*() {
    const options = yield* S2ProfileConfig
    const s2 = new S2({
      accessToken: options.accessToken,
      ...(options.endpoints === undefined ? {} : { endpoints: options.endpoints }),
      retry: {
        appendRetryPolicy: "noSideEffects",
        maxAttempts: 3,
      },
    })
    const basinName = options.basin
    const basin = s2.basin(basinName)

    yield* Effect.annotateCurrentSpan({
      "s2.basin": basinName,
    })

    yield* tryS2(() =>
      s2.basins.ensure({
        basin: basinName,
        config: {
          createStreamOnAppend: true,
          createStreamOnRead: true,
        },
      }),
    )

    return S2Profile.of({
      s2,
      basinName,
      basin,
    })
  })

export const layerConfig = (options: S2ProfileOptions): Layer.Layer<S2ProfileConfig> =>
  Layer.succeed(S2ProfileConfig, S2ProfileConfig.of(options))

export const layerProfile: Layer.Layer<S2Profile, S2ProfileError, S2ProfileConfig> =
  Layer.effect(S2Profile, makeS2Profile)
