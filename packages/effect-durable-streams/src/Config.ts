/**
 * Server configuration. The HTTP port is read from `Config` (env `PORT`, the
 * existing durable-streams server convention) with a default of 4437.
 */
import { Config } from "effect"

export const DEFAULT_PORT = 4437

export const port: Config.Config<number> = Config.integer("PORT").pipe(
  Config.withDefault(DEFAULT_PORT),
)
