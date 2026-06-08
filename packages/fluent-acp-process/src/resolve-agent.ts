import { Effect, Match } from "effect"
import { AcpProcessError, type AgentSpec } from "./types.ts"

export interface ResolvedAgent {
  readonly command: string
  readonly args: ReadonlyArray<string>
}

/**
 * Resolve a known agent key to the command that launches its ACP adapter, or
 * pass through an explicit `{ command, args }` override.
 *
 * Known adapters (installed on demand via `npx`):
 * - `claude` -> `@zed-industries/claude-code-acp`
 * - `codex`  -> `@zed-industries/codex-acp`
 */
export const resolveAgent = (
  agent: AgentSpec,
): Effect.Effect<ResolvedAgent, AcpProcessError> =>
  Match.value(agent).pipe(
    Match.when(Match.string, (key) =>
      Match.value(key).pipe(
        Match.when("claude", () =>
          Effect.succeed<ResolvedAgent>({
            command: "npx",
            args: ["-y", "@zed-industries/claude-code-acp"],
          }),
        ),
        Match.when("codex", () =>
          Effect.succeed<ResolvedAgent>({
            command: "npx",
            args: ["-y", "@zed-industries/codex-acp"],
          }),
        ),
        Match.orElse((unknown) =>
          Effect.fail(
            new AcpProcessError({
              op: "resolve",
              message: `Unknown agent: ${unknown}`,
            }),
          ),
        ),
      ),
    ),
    Match.orElse((spec) =>
      Effect.succeed<ResolvedAgent>({
        command: spec.command,
        args: spec.args ?? [],
      }),
    ),
  )
