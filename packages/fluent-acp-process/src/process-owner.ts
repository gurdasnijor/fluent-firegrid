import * as acp from "@agentclientprotocol/sdk"
import { Command } from "@effect/platform"
import { Context, Effect, Layer, Queue, Stream } from "effect"
import { resolveAgent } from "./resolve-agent.ts"
import {
  AcpProcessError,
  type AcpHarnessProcessOwnerService,
  type AcpProcessHandle,
  type AcpSpawnInput,
} from "./types.ts"

/**
 * Spawn a real ACP harness process and expose its stdio as an `acp.Stream`.
 *
 * This is the entire job of the ACP process owner: launch / kill the process and
 * hand Firegrid the stream. Firegrid's `FiregridAcpClient` (separate
 * `fluent-runtime` lane) wraps it in a `ClientSideConnection`. The owner records
 * nothing to Durable Streams (F-A1) and makes no coordination decisions (F-A4).
 *
 * The process is spawned via `@effect/platform` `Command` (the `CommandExecutor`
 * is provided by the host, e.g. `NodeContext.layer`) and its lifetime is bound
 * to the provided `Scope`; `kill` is an explicit teardown.
 */
export const spawnAcpProcess = Effect.fn("fluent-acp-process.spawn")(
  function* (input: AcpSpawnInput) {
    const resolved = yield* resolveAgent(input.agent)

    const command = Command.make(resolved.command, ...resolved.args).pipe(
      Command.workingDirectory(input.cwd),
      // A spawned ACP harness is its own session; drop the nesting marker so
      // `claude-code-acp` does not refuse to launch inside another Claude
      // Code session. `undefined` removes the key from the inherited env.
      Command.env({ ...(input.env ?? {}), CLAUDECODE: undefined }),
      // Drain harness stderr to the parent's stderr (fd2). Default "pipe" would
      // leave proc.stderr unread; a noisy harness could fill the pipe buffer and
      // block, stalling the stdout ACP stream. "inherit" keeps stdout ACP-only
      // (F-A14) while diagnostics flow through the sanctioned stderr channel.
      Command.stderr("inherit"),
    )

    const proc = yield* Command.start(command).pipe(
      Effect.mapError(
        (cause) =>
          new AcpProcessError({
            op: "spawn",
            message: `failed to start ACP harness: ${resolved.command}`,
            cause,
          }),
      ),
    )

    // Client -> agent: bytes written to the acp.Stream's writable are queued and
    // drained into the process stdin sink by a scoped fiber.
    const inbox = yield* Queue.unbounded<Uint8Array>()
    yield* Stream.fromQueue(inbox).pipe(
      Stream.run(proc.stdin),
      Effect.ignore,
      Effect.forkScoped,
    )

    const writable = new WritableStream<Uint8Array>({
      write: (chunk) => {
        Queue.unsafeOffer(inbox, chunk)
      },
    })
    // Agent -> client: stdout bytes are parsed as ACP frames by the SDK.
    const readable = Stream.toReadableStream(proc.stdout)
    const stream = acp.ndJsonStream(writable, readable)

    const kill = Queue.shutdown(inbox).pipe(
      Effect.zipRight(proc.kill()),
      Effect.ignore,
    )

    return { stream, kill } satisfies AcpProcessHandle
  },
)

/**
 * Firegrid-facing service tag for the ACP process owner. `Default` wires the
 * real `spawnAcpProcess`; tests provide a fake-harness command (unit-only —
 * real acceptance requires a real ACP process, F-A10).
 */
export class AcpHarnessProcessOwner extends Context.Tag(
  "@firegrid/fluent-acp-process/AcpHarnessProcessOwner",
)<AcpHarnessProcessOwner, AcpHarnessProcessOwnerService>() {
  static readonly Default: Layer.Layer<AcpHarnessProcessOwner> = Layer.succeed(
    this,
    { spawn: spawnAcpProcess },
  )
}
