# @firegrid/acp-process

ACP harness **process owner**. Spawn / kill a real ACP agent process and expose
its stdio as an `acp.Stream`. That is the entire job.

Per `docs/sdds/SDD_FLUENT_HARNESS_ADAPTER_CONTRACT.md`: Firegrid owns durable
coordination around the agent loop; the external harness owns the loop. This
package is the **outside** of the ACP client boundary — it hands the fluent ACP
runtime lane a stream and nothing else. `FiregridAcpClient` /
`connectFiregridAcp` (the ACP `Client` role) and `FiregridAcpConductor` (the
editor-facing ACP `Agent` role) are **separate fluent runtime lanes**; this
package implements neither.

## Surface

```ts
import { spawnAcpProcess } from "@firegrid/acp-process"

const handle = yield* spawnAcpProcess({ agent: "claude", cwd })
//   agent: "claude" | "codex" | { command, args }
//   handle.stream : acp.Stream   ({ readable, writable } of parsed ACP messages)
//   handle.kill   : Effect<void>
```

`spawnAcpProcess(input): Effect<{ stream, kill }, AcpProcessError, CommandExecutor | Scope>`.
The process is launched with `@effect/platform` `Command` (provide a
`CommandExecutor`, e.g. `NodeContext.layer`); its lifetime is bound to the
`Scope`, with `kill` as an explicit teardown. The spawned env has `CLAUDECODE`
removed so `claude-code-acp` does not refuse to nest. `AcpHarnessProcessOwner` is
the service tag (`.Default` wires `spawnAcpProcess`).

## Boundary invariants (enforced)

- **F-A1 / F-A11**: imports only `@agentclientprotocol/sdk` + `effect` +
  `@effect/platform` — no runtime package, Durable Streams, Store/Host/
  EventIngress/Sources, or projection internals.
- **F-A12**: owns no agent-db / queryable projection schema.
- **F-A13**: implements no `acp.Client`/`acp.Agent`; exposes no `Client|Agent` union.
- **F-A14**: writes nothing to stdout; process diagnostics flow via the executor.

## Testing

```bash
pnpm test                                   # unit — fake harness (F-A10: unit aid only)
ACP_RUN_REAL=1 pnpm test                    # real claude-code-acp: stream completes initialize
ACP_RUN_REAL=1 ACP_AGENT=codex pnpm test
```

Full binding acceptance (real agent + `FiregridAcpClient` + fluent runtime,
Layer 1/2, resume, cancel/interrupt) lives under
`features/fluent/agent-binding/` — a fake harness is never accepted there (F-A10).
