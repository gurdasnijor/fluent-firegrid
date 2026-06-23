# SDD: effect-s2-durable Host / Process Model

## 1. Purpose

Define the **deployable unit** for `effect-s2-durable`. Today the package is a
**library only** — `main`→`src/index.ts`, no `bin`, no host composition, no
runnable surface outside tests; `@effect/platform-node` is a devDependency and the
engine is assembled+run only in `test/ingress-support.ts`. This SDD specifies the
**host worker process** that hosts the engine in production, and the config,
recovery, and (deferred) multi-host coordination around it.

## 2. Context

`effect-s2-durable` is **Restate's broker + SDK fused into one library over S2**
(see [[durable-engine-topology]]). Restate ships the *broker* (journaling,
invocation ownership, timers, retries, recovery) as a separate server; we have no
broker, so the library **is** the broker, persisting to S2. Therefore the
deployable is not Restate's topology (SDK service + external broker) — it is the
**embedded-runtime host** proven by the predecessor `gurdasnijor/firegrid`'s
`firegrid host` (a namespace-scoped worker that connects to the log service, owns
the engine, and drives/recovers work). The bespoke engine + restate-sdk-gen
surface are deliberate and out of scope here; this SDD is only the process shell
around them.

### 2.1 Relationship to Restate — and why brokerless is the *intended* model

Restate has three HTTP surfaces; we deliberately keep only one of them:

| Restate surface | Direction | Owns durability | Ours |
| --- | --- | --- | --- |
| `restate-sdk` `serve()/listen()` | **broker → SDK** (broker POSTs invocation+journal; SDK replays handlers statelessly) | the separate **Restate Server** | **none — eliminated** |
| Ingress (`restate-sdk-clients` `connect`) | external caller → broker | broker | **`durableIngress` + `connect`** |
| Admin (registration) | operator → broker | broker | n/a (def-as-contract) |

Restate's `listen()` SDK is trivial *because a separate broker cluster does the
hard part* (journaling, invocation ownership, timers, retries, recovery,
partitioning). **This system has no broker — and that is the point, not a gap.**
It is a **distributed system of peer host processes that collaborate over the S2
event backbone**: each owner stream's guarantees — total order, conditional
(`matchSeqNum`) append where appropriate, `fencingToken`, snapshots, and trimming
— provide, *per owner*, the coordination Restate centralizes in a broker. So the
broker's guarantees are **replaced, in a lighter-weight way**, by S2 primitives
applied per-key:

- single-writer / no split-brain → fencing token per owner stream;
- crash recovery → restart-driven boot recovery over owner streams;
- bounded rebuild cost → snapshots plus trimming;
- ordered, replayable history → the owner stream itself.

The trade is explicit and intended: we give up *"operate a Restate broker cluster"*
and take on *"implement broker semantics over S2"* — which is bounded because S2
supplies the hard coordination substrate (and ships it as a tested pattern, §7).
The result is **no centralized control plane**: hosts are symmetric peers, scale by
adding processes, and coordinate only through S2. This SDD's host process is one
such peer.

## 3. Load-Bearing Decisions

1. **The deployable is a namespace-scoped host worker.** A long-running process
   that provides `S2Client` (from config), builds `serviceLayer(...catalog)`
   (mounts handlers + seeds recovery), runs boot recovery, and then runs forever.
   Launch shape: `Layer.launch(host).pipe(Effect.zipRight(Effect.never))` under
   `NodeRuntime` (the predecessor's `firegrid host` shape).
2. **The engine core stays binding-free.** `src/index.ts` and the runtime/actor
   modules import no `@effect/platform-node` and no concrete server. The Node host
   (which needs `NodeRuntime`/`NodeContext`/`NodeHttpServer`) lives behind a
   dedicated `./host` subpath + a `bin`, and is the only place `@effect/platform-node`
   is imported. (See §8 for the package-shape trade-off.)
3. **The handler catalog is compiled into the worker.** The host is constructed
   from an explicit array of `service`/`object`/`workflow` defs (as `serviceLayer`
   + `durableIngress` already take). No dynamic/remote handler registration in v1
   — the def value is the contract, the same as the in-process and ingress paths.
4. **Namespace = the S2 basin** (`S2_BASIN`). `S2Client.layerConfig` already binds
   exactly one basin (`S2_ACCESS_TOKEN` + `S2_BASIN`), and the engine names streams
   path-like *within* it (`obj/<object>/<key>`, execution-id streams) with **no
   prefix** — so the basin already *is* the isolation scope; mapping namespace→basin
   needs **zero stream-naming changes**. (The predecessor used a string *prefix*
   only because durable-streams had no basin construct; S2 does, so we use it.)
   Recovery sweeps the basin; multi-host coordination (phase 2) is *within* a basin.
   **Trade-off:** basin-per-namespace means creating a namespace = creating a basin,
   which has provisioning latency (the S2 `resumable-stream` e2e test polls
   `waitForBasinReady` up to 60s). Fine when namespaces are few/long-lived
   (deployments/environments). If many/dynamic namespaces are ever needed (e.g.
   per-tenant), switch to **prefix-within-a-shared-basin** — deferred until there's
   a driver for it.
5. **Config is env-driven, with a CLI veneer.** Since **namespace = basin**, the
   config collapses to what `S2Client.layerConfig` already reads — `S2_ACCESS_TOKEN`
   + `S2_BASIN` (the latter *is* the namespace) — plus an optional ingress port and
   otel sink. A `bin` exposes the same as flags. (Predecessor analog:
   `DURABLE_STREAMS_BASE_URL` + `FIREGRID_RUNTIME_NAMESPACE`; we fold namespace into
   `S2_BASIN`.)
6. **Ingress is an optional adapter, not the host.** `durableIngress(catalog)` is
   layered on for out-of-process callers; a host can also run headless (embedded
   callers / pure recovery worker). Adapters (HTTP ingress, later CLI/MCP) bind at
   the edge and depend on the host, never the reverse.
7. **Ownership is fenced per-owner from the start — multi-host safe for writes in
   v1.** Each owner stream is claimed before active driving and stale writers become
   followers on fencing mismatch. **Single-host is the N=1 degenerate case** (one
   host wins every claim). Lease-based peer takeover is not required for the v1
   correctness model; it is an optional later liveness mode if prompt
   coordinator-free recovery of a crashed peer becomes a product requirement.

## 4. The Host Composition

Mirror the predecessor's `composition/host-live` + `host-public` split:

```ts
// ./host  (the only module importing @effect/platform-node)
export interface DurableHostOptions {
  readonly catalog: ReadonlyArray<AnyDef>     // service/object/workflow defs
  readonly namespace: string
  readonly ingress?: { readonly port: number } // omit → headless host
}

// the host Layer: S2Client(config) + serviceLayer(catalog) + (optional) ingress + boot recovery
export const DurableHostLive: (opts: DurableHostOptions) => Layer.Layer<never, …, never>

// env-driven composition (reads S2 + namespace + port from Config)
export const DurableHostFromConfig: (catalog: ReadonlyArray<AnyDef>) => Layer.Layer<…>

// the run-forever program (Layer.launch + Effect.never), for a bin or embedding
export const startHost: (opts: DurableHostOptions) => Effect.Effect<never, …>
```

```ts
// ./bin/host.ts  — the deployable entrypoint
NodeRuntime.runMain(
  startHost(/* DurableHostFromConfig(catalog) */).pipe(Effect.provide(NodeContext.layer)),
)
```

The catalog is supplied by the deploying app (it imports its defs and calls the
host), exactly as `test/ingress-support.ts` wires `serviceLayer(...catalog)` +
`durableIngress(catalog)` today — this SDD just promotes that assembly from a test
helper to a real, configurable, run-forever entrypoint.

> In the examples below, `service`/`object`/`workflow`, the primitives
> (`run`/`sleep`/`state`/`signal`), the in-process clients
> (`client`/`sharedClient`/`objectClient`/`workflowSubmit`/…), `serviceLayer`,
> `durableIngress`, and `connect` are **existing** API. The `effect-s2-durable/host`
> entry (`startHost`/`DurableHostLive`/`DurableHostFromConfig`) is what **this SDD
> proposes**.

### 4.1 Authoring a catalog (the shared contract)

A plain module of definitions — imported by *both* the host and off-host clients;
the def value itself is the contract.

```ts
// catalog.ts
import { Duration, Effect, Option, Schema } from "effect"
import { object, objectClient, run, service, signal, state, workflow } from "effect-s2-durable"
import { primaryKey, Table } from "effect-s2-stream-db"

declare const chargeGateway: (req: { account: string; amount: number }) => Effect.Effect<string>

// stateless service — a journaled external call with retry
export const payments = service({
  name: "payments",
  handlers: {
    *charge(req: { account: string; amount: number }) {
      const ref = yield* run("gateway", chargeGateway(req), {
        output: Schema.String,
        retry: { maxAttempts: 3, initialInterval: Duration.millis(200) },
      })
      return { ref }
    },
  },
  schemas: { charge: { input: Schema.Struct({ account: Schema.String, amount: Schema.Number }), output: Schema.Struct({ ref: Schema.String }) } },
})

// keyed virtual object — per-account durable balance (exclusive write + shared read)
class BalanceRow extends Table<BalanceRow>("balance")({ id: Schema.String.pipe(primaryKey), cents: Schema.Number }) {}

export const Account = object({
  name: "account",
  handlers: {
    *deposit(cents: number) {
      const st = state(BalanceRow)
      const cur = Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.cents })
      yield* st.set({ id: "v", cents: cur + cents })
      return cur + cents
    },
  },
  shared: {
    *balance() {
      const st = state(BalanceRow)
      return Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.cents })
    },
  },
  schemas: { deposit: { input: Schema.Number, output: Schema.Number } },
  sharedSchemas: { balance: { output: Schema.Number } },
})

// workflow — run-once orchestration: await an approval signal, charge, then credit
// the account via the in-handler (replay-safe) object client
export const checkout = workflow({
  name: "checkout",
  run: function*(req: { account: string; amount: number }) {
    const approved = yield* signal("approval", Schema.Boolean)
    const charged = approved ? yield* run("charge", chargeGateway(req), { output: Schema.String }) : undefined
    if (approved) yield* objectClient(Account, req.account).deposit(req.amount)
    return { settled: approved, ref: charged }
  },
  runSchema: {
    input: Schema.Struct({ account: Schema.String, amount: Schema.Number }),
    output: Schema.Struct({ settled: Schema.Boolean, ref: Schema.optional(Schema.String) }),
  },
})

export const catalog = [payments, Account, checkout]
```

### 4.2 Composing & running a host

```ts
// host-main.ts — the deployable worker
import { NodeContext, NodeRuntime } from "@effect/platform-node"
import { Effect } from "effect"
import { startHost } from "effect-s2-durable/host"   // PROPOSED
import { catalog } from "./catalog.ts"

// explicit options:
NodeRuntime.runMain(
  startHost({ catalog, namespace: "prod", ingress: { port: 8080 } })
    .pipe(Effect.provide(NodeContext.layer)),
)
// …or env-driven (S2_ACCESS_TOKEN, S2_BASIN=namespace, INGRESS_PORT):
//   startHostFromConfig(catalog)  →  same run-forever program
```

Run it: `S2_ACCESS_TOKEN=… S2_BASIN=prod INGRESS_PORT=8080 node host-main.js`. A
**headless** worker (recovery + embedded callers, no out-of-process surface) is
just `startHost({ catalog, namespace: "prod" })` — omit `ingress`.

### 4.3 Host-side interaction (in-process / embedded)

Code that runs **with the engine in context** — inside the host process (an
adapter handler, a co-located task) or a standalone admin program over the same
basin. Uses the embedded clients; needs `DurableExecutionRuntime` (from
`serviceLayer`/the host).

```ts
import { Effect, Schema } from "effect"
import { S2Client } from "effect-s2"
import { client, resolveSignal, serviceLayer, sharedClient, workflowAttach, workflowRunId, workflowSubmit } from "effect-s2-durable"
import { Account, catalog, checkout, payments } from "./catalog.ts"

const program = Effect.gen(function*() {
  const { ref } = yield* client(payments).charge({ account: "acct-1", amount: 500 }) // submit+await
  yield* client(Account, "acct-1").deposit(500)                                       // exclusive write
  const cents = yield* sharedClient(Account, "acct-1").balance()                      // shared read

  yield* workflowSubmit(checkout, "order-42", { account: "acct-1", amount: 500 })     // run-once start
  const runId = yield* workflowRunId(checkout, "order-42")
  yield* resolveSignal(runId, "approval", Schema.Boolean, true)                        // unblock its signal
  const result = yield* workflowAttach(checkout, "order-42")                           // await completion
  return { ref, cents, result }
})

// In the host process the runtime is ambient. Standalone, build the same engine:
program.pipe(Effect.provide(serviceLayer(...catalog)), Effect.provide(S2Client.layerConfig), Effect.scoped)
```

### 4.4 Client-side interaction (off-host, over HTTP)

A **different process** that imports the catalog *values* (the contract) but not
the engine or S2 — it only needs an `HttpClient` and the host's URL.

```ts
// caller.ts — runs anywhere; no S2, no DurableExecutionRuntime
import { Effect } from "effect"
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import { connect } from "effect-s2-durable/client"
import { Account, payments } from "./catalog.ts"

const program = Effect.gen(function*() {
  const grid = yield* connect({ url: "http://durable-host.internal:8080" })

  const { ref } = yield* grid.serviceClient(payments).charge({ account: "acct-1", amount: 500 }) // typed RPC
  const cents = yield* grid.objectClient(Account, "acct-1").deposit(500)

  // fire-and-forget → handle → blocking attach / non-blocking output
  const handle = yield* grid.serviceSendClient(payments).charge({ account: "acct-1", amount: 250 }, { idempotencyKey: "chg-250" })
  const settled = yield* handle.attach
  const maybe = yield* handle.output                                                   // Option<…>

  // a SEPARATE caller (no handle) re-attaches/polls by idempotency key alone
  const again = yield* grid.serviceAttachClient(payments).charge({ idempotencyKey: "chg-250" })
  return { ref, cents, settled, again }
})

program.pipe(Effect.provide(NodeHttpClient.layerUndici))
```

**Off-host surface is service + object call/send/attach/output only** (the `connect`
ingress contract). Workflows and signal resolution are host-side today
(`workflowSubmit`/`resolveSignal`); exposing them off-host is a future ingress
workflow/admin route, not part of this SDD.

### 4.5 CLI interaction (an edge adapter)

A CLI is an edge adapter like ingress — argv in, delegate down; the engine never
imports it (§2.1). Three concerns, mirroring the predecessor's `@effect/cli`
`firegrid host` / `firegrid run`:

**(a) Launch a host** — the `host` subcommand *is* the §4.2 bin:

```sh
durable host --namespace prod --ingress-port 8080      # + S2_ACCESS_TOKEN / S2_BASIN
```

**(b) Interact with a running host** — a thin **client** that `connect()`s to the
host's ingress URL and drives call/send/attach/output. No S2 creds (the host holds
them); the path is the generic wire the ingress already serves
(`/durable/call/:name[/:key]/:method`):

```sh
durable call   payments/charge     --input '{"account":"a1","amount":500}' --url http://host:8080
durable call   account/a1/deposit  --input 500
durable send   payments/charge     --input '{…}' --idempotency-key chg-250
durable attach payments/charge     --idempotency-key chg-250
durable output account/a1/deposit  --idempotency-key …
```

Because the **server** resolves the def by name and applies its codec, the CLI can
be **catalog-free** (send/receive raw JSON) and still get full server-side codec
fidelity for opaque-JSON defs. This needs one small client addition: an *untyped*
ingress call (`connect(url).call(name, method, json)` /
`.objectCall(name, key, method, json)` — restate-sdk-gen's `genericCall`/
`genericSend`) so the CLI doesn't compile the catalog in. A catalog-aware CLI
(imports defs, uses `serviceClient(def).method`) is the typed alternative when
transform codecs need *client-side* encoding.

**(c) Embedded / admin (no running host)** — a one-shot CLI that composes the
engine itself (`serviceLayer(catalog) + S2Client.layerConfig`) and runs a single
invocation or inspection directly against S2 — the predecessor's `firegrid run`.
Needs the catalog + S2 creds; useful for local dev/ops.

Config: `--url` / `DURABLE_INGRESS_URL` for (b); `S2_*` for (a)/(c). The
interaction CLI is bounded to what the ingress exposes (call/send/attach/output) —
admin verbs (list/cancel/status) await an admin plane (deliberately absent, §6).
It's a **follow-on adapter on the same `connect` surface as §4.4**, not v1-core;
binary name TBD.

## 5. Boot Recovery

On layer build the host re-drives in-flight work for its namespace (the engine
already does this per the README): sweep the roster for `running`/`suspended`
service executions and re-drive each by `handlerName` from the seeded registry;
for objects/workflows, fold each owner stream and resume the head.

**Timer driver (the model exists in the predecessor).** The old firegrid engine's
`recoverPendingClockWakeups` ran at engine-construction time: scan `clockWakeups`
for `pending` rows and re-arm each as a future task (`Effect.delay` + `forkIn(scope)`).
That is exactly the missing piece here — `sleep`/`clockWakeups` should be re-armed
by a host-owned sweep (and ideally a live timer fiber), not only via handler re-run.
Adopt that pattern; tracked as a production-readiness item (build-plan step 4).

**Pitfall (predecessor, documented):** a forked recovery/observer daemon must
capture the layer-build-time context (`Effect.context()`) and `Effect.provide()` it
back when forking, or the re-driven body fails with "Service not found." Applies to
any daemon the host forks across the layer boundary.

## 6. What the Host Owns (the broker job)

- **Invocation execution** — embedded `submit`/`attach`/`poll`; handlers' fibers
  run in-process, journaled to S2.
- **Recovery** — §5.
- **Timers** — fire `sleep`/`clockWakeups` (driver TBD, §5).
- **Single-writer** — v1 in-process (`running` map + object-path CAS); phase-2
  cross-host via fencing (§7).
- **Ingress** (optional) — HTTP front door for out-of-process callers.

## 7. Multi-Host Ownership and Recovery

Separate correctness from liveness:

- **write correctness**: one current owner driver appends state/journal/completion
  events; stale owners fail through S2 fencing and stop driving;
- **state rebuild**: a host reconstructs an owner projection after restart or
  takeover;
- **prompt peer takeover**: a different live host detects a crashed owner and
  takes over before that process restarts.

The default v1 model needs the first two, not the third:

- **Fencing** is load-bearing for correctness. The owner driver claims before
  active work and every owner-driver append carries the fencing token. Admission
  and external signal ingress remain bus-style appends outside the owner token.
- **Snapshots** bound replay cost. A snapshot materializes the owner projection at
  a stream cursor; recovery loads it and reads from that cursor forward.
- **Trimming** is compaction after snapshots. It discards records already covered
  by a snapshot; it is not an ownership or liveness mechanism.
- **Restart-based recovery** is the default liveness model. A restarted host sweeps
  known owner streams, rebuilds projections from snapshot-plus-tail, and re-drives
  pending heads.

**What is genuinely ours (map our execution model onto the pattern):**
1. Keep fencing encapsulated in an owner-drive session; do not thread tokens or
   expected tails through user state backends.
2. Add owner snapshots and eventually trimming so cold recovery does not fold
   unbounded history.
3. Keep per-owner local serialization for same-process scheduling until a simpler
   proven replacement exists.
4. **Service-path unification** — services still use the in-process `running` map;
   move them onto the owner-stream/fence model (same work as "services on the actor
   model," already implied by the consolidation).

Lease, heartbeat, and proactive claim-sweep are optional. They are justified only
if the deployment needs prompt, coordinator-free peer takeover of a crashed host's
in-flight stream without waiting for restart. In that mode, lease says "dead if
silent for T" and heartbeat says "a live active owner stays noisy." Prefer
release-on-park for long waits so a parked handler does not heartbeat an idle
stream for minutes or hours.

**We start far ahead of the predecessor**, which had *no* fencing (fragile
per-activity `insertOrGet`, non-stable `hostSessionId`, documented double-drive
races). See [[object-exclusivity-admission-control]].

## 8. Package Shape (trade-off)

Two options for where the Node host lives:
- **(A) Same package, `./host` + `bin` subpaths**, `@effect/platform-node`
  promoted to a regular dependency. Simplest; `.` consumers that import only the
  engine still don't pull Node at *runtime* (the import graph from `.` stays
  Node-free), but the dependency appears in `package.json`. **Recommended for v1.**
- **(B) Separate `effect-s2-durable-node` (or `-host`) package** depending on the
  engine. Keeps the engine package's `package.json` pristine/publishable without
  Node. Heavier; revisit if the engine is published standalone.

## 9. Non-Goals / Deferred

- Prefix-within-a-shared-basin namespaces (§3.4) — basin-per-namespace for now.
- Dynamic/remote handler registration — catalog is compiled in.
- Admin plane (`cancel`/`kill`/`status`), autoscaling, health/readiness endpoints
  — later; note them as future adapters.
- CLI/MCP adapters beyond the minimal `bin` — later.

## 10. Build Plan

Incremental, but the end state is multi-host — no throwaway single-owner layer
(the in-process `running` map already exists; later steps *replace* it).

1. `./host` composition (`DurableHostLive` / `DurableHostFromConfig` / `startHost`)
   over the existing `serviceLayer` + `durableIngress`, `@effect/platform-node`
   promoted to a dep (option A). **First runnable milestone** — deployable single
   host (N=1) on the current engine.
2. `bin/host.ts` entrypoint (`NodeRuntime.runMain`), env config surface
   (`S2_ACCESS_TOKEN`/`S2_BASIN` + ingress port), a `start` script; smoke-run
   against `s2 lite`.
3. Re-point `test/ingress-support.ts` to build on the host composition (dogfood the
   deployable; shrink the bespoke test wiring).
4. **Snapshot-aware owner recovery** (§7): add owner snapshots, then trimming, so
   boot recovery rebuilds from snapshot-plus-tail instead of folding unbounded
   history. Keep fencing for multi-host write correctness.
5. **Service-path unification** (§7.3): move service executions onto the
   owner-stream/fence model, retiring the in-process `running` map.

## 11. Questions — Resolved by Research

Resolved against `S2Client` config + the predecessor (`gurdasnijor/firegrid`):

- **Namespace ↔ S2 → the basin** (§3.4). `S2Client.layerConfig` binds one basin;
  streams are unprefixed within it; the predecessor only used a prefix because its
  backend had no basin. Prefix-within-basin is the fallback for many/dynamic
  namespaces (deferred).
- **One host, many namespaces → one namespace (basin) per process.** The
  predecessor's `FiregridRuntimeSpec` takes a single required `namespace` and
  composes one control-plane/output table set per process; multi-tenancy = multiple
  processes. We mirror this (one `S2_BASIN` per host). *(This is what OQ#2 was
  asking: not "can a basin hold many streams" — yes — but "should one host PROCESS
  serve multiple namespaces concurrently." Answer: no, one per process.)*
- **Headless vs ingress → ingress is an optional edge adapter** (§3.6). The
  predecessor's runtime runs workflows/observers/recovery with no ingress; HTTP/MCP
  are `*IngressLive` adapters layered on. So a headless recovery/embedded-caller
  host is first-class; `durableIngress` is opt-in.
- **Graceful shutdown → none in v1; rely on recovery-on-restart.** The predecessor
  has no SIGTERM drain — `Effect.never`, scope-close interrupts daemons, in-flight
  state persists and is re-driven on next boot. Mirror that; revisit a drain once
  the timer driver + fencing exist.

## 12. Resolved Decisions (one empirical knob remains)

- **DECIDED: fenced ownership from the start** (§3.7 / §7). S2 fencing is the
  cross-host write-correctness primitive. The default recovery model is
  snapshot/trim-based state rebuild plus restart-driven liveness. Lease +
  heartbeat + claim-sweep are optional later work only if prompt
  coordinator-free peer takeover is explicitly required.
- **DECIDED: catalog wiring is Model A** (compile-time). The deploying app imports
  its defs and calls `startHost({ catalog, … })` — *its* compiled program is the
  host (restate-sdk's `bind().listen()` shape); no standalone catalog-loading
  binary. This keeps the engine a **general substrate**: a firegrid-like system and
  the (next) distributed cucumber runtime each own and compile their own catalog +
  host, rather than sharing one dynamic binary. Model B (a generic
  `durable host --catalog ./path.js` that `import()`s a catalog) is **deferred and
  purely additive** — a thin `bin` over `startHost`, no engine change — added only
  if a hosted/one-image-many-catalogs driver appears.

## 13. References

- `gurdasnijor/firegrid` — `packages/runtime` `firegrid host` (`bin/host.ts`,
  `composition/host-live`, `composition/host-public`), the process-model precedent.
- `packages/effect-s2-durable/README.md` — engine status, recovery, "Not yet" list.
- `@restatedev/restate-sdk-gen` — the authoring-surface north star (SDK half only;
  the broker half is what this host embeds).
