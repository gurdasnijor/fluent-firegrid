# Fluent Architecture

Doc-Class: internal-contract
Status: active
Date: 2026-06-05
Owner: Firegrid Architecture

This is the canonical high-level architecture reference for the fluent Firegrid
workstream. It defines package boundaries, process ownership, durable stream
read/write ownership, and schema ownership. The execution-focused design details
remain in `docs/sdds/fluent-firegrid-sdd.md`.

## Document Shape

This document follows the same structure as the Durable Streams layered consumer
spec introduced in durable-streams PR #346:

1. overview and system shape;
2. layer architecture;
3. core concepts and ownership;
4. interaction flows;
5. safety invariants and non-goals;
6. implementation gaps that must be proven.

The aim is to make every boundary falsifiable. A reader should be able to point
at an actor, a stream write, a wake, or a schema and know which package owns it.

## Architecture Doc Set

Detailed role diagrams live beside this document under
`docs/cannon/architecture/fluent/`:

- [`README.md`](README.md) is the doc-set index.
- [`execution-models.md`](execution-models.md) defines the
  replay-vs-reconstruction split: authored procedures resume by replay; managed
  sessions resume by reconstruction over the same coordination core.
- [`substrate-protocol.md`](substrate-protocol.md) owns the
  Durable Streams operation sequences for wake, wait, timer, child, attach, fork,
  and TTL.
- [`harness-io.md`](harness-io.md) is the harness I/O role map:
  ACP downstream client, ACP editor-facing conductor, future native/cloud
  adapters, and Effect AI `LanguageModel` fronted work.

## Summary

Firegrid is not the manager of the agent's reasoning loop. The external harness
owns the loop; Firegrid owns durable coordination around that loop.

The durable source of truth is a Durable Streams log. Multiple Firegrid-owned or
Firegrid-integrated actors read and write that log:

- Firegrid's harness I/O roles record observed agent events;
- Firegrid's ACP client/conductor records ACP observations when the harness or
  editor speaks ACP;
- the fluent host records coordination events and resolves waits;
- clients append user/control intents through an authorized surface;
- post-wake source handlers append timer, child, and wake results;
- projections read the log to render or verify product state.

The raw agent process does not write to Durable Streams directly. Firegrid-owned
harness I/O roles may write through fluent-runtime, but those roles are part of
the Firegrid integration boundary, not the harness itself.

The same boundary applies to authored procedures. Product authors import the
process-free `packages/fluent-firegrid` authoring surface and write Effect
bodies against a `Journal` abstraction. The target runtime deployment supplies
that journal from `packages/fluent-runtime`; authored code must not import
Durable Streams clients or own substrate leases, cursors, workers, or listeners.

The choreography promise scales across harnesses because the contract is not
"run this one agent implementation." It is: adapt any Claude, Codex, ACP, stdio,
HTTP, cloud-hosted, or future agent harness into the same durable surface. Every
harness gets the same Firegrid durable tools and the same stream-derived
observation plane; differences stay inside the adapter.

This architecture intentionally supersedes the Restate-style session reading in
which a long-lived `gen`/`run` body parks on a durable promise for the session.
That model is still useful for pure coordination workflows, sagas, and authored
multi-step procedures. It is not the session model. A session is host-driven
harness coordination: the host reacts per wake, materializes facts, reconstructs
the harness resume artifact, and records coordination outcomes. The external
harness owns the model loop. The full contrast is
[`execution-models.md`](execution-models.md): authored procedures
resume by replay; managed sessions resume by reconstruction.

## Deployment Topology

The target fluent deployment is runtime-fronted:

```text
external app / UI / editor / provider / peer
      │
      │ Firegrid API · ACP · MCP · webhook · source adapter
      ▼
┌──────────────────────────────────────────────────────────────────────┐
│ FIREGRID HOST                                                        │
│ packages/fluent-runtime                                              │
│ - terminates product ingress and adapter callbacks                   │
│ - owns Durable Streams substrate clients for Firegrid coordination    │
│ - invokes authored procedures or resumes managed harness sessions     │
│ - appends L1 observations and L2 coordination facts                   │
└───────────────┬───────────────────────────────────────┬──────────────┘
                │ DS append/read/claim/ack              │ native protocol
                ▼                                       ▼
┌──────────────────────────────┐        ┌──────────────────────────────┐
│ Durable Streams substrate     │        │ authored body or raw harness │
│ log, wake, lease, fencing      │        │ no Durable Streams writes     │
└──────────────────────────────┘        └──────────────────────────────┘
```

This is the same durable-execution topology class as "engine/broker in front of
the handler," with a narrower engine boundary:

- Durable Streams is the durable broker and substrate system of record.
- `packages/fluent-runtime` is the Firegrid semantics host in front of that
  broker.
- Authored handlers and managed harnesses are downstream of the host for
  invocation, journal service, redrive, and committed tool results.
- External apps, editors, providers, and peers call Firegrid-owned ingress
  surfaces. They do not mutate Firegrid coordination streams directly.

An embedded or in-process host may be used for local development, tests, or a
single-process deployment, but it does not change ownership. The embedded
process is still running `packages/fluent-runtime` as the host, and the
authoring layer, raw harnesses, process owners, and clients remain
Durable-Streams-client-free.

## System Contracts

| Boundary | Allowed contract | Must not happen |
|---|---|---|
| External app/client -> Firegrid host | Product API, ACP/MCP command, webhook/source delivery, authorized control request | Direct session-log mutation or Durable Streams coordination writes |
| Firegrid host -> Durable Streams | Append/read/close/fork, producer fencing, named consumer claim/ack/release, wake delivery, TTL | Firegrid-owned lease tables, cursor stores, pull queues, webhook retry loops, or task locks |
| Authored procedure -> fluent authoring | `packages/fluent-firegrid` definitions, `run`, durable primitives, typed descriptors over `Journal` | Durable Streams clients, listeners, workers, process ownership, or substrate retry/lease logic |
| Firegrid host -> authored procedure | Supply the `Journal`, invoke/replay the Effect body, serve journal hits, append terminal outcomes | Let the authored body own DS writes or bypass schemas |
| Firegrid host -> harness I/O role | Session callbacks, resume artifacts, committed tool results, Layer 1 observation recording through runtime | Let the I/O role decide waits, timers, children, or committed tool-result authority |
| Harness I/O role -> raw harness | Native ACP/cloud/provider protocol only | Raw harness Durable Streams imports, direct session fact writes, or Firegrid coordination decisions |
| Projection/UI/Firelab -> streams | Read facts and derived projections | Authoritative coordination state changes |

## System Shape

```text
┌──────────────────────────────────────────────────────────────────────┐
│ AUTHORING                                                            │
│ packages/fluent-firegrid                                             │
│ Effect-native authoring: run, keyed replay, durable primitive defs,   │
│ combinators via Effect, descriptors, typed definitions and clients.    │
│ Process-free; imported by handlers and fluent-runtime.                │
│ No Durable Streams clients or listeners.                              │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ EXTERNAL EDGES                                                       │
│ apps, editors, providers, peers, webhooks, ACP/MCP clients.           │
│ They call Firegrid ingress; they do not write coordination streams.   │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ product ingress / adapter protocol
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│ FIREGRID HOST / SESSION AUTHORITY                                    │
│ packages/fluent-runtime                                               │
│ - accepts user/control/provider facts                                 │
│ - supplies Journal to authored procedures                             │
│ - records L2 coordination: waits, timers, children, tool results       │
│ - handles DS wake claims: materialize, evaluate CEL, redrive, ack      │
│ - owns ACP client/conductor roles; raw processes remain outside        │
└───────────────┬───────────────────────────────────────┬──────────────┘
                │ append/read, DS-granted wake           │ drive/resume
                ▼                                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│ DURABLE BOUNDARY                                                     │
│ Durable Streams                                                      │
│ DS-L0 stream log: session/entity facts, append/read/close/fork        │
│ DS-L1/L2 consumer substrate: cursor, lease, claim/ack/release, wake   │
└──────────────────────────────────────────────────────────────────────┘
                                                         │
                                         ┌───────────────▼─────────────┐
                                         │ HARNESS I/O ROLES            │
                                         │ FiregridAcpClient            │
                                         │ FiregridAcpConductor         │
                                         │ native/cloud lowering        │
                                         │ LanguageModel adapter        │
                                         └───────────────┬──────────────┘
                                                         │ native protocol
                                                         ▼
                                         ┌──────────────────────────────┐
                                         │ RAW / EXTERNAL HARNESS        │
                                         │ Claude, Codex, ACP, cloud,    │
                                         │ stdio/HTTP, provider models   │
                                         │ owns loop; no DS writes       │
                                         └──────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ READ MODELS / ACCEPTANCE                                             │
│ projections, UI, Firelab, and Gherkin acceptance read stream facts.   │
│ They do not own coordination authority.                               │
└──────────────────────────────────────────────────────────────────────┘
```

The raw agent harness speaks only its native protocol and writes no Durable
Streams records. Firegrid's harness I/O roles record Layer 1 observations
through fluent-runtime. Durable Streams owns the durable stream and wake
substrate. `packages/fluent-runtime` owns product ingress and post-wake product
work: append accepted facts, evaluate Firegrid semantics, drive/resume the
harness I/O role, append Layer 2 outcomes, and ack/done through Durable Streams
only after the durable outcome is recorded.

Detailed role diagrams for ACP downstream harnesses, Zed/editor ACP conductor,
future native/cloud adapters, and Effect AI `LanguageModel` fronted work live in
[`harness-io.md`](harness-io.md).

## Relation To Proxy-Based Durable Execution

Durable execution platforms often put a durable engine, broker, gateway, or
sidecar in front of application handlers. Fluent follows that topology, but
splits the engine responsibilities across Durable Streams and a thin Firegrid
host:

- Durable Streams owns the substrate: append/read, offsets, closure, fork,
  producer fencing, named consumers, leases, claim/ack/release, wake, retry, and
  TTL.
- `packages/fluent-runtime` owns Firegrid semantics after the substrate fires:
  session facts, wait matching, timers, child lifecycle, committed tool results,
  harness redrive, external ingress acceptance, and projection inputs.
- `packages/fluent-firegrid` remains an SDK for authoring durable Effect bodies;
  it is not a process host and does not expose the Durable Streams substrate.

The proxy comparison is about ownership, not whether code imports a library.
Authored handlers may link the `fluent-firegrid` SDK just as other platforms
link their workflow SDKs. They still remain downstream of the host for durable
journaling and invocation, and they do not write Durable Streams directly.

`@durable-streams/proxy` is a different layer. It is a resumable HTTP streaming
transport for upstream calls such as AI token streams or SSE feeds. Fluent may
use it inside a LanguageModel, native, or cloud adapter to avoid rerunning an
expensive upstream stream after a dropped connection. It does not own wait,
timer, child, replay, redrive, tool-result, or session-authority semantics, and
it is not a replacement for `packages/fluent-runtime`.

## Substrate Layer Architecture

Durable Streams PR #346 introduces the substrate split this architecture should
lean on:
`https://github.com/durable-streams/durable-streams/pull/346`.
To avoid ambiguity with Firegrid's session-event layers, this document names the
substrate layers `DS-L0`, `DS-L1`, and `DS-L2`.

Durable Streams PR #343 is the implementation/conformance candidate for this
substrate:
`https://github.com/durable-streams/durable-streams/pull/343`.
Source-checkout conformance is green against the real server implementation at
commit `5f3bae712a82219608138a53e60a223c2a7dd43c`: upstream
`packages/server/test/conformance.test.ts` passed `743/743` while the upstream
Vitest config resolved `@durable-streams/server` to `packages/server/src`.
That proves the source path, not the package path.

```text
┌──────────────────────────────────────────────────────────────────┐
│ DS-L2/A: Webhook wake          │ DS-L2/B: Pull-wake              │
│ server-initiated callback      │ wake stream + worker claim      │
│ retry, callback, done          │ shared worker fleet             │
├────────────────────────────────┴─────────────────────────────────┤
│ DS-L1: Named Consumer                                             │
│ stable consumer id, stream set, acknowledged offsets,             │
│ epoch fencing, bearer token, lease, ack, release                  │
├──────────────────────────────────────────────────────────────────┤
│ DS-L0: Durable Streams core                                       │
│ append, read, offsets, close, fork, producer fencing              │
└──────────────────────────────────────────────────────────────────┘
```

Firegrid maps onto that substrate as follows.

| Durable Streams layer | Fluent use | Firegrid-owned behavior |
|---|---|---|
| `DS-L0` core stream | Session log and source logs | Event schemas, session semantics, wait predicate meaning |
| `DS-L1` named consumer | One or more stable consumers per active session or worker role | Product work to run after Durable Streams grants a claim |
| `DS-L2/A` webhook wake | Authenticated serverless wake delivery to a fluent host endpoint | Product work after an authenticated DS wake: materialize, match, redrive, append outcome |
| `DS-L2/B` pull-wake | Worker fleet receiving DS-granted claims from a wake stream | Product work after a claim: materialize, match, redrive, append outcome |

The important split is: Durable Streams owns consumer cursors, epoch fencing,
leases, heartbeat/ack/release, retry, and wake delivery. `packages/fluent-runtime`
owns the product action after a wake is claimed: materialize the session stream,
match waits, redrive the harness, append Layer 2 facts, then ack only after the
durable outcome is recorded.

### Substrate Ownership Rule

If Durable Streams exposes a primitive for a concern, fluent-runtime must not
rebuild that concern under a Firegrid name. A missing primitive in the selected
package is a substrate dependency problem: adopt the maintained fork, wait for
the upstream package, or explicitly mark the feature blocked. It is not
permission to implement a parallel lease, cursor, retry, queue, or webhook
delivery system in fluent-runtime.

Firegrid may bind product semantics after the substrate primitive fires. That is
the only allowed layer boundary:

```text
Durable Streams primitive
  append/read/close/fork · producer fencing · consumer claim/ack/release
  consumer cursor · lease · retry · webhook wake auth/callback/done
      │
      ▼
Firegrid product step
  decode session facts · evaluate CEL · record L2 outcome
  drive/resume adapter · feed committed tool result · project read models
```

| Concern | Substrate owner | Firegrid-owned work | Forbidden fluent-runtime build |
|---|---|---|---|
| Stream storage | Durable Streams append/read/offset/close/fork | Define session/entity fact schemas and projection semantics | Alternate session log, side DurableTable as authoritative truth, terminal side table |
| Idempotent append | Durable Streams producer fencing | Choose producer ids, epochs, and sequence policy for product facts | Local dedup table or task lock replacing producer fencing |
| Consumer ownership | Durable Streams named consumers and epoch fencing | Decide what product action runs after a claim is granted | Worker lease table, generation table, task-owner lock |
| Consumer cursor | Durable Streams acknowledged offsets | Read the provided offsets and materialize facts | Separate durable cursor store for subscribed streams |
| Pull wake | Durable Streams pull-wake claim/ack/release | Post-claim materialize/evaluate/append/ack-after-durable-result | Custom pull queue, competing-claim algorithm, stale-lease scanner |
| Webhook wake | Durable Streams webhook signing-key discovery, signature check, callback, ack/done, retry, idle transitions | HTTP endpoint body that performs the product step after authenticated wake delivery | Webhook wake signing, callback lifecycle, retry loop, done/idling state machine |
| Provider event ingress | Provider/source adapter accepts or rejects the provider delivery; Durable Streams producer fencing dedups the append | Map accepted delivery to a queryable state-change fact and candidate wake | Treat provider delivery as a Durable Streams wake protocol or rebuild DS webhook delivery for providers |
| Timer wake | Durable Streams or adopted substrate scheduled source, if supplied | Convert a fired schedule into a timer/state fact and redrive | Process-local sleep, timer lease table, timer retry queue; if no scheduled source exists, mark a source integration gap |
| Child/session fork | Durable Streams fork/close/producer-boundary primitives where applicable | Define child lifecycle facts and parent wait semantics | Copy-on-write session store or child routine/reclaim table |
| MCP/tool binding | Firegrid edge schemas and auth; fluent-runtime product services | Translate observed tool calls to L2 facts and return committed results | Durable coordination hidden inside the MCP host |

Design artifacts backing this ownership split:

- Durable Streams PR #343: named consumers, pull-wake, webhook wake, and
  conformance tests for the adopted server surface.
- Durable Streams PR #346 and `COORDINATION-PATTERNS.md`: layered consumer
  vocabulary and coordination patterns.
- Durable Streams protocol §4.2: stream fork and fork-boundary producer-state
  behavior.
- Durable Streams protocol §5.2.1: idempotent producers, producer sequence, and
  epoch fencing.
- Durable Streams protocol §6.5: webhook signing key discovery for Durable
  Streams webhook wake delivery.
- Durable Streams protocol §7.2/§7.3: pull-wake claim/ack/release and generation
  fencing.

### What The Consumer Substrate Solves For Fluent

The layered consumer model removes several pieces of machinery fluent should not
rebuild:

- **No bespoke worker lease table.** `DS-L1` epoch acquisition and stale-epoch
  rejection fence concurrent redrivers.
- **No separate cursor store.** `DS-L1` acknowledged offsets are the durable
  "processed through here" cursor for session/source streams.
- **No hand-rolled webhook retry loop.** `DS-L2/A` handles wake delivery,
  callback, retry, and done/idling.
- **No custom pull queue.** `DS-L2/B` is a wake stream plus race-to-claim through
  `DS-L1`.
- **No lost-wakeup gap for subscribed streams.** A session or worker consumer can
  track a set of streams and wake when any has pending work.

The remaining Firegrid work is smaller and more specific: define the session
event schemas, bind CEL `wait_for` predicates to state-change facts, implement
the host's post-claim product handler, and prove real harness resume without
duplicate side effects.

This mapping is no longer purely speculative: the upstream source checkout has
passed conformance. The remaining adoption risk is dependency packaging and
maintenance. The latest published `@durable-streams/server` package available to
Firegrid at the time of this document does not expose the PR #343 consumer
surface, so package-integrated Firegrid tests cannot yet run it through normal
workspace dependency resolution.

Firegrid's interim substrate source is the maintained fork branch:
`https://github.com/gurdasnijor/durable-streams/tree/firegrid/pr343-consumer-substrate`.
It was seeded from `5f3bae712a82219608138a53e60a223c2a7dd43c` and should be
kept current with upstream `main` until the upstream package path is available.

| Adoption state | Status | Evidence or next action |
|---|---|---|
| Source conformance | Green | `743/743` upstream conformance tests passed against real PR #343 server source. |
| Published package path | Pending | The available package does not expose the consumer routes/surface needed by fluent. |
| Firegrid fork path | Interim source | Maintain `gurdasnijor/durable-streams@firegrid/pr343-consumer-substrate` and rerun conformance after rebases. |
| Firegrid integration witness | Pending | Prove a fluent post-claim actor uses a Durable Streams-granted claim, materializes facts, appends Layer 2 outcome, and ack/dones through the substrate after the durable result. |

Until the package path or maintained fork path is integrated, fluent-runtime must
not build bypass infrastructure that competes with the substrate: no local lease
table, cursor store, pull queue, webhook retry loop, or task-claim lock. The
correct interim state is "blocked on substrate adoption," not "rebuild the
substrate inside fluent-runtime."

## RFC Delivery Shape

The original Fireline promise is choreography across many agent implementations:
the model chooses the schedule at runtime, and durability comes from the small
tool set every harness can call. Fluent delivers that by keeping harness
differences below the adapter boundary.

```text
Claude ACP      Codex ACP      cloud agent      stdio/HTTP agent
    │              │              │                  │
    ▼              ▼              ▼                  ▼
adapter A      adapter B      adapter C          adapter D
    │              │              │                  │
    └──────────────┴──────────────┴──────────────────┘
                           │
                           ▼
                Firegrid choreography tools
          wait_for · sleep · spawn · spawn_all · schedule_me · execute
                           │
                           ▼
                 same Durable Streams session model
              L1 harness facts + L2 coordination facts
                           │
                           ▼
             projections for humans, agents, firelab, audit
```

The schedule is not a DAG in Firegrid. One agent can `spawn` a child agent of a
different harness type, `wait_for` a webhook or peer result, call `execute`, then
schedule itself, all by appending and observing durable facts. That is the RFC's
"append durable facts, read durable facts, derive everything else" rule made
concrete.

Target-state round trip:

```text
client prompt
  -> fluent-runtime appends input fact
  -> Claude ACP harness receives prompt through adapter
  -> Claude calls wait_for("github.pr.merged && repo == self.repo")
  -> fluent-runtime appends L2 wait intent and parks the turn
  -> GitHub webhook arrives through external ingress
  -> fluent-runtime appends state-change fact, records wait_matched, redrives
  -> Claude calls spawn("codex", "verify the merge")
  -> fluent-runtime forks/creates child session and starts Codex adapter
  -> Codex calls execute("sandbox", "pnpm test")
  -> fluent-runtime records committed result and child terminal fact
  -> projections expose prompt, wait, webhook, child, test result, terminal state
```

No authored DAG coordinates that sequence. The model chose the calls; Firegrid
made each call durable, observable, and recoverable through stream facts.
This is the architecture's north-star acceptance story, not the first milestone.
It requires a composite proof before it can be treated as a shipped capability:
parent harness A spawns child harness B, the child reaches terminal state, the
parent is woken by the child terminal fact, and both harnesses survive kill/resume
without duplicated Layer 1 side effects.

## Component Ownership

| Component | Owns | Does not own |
|---|---|---|
| `packages/fluent-firegrid` | Process-free authoring API: `run`, keyed replay, durable primitive definitions, Effect-based composition, descriptors, typed definitions and clients | Bespoke `Operation`/`Future` runtime, processes, HTTP servers, MCP servers, Durable Streams workers, agent loop ownership |
| `packages/fluent-runtime` | Fluent host: store, ingress, sources, workers, future HTTP/MCP surfaces, ACP client/conductor roles, wake/redrive semantics, Durable Streams reads/writes | The agent's model loop, UI read-model schema, raw process ownership |
| ACP process owner package | Spawning/killing downstream ACP agent processes and exposing ACP streams | Firegrid ACP client callbacks, Durable Streams writes, Layer 1/Layer 2 facts, read-model schemas |
| Native/cloud harness adapters | Native protocol fidelity, native resume, replay suppression for harness-native side effects | Firegrid coordination semantics such as wait matching, timer firing, child lifecycle ownership |
| External harness | The model loop and native protocol behavior | Direct Durable Streams writes, durable coordination, redrive decisions |
| Durable Streams | Append-only log, stream closure, fork, producer fencing, subscriptions/wake delivery substrate | Product semantics, schema projection decisions, wait predicate meaning |
| Client/control plane | Authorized user intent, prompt, approval, cancel, send, fork/tag/schedule requests | Bypassing fluent-runtime coordination or directly mutating coordination state |
| Firelab / acceptance | Product-observable verification over streams and projections | Production implementation behavior |

## Durable Stream Layers

Each session stream carries two logical layers.

These are Firegrid session-event layers, not the Durable Streams substrate
layers described above. When discussing substrate behavior, use `DS-L0` /
`DS-L1` / `DS-L2`; when discussing facts inside a session stream, use Layer 1
and Layer 2.

**Layer 1: harness observation.** These are facts observed from the external
agent harness: assistant text, reasoning, tool calls, tool results, permission
requests, file changes, and turn completion. The harness I/O boundary records
the protocol event; projection/read-model code owns normalized query shapes.

**Layer 2: Firegrid coordination.** These are facts Firegrid owns: wait intents,
wait matches, timers, child session lifecycle, committed tool results, approvals,
terminal turn records, and other wake/redrive facts.

Some user-visible concepts appear in both layers with different meanings. For
example, a permission request in Layer 1 is what the harness asked for; an
approval wait/match in Layer 2 is what Firegrid durably waited on and resolved.

The L1-to-L2 lift is the legitimate reconciliation seam. For example, a harness
emits a Firegrid tool call in Layer 1; the fluent host validates and commits its
durable result in Layer 2; the adapter feeds that result back to the harness.
The one-log design removes cross-store reconciliation, not this intra-log lift.

## Read/Write Boundaries

| Actor | Writes to Durable Streams | Reads from Durable Streams |
|---|---|---|
| Raw agent process | Nothing directly | Nothing directly |
| Firegrid ACP client / conductor | Layer 1 ACP observations, role/process lifecycle records, Layer 2 outcomes through fluent-runtime | Session stream for redrive and resume context |
| ACP process owner | Nothing | Nothing; it owns process stdio only |
| Native/cloud harness adapter | Layer 1 native observations through fluent-runtime | Existing history for native resume |
| Client / control plane | User intents, prompt/cancel/approval responses, addressed sends | Projections and current session state |
| Fluent host | Layer 2 coordination events, durable tool results, terminal records | Session/turn state before wake handling and redrive |
| Event ingress | Fenced external state-change facts | Pending waits through fluent-runtime sources |
| Post-wake source handlers | Timer-fired, child-complete, wake-result records after substrate delivery or claim | Product source state needed to derive those facts |
| Projection/UI | Nothing authoritative | Layer 1 and Layer 2 logs projected into read models |
| Firelab | Usually nothing authoritative except scenario setup | Product-observable stream facts and projections |

## Harness I/O Contract

A harness I/O binding lets a harness participate without making Firegrid own
that harness's model loop. The binding details are now split into
[`harness-io.md`](harness-io.md) because this is a load-bearing
role contract.

The short rule is:

- ACP downstream subprocesses use `FiregridAcpClient implements acp.Client`.
- Zed/editor-launched sessions use `FiregridAcpConductor implements acp.Agent`.
- Future native/cloud harnesses use native lowering adapters.
- Process-owner packages spawn/kill and expose protocol streams only; they do
  not write Layer 1, Layer 2, projections, or Durable Streams facts.
- Resume must not re-execute any already-observed Layer 1 side effect.

## MCP Host And Tool Binding

The MCP host is a tool edge, not the runtime core. It exposes Firegrid's
choreography and session-plane tools to harnesses that can call MCP tools.
Handlers are thin bindings into `packages/fluent-runtime`; they do not own
durable state themselves.

```text
harness tool call
  wait_for / sleep / spawn / spawn_all / schedule_me / execute
      │
      ▼
harness I/O boundary records L1 tool_call
      │
      ▼
Firegrid MCP host
  schema + auth + tool dispatch only
      │
      ▼
fluent-runtime tool service
  append L2 intent/result/terminal facts on session stream
  park or return host-committed result
      │
      ▼
harness I/O boundary returns native tool_result to harness
```

Two tool families share the edge:

- **Choreography tools** change durable coordination: `wait_for`, `sleep`,
  `spawn`, `spawn_all`, `schedule_me`, and `execute`.
- **Session-plane tools** query or address durable session state: observe/read
  projections, append input, send to another entity, tag, fork, and schedule.

Both families are backed by the same session-stream rules. The MCP host may be
built with Effect's `Tool`, `Toolkit`, and `McpServer` shapes, but those are edge
composition helpers. The durable semantics live in fluent-runtime services and
Durable Streams facts.

## Schema Ownership

| Schema family | Owner | Notes |
|---|---|---|
| ACP protocol envelopes | ACP SDK; observed by `FiregridAcpClient` / `FiregridAcpConductor` | Protocol fidelity boundary for ACP roles. |
| Native harness envelopes | Native/cloud lowering adapter | Protocol fidelity boundary for non-ACP harnesses. |
| `NormalizedEvent` taxonomy | projection/read-model layer | Shared read-model input for harness events. |
| Agent DB rows: messages, turns, tool calls, permission requests, participants | projection/read-model layer | UI/query schema over Layer 1. Not fluent-runtime coordination state. Prior art may be ported from `coding-agents/src/agent-db-schema.ts`, but ownership moves to Firegrid projections. |
| Firegrid coordination rows: waits, timers, child lifecycle, committed tool results, terminal records | `packages/fluent-runtime` | Layer 2 durable coordination facts. |
| Authoring types: `run`, durable primitive definitions, descriptors, typed clients | `packages/fluent-firegrid` | Effect-native library surface; no bespoke `Operation`/`Future` runtime, process, or worker ownership. |
| Durable Streams protocol: append/read/close/fork/subscription/fencing | Durable Streams packages | Substrate, not product semantics. |

## External Producers

An external producer is any actor outside the parked harness turn that appends a
candidate wake fact. Examples include webhook ingress, approval UIs, tool
callbacks, timer source actors, child-session actors, or peer sessions.

External producers do not decide the session outcome. They append fenced facts.
The fluent host evaluates waits, records matches, and redrives the session.
If an external producer runs as a worker, the Durable Streams consumer substrate
owns delivery, lease, cursor, retry, and competing-claim behavior. Firegrid owns
only the product handler that runs after delivery or claim.

Producer fencing and wake-claim fencing are separate. External producers append
under Durable Streams producer fencing so retries are idempotent. The fluent
host's Layer 2 writes for a wake, such as wait matches and redrive outcomes, are
serialized by the wake claim/generation fence so there is one active
decision-writer per session wake. Redrive serves the recorded match; it does not
re-evaluate a moving world after the fact.

Durable waits, timers, and external promise-like resolutions are session-stream
facts. Implementing them over a second journal, such as `DurableDeferred` or a
workflow table beside the stream, reintroduces the impedance this architecture is
removing. The primitive shape is: record intent on the session stream before
parking, receive wake via the Durable Streams subscription/claim mechanism, append
the match/resolution to the same stream, then redrive from that recorded fact.

## External Ingress And Webhooks

Webhook ingress is a specialized external producer. The legacy
`packages/runtime/src/verified-webhook-ingest` path verifies a product-owned HTTP
request and writes a `VerifiedWebhookFactTable` row. In fluent, the equivalent
accepted fact is a session-stream state-change event; no side DurableTable is the
authoritative store.

```text
product HTTP route / Worker
  owns route, raw body capture, provider-specific acceptance policy
      │
      ▼
source adapter
  accept or reject delivery, decode payload, derive delivery id and event key
      │
      ▼
fluent-runtime EventIngress
  append fenced State Protocol change message to the session stream
  producer id = source + delivery id
      │
      ├─ duplicate delivery -> Durable Streams producer dedup; no redrive
      │
      └─ new fact -> match pending wait_for predicates
                    append L2 wait_matched fact
                    wake/redrive session under claim fencing
```

The event is queryable because it is now a durable stream fact. Read models fold
the session stream into provider-specific or generic collections; they do not own
the webhook truth. `wait_for` predicates evaluate against the State Protocol
change message shape (`event` plus the waiting session's `self` correlation
data), so the same fact that wakes the session is the fact humans and agents can
query.

`self` is not a live mutable projection read at match time. It is the session's
recorded correlation context for that wait: either embedded in the `WaitIntent`
or referenced by an immutable stream offset captured before park. When a match is
recorded, the Layer 2 match fact records the predicate, the matched event, and
the `self` snapshot or immutable reference used for evaluation. Replay serves
that recorded match; it does not rebuild `self` from a newer projection.

Durable Streams subscription delivery supplies the transport mechanisms:
webhook delivery/callback can deliver a wake to an HTTP endpoint, and pull-wake
claim/ack/release with generation fencing/leases can drive redrive scheduling.
The Firegrid-specific mechanism above is what the post-wake product actor does
with that delivery: append or derive the session fact, match waits, redrive, then
ack only after the durable result is recorded.

Durable Streams webhook wakes are authenticated at the substrate boundary.
Webhook signing-key discovery, callback signatures, generation fencing, callback
ack/done, retry, and idle transitions belong to Durable Streams, not
fluent-runtime.

Two webhook meanings must stay separate:

- **Provider webhook**: GitHub, Stripe, Linear, or another outside product calls
  a Firegrid-owned/product-owned HTTP route. The source adapter applies that
  provider's acceptance policy and appends a state-change fact.
- **Durable Streams webhook wake**: the Durable Streams server notifies a fluent
  host endpoint that a named consumer has pending work. The host then reads the
  stream and acks/dones through the substrate callback.

They can compose in one request path, but they are not the same protocol event.

## Runtime Wake Loop

The production wake path is:

1. A durable wake source arrives: input append, state change, timer, child result,
   approval, or webhook.
2. The Durable Streams substrate delivers or grants a wake claim to the fluent
   host.
3. `handleSession(wake)` materializes the session stream.
4. The host reconstructs the resume/native artifact through the adapter.
5. The host drives the external harness, not `agent.run`.
6. If the harness calls a Firegrid durable tool, the host records Layer 2 intent.
7. If the tool parks, the host ends the turn and waits for another wake.
8. On a matching external fact, the host records the match and redrives.

Step 4 is the highest-risk integration contract. Because the model loop is not
replayed as the durable mechanism, resume must reconstruct the harness-native
state from the stream without re-executing already-observed Layer 1 side effects.
Firegrid-mediated durable tools are the easy subset: their observed L1 tool calls
must be paired with recorded Layer 2 results and fed back rather than executed
again. Harness-native side effects, such as shell commands, file edits, tests, or
agent-owned tools that Firegrid did not mediate, need native resume or explicit
replay suppression. This must be proven with a real harness, not a fake codec.

## Safety Invariants

These invariants are the review handles for fluent implementation work.

| ID | Invariant | Why it matters |
|---|---|---|
| F-S1 | The raw agent process never writes to Durable Streams directly. | Keeps harness behavior adapter-mediated and auditable. |
| F-S2 | Every parking tool records intent on the session stream before the harness turn ends. | Prevents lost wakeups. |
| F-S3 | A claimed wake has one Layer 2 decision-writer, fenced by the substrate consumer epoch. | Prevents duplicate wait matches or double redrive. |
| F-S4 | Redrive serves the recorded match/result; it does not re-evaluate a moving world after the fact. | Makes resume deterministic. |
| F-S5 | The fluent host acks/dones a substrate wake only after the durable Layer 2 outcome is recorded. | Prevents acknowledged-but-lost progress. |
| F-S6 | Resume must not re-execute any already-observed Layer 1 side effect. | Prevents duplicate shell/file/tool effects. |
| F-S7 | External facts that can wake a wait are also queryable as stream facts. | Keeps ingress, wake, UI, audit, and Firelab on the same truth. |
| F-S8 | Durable waits, timers, and promises do not use a second authoritative journal beside the session stream. | Preserves the one-log architecture. |
| F-S9 | Cancel during a parked wait and interrupt during an active turn leave a durable terminal or continuation fact before process teardown. | Prevents corrupt sessions and duplicate effects on the next redrive. |
| F-S10 | Subscription registration is followed by a catch-up read when a lost-wakeup window is possible. | Prevents events that land between last read and subscription creation from being missed. |
| F-S11 | Producer epoch fences journal writes; subscription generation fences cursor movement and wake ownership. | Keeps append idempotency separate from claim/ack ownership. |
| F-S12 | A durable tool implemented as an authored procedure runs as a child invocation on its own stream, not inline on a managed-session stream. | Keeps replay-model tool bodies from mixing into reconstruction-model sessions. |

## Implementation Gaps To Prove

The architecture document is a contract, not proof that the contract is already
implemented. The first load-bearing proofs are:

| Gap | Required proof |
|---|---|
| Substrate dependency path | Firegrid depends on either a published Durable Streams package or the maintained fork branch that exposes PR #343 consumer APIs, and the upstream conformance suite runs in the Firegrid dependency context. |
| Firegrid post-claim witness | A fluent post-claim actor accepts a substrate-granted wake claim, reads from the provided offsets, materializes session facts, appends the Layer 2 outcome, and acks/dones only after that durable outcome is recorded. |
| DS-native durable wait | `wait_for` appends intent before park, wakes through a named consumer, records the matched fact, and redrives. |
| DS-native durable timer | A scheduled source appends or wakes through the substrate without process-local sleep; if the fork does not supply scheduled wake sources, the missing piece is a source integration, not a fluent lease/cursor/retry system. |
| Real harness resume | Killing and resuming a real Claude/Codex/ACP harness does not duplicate already-observed side effects. |
| Provider event ingress | An accepted external delivery becomes a queryable session-stream fact and can wake a CEL predicate; Durable Streams webhook wake authentication remains substrate-owned. |
| MCP/tool binding | Harness tool calls are observed in Layer 1, resolved by fluent-runtime as Layer 2 facts, and returned through the adapter. |
| Cancel/interrupt safety | Cancel during a parked wait and interrupt during an active harness turn do not corrupt the session, drop owed native responses, or duplicate side effects on subsequent redrive. |
| Cross-harness spawn | A parent session running harness A spawns a child session running harness B; the child terminal fact wakes the parent; both adapters resume safely after kill/restart. |

## Difference From `packages/runtime`

The fluent architecture is not a reshaped copy of `packages/runtime`. The main
difference is durable topology: which facts live on the session stream, and
which still require side machinery.

| Concern | `packages/runtime` | Fluent architecture |
|---|---|---|
| Agent loop ownership | Post-cutover runtime already trends toward a per-event adapter-forwarding shape where the harness owns the loop. | Same principle, made explicit as the package/process contract. |
| Durable topology | Runtime keeps key coordination durability in workflow tables/context/channel machinery beside the event stream. | Durable Streams session log is the single durable boundary; Layer 1 and Layer 2 are explicit stream facts. |
| Primary runtime unit | Runtime context, channel router, workflow-engine bodies, subscribers, and edge adapters. | `handleSession(wake)` over a materialized stream, plus role-specific harness I/O resume. |
| Public authoring layer | Historically coupled to runtime execution and protocol projections. | `packages/fluent-firegrid` is process-free: `run`, keyed replay, durable primitive definitions, Effect-based composition, descriptors, and typed clients only. |
| Process/host layer | Large runtime package owns edge routing, workflows, substrate adapters, and session machinery together. | `packages/fluent-runtime` is the fluent host: store, ingress, wake/redrive, workers, and future HTTP/MCP surfaces. |
| Agent events | Runtime-specific observation/projection paths. | Firegrid harness I/O roles record Layer 1 harness events; projection/read-model schemas stay outside fluent-runtime. |
| Coordination events | Spread across runtime workflows, channels, context state, and subscribers. | Fluent host owns Layer 2 facts: waits, timers, children, committed tool results, approvals, terminal records. |
| Schema ownership | Multiple surfaces historically projected through runtime/client/protocol edges. | Schema families have explicit owners: projection for agent DB; fluent-runtime for coordination; fluent-firegrid for authoring types. |
| Extensibility | Adding surfaces tends to add runtime-specific adapters or route plumbing. | New harnesses plug in through role-specific harness I/O contracts; new coordination behavior lands as Layer 2 facts plus fluent host handling. |
| Verification target | Often requires understanding internal runtime workflows and traces. | Firelab/acceptance verifies product-observable stream facts and projections; traces are diagnostic. |

The replacement goal is architectural reduction: keep the composable authoring
API small, keep the host boundary explicit, and make Durable Streams facts the
reviewable runtime state. Anything that pushes model-loop ownership, UI
projection schemas, or harness protocol details back into `packages/fluent-runtime`
is drift toward the old runtime shape.

This does not mean legacy runtime knowledge is useless. `packages/runtime` is not
the durability architecture reference, but its edge-case inventory is valuable:
permission timeout behavior, adapter quirks, packaged-agent environment allow
lists, tool-use modes, streaming tool-call shape drift, and other integration
lessons should be mined and revalidated against the fluent architecture.

## Import And Dependency Rules

- `fluent-runtime` may depend on `fluent-firegrid`.
- `fluent-firegrid` must not depend on `fluent-runtime`, Durable Streams workers,
  MCP servers, HTTP servers, or process/sandbox launchers.
- Product code must not import vendored references under `repos/`.
- Projection packages may consume stream logs and normalized events, but must not
  own coordination semantics.
- Process-owner packages may spawn/kill harness processes, but must not
  implement Firegrid wait/timer/child semantics or write Durable Streams facts.
- Harness I/O packages may translate protocol traffic into Layer 1 facts, but
  Layer 2 coordination authority stays in fluent-runtime.
- Legacy `packages/runtime` is not a durability design reference for fluent
  architecture. It may be read for integration edge cases that must be tested
  through the fluent harness I/O roles and host.

## Non-Goals

- Do not replay the model loop as the durable mechanism.
- Do not implement a session as a long-lived `fluent-firegrid` generator body
  parked on durable promises; use generators for coordination workflows, not the
  external agent loop.
- Do not make the raw agent process a Durable Streams writer.
- Do not collapse UI/projection schemas into fluent-runtime coordination state.
- Do not rebuild the legacy runtime with renamed packages.
- Do not use Firelab-only mocks as proof of product architecture.
