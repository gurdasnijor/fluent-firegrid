# 35. Relationship to Fireline

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

Fireline is one implementation family of this RFC. This section is the only normative place where the RFC binds generic substrate terms to Fireline-specific names.

The primitive selection in this RFC is informed by Anthropic's Managed Agents post and by the historical Fireline mapping in `vault/explorations/managed-agents-mapping.md`. That mapping is retained only as historical reasoning evidence; current Fireline app guidance lives in `packages/client/README.md` and `packages/client/COOKBOOK.md`, with operator decisions isolated behind `@fireline/client/operator`.

Fireline's application-layer posture is choreography-first, as canonically described in `vault/canon/concepts/choreography-vs-orchestration.md`. Fireline provides durable tools, stream-derived observation, and agent-side introspection so the model can own control flow; it does not make a workflow orchestration SDK the substrate's center.

## 35.1 Anthropic Primitive Mapping

| Anthropic primitive | Fireline implementation-family mapping | Notes |
| --- | --- | --- |
| Session | Durable Streams / state stream, `SessionProxyRegistry`, `PromptSessionClient`, `SessionProxy`, durable session rows | Durable stream rows are truth; live promptability still requires runtime ownership. |
| Orchestration | Subscriber/wake loops over durable stream events, launch-control/runtime wake surfaces, `wake(sessionId)` helper shape | This is Anthropic `wake(session_id)`, not a Fireline workflow SDK. |
| Harness | ACP conductor/session path, stdio adapter path, provider-hosted agent loop | Fireline usually wraps an external harness rather than owning the model loop directly. |
| Sandbox | `SandboxProvider`, `ProviderDispatcher`, local subprocess/Docker/remote provider adapters | Provider lifecycle/readiness is separate from prompt dispatch semantics. |
| Resources | Resource refs, mounters, fs backend components, artifact/resource rows | Resource references and mounts are substrate inputs; payload storage stays behind resource refs. |
| Tools | Topology tool registration, MCP/Smithery/peer tools, host-tool bridges | Tool descriptors remain schema-shaped and transport-neutral at the client boundary. |

## 35.2 Combinator Mapping

| RFC combinator | Fireline implementation-family mapping | Example use |
| --- | --- | --- |
| `observe(sink)` | Observer/tracer components and metrics/audit sinks | Logging, metrics, trace export. |
| `mapEffect(fn)` | Context injection and topology components that rewrite init/prompt/tool effects | Context injection, tool registration, prompt shaping. |
| `appendToSession(mk)` | Durable stream tracer / audit tracer / state appenders | Audit rows, trace rows, state rows. |
| `filter(pred, reject)` | Budget, approval, and policy gate components | Budget block, deny-by-policy. |
| `substitute(rewrite)` | Peer routing, host-tool bridges, fs backend substitution | Peer call routing, backend read/write handling. |
| `suspend(reason)` | Approval gate, durable wait, timer/wait-for/choreography suspension | Permission wait, sleep, wait_for. |
| `fanout(split, merge)` | Spawn/spawn_all and peer/delegation fanout components | Parallel child agents, parallel tool calls. |

## 35.3 Materializer And Projection Mapping

| RFC concept | Fireline implementation-family mapping | Notes |
| --- | --- | --- |
| Materializer-as-fold | `@fireline/substrate`, `firelineSubstrateState`, `createStreamDB` collections | Fold durable stream records into queryable state. Historical Rust names such as `SessionIndex` and `RuntimeMaterializer` are retired context, not the v2 target. |
| Projection engine | `@fireline/state`, StreamDB collections, substrate projections | Rebuildable cache over the stream. |
| Snapshot-first wait | StreamDB/fireline.db-style snapshot then subscribe behavior | Prevents missed terminal rows or permission resolutions. |
| Row-family ownership | Launch/prompt dispatchers, adapters, approval gates, runtime supervisor | Fireline uses the same ownership discipline for prompt, chunk, permission, session, runtime, and resource rows. |

## 35.4 Durable Streams And Channels Mapping

| RFC concept | Fireline implementation-family mapping | Notes |
| --- | --- | --- |
| Append | Durable Streams append / shared Fireline substrate stream / configured state stream | Appends durable facts to the shared substrate stream. Per-class stream URLs are retired context, not the v2 target. |
| Cursor / offset | Durable Streams offset / next offset | Stream coordinate only; not business identity. |
| Replay | Durable Streams replay from offset | Used by materializers, subscribers, runtime recovery. |
| Live tail | Durable Streams live/SSE tail | Used for subscriptions, dashboards, and wake loops. |
| Sync ask-and-wait channel | `spawn`/`call` over awakeables / durable promises | Durable handshake; caller waits for completion key. |
| Async insert-and-move-on channel | State Protocol insert plus `state.changes(...).onInsert` / mailbox pattern | Durable buffered channel; sender continues. |
| Typed channel keys | Channel names, state collection keys, permission/timer/spawn keys | Keys are semantic ids, not stream offsets. |

## 35.5 Glossary Cross-Reference

Fireline terminology should be read through the neutral RFC terms:

| RFC term | Fireline mapping | Notes |
| --- | --- | --- |
| Durable Log | Durable Streams / state stream | Append, save offset, replay, project, resume. |
| Durable Channel | Durable Channels | Sync is `spawn`/`call` over awakeables; async is state inserts plus `state.changes` waits. |
| Record / Envelope | State row, stream entry, protocol envelope | Fireline record names are implementation schema names. |
| Projection | StreamDB collection / materialized state | Rebuildable cache, not truth. |
| Operator | Launch, prompt, timer, approval, subscriber workers | Side-effecting operators require durable claim discipline when multi-worker. |
| Agent Adapter | ACP, stdio, vendor API, in-process adapter | ACP is one adapter, not the substrate. |
| Session | ACP session or adapter-equivalent conversation identity | Promptability still requires live runtime ownership. |
| Prompt / Turn | ACP prompt turn or adapter-equivalent request | Prompt identity preserves adapter canonical ids when available. |
| Provider | SandboxProvider / ProviderDispatcher | Provider lifecycle is separate from prompt dispatch semantics. |
| Conductor | ACP conductor / proxy chain | Optional protocol-aware middleware plane. |
| Durable Promise | Awakeable / completion-key wait | Durable wait over log-backed completion rows. |
| Required Action | Permission request/resolution | Durable approval row, not callback-only control flow. |
| Middleware Spec | Topology/component spec | Serializable data lowering to components/operators/adapters. |

## 35.6 Canon Concept Mapping

| RFC concept | Fireline canon source | Mapping |
| --- | --- | --- |
| Session-stable tool descriptors | `vault/canon/concepts/tool-attachment.md`; `vault/canon/concepts/fireline-tool-publication-path.md` | Fireline-owned tool descriptors are validated/frozen before ACP session initialization; runtime context binding must not mutate the visible tool set. |
| Pure middleware specs | `vault/canon/api/client-middleware.md`; `vault/canon/concepts/middleware-composition.md` | `@fireline/client/middleware` helpers build serializable specs; host materialization preserves order and resolves credentials by reference. |
| Mailbox bridge safety | `rfc/internals/projections-and-channels.md`; `packages/client/COOKBOOK.md`; retired archaeology under `vault/retired/superseded/sdds/active/*mailbox*` | Mailbox is async delivery pattern over state insert plus `state.changes(...).onInsert` waits, not hidden session prompt input, not a default MCP tool surface, and not a Fireline class. |
| Live adapter metadata scope | `vault/canon/protocols/meta-fireline.md`; `packages/client/README.md`; `packages/client/COOKBOOK.md` | `_meta.fireline` is live ACP extension metadata for narrow lineage/load-error cases; durable/queryable state belongs in streams, and normal apps consume curated client read helpers rather than ACP transport details. |
| Log durability vs queryability | `vault/canon/concepts/sql-persistence.md` | Persistent durable-stream storage is log durability; SQL/search/archive are Fireline-built projection consumers, not alternate truth. |
| Model and drift guards | `vault/canon/verification.md`; `vault/canon/verification/quint-models.md`; `vault/canon/verification/architecture-drift-guards.md` | Quint/Stateright/Rust semantics and architecture guards cover append/replay/dedupe, no-side-effects replay, retired-surface boundaries, and model-to-code drift. |

The production cleanup SDD is a Fireline-specific migration toward this target
model. It owns the detailed inventory of legacy bypass paths and migration
names. This RFC mapping intentionally keeps only the stable conceptual
relationship between Fireline and the neutral substrate vocabulary.

The Effect toy runtime SDD is a separate executable model of these semantics.

---
