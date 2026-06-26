# Fluent Harness I/O

Doc-Class: internal-contract
Status: active
Date: 2026-06-05
Owner: Firegrid Architecture

This document is the role map for harness I/O. It exists to remove one recurring
ambiguity: Firegrid sometimes plays the ACP client role and sometimes plays the
ACP agent/conductor role. Those are not competing designs. They are different
edges around the same fluent-runtime authority.

## One Rule

The raw harness process owns the model loop and writes no Durable Streams facts.
Firegrid-owned I/O roles observe or accept protocol traffic, then write session
facts through `packages/fluent-runtime`.

```text
                         Durable Streams session stream
                   L1 observed facts + L2 coordination facts
                                      ▲
                                      │ append/read
                                      │
                         packages/fluent-runtime
              session authority, wait/timer/child/tool semantics
                                      ▲
                                      │
                  ┌───────────────────┴───────────────────┐
                  │                                       │
                  ▼                                       ▼
        Firegrid ACP client                      Firegrid ACP conductor
    downstream harness edge                         editor-facing edge
                  │                                       │
                  ▼                                       ▼
       ACP harness subprocess                      Zed / ACP editor

                  ┌───────────────────────────────────────┐
                  │ future native/cloud lowering adapters │
                  └───────────────────────────────────────┘
```

If a component cannot write through fluent-runtime, it is not the Firegrid
session authority. It may be a process owner, transport, projection, or native
protocol adapter, but it does not decide wait matches, timer fires, child
lifecycle, or committed tool results.

The Durable Streams mechanics behind those decisions are specified in
[`substrate-protocol.md`](substrate-protocol.md). This document covers the
protocol edges that feed that substrate: which component observes harness
traffic, which component records Layer 1 facts, and which component returns
committed Layer 2 results to the harness.

## Role Matrix

| Harness class | Firegrid role | External role | Firegrid component | Raw harness writes Durable Streams? |
|---|---|---|---|---|
| ACP subprocess, such as Claude ACP or Codex ACP | ACP client | ACP agent | `FiregridAcpClient` plus `AcpHarnessProcessOwner` | No |
| Zed external agent / editor-launched session | ACP agent/conductor | ACP client | `FiregridAcpConductor` | No external harness process on this edge |
| Future Claude/Codex native protocol | Native protocol host | Native harness | Native lowering adapter | No |
| Future cloud-hosted agent | Cloud API adapter | Cloud agent service | Cloud lowering adapter | No |
| Effect AI `LanguageModel` fronted work | Model/tool facade | Model provider | LanguageModel adapter into fluent-runtime tools | No |

## ACP Downstream Harness

Use this path when Firegrid launches a real ACP agent process. The harness is the
ACP agent. Firegrid is the ACP client.

```text
┌──────────────────────────────────────────────────────────────────────┐
│ packages/fluent-runtime                                              │
│ session authority: materialize stream, choose prompt/control work,    │
│ commit L1/L2 facts, resolve durable tools, provide resume context     │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ Effect services / session callbacks
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│ FiregridAcpClient implements acp.Client                              │
│ - sessionUpdate -> append Layer 1 observation through fluent-runtime  │
│ - requestPermission -> record L1, resolve durable approval            │
│ - ext/tool call -> record L1, commit L2, return committed result      │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ ACP ClientSideConnection over acp.Stream
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│ AcpHarnessProcessOwner                                               │
│ spawn/kill only; owns process stdio; no Firegrid semantics;           │
│ no Durable Streams imports; no fluent-runtime imports                 │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ stdio / process protocol
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Real ACP harness process                                             │
│ Claude ACP / Codex ACP / custom ACP agent; owns model loop;           │
│ speaks ACP; writes no Durable Streams                                 │
└──────────────────────────────────────────────────────────────────────┘
```

Reads and writes:

| Component | Reads | Writes |
|---|---|---|
| Real ACP harness process | ACP messages from Firegrid | ACP messages to Firegrid |
| `AcpHarnessProcessOwner` | Process stdio bytes | Process stdio bytes only |
| `FiregridAcpClient` | ACP callbacks from the harness, fluent-runtime services | Layer 1 observations and Layer 2 outcomes through fluent-runtime |
| `packages/fluent-runtime` | Session stream, source facts, ACP client callbacks | Durable session facts on Durable Streams |

The process owner package is intentionally small. It is allowed to depend on the
ACP SDK and Effect process/platform APIs. It is not allowed to import Durable
Streams, fluent-runtime, Store/Host/EventIngress/Sources, projections, or
Firegrid ACP client code.

## Zed / Editor ACP Conductor

Use this path when an ACP editor or external ACP client launches Firegrid. Zed is
the ACP client. Firegrid presents as an ACP agent/conductor.

```text
┌──────────────────────────────────────────────────────────────────────┐
│ Zed editor or ACP client                                             │
│ sends initialize/session/prompt/cancel over stdio; receives ACP       │
│ responses and session updates                                        │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ ACP AgentSideConnection over stdio
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│ FiregridAcpConductor implements acp.Agent                            │
│ - stdout is ACP frames only                                           │
│ - binds editor session to fluent-runtime session authority            │
│ - appends accepted user/control intent through fluent-runtime         │
│ - may delegate downstream through FiregridAcpClient                   │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ Effect services / optional delegation
                 ┌──────────────┴──────────────┐
                 ▼                             ▼
┌──────────────────────────────┐   ┌───────────────────────────────────┐
│ packages/fluent-runtime      │   │ optional downstream ACP harness    │
│ session authority and facts  │   │ FiregridAcpClient -> process owner │
└──────────────────────────────┘   └───────────────────────────────────┘
```

Reads and writes:

| Component | Reads | Writes |
|---|---|---|
| Zed/editor | ACP responses and session updates | ACP initialize/session/prompt/cancel requests |
| `FiregridAcpConductor` | ACP requests from editor, fluent-runtime services, optional downstream ACP responses | ACP responses/notifications to editor; Durable Streams writes through fluent-runtime |
| Optional downstream `FiregridAcpClient` | Downstream ACP callbacks | Layer 1/Layer 2 facts through fluent-runtime |

Do not export a public `acp.Client | acp.Agent` union. The conductor is the
composition point that may hold both roles internally. Public surfaces stay
role-specific:

- `FiregridAcpClient implements acp.Client` for downstream ACP harnesses.
- `FiregridAcpConductor implements acp.Agent` for editor-facing ACP stdio.

## Future Native And Cloud Harnesses

Native Claude Code, native Codex, cloud agents, and other non-ACP harnesses
should lower their protocol into the same Layer 1 / Layer 2 split.

```text
┌──────────────────────────────────────────────────────────────────────┐
│ native or cloud harness                                              │
│ owns model loop, native resume semantics, native side effects         │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ native protocol
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│ native/cloud lowering adapter                                        │
│ protocol fidelity, resume artifact construction, replay suppression   │
│ for harness-native side effects                                      │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ Firegrid session calls
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│ packages/fluent-runtime                                              │
│ append Layer 1 observations, commit Layer 2 coordination facts,       │
│ return committed results through the native protocol adapter          │
└──────────────────────────────────────────────────────────────────────┘
```

Native adapters may know native protocol quirks. They still do not own Durable
Streams subscriptions, wait matching, timer firing, child lifecycle, or
queryable read-model schemas.

## Effect AI `LanguageModel` Fronted Work

Effect AI `LanguageModel` is a useful front door for authored model calls or
Firegrid-owned model workflows. It is not the managed-agent harness loop.

```text
┌──────────────────────────────────────────────────────────────────────┐
│ Effect AI LanguageModel.generateText / streamText                    │
│ with Tool / Toolkit definitions                                      │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ tool call / model response
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Firegrid LanguageModel adapter                                       │
│ maps tool calls into fluent-runtime services; records L1/L2 facts     │
│ when used as session work                                            │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ provider/model protocol
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│ provider model or downstream harness                                 │
└──────────────────────────────────────────────────────────────────────┘
```

Use this for typed model/tool composition. Do not use it to hide ACP stdio,
Durable Streams wake handling, native harness resume, or long-lived external
agent loops behind a fake synchronous model call.

## Harness Observation Contract

The Firegrid harness I/O boundary is the only component that turns protocol
traffic into Layer 1 facts:

- ACP downstream: `FiregridAcpClient`.
- ACP editor/conductor: `FiregridAcpConductor`.
- Non-ACP native/cloud: native lowering adapter.
- Process-only ACP package: no Layer 1 writes; process stdio only.

```text
external protocol event
  text · reasoning · tool_call · tool_result · permission_request
  file_change · status · turn_complete
      │
      ▼
Firegrid harness I/O boundary
  classify and record faithful Layer 1 observation
      │
      ├─ ordinary observation -> append L1 fact
      │
      └─ Firegrid durable tool call
            append L1 tool-call fact
            invoke fluent-runtime tool service
            append L2 intent/result/park fact
            return committed native response or end turn
```

Layer 1 is evidence. Layer 2 is authority. The harness I/O boundary may normalize
protocol traffic enough to append faithful observations, but it does not decide
wait matches, timer fires, child completion, or committed tool results.

## Resume And Side-Effect Safety

Every harness I/O role must preserve this invariant: resume must not re-execute
any already-observed Layer 1 side effect.

Firegrid-mediated durable tools are paired with recorded Layer 2 results and fed
back rather than executed again. Harness-native side effects, such as shell
commands, file edits, tests, or agent-owned tools that Firegrid did not mediate,
require native resume or explicit replay suppression in the role-specific
adapter/conductor.

This is the hardest integration risk and must be proven with real harnesses, not
fake recorders.

## Read Next

- [`README.md`](README.md): the provider/role model for Durable Streams, Effect,
  fluent, fluent-runtime, and harnesses.
- [`execution-models.md`](execution-models.md): why managed sessions resume by
  reconstruction while authored procedures resume by replay.
- [`substrate-protocol.md`](substrate-protocol.md): the concrete Durable Streams
  operation sequences for waits, timers, child sessions, attach, fork, and TTL.
