# 4. Terminology

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

## Durable Log

An append-only, ordered, durable sequence of records.

A durable log **MUST** support replay from a known position and **SHOULD** support live tailing. An implementation may provide this with ordered, replayable byte streams, database commit logs, event stores, or another append-only mechanism that preserves the same cursor and live-read semantics.

## Record

A single durable fact appended to the log.

A record includes at least:

```txt
type
key or subject
payload
timestamp or logical order
metadata / headers
```

Implementations **MAY** encode records as JSON, Protobuf, CBOR, bytes, or any other format.

## Envelope

A structured wrapper around a record payload.

An envelope **SHOULD** include enough metadata to route, project, validate, and audit the record.

## Projection

A materialized read model derived from log records.

A projection is not the source of truth. It **MUST** be rebuildable from the log, subject to retention limits.

## Operator

A long-running or replayable process that consumes log records and may update projections or append new records.

Operators are not the source of truth. Their local state **MUST** be reconstructable from the log or explicitly treated as ephemeral.

## Claimed Work Operator

An operator that performs externally visible side effects only after it has durably claimed the relevant work.

## Durable Claim

A record that declares ownership intent for a unit of work.

Claim records are part of the durable log and can be replayed to determine which actor owned or attempted to own work.

## Live Resource

A process-local resource that cannot be represented solely by a durable record.

Examples:

```txt
open ACP session connection
stdio child process
websocket connection
container handle
filesystem mount
in-process agent fiber
```

A durable record may describe a live resource, but it is not the resource itself.

## Session

A durable conversation identity between a client/runtime and an agent.

A session is adapter-defined when the underlying agent wire format has a session concept, such as ACP `SessionId` as described by the ACP Session Setup documentation. In a non-ACP adapter, a session is the closest equivalent durable conversation identity.

## Prompt / Turn

A single request to an agent inside a session.

A prompt identity **SHOULD** be scoped by session identity plus protocol request identity when the underlying agent wire format provides one. ACP's Prompt Turn documentation is one reference example; it is not the only valid model.

## Agent

A computational actor that accepts requests, performs work, emits updates, and may call tools.

An agent may be local or remote, stdio-based or networked, protocol-specific or in-process.

## Agent Adapter

A component that connects an agent wire format or process shape to the substrate.

Examples:

```txt
ACP adapter
stdio adapter
HTTP adapter
gRPC adapter
in-process adapter
MCP-capable adapter
vendor API adapter
```

## Host / Runtime

The process or service that owns live resources, executes operators, provisions providers, and bridges between durable log semantics and agent protocol semantics.

## Client

An application-facing API or program that appends intents and observes projections.

A stream-first client **MUST NOT** require direct agent transport access to perform normal launch/prompt/approval/stop flows.

## Provider

A component that provisions resources needed to run an agent.

Examples:

```txt
local process provider
local filesystem provider
container provider
remote API provider
serverless provider
VM provider
```

## Sandbox

An isolated or scoped environment for an agent.

A sandbox may be a local directory, container, VM, remote filesystem, in-memory environment, or API-scoped capability set.

## Durable Promise / Awaitable

A promise-shaped API over a durable wait.

It is not a second workflow engine. It is an ergonomic handle over log-backed wait and completion records. A conforming awaitable uses canonical completion keys and reconstructs waits by replaying the durable stream.

## Orchestration

A wake mechanism that can call a function with a durable session identity and retry on failure.

Orchestration **MAY** be a queue, cron job, subscriber loop, control-plane scheduler, workflow runner, or local process. It is a primitive because some durable event must eventually cause a harness to resume work.

## Harness

The loop that consumes session context, yields effects, receives effect results, and appends progress to the Session.

A harness may be external to the runtime, embedded in the runtime, or hosted by a provider. The substrate constrains its observable effects, not its internal reasoning strategy.

## Tool

A named capability with a description and input shape.

Tools are invoked through the Harness and may be backed by local functions, MCP servers, provider APIs, sandbox execution, or adapter-native capabilities.

## Resource

A durable reference to content or capability mounted into a Sandbox or made available to a Tool.

Resources **SHOULD** be described by references and mount/use locations rather than by embedding large payloads directly in launch specs.

---
