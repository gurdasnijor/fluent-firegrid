# Alternatives Considered

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

## Connection-Pool / Delegate Runtime

One rejected design is to keep a pool of live agent connections and delegate prompt, approval, child-session, and tool work through whichever connection appears available. That reduces initial substrate work but makes restart semantics ambiguous: a durable session id can point at a live connection, a dead connection, or a connection owned by a different process. This RFC instead requires explicit live ownership, reattach profiles, and terminal/recovered records for restart decisions.

## Workflow-Orchestration SDK as the Product Surface

Another rejected design is to expose a first-party workflow SDK with `createFunction`, `step.run`, `step.waitForEvent`, `step.dispatch`, or YAML DAGs. That shape provides familiar durable execution but centers developer-authored control flow. This RFC keeps workflow engines as allowed external drivers and makes the substrate choreography-first: the model decides sequence, branching, parallelism, and recovery through durable tools.

## Adapter-Specific Client APIs

The substrate could expose ACP, stdio, HTTP, or vendor API shapes directly to applications. That makes simple demos easy, but couples the application API to one agent wire format and leaks protocol-specific failure modes into product code. This RFC instead requires clients to append durable intents and observe projections; protocol adapters sit below runtime semantics.

## Projection Store as Source of Truth

The system could make SQL, search, or a state table the primary write surface and derive logs from that store. That improves query ergonomics but weakens replay, append-order authority, and multi-consumer observation. This RFC makes the durable log authoritative and treats query stores as projection consumers.

## Callback Middleware and Dynamic Tool Mutation

Approval, tracing, tool routing, and context injection could be implemented as process-local callbacks that mutate live tool or prompt surfaces. That shape is hard to replay, hard to move between hosts, and unsafe for session-stable tool catalogs. This RFC requires serializable middleware specs, deterministic topology identity, host-resolved credentials, and session-lifetime tool descriptor stability.

---
