# 23. Observability

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

Observability is derived from durable facts.

For a choreography-first system, observability is existential, not optional. When the model owns sequence, branching, parallelism, and recovery, static workflow inspection cannot explain what the system will do. The substrate **MUST** make the dynamic schedule legible from durable records.

A conforming system SHOULD expose:

```txt
session projections
prompt projections
chunks/updates
required actions
runtime/resource status
operator status
audit records
trace records
metrics
```

A dashboard SHOULD observe projections, not poll agent process memory.

A stream-first observation model explicitly separates control plane, session plane, and observation plane.

Trace context may ride through adapter metadata, but trace history that must survive restart or support agent-side introspection **MUST** be durable log/projection data. Adapter-private metadata is not an observability store.

## 23.1 Choreography Trace Requirements

All choreography primitive invocations **MUST** emit durable trace or session records. At minimum, records SHOULD cover:

```txt
sleep scheduled/fired/cancelled
wait_for registered/resolved/timed_out/cancelled
spawn requested/started/completed/failed
spawn_all requested/child status/aggregate terminal state
schedule_me registered/fired/cancelled
execute requested/result/failed
```

These records **MUST** include enough identity to correlate the invocation with session id, request id or turn id when available, tool call id when available, child session id when applicable, causation id, and terminal state.

All durable tool calls and choreography traces **MUST** be readable by the agent itself through an authorized tool or query surface. A conforming implementation MAY restrict scope by tenant, session, role, or policy, but it **MUST** provide an agent-side introspection path for the agent's own execution history.

Agent-side introspection is what makes choreography adaptive. Agents read actual execution history, not a frozen workflow specification, and choose future actions accordingly.

## 23.2 Tracing

Trace context SHOULD be carried in record metadata where practical.

Agent protocol adapters SHOULD preserve trace/correlation metadata across protocol boundaries when possible.

## 23.3 Audit

Audit consumers SHOULD be able to replay log records.

A system MAY maintain separate audit projections, but audit truth SHOULD derive from the durable log.

---
