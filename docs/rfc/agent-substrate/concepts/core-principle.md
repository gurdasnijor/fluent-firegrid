# 5. Core Principle

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A conforming system centers on this invariant:

```txt
The durable log is the source of truth.
```

All application-observable state **MUST** be derivable from durable records.

This does not mean every live detail is durable. It means every application-visible fact that matters for replay, observation, recovery, or coordination is durable.

Examples of durable facts:

```txt
launch requested
runtime provision started
session created
prompt requested
prompt claimed
chunk emitted
permission requested
permission resolved
timer scheduled
timer fired
child agent spawned
child agent completed
session stopped
runtime failed
```

Examples of non-durable live resources:

```txt
current TCP socket
current WebSocket connection
current stdio pipe
current process id
current in-memory request handler
current fiber id
current container handle
```

A durable row may reference a live resource, but the reference **MUST NOT** be treated as proof the resource is still owned by the current process.

---
