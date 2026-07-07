# 11. Client Model

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A stream-first client appends intents and observes projections.

A conforming client **SHOULD NOT** require direct access to agent transport for normal operations.

## 11.1 Client Operations

A client MAY expose high-level operations such as:

```txt
start session
load session
prompt session
respond to required action
stop session
observe updates
observe required actions
snapshot projections
```

These operations SHOULD be implemented as:

```txt
append canonical intent
await projection change
return handle or result
```

The intent is not the authoritative runtime state. A client prompt or required
action response writes a command/control record that the owning runtime
workflow later consumes. The workflow or operator remains the single writer for
runtime-owned rows, workflow deferred completions, claims, terminal state, and
live adapter delivery.

The client-side wait algorithm **SHOULD** use the projection subscription contract in §10.4. For example, a prompt API should:

```txt
append prompt intent with session id, request id, and idempotency key
read prompt/session snapshot at a known cursor
return immediately if terminal state is already present
subscribe after the snapshot cursor
resolve when terminal state or typed failure appears
unsubscribe on completion or caller cancellation
```

If a client issues a duplicate operation with an idempotency key, it **MUST** resolve from current durable/projection state when possible. It **MUST NOT** append a new prompt intent against a stale session after the runtime has durably reported that the session is not live.

## 11.2 Client MUST NOT

A stream-first client **MUST NOT**:

```txt
open agent protocol transport directly for normal prompt dispatch
depend on WebSocket/stdio/HTTP details of the agent
write runtime-owned state rows or workflow deferred completions directly
mint replacement identities when the agent wire format provides canonical ids
treat local timeout as the normal signal of runtime unavailability
```

A client MAY use direct protocol access in diagnostic, development, or adapter-specific tools, but those surfaces MUST be explicitly distinct from the stream-first application client.

## 11.3 Client Error Contract

Client APIs **SHOULD** expose typed errors for expected durable failures. At minimum, a full implementation SHOULD distinguish:

```txt
not found
not live / not promptable
permission denied
idempotency conflict
timeout waiting for durable state
projection unavailable
transport unavailable to the substrate API
```

A timeout waiting for a projection is not proof that the agent failed. If the runtime can determine a durable failure, the client **SHOULD** surface that typed failure instead of a generic timeout.

## 11.4 Async-to-Session Bridge Safety

Asynchronous delivery channels such as mailboxes, webhooks, and subscriber callbacks **MUST NOT** become hidden session prompt input. A bridge from async work to session work **MUST** be explicit application, agent, or operator logic:

```txt
1. receive and, if applicable, claim async work
2. validate origin, payload, policy, and current state
3. choose the session action, if any
4. derive a session idempotency key distinct from the async message id
5. append or submit the chosen durable session/prompt side effect
6. ack/fail/complete the async work only after that side effect is durably accepted or intentionally complete
```

Reading a mailbox or async projection does not itself authorize a prompt, required-action resolution, or session mutation. The bridge decision **SHOULD** be auditable.

---
