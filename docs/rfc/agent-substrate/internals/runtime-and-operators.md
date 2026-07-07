# 12. Runtime Model

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

The runtime owns live resources and side effects.

A runtime SHOULD provide:

```txt
operator execution
provider provisioning
agent transport adapters
session ownership
protocol/conductor handling
projection emission
resource cleanup
```

A runtime MUST distinguish:

```txt
durable facts
live resources
```

A runtime MAY be implemented as:

```txt
single process
multi-process host
distributed workers
serverless functions
browser worker
local CLI
containerized service
```

The language does not matter.

## 12.1 Runtime Responsibilities

A conforming runtime **MUST** provide or delegate:

```txt
log connection and append authority for runtime-owned records
projection subscriptions needed by operators
operator supervision
adapter registry
provider registry
live session/resource registry
shutdown and cleanup handling
typed durable failure emission
```

The runtime **MUST** keep durable identity separate from live ownership. A live registry entry MAY prove ownership only inside the runtime process or cluster that maintains it. A durable row MAY record that ownership was established at a point in time, but it does not prove the current process still owns the handle after restart, failover, or lease expiry.

## 12.2 Runtime Interface Contracts

`Runtime.boot(config)` **MUST** initialize enough infrastructure to append runtime lifecycle records before it starts executing claimed side effects. `Runtime.shutdown(reason)` **SHOULD** append shutdown or stopped facts for resources it owns when doing so is reliable.

`Runtime.own_session(session_id)` **MUST** return one of:

```txt
owned_promptable
owned_not_promptable
not_owned
unknown
```

Only `owned_promptable` permits prompt dispatch without reattach/reload. `unknown` **MUST NOT** be treated as success.

`Runtime.reattach(session_id, options)` **MAY** use protocol load, provider lookup, session migration, or another documented mechanism. It **MUST** append or expose a typed failure if reattach is unsupported or fails.

## 12.3 Runtime Invariants

A runtime **MUST** satisfy these invariants:

```txt
No ready session fact before live promptability is proven.
No prompt side effect before the prompt intent is durable.
No externally visible side effect during replay.
No production prompt dispatch through adapter bypass paths outside an explicitly isolated compatibility surface.
No provider handle treated as durable state.
```

If a runtime is distributed, it **MUST** define how owners are fenced. Fencing MAY be implemented with durable claims, leases, generation numbers, exclusive provider handles, or another documented mechanism.

---

# 13. Operator Model

Operators consume log records.

In the managed-agent framing, operators are the stream-first machinery around Orchestration and Harness. They wake work, claim work, fold projections, and append terminal facts; they do not replace the Harness primitive itself.

There are two broad operator classes:

```txt
Projection operators
Claimed work operators
```

## 13.1 Projection Operators

Projection operators derive read models.

They SHOULD be deterministic and replayable.

They SHOULD NOT perform externally visible side effects.

Example:

```txt
prompt.chunk records -> chunks projection
session.created records -> sessions projection
permission.requested/resolved records -> permissions projection
```

Projection operators **MUST** persist or expose their processed cursor. On restart, they **MUST** resume from a durable cursor or rebuild from a known earlier cursor. If they process a record more than once, the fold **MUST** be idempotent or deterministic so the materialized row is unchanged.

## 13.2 Claimed Work Operators

Claimed work operators perform side effects.

A claimed work operator **MUST** follow this lifecycle:

```txt
1. replay historical records and fold state
2. wait until live tail
3. identify eligible unclaimed work
4. append durable claim
5. observe claim ownership
6. execute only if ownership is established
7. append terminal success/failure records
```

During replay, claimed work operators **MUST NOT** perform side effects.

The shared claimed-work machinery owns mechanics only:

```txt
replay/live barrier
claim append
claim observation
owner evaluation
execute-owned scheduling
duplicate suppression
heartbeat or lease observation, if used
supervision hooks
```

Domain operators own semantics:

```txt
eligibility rules
claim record schema
terminal record schema
side-effect implementation
dead-owner policy
error mapping
retry policy
```

This split prevents a generic operator framework from becoming a domain-specific workflow engine.

## 13.3 Durable Claims

Durable claim records **MUST** include:

```txt
work_key
claim_id
owner_id
claimed_at
attempt_number
```

They **MUST** include `lease` or `heartbeat` fields if the domain uses leases, heartbeats, or owner expiry. They **SHOULD** include the source cursor or causation id for the work record that made the claim eligible.

Example:

```json
{
  "type": "agent.prompt.claimed",
  "key": "session:s1:request:r1",
  "value": {
    "sessionId": "s1",
    "requestId": "r1",
    "claimId": "claim-1",
    "ownerId": "runtime-a",
    "claimedAt": 1234567890,
    "attemptNumber": 1
  }
}
```

Claim semantics are:

```txt
first valid active claim wins for a work key
duplicate claim by the winning owner is idempotent
conflicting active claim blocks execution by other owners
expired-owner handling is domain-specific
```

A valid claim is one that passes schema validation, references eligible work, and is not fenced by an earlier active claim. A duplicate owner claim **SHOULD** return or project the existing winning claim rather than creating another active owner. A conflicting active claim **MUST** prevent the later claimant from performing side effects unless domain policy first marks the earlier owner expired, failed, or superseded.

Claim records **SHOULD** carry either a lease/heartbeat model or an explicit no-lease policy. If leases are used:

```txt
lease duration MUST be durable or derivable from durable policy
heartbeat records MUST identify claim id, owner id, and observed time
owner expiry MUST include clock-skew tolerance
takeover MUST append a durable takeover or replacement claim
```

If leases are not used, the domain **MUST** specify whether claimed work can be retried after owner death and what record authorizes retry.

Durable claims are not a substrate-level lease API by default. A generic claim helper MAY exist only if it operates over explicit domain claim rows and preserves row visibility.

## 13.4 Dead Owners

A system MAY define dead-owner handling. If it does, dead-owner policy **MUST** be explicit per domain and **MUST** preserve the first-valid-claim and first-valid-terminal rules.

Acceptable policy families are:

| Policy | Contract | Invariants |
| --- | --- | --- |
| Fail | Append durable failure for the work after owner death is proven. | Failure is terminal; later takeover cannot execute the same work unless the domain appends new work. |
| Takeover | Append durable takeover/replacement claim after expiry or fencing. | New owner must observe its own winning takeover claim before side effects. |
| Manual intervention | Project blocked/requires-operator state. | No automated side effect occurs until an authorized intervention record appears. |
| Reattach and resume | Recover the same live resource or equivalent owner context. | Reattach proof must be durable or derivable; stale handles are not accepted as proof. |

Skipping forever is allowed only when the domain documents that abandoned claimed work is intentionally non-retryable and exposes that state for audit or operations.

The generic operator framework SHOULD NOT own domain-specific dead-owner policy. It may provide mechanics for lease observation, takeover claims, and blocked-state projection, but the domain owns which policy applies and when.

## 13.5 Replay and Fencing Invariants

Claimed-work operators **MUST** satisfy:

```txt
Replay mode never appends claims, executes side effects, or writes terminal records.
Live mode evaluates only work visible at or after the live cursor, plus explicitly eligible recovered work.
Execution starts only after the operator observes its own winning claim through the log or a projection derived from the log.
Terminal success/failure records include enough identity to match work key and claim id.
If execution is cancelled after a claim, terminal state or retry eligibility is domain-defined and durable.
```

For multi-worker externally visible side effects, durable claim records are REQUIRED. Single-owner idempotent completion flows MAY skip durable claims if duplicate execution cannot create externally visible inconsistency and the domain documents that property.

---
