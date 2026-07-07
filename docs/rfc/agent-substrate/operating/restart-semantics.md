# 25. Restart Semantics

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A conforming system MUST define what survives restart.

## 25.1 Survives Restart

```txt
durable log records
projection-rebuildable state
idempotency facts
durable claims
durable waits
durable completions
audit records
```

## 25.2 Does Not Survive Restart Unless Reattached

```txt
open protocol sessions
stdio child processes
websocket connections
in-memory promises
in-flight fibers/tasks
provider handles
conductor connections
local queues
```

## 25.3 Restart Rule

After restart, the runtime MUST replay the log to reconstruct durable state and MUST NOT assume it still owns any live resource unless it explicitly reacquires or reattaches that resource.

## 25.4 Replay Ordering

Restart replay **MUST** process records in log order for each stream. If the system uses multiple streams, it **MUST** define the ordering boundary used for each recovered subsystem. For example, prompt recovery may require session, prompt, claim, and chunk records to be folded to a consistent cursor before promptability decisions resume.

Operators **MUST** finish replay to their live boundary before executing new side effects for replayed work. They MAY accept new intents while replaying only if those intents are queued durably and not executed until the operator reaches a safe live boundary.

## 25.5 Owner Reacquisition

After restart, owners of claimed work and live sessions **MUST** be reacquired explicitly.

Claimed work recovery:

```txt
replay work, claims, heartbeats, and terminal records
classify each claimed item as terminal, active-owned, expired, or blocked
apply domain dead-owner policy
append takeover/retry/failure records before re-executing side effects
```

Live session recovery:

```txt
replay session lifecycle
attempt reattach/load if supported
otherwise classify not_live or reprovision according to domain policy
block prompt dispatch until classification is promptable
```

The runtime **MUST NOT** use the mere presence of a durable session row, claim row, or provider row as proof that the current process owns the corresponding live resource.

## 25.6 Restart With In-Flight Suspensions

When a runtime dies while a permission wait, prompt wait, tool wait, timer wait, or other awaitable suspension is pending, recovery **MUST** make a durable decision. The recovery protocol is:

```txt
1. replay durable log from the last known cursor or from the retained start
2. identify pending suspensions from projections: approvals, prompts, tools, timers, awaitables
3. for each suspension, apply the adapter's declared reattach profile
4. append a durable terminal or reattach/recovered record
5. resolve waiters through the snapshot-first projection contract
6. apply first-valid-terminal-wins for duplicate or racing terminal records
7. project pending, recovered, terminal, orphaned, timeout, and cancelled states distinctly
```

The adapter reattach profile controls the recovery action:

| Reattach profile | Recovery action for pending suspension |
| --- | --- |
| `no_reattach_must_fail` | Append a durable terminal record, normally `cancelled` or `failed` with reason `runtime_restart_no_reattach`. |
| `load_via_protocol` | Attempt protocol-level re-bind. On success, append or project recovered ownership and resume via projection observation. On failure, append terminal failure. |
| `reprovision_replacement` | Spawn or bind a replacement resource, replay documented context, and continue only if semantic equivalence of that context is declared. Otherwise terminalize. |
| `supervised_reattach` | Let the supervisor re-bind out of band, then append or expose durable proof of recovered ownership before any waiter resumes. |

A runtime **MUST NOT** silently treat a pending suspension as still live after restart unless the adapter's declared reattach profile permits that recovery and the runtime has durable or derivable proof that recovery succeeded.

A runtime **MUST** append a durable terminal record or a durable reattach/recovered record for every pending suspension before considering restart recovery complete. Suspensions cannot remain pending across restarts indefinitely without a recovery decision. Transitional orphaned, timeout, or cancelled rows are acceptable when a final schema is not yet available, as long as they are durable, projected, and terminal under §10.7.

The reattach profile **MUST** be declared at adapter registration. The runtime **MUST NOT** infer reattach capability dynamically from surviving in-memory continuation state.

---
