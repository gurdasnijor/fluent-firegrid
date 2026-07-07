# 10. Projection Model

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A projection is a queryable view derived from the durable log.

The projection plane **MUST NOT** be treated as the source of truth. The source of truth is the log.

Log durability and queryability are separate concerns. A persistent log backend makes facts survive restart; SQL, search, archive, and analytics stores are projection consumers. Query sinks **MUST NOT** become alternate truth, and their schema evolution, indexing lag, or backpressure **MUST NOT** alter log authority or append ordering.

A projection system SHOULD support:

```txt
snapshot
subscribe
rebuild
collection query
incremental update
schema validation
```

The stream-derived observation model uses exactly this shape: append durable state, materialize into live collections, and subscribe to those collections.

## 10.1 Standard Projection Families

A stream-first managed-agent implementation SHOULD provide projections for:

```txt
sessions
prompts / turns
chunks / updates
permissions / required actions
runtimes
resources
claims
awaitables
timers
launches / runs
```

## 10.2 Projection Consistency

A projection **SHOULD** be eventually consistent with the log.

A projection **MAY** be strongly consistent within one process if reads and writes share a transaction boundary.

A projection consumer **MUST NOT** assume a projection row exists before the corresponding log record is durably appended.

## 10.3 Rebuildability Contract

Every projection family **MUST** declare:

```txt
source streams or record types
fold function version
schema version
retention assumptions
ordering assumptions
snapshot cursor semantics
```

Given all retained source records and the same fold version, a rebuild **MUST** produce the same logical rows. If a projection depends on non-retained records, an implementation **MUST** either fail rebuild with a retention-gap error or document that the projection is only partially rebuildable.

A projection that claims bounded replay across trimmed logs **MUST** persist
snapshot/index state keyed by semantic operation ids, not stream offsets; those
indexes are derived projection state, not alternate log truth.

Projection migrations **MUST** be explicit. A new fold version MAY rebuild into a new projection namespace, backfill in place behind a version marker, or dual-write derived rows during migration. It **MUST NOT** silently reinterpret old rows under a new schema without a declared migration rule.

## 10.4 Snapshot and Subscription Contract

A projection-backed wait **MUST** be snapshot-first:

```txt
1. read a snapshot at a known projection/log cursor
2. evaluate the wait predicate against the snapshot
3. if not satisfied, subscribe to changes after that cursor
4. evaluate each change in projection order
5. unsubscribe on completion, cancellation, timeout, or caller interruption
```

The snapshot prevents missing rows that were committed before subscription started. The subscription prevents polling. The projection remains a cache over the log; if the projection and log disagree, the log is authoritative.

Every stream-first wait operation **MUST** expose or internally preserve the cursor used for the snapshot and **MUST** subscribe strictly after that cursor. If a projection API cannot provide a stable snapshot cursor, it is not sufficient for restart-safe waits unless the implementation can prove an equivalent no-gap subscription boundary.

Projection subscriptions **MUST** preserve per-row causal order for changes derived from a single stream. If the projection merges multiple streams, it **MUST** document the merge order and whether consumers can rely on it. A subscription event **SHOULD** include:

```txt
projection name
row key
change kind
new row value or tombstone
source record cursor
projection cursor
```

Prompt-scoped and required-action-scoped subscriptions **MUST** filter by the logical operation identity before emitting changes to a caller. They **MUST NOT** replay rows that were already materialized before the subscription cursor, and they **MUST NOT** deliver chunks or required-action updates from a different prompt/session merely because those rows share the same underlying stream. Snapshot-first terminal reads remain separate from live scoped update streams.

## 10.5 Row-Family Ownership

Conforming implementations **SHOULD** document canonical writers for each row family. The following ownership table is the default for a stream-first managed-agent substrate:

| Row family | Canonical writer | May observe | Notes |
| --- | --- | --- | --- |
| Prompt intent | Client or trusted application gateway | Runtime, prompt operator, dashboards | Appended before any prompt side effect. |
| Prompt claim | Prompt operator | Clients, runtimes, audit, supervisors | First valid claim wins for a work key. |
| Prompt chunks / updates | Protocol adapter or session runtime that receives agent updates | Clients, projections, audit | Chunks are durable rows, not transient UI messages. |
| Prompt terminal state | Prompt operator, using adapter result | Clients, projections, audit | Exactly one terminal state SHOULD win for a prompt request. |
| Permission requested | Approval gate or required-action middleware | Approvers, clients, runtime waiters | Represents pending required action. |
| Permission resolved | Authorized approver or client API | Approval gate, prompt/tool waiter, audit | Resolution is durable and idempotent by permission id. |
| Session rows | Runtime/session operator | Clients, adapters, providers, audit | Ready session rows require live promptability proof unless marked historical/imported. |
| Runtime rows | Runtime supervisor | Clients, operators, audit | Describe runtime lifecycle and ownership, not arbitrary process memory. |
| Provider/resource rows | Provider operator or runtime provider service | Runtime, clients, audit | Describe provisioned facts; handles remain live resources. |

Multiple components MAY contribute records to one row family only when their ownership boundaries are explicit. No component may claim sole ownership of a row family while another production path also writes it.

## 10.6 Durable Channels

A durable channel is a typed communication path represented by durable records plus projection/wait semantics. This RFC defines two general channel modes over durable logs:

```txt
sync channel: ask and wait, backed by a durable completion handle
async channel: insert and move on, backed by state insert plus state.changes wait
```

These modes do not require implementation-specific channel names or storage paths.

Sync channels are used when a caller needs the answer before continuing:

```txt
spawn child agent and await completion
call sandbox/tool and await result
request approval and await decision
```

The sync path **SHOULD** be represented as a durable wait with a completion key. It inherits the durable promise invariants in §20.

Async channels are used when a sender can enqueue work or publish a signal and continue:

```txt
state.insert(mailbox-like row)
state.insert(event row)
wait_for(state.changes(query).onInsert)
```

The async path **MUST** be expressed as durable state or event records plus projection subscriptions. It **MUST NOT** require an in-memory mailbox to preserve messages across restart.

Channel records **SHOULD** declare:

```txt
channel name or type
semantic key
sender identity
receiver identity or query predicate, if applicable
payload schema
ordering scope
completion key, for sync channels
```

Typed event channels are the natural home for permission requests, child-session spawn events, timer events, choreography traces, and durable subscriber completions. A channel is not a new source of truth; it is a typed view over the durable log plus the projection/wait contract.

## 10.7 First Terminal Wins Semantics

For any durable operation that can end once, multiple terminal records can land because of retries, duplicate producers, restarts, or races. Projection and waiter semantics **MUST** use first-valid-terminal-wins within the operation's documented ordering boundary:

```txt
first valid terminal record in log order wins
later identical terminal records are idempotent duplicates
later conflicting terminal records are ignored for state but recorded or surfaced as conflicts
invalid terminal records do not resolve the wait
```

For these terminal-bearing domains, operations **MUST** use logical keys equivalent to:

```txt
prompt:<sessionId>:<requestId>
permission:<permissionId>
tool:<sessionId>:<toolCallId>
timer:<timerId>
```

For example, if the log contains `permission.pending`, then `permission.resolved denied`, then `permission.resolved approved` for the same permission key, the projection remains denied because denied was the first valid terminal in append order. The later approved record is retained for audit and conflict reporting but does not flip state.

The winning-terminal rule applies to prompt completion, permission resolution, tool-call completion, timer firing/cancellation, and durable promise completion unless a domain explicitly defines a stricter terminal policy. It is independent of timestamp fields; append order or the documented projection cursor is authoritative.

---
