@unverified
Feature: Stateful Execution
  Keyed stateful executions (public object(...) semantics): one authoritative
  durable log per object key holding state, the exclusive-call accept-log,
  per-call journals, ingress rows, and results. Exclusive handlers are
  single-writer per key; shared handlers run concurrently read-only. Design:
  docs/sdds/effect-s2-durable-consolidation-sdd.md.

  Rule: Stateful execution layer

    Scenario: The stateful execution implementation is internal to effect-s2-durable; it is not a replacement for the public service/object/client/sendClient/attach/poll API
      Then The stateful execution implementation is internal to effect-s2-durable; it is not a replacement for the public service/object/client/sendClient/attach/poll API.

    Scenario: A stateful execution instance is one schema-addressed durable object stream plus a deterministic interpreter over that stream
      Then A stateful execution instance is one schema-addressed durable object stream plus a deterministic interpreter over that stream.

    Scenario: effect-s2-durable owns admission, callId routing, ordered replay, per-key draining, shared-handler snapshots, signal ingress, completion, recovery, and result status
      Then effect-s2-durable owns admission, callId routing, ordered replay, per-key draining, shared-handler snapshots, signal ingress, completion, recovery, and result status.

    Scenario: effect-s2-stream-db owns the latest-value ChangeMessage projection mechanism: StreamDb key schemas, Table schemas, primary keys, latest-value materialization, transactions, and compaction. Stateful execution may reuse that latest-value-per-key fold mechanism for the StateChanged subset of the ActorEvent log
      Then effect-s2-stream-db owns the latest-value ChangeMessage projection mechanism: StreamDb key schemas, Table schemas, primary keys, latest-value materialization, transactions, and compaction. Stateful execution may reuse that latest-value-per-key fold mechanism for the StateChanged subset of the ActorEvent log.

    Scenario: S2 owns durable append order through seq_num plus stream reads, durability, and trim
      Then S2 owns durable append order through seq_num plus stream reads, durability, and trim.

    Scenario: Engine bookkeeping (accepts/journals/signals/timers/completions/checkpoints) is one ordered ActorEvent log read by S2 seq_num; the latest-value table fold is the projection lens for user state and materialized views, not the source of engine-event order. This supersedes modelling engine concerns as several independent latest-value tables in one stream
      Then Engine bookkeeping (accepts/journals/signals/timers/completions/checkpoints) is one ordered ActorEvent log read by S2 seq_num; the latest-value table fold is the projection lens for user state and materialized views, not the source of engine-event order. This supersedes modelling engine concerns as several independent latest-value tables in one stream.

    Scenario: The actor log is read and written via effect-s2.readDecoded(ActorEvent) + S2Client.append on the stream path derived from the owner key codec
      Then The actor log is read and written via effect-s2.readDecoded(ActorEvent) + S2Client.append on the stream path derived from the owner key codec.

  Rule: Exclusive vs shared handler behaviour

    Scenario: Object handlers are exclusive by default; an exclusive handler may read and write the object's user state
      Then Object handlers are exclusive by default; an exclusive handler may read and write the object's user state.

    Scenario: At most one exclusive handler runs at a time per object key (single-writer)
      Then At most one exclusive handler runs at a time per object key (single-writer).

    Scenario: Shared handlers are declared explicitly (e.g. object.shared) and run concurrently with the exclusive handler and with one another
      Then Shared handlers are declared explicitly (e.g. object.shared) and run concurrently with the exclusive handler and with one another.

    Scenario: A shared handler must not mutate user state; this is enforced at the type level, analogously to the guard that forbids durable primitives inside a run action
      Then A shared handler must not mutate user state; this is enforced at the type level, analogously to the guard that forbids durable primitives inside a run action.

    Scenario: For a shared handler, "read-only" means no user-state writes — it MAY append system ingress events (SignalResolved for signal/promise resolutions) to the object stream
      Then For a shared handler, "read-only" means no user-state writes — it MAY append system ingress events (SignalResolved for signal/promise resolutions) to the object stream.

  Rule: Durable accept-log (mailbox) for exclusive calls

    Scenario: An exclusive call is admitted by appending an Accepted ActorEvent to the owner stream; admission is durable and decoupled from execution
      Then An exclusive call is admitted by appending an Accepted ActorEvent to the owner stream; admission is durable and decoupled from execution.

    Scenario: An Accepted event carries `{ callId, method, input }`; admission order is the S2 seq_num assigned to the append
      Then An Accepted event carries `{ callId, method, input }`; admission order is the S2 seq_num assigned to the append.

    Scenario: The accept-log is append-only — Accepted events are never mutated; there is no status field
      Then The accept-log is append-only — Accepted events are never mutated; there is no status field.

    Scenario: S2 seq_num establishes a durable total order over a key's exclusive calls; the engine does not invent a second app-level sequence field
      Then S2 seq_num establishes a durable total order over a key's exclusive calls; the engine does not invent a second app-level sequence field.

    Scenario: Admission is idempotent by callId via a read-then-CAS-append protocol, not a StreamDb insertOrGet (the object stream is an ActorEvent log, not a table). Dedup is checked against the callId IDEMPOTENCY PROJECTION folded from the log INCLUDING the latest Checkpointed snapshot — a pending Accepted, a retained Completed result (within the horizon), or an Expired watermark — not a literal scan for an Accepted event, since a completed call's original Accepted may already be trimmed under a checkpoint (CHECKPOINTING.3/.5). A producer CAS-appends a new Accepted (matchSeqNum) only when the callId is unknown to that projection; concurrent producers serialize on the CAS, and a loser re-reads and returns the existing/expired status rather than appending a duplicate
      Then Admission is idempotent by callId via a read-then-CAS-append protocol, not a StreamDb insertOrGet (the object stream is an ActorEvent log, not a table). Dedup is checked against the callId IDEMPOTENCY PROJECTION folded from the log INCLUDING the latest Checkpointed snapshot — a pending Accepted, a retained Completed result (within the horizon), or an Expired watermark — not a literal scan for an Accepted event, since a completed call's original Accepted may already be trimmed under a checkpoint (CHECKPOINTING.3/.5). A producer CAS-appends a new Accepted (matchSeqNum) only when the callId is unknown to that projection; concurrent producers serialize on the CAS, and a loser re-reads and returns the existing/expired status rather than appending a duplicate.

    Scenario: Shared calls are never admitted to the accept-log
      Then Shared calls are never admitted to the accept-log.

    Scenario: Admission is producer-only — any producer admits a call by appending the Accepted event to the owner stream without hosting the per-key drainer; the owning drainer picks it up. (The dispatch-side twin of residency-independent ingress; load-bearing for multi-process.)
      Then Admission is producer-only — any producer admits a call by appending the Accepted event to the owner stream without hosting the per-key drainer; the owning drainer picks it up. (The dispatch-side twin of residency-independent ingress; load-bearing for multi-process.)

  Rule: Serial drainer

    Scenario: A single per-key drainer runs the pending exclusive call with the lowest S2 seq_num to completion (through parks) before starting the next
      Then A single per-key drainer runs the pending exclusive call with the lowest S2 seq_num to completion (through parks) before starting the next.

    Scenario: A call's active invocation points its durable primitives (run/sleep/signal/state) at the owner stream, with journal facts written as Journaled ActorEvents keyed by callId plus a kind-specific step identity — the durable-primitive API is reused, not a separate StreamDb table
      Then A call's active invocation points its durable primitives (run/sleep/signal/state) at the owner stream, with journal facts written as Journaled ActorEvents keyed by callId plus a kind-specific step identity — the durable-primitive API is reused, not a separate StreamDb table.

    Scenario: A shared handler runs immediately as an ephemeral execution over a snapshot of the object's materialized state; it never enters the accept-log and is never blocked by, nor blocks, the drainer
      Then A shared handler runs immediately as an ephemeral execution over a snapshot of the object's materialized state; it never enters the accept-log and is never blocked by, nor blocks, the drainer.

  Rule: Done is derived, not stored

    Scenario: A call settles by appending a single Completed event carrying an `Exit` (encoding success | failure | interrupt | defect) to the object stream — not a mutable status row
      Then A call settles by appending a single Completed event carrying an `Exit` (encoding success | failure | interrupt | defect) to the object stream — not a mutable status row.

    Scenario: A call is done iff its Completed event exists; pending = an accept-log entry with no Completed event
      Then A call is done iff its Completed event exists; pending = an accept-log entry with no Completed event.

    Scenario: There is no dequeue step and no status mutation — "advance" is re-deriving pending, so a completed call cannot be re-run after a crash (window-2 is structurally impossible, not patched)
      Then There is no dequeue step and no status mutation — "advance" is re-deriving pending, so a completed call cannot be re-run after a crash (window-2 is structurally impossible, not patched).

    Scenario: attach(callId) reads the result (awaiting while pending); a duplicate completed callId is served from it and never re-run
      Then attach(callId) reads the result (awaiting while pending); a duplicate completed callId is served from it and never re-run.

    Scenario: The result/status view normalizes to Pending | Success | Failure | Interrupted | Defect | Expired; Expired comes from the idempotency horizon (CHECKPOINTING.5) and is never re-run
      Then The result/status view normalizes to Pending | Success | Failure | Interrupted | Defect | Expired; Expired comes from the idempotency horizon (CHECKPOINTING.5) and is never re-run.

  Rule: External resolution is an append

    Scenario: resolveSignal(callId, name, value) appends a SignalResolved event to the owner stream and succeeds whether or not the target call is currently resident or running
      Then resolveSignal(callId, name, value) appends a SignalResolved event to the owner stream and succeeds whether or not the target call is currently resident or running.

    Scenario: An in-process waiter is poked best-effort; the durable row is the source of truth (row-is-truth, poke-is-best-effort)
      Then An in-process waiter is poked best-effort; the durable row is the source of truth (row-is-truth, poke-is-best-effort).

  Rule: callId self-routes to its owner stream

    Scenario: A callId carries a schema-decodable owner identity; object calls route by deriving the owner stream path from that owner through the owner key codec (then reading/writing it as an ActorEvent log), while service calls resolve to their execution stream
      Then A callId carries a schema-decodable owner identity; object calls route by deriving the owner stream path from that owner through the owner key codec (then reading/writing it as an ActorEvent log), while service calls resolve to their execution stream.

    Scenario: attach / poll / resolveSignal derive the owner stream from the callId alone, with no roster, side index, or delimiter-parsed stream string
      Then attach / poll / resolveSignal derive the owner stream from the callId alone, with no roster, side index, or delimiter-parsed stream string.

    Scenario: The callId encoding is a reversible Effect Schema codec (decode∘encode and encode∘decode round-trip); owner recovery is a pure decode, and the owner becomes an S2 path segment only by encoding it through the owner key codec (the same codec a StreamDb derives its stream name with) — never by opening a StreamDb table fold or by hand-building a path string
      Then The callId encoding is a reversible Effect Schema codec (decode∘encode and encode∘decode round-trip); owner recovery is a pure decode, and the owner becomes an S2 path segment only by encoding it through the owner key codec (the same codec a StreamDb derives its stream name with) — never by opening a StreamDb table fold or by hand-building a path string.

  Rule: Recover by draining pending work per key

    Scenario: Boot enumerates schema-decoded object stream keys via StreamDb.list() (name-based enumeration + key-codec decode; object stream contents are not opened as a StreamDb fold) and starts a drainer only for keys whose actor projection has a pending head
      Then Boot enumerates schema-decoded object stream keys via StreamDb.list() (name-based enumeration + key-codec decode; object stream contents are not opened as a StreamDb fold) and starts a drainer only for keys whose actor projection has a pending head.

    Scenario: Stream existence is not liveness — an object stream persists as state with an empty accept-log; the pending head is the work signal
      Then Stream existence is not liveness — an object stream persists as state with an empty accept-log; the pending head is the work signal.

    Scenario: Recovery never re-races invocations: historical records are folded into the snapshot WITHOUT executing actions (no handler is forked during replay) — only records appended after the recovered cursor drive interpretation, so replaying an old Accepted ahead of its Completed never re-runs completed work. The ordered accept-log plus derived-pending replays deterministically
      Then Recovery never re-races invocations: historical records are folded into the snapshot WITHOUT executing actions (no handler is forked during replay) — only records appended after the recovered cursor drive interpretation, so replaying an old Accepted ahead of its Completed never re-runs completed work. The ordered accept-log plus derived-pending replays deterministically.

    Scenario: The snapshot's active is the DURABLE head pointer (pending[0] — the call that should be running), not a liveness flag; after a cold fold-only replay it can be Some with no resident fiber. Recovery therefore restarts the durable head (the lowest-seq Accepted without a Completed) whenever its fiber is not resident — never gating that decision on active === None — so a call that was mid-flight at the crash resumes; steady-state starts thereafter come from live-tail transition actions
      Then The snapshot's active is the DURABLE head pointer (pending[0] — the call that should be running), not a liveness flag; after a cold fold-only replay it can be Some with no resident fiber. Recovery therefore restarts the durable head (the lowest-seq Accepted without a Completed) whenever its fiber is not resident — never gating that decision on active === None — so a call that was mid-flight at the crash resumes; steady-state starts thereafter come from live-tail transition actions.

  Rule: Deterministic planning, snapshots, and scoped work

    Scenario: The object drainer plans the next action from the actor-stream projection before running external effects; recovery can re-plan from durable events without consulting in-memory state
      Then The object drainer plans the next action from the actor-stream projection before running external effects; recovery can re-plan from durable events without consulting in-memory state.

    Scenario: Runtime state is exposed as an actor snapshot/projection at an S2 cursor; attach and poll are views over that projection
      Then Runtime state is exposed as an actor snapshot/projection at an S2 cursor; attach and poll are views over that projection.

    Scenario: Durable engine events include schema-aware, collision-free identity so events sharing one object stream cannot collide by tag alone
      Then Durable engine events include schema-aware, collision-free identity so events sharing one object stream cannot collide by tag alone.

    Scenario: Schema decode failures identify the failed boundary (accepted-input, journal, signal, timer, state, result, or checkpoint) and include path/callId when known
      Then Schema decode failures identify the failed boundary (accepted-input, journal, signal, timer, state, result, or checkpoint) and include path/callId when known.

    Scenario: Timers, signal waits, child workflows, and spawned/background work have a durable owner scope so checkpoint, trim, recovery, and rerun can determine whether the work is live
      Then Timers, signal waits, child workflows, and spawned/background work have a durable owner scope so checkpoint, trim, recovery, and rerun can determine whether the work is live.

    Scenario: Completion-derived continuations are idempotent and can be re-planned from durable facts without double-applying
      Then Completion-derived continuations are idempotent and can be re-planned from durable facts without double-applying.

    Scenario: Runtime ordering, parking, resume, completion, and checkpoint-eligibility live in a PURE transition `(snapshot, event) -> (snapshot, action[])`; an interpreter executes the emitted actions only after durable facts exist. The transition decides validity; the interpreter never does
      Then Runtime ordering, parking, resume, completion, and checkpoint-eligibility live in a PURE transition `(snapshot, event) -> (snapshot, action[])`; an interpreter executes the emitted actions only after durable facts exist. The transition decides validity; the interpreter never does.

    Scenario: Because the transition is pure, the bulk of execution behaviour is testable as `snapshot + event -> snapshot + actions` without S2, timers, fibers, or real handlers
      Then Because the transition is pure, the bulk of execution behaviour is testable as `snapshot + event -> snapshot + actions` without S2, timers, fibers, or real handlers.

  Rule: Checkpointing, compaction, and idempotency horizon

    Scenario: Object streams are not garbage-collected by age retention alone — they hold permanent user state; replay is bounded by explicit checkpoints plus S2 trim, never by passive age/size retention on the object stream
      Then Object streams are not garbage-collected by age retention alone — they hold permanent user state; replay is bounded by explicit checkpoints plus S2 trim, never by passive age/size retention on the object stream.

    Scenario: The per-key drainer owns checkpointing and performs it only at safe boundaries (between exclusive calls, or at a cursor that preserves the active call) — emitted as a planned Checkpoint action, written as a Checkpointed event
      Then The per-key drainer owns checkpointing and performs it only at safe boundaries (between exclusive calls, or at a cursor that preserves the active call) — emitted as a planned Checkpoint action, written as a Checkpointed event.

    Scenario: A checkpoint covers user state, pending accepts, active per-call journals, unresolved signals/timers, retained completed-call results/idempotency metadata, and a completed/expired watermark
      Then A checkpoint covers user state, pending accepts, active per-call journals, unresolved signals/timers, retained completed-call results/idempotency metadata, and a completed/expired watermark.

    Scenario: Records before a checkpoint cursor may be S2-trimmed only when their semantic state is represented by the checkpoint
      Then Records before a checkpoint cursor may be S2-trimmed only when their semantic state is represented by the checkpoint.

    Scenario: Result/idempotency retention is explicit (a horizon) — within the horizon a duplicate callId returns the existing/pending result; after expiry, attach or a duplicate callId resolves as Expired and is never re-run
      Then Result/idempotency retention is explicit (a horizon) — within the horizon a duplicate callId returns the existing/pending result; after expiry, attach or a duplicate callId resolves as Expired and is never re-run.

    Scenario: A Checkpointed event whose snapshot exceeds a single S2 batch (the effect-s2-stream-db MAX_BATCH_RECORDS limit) requires framed/chunked snapshots; a single-batch checkpoint is a documented v1 limit on per-object live footprint
      Then A Checkpointed event whose snapshot exceeds a single S2 batch (the effect-s2-stream-db MAX_BATCH_RECORDS limit) requires framed/chunked snapshots; a single-batch checkpoint is a documented v1 limit on per-object live footprint.

    Scenario: Checkpoint fidelity — replaying the log forward from a Checkpointed event's cursor reconstructs a snapshot equal to the one the checkpoint recorded; the Checkpointed event is durable before any trim below its cursor
      Then Checkpoint fidelity — replaying the log forward from a Checkpointed event's cursor reconstructs a snapshot equal to the one the checkpoint recorded; the Checkpointed event is durable before any trim below its cursor.

  Rule: Resident owner loop and projection freshness

    Scenario: A resident owner maintains an ActorProjection, lastAppliedSeqNum, read cursor, local waiter registry, and optional lease/fence metadata derived from the owner stream
      Then A resident owner maintains an ActorProjection, lastAppliedSeqNum, read cursor, local waiter registry, and optional lease/fence metadata derived from the owner stream.

    Scenario: The hot path must not re-read the whole owner log for every primitive operation; whole-log collection is allowed only for bootstrap, recovery scans, tests, and bounded diagnostics
      Then The hot path must not re-read the whole owner log for every primitive operation; whole-log collection is allowed only for bootstrap, recovery scans, tests, and bounded diagnostics.

    Scenario: The owner loop tails ordered S2 records from its cursor and applies records incrementally; append acknowledgements advance durability, and the projection advances only after the acknowledged records are applied
      Then The owner loop tails ordered S2 records from its cursor and applies records incrementally; append acknowledgements advance durability, and the projection advances only after the acknowledged records are applied.

    Scenario: A strong attach/poll/state view is served only from a projection known to be caught up to the relevant tail; non-owner or uncertain views use check-tail/read freshness before returning
      Then A strong attach/poll/state view is served only from a projection known to be caught up to the relevant tail; non-owner or uncertain views use check-tail/read freshness before returning.

    Scenario: A running handler observes its own planned state writes through a local overlay, but external visibility and completion still wait for the corresponding S2 append acknowledgement and projection application
      Then A running handler observes its own planned state writes through a local overlay, but external visibility and completion still wait for the corresponding S2 append acknowledgement and projection application.

  Rule: Recoverable owner-key discovery

    Scenario: Production object recovery discovers owner keys from an append-only owner-key registry stream, not from a timing assumption that a just-created owner stream is immediately listable
      Then Production object recovery discovers owner keys from an append-only owner-key registry stream, not from a timing assumption that a just-created owner stream is immediately listable.

    Scenario: The owner key is appended to the registry before the first Accepted event for that owner; callers are acknowledged only after the owner-stream Accepted append is durable
      Then The owner key is appended to the registry before the first Accepted event for that owner; callers are acknowledged only after the owner-stream Accepted append is durable.

    Scenario: Registry crash windows are safe: before registry append nothing was promised; after registry-before-Accepted recovery sees an empty/no-pending owner; after Accepted the call is discoverable and recoverable
      Then Registry crash windows are safe: before registry append nothing was promised; after registry-before-Accepted recovery sees an empty/no-pending owner; after Accepted the call is discoverable and recoverable.

    Scenario: StreamDb.list() name enumeration may remain a temporary bootstrap/debugging fallback, but it is not the final correctness source for production object recovery
      Then StreamDb.list() name enumeration may remain a temporary bootstrap/debugging fallback, but it is not the final correctness source for production object recovery.

  Rule: Multi-worker ownership and fencing

    Scenario: Multi-worker object execution is not supported unless ownership is protected by a log-backed lease-renewal protocol plus S2 fencing
      Then Multi-worker object execution is not supported unless ownership is protected by a log-backed lease-renewal protocol plus S2 fencing.

    Scenario: A worker may campaign for ownership only from a caught-up projection after the prior lease has expired; lease renewal is an ActorEvent in the owner log, not a storage TTL
      Then A worker may campaign for ownership only from a caught-up projection after the prior lease has expired; lease renewal is an ActorEvent in the owner log, not a storage TTL.

    Scenario: Protected owner/checkpoint writes must use the active fencing token; a worker that cannot renew its lease or append with the expected token self-demotes before serving more exclusive work
      Then Protected owner/checkpoint writes must use the active fencing token; a worker that cannot renew its lease or append with the expected token self-demotes before serving more exclusive work.

    Scenario: Fencing is cooperative: all writers participating in the protected protocol must supply the expected token, and unfenced writes are forbidden for protected operations
      Then Fencing is cooperative: all writers participating in the protected protocol must supply the expected token, and unfenced writes are forbidden for protected operations.

  Rule: Durable event version compatibility

    Scenario: Every ActorEvent carries a producer schema/runtime version sufficient for readers to decide compatibility during replay
      Then Every ActorEvent carries a producer schema/runtime version sufficient for readers to decide compatibility during replay.

    Scenario: A reader that encounters a newer unsupported event version must stop folding and surface an alarm/error rather than reinterpret or skip the record
      Then A reader that encounters a newer unsupported event version must stop folding and surface an alarm/error rather than reinterpret or skip the record.

    Scenario: During rolling deploys, checkpointing remains compatible with the oldest still-running reader until every process can understand the newer event vocabulary
      Then During rolling deploys, checkpointing remains compatible with the oldest still-running reader until every process can understand the newer event vocabulary.

  Rule: Append cost and batching

    Scenario: Durable primitive writes may be coalesced into one S2 append batch within a deterministic turn, preserving record order and treating the append acknowledgement as the single external durability point
      Then Durable primitive writes may be coalesced into one S2 append batch within a deterministic turn, preserving record order and treating the append acknowledgement as the single external durability point.

    Scenario: Batching must not change replay semantics: no fact in a batch is externally durable or externally visible until the whole append is acknowledged and applied
      Then Batching must not change replay semantics: no fact in a batch is externally durable or externally visible until the whole append is acknowledged and applied.

    Scenario: Cucumber trace evidence should track append count, whole-log read count, and tail/read-session usage for representative object and workflow scenarios
      Then Cucumber trace evidence should track append count, whole-log read count, and tail/read-session usage for representative object and workflow scenarios.

  Rule: Append acknowledgement is the commit point

    Scenario: Admission, state mutation, primitive journal, ingress, and completion are not durable until S2 acknowledges the append and assigns seq_num
      Then Admission, state mutation, primitive journal, ingress, and completion are not durable until S2 acknowledges the append and assigns seq_num.

    Scenario: The write path may coalesce same-turn durable facts into one S2 append batch; the batch preserves per-record order and none of its facts are externally durable until the batch is acknowledged
      Then The write path may coalesce same-turn durable facts into one S2 append batch; the batch preserves per-record order and none of its facts are externally durable until the batch is acknowledged.

    Scenario: Append latency is the dominant primitive cost; reducing the per-primitive append tax is the main performance lever
      Then Append latency is the dominant primitive cost; reducing the per-primitive append tax is the main performance lever.

  Rule: S2 seq_num is authoritative order

    Scenario: Admission, replay, wait-resolution, and checkpoint-cursor order all come from S2 seq_num
      Then Admission, replay, wait-resolution, and checkpoint-cursor order all come from S2 seq_num.

    Scenario: Do not invent an application-level sequence field to recover order from a latest-value projection
      Then Do not invent an application-level sequence field to recover order from a latest-value projection.

  Rule: Replay is fold-only

    Scenario: Historical events rebuild the snapshot but do not execute actions
      Then Historical events rebuild the snapshot but do not execute actions.

    Scenario: Only the recovered pending head and live-tail events are interpreted and executed
      Then Only the recovered pending head and live-tail events are interpreted and executed.

  Rule: Owner identity is schema-owned

    Scenario: The owner stream is derived by exactly one reversible owner key codec; hand-built path parsing is not part of the model
      Then The owner stream is derived by exactly one reversible owner key codec; hand-built path parsing is not part of the model.

    Scenario: Object name and key cannot collide through delimiter composition
      Then Object name and key cannot collide through delimiter composition.

    Scenario: Durable primitive and state keys at the store boundary are schema-derived typed identities or opaque encoded keys, never delimiter-composed string pairs
      Then Durable primitive and state keys at the store boundary are schema-derived typed identities or opaque encoded keys, never delimiter-composed string pairs.

  Rule: S2 command records are stream control, not ActorEvents

    Scenario: Typed facts are ActorEvents decoded via readDecoded(ActorEvent)
      Then Typed facts are ActorEvents decoded via readDecoded(ActorEvent).

    Scenario: S2 command records such as fence and trim are stream-control directives interpreted by S2; they consume seq_num and may appear in reads
      Then S2 command records such as fence and trim are stream-control directives interpreted by S2; they consume seq_num and may appear in reads.

    Scenario: The actor read path filters or handles command records separately from typed ActorEvent decoding
      Then The actor read path filters or handles command records separately from typed ActorEvent decoding.

    Scenario: A trim command may be appended only after a durable Checkpointed event covers every semantic fact below the trim cursor
      Then A trim command may be appended only after a durable Checkpointed event covers every semantic fact below the trim cursor.

    Scenario: A fence command is a stream-control fact used for cooperative ownership/checkpoint protocols; it is not a substitute for a typed ActorEvent lease or result
      Then A fence command is a stream-control fact used for cooperative ownership/checkpoint protocols; it is not a substitute for a typed ActorEvent lease or result.

  Rule: StreamDb is not a generic event log

    Scenario: Object owner streams must not be opened through StreamDb.open; its ChangeMessage latest-value fold would collapse admission and replay order
      Then Object owner streams must not be opened through StreamDb.open; its ChangeMessage latest-value fold would collapse admission and replay order.

    Scenario: The actor projection may reuse the latest-value-per-key fold mechanism for the StateChanged subset only
      Then The actor projection may reuse the latest-value-per-key fold mechanism for the StateChanged subset only.

    Scenario: When a lower layer already exposes an affordance such as pagination, stream listing, key decoding, or tracing, the durable layer uses it rather than reimplementing it
      Then When a lower layer already exposes an affordance such as pagination, stream listing, key decoding, or tracing, the durable layer uses it rather than reimplementing it.

  Rule: SYSTEM OF RECORD

    Scenario: The schema-derived object stream is the single durable system of record for a key — state, accept-log, per-call journals, ingress events, and results all live in it as one ActorEvent log
      Then The schema-derived object stream is the single durable system of record for a key — state, accept-log, per-call journals, ingress events, and results all live in it as one ActorEvent log.

    Scenario: In-memory state (running fibers, waiters, the drainer) is cache derived from the stream; it is never required for durability or ingress
      Then In-memory state (running fibers, waiters, the drainer) is cache derived from the stream; it is never required for durability or ingress.

    Scenario: No object call uses a separate per-call execution stream or a shared roster row
      Then No object call uses a separate per-call execution stream or a shared roster row.

  Rule: LIFECYCLE

    Scenario: GC of completed calls is the checkpoint+trim path (CHECKPOINTING) — a call's accept entry, journal, and result are reclaimed together below the checkpoint cursor, so a trimmed result can never make a call appear pending again
      Then GC of completed calls is the checkpoint+trim path (CHECKPOINTING) — a call's accept entry, journal, and result are reclaimed together below the checkpoint cursor, so a trimmed result can never make a call appear pending again.

    Scenario: Object state persists until explicitly cleared; an object lifecycle (clearAll/destroy) is a named follow-up
      Then Object state persists until explicitly cleared; an object lifecycle (clearAll/destroy) is a named follow-up.

    Scenario: Stateless services keep the ephemeral one-stream-per-call model (dropped on completion); the asymmetry is deliberate and matches the semantics
      Then Stateless services keep the ephemeral one-stream-per-call model (dropped on completion); the asymmetry is deliberate and matches the semantics.

  Rule: DEPENDENCIES

    Scenario: The object engine consumes storage-primitives.ENUMERATE (StreamDb.list — name enumeration ONLY: list stream names under basePath + decode through the key codec, never folding stream contents) and the schema-derived key codec (owner→path identity); it reads and writes the object stream as an ordered ActorEvent log via effect-s2.readDecoded(ActorEvent) + S2Client.append (LAYERING.6/7), and does NOT open object streams as StreamDb table folds nor use StreamDb.openExisting/checkpoint/compact on them — object-stream existence is an S2 checkTail and object-stream checkpointing is a Checkpointed event + S2 trim (CHECKPOINTING). Ephemeral service streams keep using the StreamDb ChangeMessage primitives (EXISTENCE, CHECKPOINT). A dedicated lower primitive StreamNamespace.list(basePath, keyCodec) may later replace constructing a StreamDb class purely to enumerate keys (a follow-up). Policy (basin, retention, namespace) is injected behind the DurableStore port, not hardcoded
      Then The object engine consumes storage-primitives.ENUMERATE (StreamDb.list — name enumeration ONLY: list stream names under basePath + decode through the key codec, never folding stream contents) and the schema-derived key codec (owner→path identity); it reads and writes the object stream as an ordered ActorEvent log via effect-s2.readDecoded(ActorEvent) + S2Client.append (LAYERING.6/7), and does NOT open object streams as StreamDb table folds nor use StreamDb.openExisting/checkpoint/compact on them — object-stream existence is an S2 checkTail and object-stream checkpointing is a Checkpointed event + S2 trim (CHECKPOINTING). Ephemeral service streams keep using the StreamDb ChangeMessage primitives (EXISTENCE, CHECKPOINT). A dedicated lower primitive StreamNamespace.list(basePath, keyCodec) may later replace constructing a StreamDb class purely to enumerate keys (a follow-up). Policy (basin, retention, namespace) is injected behind the DurableStore port, not hardcoded.

    Scenario: Runtime/control-plane basins are provisioned before recovery with createStreamOnAppend false, using existing effect-s2 control-plane operations or external S2 tooling, so stream enumeration is trustworthy
      Then Runtime/control-plane basins are provisioned before recovery with createStreamOnAppend false, using existing effect-s2 control-plane operations or external S2 tooling, so stream enumeration is trustworthy.
