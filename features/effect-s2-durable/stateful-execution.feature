@product:effect-s2-durable @feature:stateful-execution @spec-only
Feature: Stateful Execution
  Keyed stateful executions (public object(...) semantics): one authoritative
  durable log per object key holding state, the exclusive-call accept-log,
  per-call journals, ingress rows, and results. Exclusive handlers are
  single-writer per key; shared handlers run concurrently read-only. Design:
  docs/sdds/effect-s2-durable-consolidation-sdd.md.

  @component:LAYERING
  Rule: Stateful execution layer

    @requirement:LAYERING.1
    Scenario: The stateful execution implementation is internal to effect-s2-durable; it is not a...
      Then the stateful-execution contract includes:
        """
        The stateful execution implementation is internal to effect-s2-durable; it
        is not a replacement for the public
        service/object/client/sendClient/attach/poll API.
        """

    @requirement:LAYERING.2
    Scenario: A stateful execution instance is one schema-addressed durable object stream plus a...
      Then the stateful-execution contract includes:
        """
        A stateful execution instance is one schema-addressed durable object stream
        plus a deterministic interpreter over that stream.
        """

    @requirement:LAYERING.3
    Scenario: effect-s2-durable owns admission, callId routing, ordered replay, per-key draining,...
      Then the stateful-execution contract includes:
        """
        effect-s2-durable owns admission, callId routing, ordered replay, per-key
        draining, shared-handler snapshots, signal ingress, completion, recovery,
        and result status.
        """

    @requirement:LAYERING.4
    Scenario: effect-s2-stream-db owns the latest-value ChangeMessage projection mechanism: StreamDb...
      Then the stateful-execution contract includes:
        """
        effect-s2-stream-db owns the latest-value ChangeMessage projection
        mechanism: StreamDb key schemas, Table schemas, primary keys, latest-value
        materialization, transactions, and compaction. Stateful execution may reuse
        that latest-value-per-key fold mechanism for the StateChanged subset of the
        ActorEvent log.
        """

    @requirement:LAYERING.5
    Scenario: S2 owns durable append order through seq_num plus stream reads, durability, and trim.
      Then the stateful-execution contract includes:
        """
        S2 owns durable append order through seq_num plus stream reads, durability,
        and trim.
        """

    @requirement:LAYERING.6
    Scenario: Engine bookkeeping (accepts/journals/signals/timers/completions/checkpoints) is one...
      Then the stateful-execution contract includes:
        """
        Engine bookkeeping (accepts/journals/signals/timers/completions/checkpoints)
        is one ordered ActorEvent log read by S2 seq_num; the latest-value table
        fold is the projection lens for user state and materialized views, not the
        source of engine-event order. This supersedes modelling engine concerns as
        several independent latest-value tables in one stream.
        """

    @requirement:LAYERING.7
    Scenario: The actor log is read and written via effect-s2.readDecoded(ActorEvent) +...
      Then the stateful-execution contract includes:
        """
        The actor log is read and written via effect-s2.readDecoded(ActorEvent) +
        S2Client.append on the stream path derived from the owner key codec.
        """

  @component:HANDLERS
  Rule: Exclusive vs shared handler behaviour

    @requirement:HANDLERS.1
    Scenario: Object handlers are exclusive by default; an exclusive handler may read and write the...
      Then the stateful-execution contract includes:
        """
        Object handlers are exclusive by default; an exclusive handler may read and
        write the object's user state.
        """

    @requirement:HANDLERS.2
    Scenario: At most one exclusive handler runs at a time per object key (single-writer).
      Then the stateful-execution contract includes:
        """
        At most one exclusive handler runs at a time per object key (single-writer).
        """

    @requirement:HANDLERS.3
    Scenario: Shared handlers are declared explicitly (e.g. object.shared) and run concurrently with...
      Then the stateful-execution contract includes:
        """
        Shared handlers are declared explicitly (e.g. object.shared) and run
        concurrently with the exclusive handler and with one another.
        """

    @requirement:HANDLERS.4
    Scenario: A shared handler must not mutate user state; this is enforced at the type level,...
      Then the stateful-execution contract includes:
        """
        A shared handler must not mutate user state; this is enforced at the type
        level, analogously to the guard that forbids durable primitives inside a run
        action.
        """

    @requirement:HANDLERS.5
    Scenario: For a shared handler, "read-only" means no user-state writes — it MAY append system...
      Then the stateful-execution contract includes:
        """
        For a shared handler, "read-only" means no user-state writes — it MAY append
        system ingress events (SignalResolved for signal/promise resolutions) to the
        object stream.
        """

  @component:ADMISSION
  Rule: Durable accept-log (mailbox) for exclusive calls

    @requirement:ADMISSION.1
    Scenario: An exclusive call is admitted by appending an Accepted ActorEvent to the owner stream;...
      Then the stateful-execution contract includes:
        """
        An exclusive call is admitted by appending an Accepted ActorEvent to the
        owner stream; admission is durable and decoupled from execution.
        """

    @requirement:ADMISSION.1-1
    Scenario: An Accepted event carries callId, method, input ; admission order is the S2 seq_num...
      Then the stateful-execution contract includes:
        """
        An Accepted event carries `{ callId, method, input }`; admission order is
        the S2 seq_num assigned to the append.
        """

    @requirement:ADMISSION.2
    Scenario: The accept-log is append-only — Accepted events are never mutated; there is no status field.
      Then the stateful-execution contract includes:
        """
        The accept-log is append-only — Accepted events are never mutated; there is
        no status field.
        """

    @requirement:ADMISSION.3
    Scenario: S2 seq_num establishes a durable total order over a key's exclusive calls; the engine...
      Then the stateful-execution contract includes:
        """
        S2 seq_num establishes a durable total order over a key's exclusive calls;
        the engine does not invent a second app-level sequence field.
        """

    @requirement:ADMISSION.4
    Scenario: Admission is idempotent by callId via a read-then-CAS-append protocol, not a StreamDb...
      Then the stateful-execution contract includes:
        """
        Admission is idempotent by callId via a read-then-CAS-append protocol, not a
        StreamDb insertOrGet (the object stream is an ActorEvent log, not a table).
        Dedup is checked against the callId IDEMPOTENCY PROJECTION folded from the
        log INCLUDING the latest Checkpointed snapshot — a pending Accepted, a
        retained Completed result (within the horizon), or an Expired watermark —
        not a literal scan for an Accepted event, since a completed call's original
        Accepted may already be trimmed under a checkpoint (CHECKPOINTING.3/.5). A
        producer CAS-appends a new Accepted (matchSeqNum) only when the callId is
        unknown to that projection; concurrent producers serialize on the CAS, and a
        loser re-reads and returns the existing/expired status rather than appending
        a duplicate.
        """

    @requirement:ADMISSION.5
    Scenario: Shared calls are never admitted to the accept-log.
      Then the stateful-execution contract includes:
        """
        Shared calls are never admitted to the accept-log.
        """

    @requirement:ADMISSION.6
    Scenario: Admission is producer-only — any producer admits a call by appending the Accepted event...
      Then the stateful-execution contract includes:
        """
        Admission is producer-only — any producer admits a call by appending the
        Accepted event to the owner stream without hosting the per-key drainer; the
        owning drainer picks it up. (The dispatch-side twin of residency-independent
        ingress; load-bearing for multi-process.)
        """

  @component:EXECUTION
  Rule: Serial drainer

    @requirement:EXECUTION.1
    Scenario: A single per-key drainer runs the pending exclusive call with the lowest S2 seq_num to...
      Then the stateful-execution contract includes:
        """
        A single per-key drainer runs the pending exclusive call with the lowest S2
        seq_num to completion (through parks) before starting the next.
        """

    @requirement:EXECUTION.2
    Scenario: A call's active invocation points its durable primitives (run/sleep/signal/state) at the...
      Then the stateful-execution contract includes:
        """
        A call's active invocation points its durable primitives
        (run/sleep/signal/state) at the owner stream, with journal facts written as
        Journaled ActorEvents keyed by callId plus a kind-specific step identity —
        the durable-primitive API is reused, not a separate StreamDb table.
        """

    @requirement:EXECUTION.3
    Scenario: A shared handler runs immediately as an ephemeral execution over a snapshot of the...
      Then the stateful-execution contract includes:
        """
        A shared handler runs immediately as an ephemeral execution over a snapshot
        of the object's materialized state; it never enters the accept-log and is
        never blocked by, nor blocks, the drainer.
        """

  @component:COMPLETION
  Rule: Done is derived, not stored

    @requirement:COMPLETION.1
    Scenario: A call settles by appending a single Completed event carrying an Exit (encoding success...
      Then the stateful-execution contract includes:
        """
        A call settles by appending a single Completed event carrying an `Exit`
        (encoding success | failure | interrupt | defect) to the object stream — not
        a mutable status row.
        """

    @requirement:COMPLETION.2
    Scenario: A call is done iff its Completed event exists; pending = an accept-log entry with no...
      Then the stateful-execution contract includes:
        """
        A call is done iff its Completed event exists; pending = an accept-log entry
        with no Completed event.
        """

    @requirement:COMPLETION.3
    Scenario: There is no dequeue step and no status mutation — "advance" is re-deriving pending, so a...
      Then the stateful-execution contract includes:
        """
        There is no dequeue step and no status mutation — "advance" is re-deriving
        pending, so a completed call cannot be re-run after a crash (window-2 is
        structurally impossible, not patched).
        """

    @requirement:COMPLETION.4
    Scenario: attach(callId) reads the result (awaiting while pending); a duplicate completed callId...
      Then the stateful-execution contract includes:
        """
        attach(callId) reads the result (awaiting while pending); a duplicate
        completed callId is served from it and never re-run.
        """

    @requirement:COMPLETION.5
    Scenario: The result/status view normalizes to Pending | Success | Failure | Interrupted | Defect...
      Then the stateful-execution contract includes:
        """
        The result/status view normalizes to Pending | Success | Failure |
        Interrupted | Defect | Expired; Expired comes from the idempotency horizon
        (CHECKPOINTING.5) and is never re-run.
        """

  @component:INGRESS
  Rule: External resolution is an append

    @requirement:INGRESS.1
    Scenario: resolveSignal(callId, name, value) appends a SignalResolved event to the owner stream...
      Then the stateful-execution contract includes:
        """
        resolveSignal(callId, name, value) appends a SignalResolved event to the
        owner stream and succeeds whether or not the target call is currently
        resident or running.
        """

    @requirement:INGRESS.2
    Scenario: An in-process waiter is poked best-effort; the durable row is the source of truth...
      Then the stateful-execution contract includes:
        """
        An in-process waiter is poked best-effort; the durable row is the source of
        truth (row-is-truth, poke-is-best-effort).
        """

  @component:ROUTING
  Rule: callId self-routes to its owner stream

    @requirement:ROUTING.1
    Scenario: A callId carries a schema-decodable owner identity; object calls route by deriving the...
      Then the stateful-execution contract includes:
        """
        A callId carries a schema-decodable owner identity; object calls route by
        deriving the owner stream path from that owner through the owner key codec
        (then reading/writing it as an ActorEvent log), while service calls resolve
        to their execution stream.
        """

    @requirement:ROUTING.2
    Scenario: attach / poll / resolveSignal derive the owner stream from the callId alone, with no...
      Then the stateful-execution contract includes:
        """
        attach / poll / resolveSignal derive the owner stream from the callId alone,
        with no roster, side index, or delimiter-parsed stream string.
        """

    @requirement:ROUTING.3
    Scenario: The callId encoding is a reversible Effect Schema codec (decode∘encode and encode∘decode...
      Then the stateful-execution contract includes:
        """
        The callId encoding is a reversible Effect Schema codec (decode∘encode and
        encode∘decode round-trip); owner recovery is a pure decode, and the owner
        becomes an S2 path segment only by encoding it through the owner key codec
        (the same codec a StreamDb derives its stream name with) — never by opening
        a StreamDb table fold or by hand-building a path string.
        """

  @component:RECOVERY
  Rule: Recover by draining pending work per key

    @requirement:RECOVERY.1
    Scenario: Boot enumerates schema-decoded object stream keys via StreamDb.list() (name-based...
      Then the stateful-execution contract includes:
        """
        Boot enumerates schema-decoded object stream keys via StreamDb.list()
        (name-based enumeration + key-codec decode; object stream contents are not
        opened as a StreamDb fold) and starts a drainer only for keys whose actor
        projection has a pending head.
        """

    @requirement:RECOVERY.2
    Scenario: Stream existence is not liveness — an object stream persists as state with an empty...
      Then the stateful-execution contract includes:
        """
        Stream existence is not liveness — an object stream persists as state with
        an empty accept-log; the pending head is the work signal.
        """

    @requirement:RECOVERY.3
    Scenario: Recovery never re-races invocations: historical records are folded into the snapshot...
      Then the stateful-execution contract includes:
        """
        Recovery never re-races invocations: historical records are folded into the
        snapshot WITHOUT executing actions (no handler is forked during replay) —
        only records appended after the recovered cursor drive interpretation, so
        replaying an old Accepted ahead of its Completed never re-runs completed
        work. The ordered accept-log plus derived-pending replays deterministically.
        """

    @requirement:RECOVERY.4
    Scenario: The snapshot's active is the DURABLE head pointer (pending[0] — the call that should be...
      Then the stateful-execution contract includes:
        """
        The snapshot's active is the DURABLE head pointer (pending[0] — the call
        that should be running), not a liveness flag; after a cold fold-only replay
        it can be Some with no resident fiber. Recovery therefore restarts the
        durable head (the lowest-seq Accepted without a Completed) whenever its
        fiber is not resident — never gating that decision on active === None — so a
        call that was mid-flight at the crash resumes; steady-state starts
        thereafter come from live-tail transition actions.
        """

  @component:PLANNING
  Rule: Deterministic planning, snapshots, and scoped work

    @requirement:PLANNING.1
    Scenario: The object drainer plans the next action from the actor-stream projection before running...
      Then the stateful-execution contract includes:
        """
        The object drainer plans the next action from the actor-stream projection
        before running external effects; recovery can re-plan from durable events
        without consulting in-memory state.
        """

    @requirement:PLANNING.2
    Scenario: Runtime state is exposed as an actor snapshot/projection at an S2 cursor; attach and...
      Then the stateful-execution contract includes:
        """
        Runtime state is exposed as an actor snapshot/projection at an S2 cursor;
        attach and poll are views over that projection.
        """

    @requirement:PLANNING.3
    Scenario: Durable engine events include schema-aware, collision-free identity so events sharing...
      Then the stateful-execution contract includes:
        """
        Durable engine events include schema-aware, collision-free identity so
        events sharing one object stream cannot collide by tag alone.
        """

    @requirement:PLANNING.4
    Scenario: Schema decode failures identify the failed boundary (accepted-input, journal, signal,...
      Then the stateful-execution contract includes:
        """
        Schema decode failures identify the failed boundary (accepted-input,
        journal, signal, timer, state, result, or checkpoint) and include
        path/callId when known.
        """

    @requirement:PLANNING.5
    Scenario: Timers, signal waits, child workflows, and spawned/background work have a durable owner...
      Then the stateful-execution contract includes:
        """
        Timers, signal waits, child workflows, and spawned/background work have a
        durable owner scope so checkpoint, trim, recovery, and rerun can determine
        whether the work is live.
        """

    @requirement:PLANNING.6
    Scenario: Completion-derived continuations are idempotent and can be re-planned from durable facts...
      Then the stateful-execution contract includes:
        """
        Completion-derived continuations are idempotent and can be re-planned from
        durable facts without double-applying.
        """

    @requirement:PLANNING.7
    Scenario: Runtime ordering, parking, resume, completion, and checkpoint-eligibility live in a PURE...
      Then the stateful-execution contract includes:
        """
        Runtime ordering, parking, resume, completion, and checkpoint-eligibility
        live in a PURE transition `(snapshot, event) -> (snapshot, action[])`; an
        interpreter executes the emitted actions only after durable facts exist. The
        transition decides validity; the interpreter never does.
        """

    @requirement:PLANNING.8
    Scenario: Because the transition is pure, the bulk of execution behaviour is testable as snapshot...
      Then the stateful-execution contract includes:
        """
        Because the transition is pure, the bulk of execution behaviour is testable
        as `snapshot + event -> snapshot + actions` without S2, timers, fibers, or
        real handlers.
        """

  @component:CHECKPOINTING
  Rule: Checkpointing, compaction, and idempotency horizon

    @requirement:CHECKPOINTING.1
    Scenario: Object streams are not garbage-collected by age retention alone — they hold permanent...
      Then the stateful-execution contract includes:
        """
        Object streams are not garbage-collected by age retention alone — they hold
        permanent user state; replay is bounded by explicit checkpoints plus S2
        trim, never by passive age/size retention on the object stream.
        """

    @requirement:CHECKPOINTING.2
    Scenario: The per-key drainer owns checkpointing and performs it only at safe boundaries (between...
      Then the stateful-execution contract includes:
        """
        The per-key drainer owns checkpointing and performs it only at safe
        boundaries (between exclusive calls, or at a cursor that preserves the
        active call) — emitted as a planned Checkpoint action, written as a
        Checkpointed event.
        """

    @requirement:CHECKPOINTING.3
    Scenario: A checkpoint covers user state, pending accepts, active per-call journals, unresolved...
      Then the stateful-execution contract includes:
        """
        A checkpoint covers user state, pending accepts, active per-call journals,
        unresolved signals/timers, retained completed-call results/idempotency
        metadata, and a completed/expired watermark.
        """

    @requirement:CHECKPOINTING.4
    Scenario: Records before a checkpoint cursor may be S2-trimmed only when their semantic state is...
      Then the stateful-execution contract includes:
        """
        Records before a checkpoint cursor may be S2-trimmed only when their
        semantic state is represented by the checkpoint.
        """

    @requirement:CHECKPOINTING.5
    Scenario: Result/idempotency retention is explicit (a horizon) — within the horizon a duplicate...
      Then the stateful-execution contract includes:
        """
        Result/idempotency retention is explicit (a horizon) — within the horizon a
        duplicate callId returns the existing/pending result; after expiry, attach
        or a duplicate callId resolves as Expired and is never re-run.
        """

    @requirement:CHECKPOINTING.6
    Scenario: A Checkpointed event whose snapshot exceeds a single S2 batch (the effect-s2-stream-db...
      Then the stateful-execution contract includes:
        """
        A Checkpointed event whose snapshot exceeds a single S2 batch (the
        effect-s2-stream-db MAX_BATCH_RECORDS limit) requires framed/chunked
        snapshots; a single-batch checkpoint is a documented v1 limit on per-object
        live footprint.
        """

    @requirement:CHECKPOINTING.7
    Scenario: Checkpoint fidelity — replaying the log forward from a Checkpointed event's cursor...
      Then the stateful-execution contract includes:
        """
        Checkpoint fidelity — replaying the log forward from a Checkpointed event's
        cursor reconstructs a snapshot equal to the one the checkpoint recorded; the
        Checkpointed event is durable before any trim below its cursor.
        """

  @component:OWNER_LOOP
  Rule: Resident owner loop and projection freshness

    @requirement:OWNER_LOOP.1
    Scenario: A resident owner maintains an ActorProjection, lastAppliedSeqNum, read cursor, local...
      Then the stateful-execution contract includes:
        """
        A resident owner maintains an ActorProjection, lastAppliedSeqNum, read
        cursor, local waiter registry, and optional lease/fence metadata derived
        from the owner stream.
        """

    @requirement:OWNER_LOOP.2
    Scenario: The hot path must not re-read the whole owner log for every primitive operation;...
      Then the stateful-execution contract includes:
        """
        The hot path must not re-read the whole owner log for every primitive
        operation; whole-log collection is allowed only for bootstrap, recovery
        scans, tests, and bounded diagnostics.
        """

    @requirement:OWNER_LOOP.3
    Scenario: The owner loop tails ordered S2 records from its cursor and applies records...
      Then the stateful-execution contract includes:
        """
        The owner loop tails ordered S2 records from its cursor and applies records
        incrementally; append acknowledgements advance durability, and the
        projection advances only after the acknowledged records are applied.
        """

    @requirement:OWNER_LOOP.4
    Scenario: A strong attach/poll/state view is served only from a projection known to be caught up...
      Then the stateful-execution contract includes:
        """
        A strong attach/poll/state view is served only from a projection known to be
        caught up to the relevant tail; non-owner or uncertain views use
        check-tail/read freshness before returning.
        """

    @requirement:OWNER_LOOP.5
    Scenario: A running handler observes its own planned state writes through a local overlay, but...
      Then the stateful-execution contract includes:
        """
        A running handler observes its own planned state writes through a local
        overlay, but external visibility and completion still wait for the
        corresponding S2 append acknowledgement and projection application.
        """

  @component:OWNER_REGISTRY
  Rule: Recoverable owner-key discovery

    @requirement:OWNER_REGISTRY.1
    Scenario: Production object recovery discovers owner keys from an append-only owner-key registry...
      Then the stateful-execution contract includes:
        """
        Production object recovery discovers owner keys from an append-only
        owner-key registry stream, not from a timing assumption that a just-created
        owner stream is immediately listable.
        """

    @requirement:OWNER_REGISTRY.2
    Scenario: The owner key is appended to the registry before the first Accepted event for that...
      Then the stateful-execution contract includes:
        """
        The owner key is appended to the registry before the first Accepted event
        for that owner; callers are acknowledged only after the owner-stream
        Accepted append is durable.
        """

    @requirement:OWNER_REGISTRY.3
    Scenario: Registry crash windows are safe: before registry append nothing was promised; after...
      Then the stateful-execution contract includes:
        """
        Registry crash windows are safe: before registry append nothing was
        promised; after registry-before-Accepted recovery sees an empty/no-pending
        owner; after Accepted the call is discoverable and recoverable.
        """

    @requirement:OWNER_REGISTRY.4
    Scenario: StreamDb.list() name enumeration may remain a temporary bootstrap/debugging fallback,...
      Then the stateful-execution contract includes:
        """
        StreamDb.list() name enumeration may remain a temporary bootstrap/debugging
        fallback, but it is not the final correctness source for production object
        recovery.
        """

  @component:LEASING
  Rule: Multi-worker ownership and fencing

    @requirement:LEASING.1
    Scenario: Multi-worker object execution is not supported unless ownership is protected by a...
      Then the stateful-execution contract includes:
        """
        Multi-worker object execution is not supported unless ownership is protected
        by a log-backed lease-renewal protocol plus S2 fencing.
        """

    @requirement:LEASING.2
    Scenario: A worker may campaign for ownership only from a caught-up projection after the prior...
      Then the stateful-execution contract includes:
        """
        A worker may campaign for ownership only from a caught-up projection after
        the prior lease has expired; lease renewal is an ActorEvent in the owner
        log, not a storage TTL.
        """

    @requirement:LEASING.3
    Scenario: Protected owner/checkpoint writes must use the active fencing token; a worker that...
      Then the stateful-execution contract includes:
        """
        Protected owner/checkpoint writes must use the active fencing token; a
        worker that cannot renew its lease or append with the expected token
        self-demotes before serving more exclusive work.
        """

    @requirement:LEASING.4
    Scenario: Fencing is cooperative: all writers participating in the protected protocol must supply...
      Then the stateful-execution contract includes:
        """
        Fencing is cooperative: all writers participating in the protected protocol
        must supply the expected token, and unfenced writes are forbidden for
        protected operations.
        """

  @component:VERSIONING
  Rule: Durable event version compatibility

    @requirement:VERSIONING.1
    Scenario: Every ActorEvent carries a producer schema/runtime version sufficient for readers to...
      Then the stateful-execution contract includes:
        """
        Every ActorEvent carries a producer schema/runtime version sufficient for
        readers to decide compatibility during replay.
        """

    @requirement:VERSIONING.2
    Scenario: A reader that encounters a newer unsupported event version must stop folding and surface...
      Then the stateful-execution contract includes:
        """
        A reader that encounters a newer unsupported event version must stop folding
        and surface an alarm/error rather than reinterpret or skip the record.
        """

    @requirement:VERSIONING.3
    Scenario: During rolling deploys, checkpointing remains compatible with the oldest still-running...
      Then the stateful-execution contract includes:
        """
        During rolling deploys, checkpointing remains compatible with the oldest
        still-running reader until every process can understand the newer event
        vocabulary.
        """

  @component:PERFORMANCE
  Rule: Append cost and batching

    @requirement:PERFORMANCE.1
    Scenario: Durable primitive writes may be coalesced into one S2 append batch within a...
      Then the stateful-execution contract includes:
        """
        Durable primitive writes may be coalesced into one S2 append batch within a
        deterministic turn, preserving record order and treating the append
        acknowledgement as the single external durability point.
        """

    @requirement:PERFORMANCE.2
    Scenario: Batching must not change replay semantics: no fact in a batch is externally durable or...
      Then the stateful-execution contract includes:
        """
        Batching must not change replay semantics: no fact in a batch is externally
        durable or externally visible until the whole append is acknowledged and
        applied.
        """

    @requirement:PERFORMANCE.3
    Scenario: Cucumber trace evidence should track append count, whole-log read count, and...
      Then the stateful-execution contract includes:
        """
        Cucumber trace evidence should track append count, whole-log read count, and
        tail/read-session usage for representative object and workflow scenarios.
        """

  @constraint:DURABLE_COMMIT
  Rule: Append acknowledgement is the commit point

    @requirement:DURABLE_COMMIT.1
    Scenario: Admission, state mutation, primitive journal, ingress, and completion are not durable...
      Then the stateful-execution contract includes:
        """
        Admission, state mutation, primitive journal, ingress, and completion are
        not durable until S2 acknowledges the append and assigns seq_num.
        """

    @requirement:DURABLE_COMMIT.2
    Scenario: The write path may coalesce same-turn durable facts into one S2 append batch; the batch...
      Then the stateful-execution contract includes:
        """
        The write path may coalesce same-turn durable facts into one S2 append
        batch; the batch preserves per-record order and none of its facts are
        externally durable until the batch is acknowledged.
        """

    @requirement:DURABLE_COMMIT.3
    Scenario: Append latency is the dominant primitive cost; reducing the per-primitive append tax is...
      Then the stateful-execution contract includes:
        """
        Append latency is the dominant primitive cost; reducing the per-primitive
        append tax is the main performance lever.
        """

  @constraint:ORDER
  Rule: S2 seq_num is authoritative order

    @requirement:ORDER.1
    Scenario: Admission, replay, wait-resolution, and checkpoint-cursor order all come from S2 seq_num.
      Then the stateful-execution contract includes:
        """
        Admission, replay, wait-resolution, and checkpoint-cursor order all come
        from S2 seq_num.
        """

    @requirement:ORDER.2
    Scenario: Do not invent an application-level sequence field to recover order from a latest-value...
      Then the stateful-execution contract includes:
        """
        Do not invent an application-level sequence field to recover order from a
        latest-value projection.
        """

  @constraint:REPLAY
  Rule: Replay is fold-only

    @requirement:REPLAY.1
    Scenario: Historical events rebuild the snapshot but do not execute actions.
      Then the stateful-execution contract includes:
        """
        Historical events rebuild the snapshot but do not execute actions.
        """

    @requirement:REPLAY.2
    Scenario: Only the recovered pending head and live-tail events are interpreted and executed.
      Then the stateful-execution contract includes:
        """
        Only the recovered pending head and live-tail events are interpreted and
        executed.
        """

  @constraint:OWNER_IDENTITY
  Rule: Owner identity is schema-owned

    @requirement:OWNER_IDENTITY.1
    Scenario: The owner stream is derived by exactly one reversible owner key codec; hand-built path...
      Then the stateful-execution contract includes:
        """
        The owner stream is derived by exactly one reversible owner key codec;
        hand-built path parsing is not part of the model.
        """

    @requirement:OWNER_IDENTITY.2
    Scenario: Object name and key cannot collide through delimiter composition.
      Then the stateful-execution contract includes:
        """
        Object name and key cannot collide through delimiter composition.
        """

    @requirement:OWNER_IDENTITY.3
    Scenario: Durable primitive and state keys at the store boundary are schema-derived typed...
      Then the stateful-execution contract includes:
        """
        Durable primitive and state keys at the store boundary are schema-derived
        typed identities or opaque encoded keys, never delimiter-composed string
        pairs.
        """

  @constraint:RECORD_KINDS
  Rule: S2 command records are stream control, not ActorEvents

    @requirement:RECORD_KINDS.1
    Scenario: Typed facts are ActorEvents decoded via readDecoded(ActorEvent).
      Then the stateful-execution contract includes:
        """
        Typed facts are ActorEvents decoded via readDecoded(ActorEvent).
        """

    @requirement:RECORD_KINDS.2
    Scenario: S2 command records such as fence and trim are stream-control directives interpreted by...
      Then the stateful-execution contract includes:
        """
        S2 command records such as fence and trim are stream-control directives
        interpreted by S2; they consume seq_num and may appear in reads.
        """

    @requirement:RECORD_KINDS.3
    Scenario: The actor read path filters or handles command records separately from typed ActorEvent...
      Then the stateful-execution contract includes:
        """
        The actor read path filters or handles command records separately from typed
        ActorEvent decoding.
        """

    @requirement:RECORD_KINDS.4
    Scenario: A trim command may be appended only after a durable Checkpointed event covers every...
      Then the stateful-execution contract includes:
        """
        A trim command may be appended only after a durable Checkpointed event
        covers every semantic fact below the trim cursor.
        """

    @requirement:RECORD_KINDS.5
    Scenario: A fence command is a stream-control fact used for cooperative ownership/checkpoint...
      Then the stateful-execution contract includes:
        """
        A fence command is a stream-control fact used for cooperative
        ownership/checkpoint protocols; it is not a substitute for a typed
        ActorEvent lease or result.
        """

  @constraint:STORAGE_BOUNDARY
  Rule: StreamDb is not a generic event log

    @requirement:STORAGE_BOUNDARY.1
    Scenario: Object owner streams must not be opened through StreamDb.open; its ChangeMessage...
      Then the stateful-execution contract includes:
        """
        Object owner streams must not be opened through StreamDb.open; its
        ChangeMessage latest-value fold would collapse admission and replay order.
        """

    @requirement:STORAGE_BOUNDARY.2
    Scenario: The actor projection may reuse the latest-value-per-key fold mechanism for the...
      Then the stateful-execution contract includes:
        """
        The actor projection may reuse the latest-value-per-key fold mechanism for
        the StateChanged subset only.
        """

    @requirement:STORAGE_BOUNDARY.3
    Scenario: When a lower layer already exposes an affordance such as pagination, stream listing, key...
      Then the stateful-execution contract includes:
        """
        When a lower layer already exposes an affordance such as pagination, stream
        listing, key decoding, or tracing, the durable layer uses it rather than
        reimplementing it.
        """

  @constraint:SYSTEM_OF_RECORD
  Rule: SYSTEM OF RECORD

    @requirement:SYSTEM_OF_RECORD.1
    Scenario: The schema-derived object stream is the single durable system of record for a key —...
      Then the stateful-execution contract includes:
        """
        The schema-derived object stream is the single durable system of record for
        a key — state, accept-log, per-call journals, ingress events, and results
        all live in it as one ActorEvent log.
        """

    @requirement:SYSTEM_OF_RECORD.2
    Scenario: In-memory state (running fibers, waiters, the drainer) is cache derived from the stream;...
      Then the stateful-execution contract includes:
        """
        In-memory state (running fibers, waiters, the drainer) is cache derived from
        the stream; it is never required for durability or ingress.
        """

    @requirement:SYSTEM_OF_RECORD.3
    Scenario: No object call uses a separate per-call execution stream or a shared roster row.
      Then the stateful-execution contract includes:
        """
        No object call uses a separate per-call execution stream or a shared roster
        row.
        """

  @constraint:LIFECYCLE
  Rule: LIFECYCLE

    @requirement:LIFECYCLE.1
    Scenario: GC of completed calls is the checkpoint+trim path (CHECKPOINTING) — a call's accept...
      Then the stateful-execution contract includes:
        """
        GC of completed calls is the checkpoint+trim path (CHECKPOINTING) — a call's
        accept entry, journal, and result are reclaimed together below the
        checkpoint cursor, so a trimmed result can never make a call appear pending
        again.
        """

    @requirement:LIFECYCLE.2
    Scenario: Object state persists until explicitly cleared; an object lifecycle (clearAll/destroy)...
      Then the stateful-execution contract includes:
        """
        Object state persists until explicitly cleared; an object lifecycle
        (clearAll/destroy) is a named follow-up.
        """

    @requirement:LIFECYCLE.3
    Scenario: Stateless services keep the ephemeral one-stream-per-call model (dropped on completion);...
      Then the stateful-execution contract includes:
        """
        Stateless services keep the ephemeral one-stream-per-call model (dropped on
        completion); the asymmetry is deliberate and matches the semantics.
        """

  @constraint:DEPENDENCIES
  Rule: DEPENDENCIES

    @requirement:DEPENDENCIES.1
    Scenario: The object engine consumes storage-primitives.ENUMERATE (StreamDb.list — name...
      Then the stateful-execution contract includes:
        """
        The object engine consumes storage-primitives.ENUMERATE (StreamDb.list —
        name enumeration ONLY: list stream names under basePath + decode through the
        key codec, never folding stream contents) and the schema-derived key codec
        (owner→path identity); it reads and writes the object stream as an ordered
        ActorEvent log via effect-s2.readDecoded(ActorEvent) + S2Client.append
        (LAYERING.6/7), and does NOT open object streams as StreamDb table folds nor
        use StreamDb.openExisting/checkpoint/compact on them — object-stream
        existence is an S2 checkTail and object-stream checkpointing is a
        Checkpointed event + S2 trim (CHECKPOINTING). Ephemeral service streams keep
        using the StreamDb ChangeMessage primitives (EXISTENCE, CHECKPOINT). A
        dedicated lower primitive StreamNamespace.list(basePath, keyCodec) may later
        replace constructing a StreamDb class purely to enumerate keys (a
        follow-up). Policy (basin, retention, namespace) is injected behind the
        DurableStore port, not hardcoded.
        """

    @requirement:DEPENDENCIES.2
    Scenario: Runtime/control-plane basins are provisioned before recovery with createStreamOnAppend...
      Then the stateful-execution contract includes:
        """
        Runtime/control-plane basins are provisioned before recovery with
        createStreamOnAppend false, using existing effect-s2 control-plane
        operations or external S2 tooling, so stream enumeration is trustworthy.
        """
