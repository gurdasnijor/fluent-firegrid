@product:effect-s2-stream-db @feature:storage-primitives
Feature: Storage primitives

  Scenario: checkpoint snapshots the live set and reopens from the compacted stream
    Given an open storage db with infinite retention at key "cart"
    When I insert item "a" value 1
    And I upsert item "a" value 2
    And I insert item "b" value 3
    And I delete item "a"
    And I checkpoint
    Then reopening, item "a" is absent
    And reopening, item "b" is 3
    And the trace should satisfy:
      """
      SELECT
        countIf(SpanName = 'effect-s2-stream-db.checkpoint') > 0
        AND countIf(SpanName = 'S2.append') > 0
        AND countIf(SpanName = 'S2.readBatch') > 0 AS ok
      FROM otel_traces
      WHERE TraceId IN (
        SELECT TraceId
        FROM otel_traces
        WHERE SpanAttributes['firegrid.scenario.id'] = {scenario_id:String}
      )
      """

  # Non-executable requirement inventory.
  # Scenarios stay @spec-only until step definitions make them executable.

  @component:TABLES
  Rule: Schema-owned table definitions

    @spec-only @requirement:TABLES.1
    Scenario: Table(name)(fields) derives tableName, row schema, and primary-key field from the Effect...
      Then the storage-primitives contract includes:
        """
        Table(name)(fields) derives tableName, row schema, and primary-key field
        from the Effect Schema definition.
        """

    @spec-only @requirement:TABLES.2
    Scenario: A primary key field is declared by annotating a field schema with primaryKey; defining a...
      Then the storage-primitives contract includes:
        """
        A primary key field is declared by annotating a field schema with
        primaryKey; defining a table without a primary key fails at definition time.
        """

    @spec-only @requirement:TABLES.3
    Scenario: Row input/output crosses the storage boundary through the table schema; invalid rows...
      Then the storage-primitives contract includes:
        """
        Row input/output crosses the storage boundary through the table schema;
        invalid rows fail before appending and stored rows are decoded before
        get/query returns them.
        """

    @spec-only @requirement:TABLES.4
    Scenario: A Table class is self-describing, so db.table(TableClass) can produce a typed facade...
      Then the storage-primitives contract includes:
        """
        A Table class is self-describing, so db.table(TableClass) can produce a
        typed facade without mutating a registry.
        """

  @component:INSTANCE_KEYS
  Rule: Schema-owned instance keys

    @spec-only @requirement:INSTANCE_KEYS.1
    Scenario: StreamDb(basePath)(tables, keySchema?) derives the full stream name by encoding the...
      Then the storage-primitives contract includes:
        """
        StreamDb(basePath)(tables, keySchema?) derives the full stream name by
        encoding the instance key through the key schema.
        """

    @spec-only @requirement:INSTANCE_KEYS.2
    Scenario: The key schema defaults to Schema.String, but branded/refined key schemas are preserved...
      Then the storage-primitives contract includes:
        """
        The key schema defaults to Schema.String, but branded/refined key schemas
        are preserved through open/list/openExisting.
        """

    @spec-only @requirement:INSTANCE_KEYS.3
    Scenario: Instance enumeration decodes stream-name suffixes back through the key schema before...
      Then the storage-primitives contract includes:
        """
        Instance enumeration decodes stream-name suffixes back through the key
        schema before returning keys.
        """

  @component:STREAM_CONFIG
  Rule: Per-stream config on open

    @spec-only @requirement:STREAM_CONFIG.1
    Scenario: StreamDb.open accepts an optional StreamConfig (retentionPolicy / deleteOnEmpty /...
      Then the storage-primitives contract includes:
        """
        StreamDb.open accepts an optional StreamConfig (retentionPolicy /
        deleteOnEmpty / storageClass) applied to the create-if-absent.
        """

    @spec-only @requirement:STREAM_CONFIG.2
    Scenario: Omitting config preserves current behaviour (create-if-absent, inherit basin defaults) —...
      Then the storage-primitives contract includes:
        """
        Omitting config preserves current behaviour (create-if-absent, inherit basin
        defaults) — fully backward compatible.
        """

    @spec-only @requirement:STREAM_CONFIG.3
    Scenario: Age/size retention is for ephemeral streams only; a stream that mixes permanent state...
      Then the storage-primitives contract includes:
        """
        Age/size retention is for ephemeral streams only; a stream that mixes
        permanent state with transient records must not be GC'd by age retention (it
        uses CHECKPOINT instead).
        """

  @component:ENUMERATE
  Rule: Enumerate instances

    @spec-only @requirement:ENUMERATE.1
    Scenario: StreamDb.list() returns typed instance keys for existing streams under the db base path,...
      Then the storage-primitives contract includes:
        """
        StreamDb.list() returns typed instance keys for existing streams under the
        db base path, decoded through the key schema. The operation is discovery,
        not name construction; raw encoded-prefix filtering is intentionally out of
        scope.
        """

    @spec-only @requirement:ENUMERATE.2
    Scenario: Existence equals liveness only for streams dropped on completion; for persistent...
      Then the storage-primitives contract includes:
        """
        Existence equals liveness only for streams dropped on completion; for
        persistent streams, existence means the instance exists, and a separate work
        check is required. includeDeleted defaults false — a dropped instance no
        longer enumerates.
        """

  @component:EXISTENCE
  Rule: Non-creating open

    @spec-only @requirement:EXISTENCE.1
    Scenario: openExisting(key) returns None for a missing stream and never creates it; the...
      Then the storage-primitives contract includes:
        """
        openExisting(key) returns None for a missing stream and never creates it;
        the non-creating open path is authoritative, with no separate public
        exists/probe API that can race with deletion.
        """

  @component:PROJECTION
  Rule: Latest-value table projection

    @spec-only @requirement:PROJECTION.1
    Scenario: The latest-value materialized fold is the read lens for user state and materialized...
      Then the storage-primitives contract includes:
        """
        The latest-value materialized fold is the read lens for user state and
        materialized views — get/query read the current value per (table, key).
        """

    @spec-only @requirement:PROJECTION.2
    Scenario: The same MaterializedState fold is used for cold preload and live apply-after-ack, so...
      Then the storage-primitives contract includes:
        """
        The same MaterializedState fold is used for cold preload and live
        apply-after-ack, so replay and live state cannot diverge by using different
        materializers.
        """

    @spec-only @requirement:PROJECTION.3
    Scenario: Control messages are fold controls: snapshot-start/reset clear the materialized view,...
      Then the storage-primitives contract includes:
        """
        Control messages are fold controls: snapshot-start/reset clear the
        materialized view, snapshot-end is a boundary marker with no table effect.
        """

    @spec-only @requirement:PROJECTION.4
    Scenario: S2 command records such as trim/fence are skipped by the ChangeMessage fold and are...
      Then the storage-primitives contract includes:
        """
        S2 command records such as trim/fence are skipped by the ChangeMessage fold
        and are never decoded as table rows.
        """

  @component:WRITES
  Rule: Table writes and read-after-ack visibility

    @spec-only @requirement:WRITES.1
    Scenario: insert appends an insert ChangeMessage and does not perform a first-writer check.
      Then the storage-primitives contract includes:
        """
        insert appends an insert ChangeMessage and does not perform a first-writer
        check.
        """

    @spec-only @requirement:WRITES.2
    Scenario: insertOrGet is first-writer-wins within the opened single-writer instance: it returns...
      Then the storage-primitives contract includes:
        """
        insertOrGet is first-writer-wins within the opened single-writer instance:
        it returns Found with the decoded existing row when the key is already live,
        otherwise appends the insert and returns Inserted.
        """

    @spec-only @requirement:WRITES.3
    Scenario: upsert appends insert for an absent key and update for a live key.
      Then the storage-primitives contract includes:
        """
        upsert appends insert for an absent key and update for a live key.
        """

    @spec-only @requirement:WRITES.4
    Scenario: delete appends a delete ChangeMessage and removes the key from subsequent get/query...
      Then the storage-primitives contract includes:
        """
        delete appends a delete ChangeMessage and removes the key from subsequent
        get/query projection reads.
        """

    @spec-only @requirement:WRITES.5
    Scenario: Writes become visible to get/query only after the S2 append acknowledgement and local...
      Then the storage-primitives contract includes:
        """
        Writes become visible to get/query only after the S2 append acknowledgement
        and local projection application.
        """

    @spec-only @requirement:WRITES.6
    Scenario: Reusing a get/query Effect observes state at run time, not at Effect construction time.
      Then the storage-primitives contract includes:
        """
        Reusing a get/query Effect observes state at run time, not at Effect
        construction time.
        """

  @component:TRANSACTIONS
  Rule: Atomic cross-table commits

    @spec-only @requirement:TRANSACTIONS.1
    Scenario: transact buffers insert/upsert/delete intents across tables and commits them as one...
      Then the storage-primitives contract includes:
        """
        transact buffers insert/upsert/delete intents across tables and commits them
        as one conditional S2 append batch.
        """

    @spec-only @requirement:TRANSACTIONS.2
    Scenario: A successful transaction applies every buffered mutation to the local projection after...
      Then the storage-primitives contract includes:
        """
        A successful transaction applies every buffered mutation to the local
        projection after the append acknowledgement.
        """

    @spec-only @requirement:TRANSACTIONS.3
    Scenario: A failed transaction does not partially apply buffered mutations to the local projection.
      Then the storage-primitives contract includes:
        """
        A failed transaction does not partially apply buffered mutations to the
        local projection.
        """

    @spec-only @requirement:TRANSACTIONS.4
    Scenario: Transaction writes are keyed by self-describing Table classes, so declared db tables and...
      Then the storage-primitives contract includes:
        """
        Transaction writes are keyed by self-describing Table classes, so declared
        db tables and db.table(TableClass) participate in the same atomic commit.
        """

  @component:CHECKPOINT
  Rule: Caller-driven checkpoint + trim

    @spec-only @requirement:CHECKPOINT.1
    Scenario: An opened StreamDb instance exposes checkpoint, which appends a snapshot of its live set...
      Then the storage-primitives contract includes:
        """
        An opened StreamDb instance exposes checkpoint, which appends a snapshot of
        its live set at a cursor, then trims records before that cursor (surfacing
        today's internal compact as a first-class operation).
        """

    @spec-only @requirement:CHECKPOINT.2
    Scenario: An opened StreamDb instance exposes trim(cursor), which issues an explicit trim before...
      Then the storage-primitives contract includes:
        """
        An opened StreamDb instance exposes trim(cursor), which issues an explicit
        trim before cursor.
        """

    @spec-only @requirement:CHECKPOINT.3
    Scenario: A snapshot must be durable before any trim that depends on it.
      Then the storage-primitives contract includes:
        """
        A snapshot must be durable before any trim that depends on it.
        """

    @spec-only @requirement:CHECKPOINT.4
    Scenario: A checkpoint snapshot must fit one S2 batch (MAX_BATCH_RECORDS); larger snapshots...
      Then the storage-primitives contract includes:
        """
        A checkpoint snapshot must fit one S2 batch (MAX_BATCH_RECORDS); larger
        snapshots require framed/chunked snapshots (a follow-up).
        """

  @component:LIFECYCLE
  Rule: Stream lifecycle

    @spec-only @requirement:LIFECYCLE.1
    Scenario: compact remains a compatibility alias for checkpoint and preserves its snapshot+trim...
      Then the storage-primitives contract includes:
        """
        compact remains a compatibility alias for checkpoint and preserves its
        snapshot+trim semantics.
        """

    @spec-only @requirement:LIFECYCLE.2
    Scenario: drop deletes the underlying S2 stream; after drop, default enumeration excludes the...
      Then the storage-primitives contract includes:
        """
        drop deletes the underlying S2 stream; after drop, default enumeration
        excludes the instance.
        """

    @spec-only @requirement:LIFECYCLE.3
    Scenario: openExisting is the only public non-creating existence path; there is no separate exists...
      Then the storage-primitives contract includes:
        """
        openExisting is the only public non-creating existence path; there is no
        separate exists probe.
        """

  @constraint:BOUNDARY
  Rule: Latest-value storage, not generic event log

    @spec-only @requirement:BOUNDARY.1
    Scenario: Stream-db exposes a latest-value ChangeMessage projection over S2; it is not a generic...
      Then the storage-primitives contract includes:
        """
        Stream-db exposes a latest-value ChangeMessage projection over S2; it is not
        a generic ordered event-log abstraction.
        """

    @spec-only @requirement:BOUNDARY.2
    Scenario: Ordered ActorEvent replay by S2 seq_num is a schema-owned actor log over...
      Then the storage-primitives contract includes:
        """
        Ordered ActorEvent replay by S2 seq_num is a schema-owned actor log over
        effect-s2.readDecoded (stateful-execution LAYERING.6), not a StreamDb
        primitive.
        """

    @spec-only @requirement:BOUNDARY.3
    Scenario: Stream-db must not add readLog/collectLog-style event-log APIs as a convenience over...
      Then the storage-primitives contract includes:
        """
        Stream-db must not add readLog/collectLog-style event-log APIs as a
        convenience over this latest-value abstraction.
        """

  @constraint:IDENTITY
  Rule: Schema-derived identity only

    @spec-only @requirement:IDENTITY.1
    Scenario: Public callers open/list by typed keys, not by hand-built stream paths.
      Then the storage-primitives contract includes:
        """
        Public callers open/list by typed keys, not by hand-built stream paths.
        """

    @spec-only @requirement:IDENTITY.2
    Scenario: StreamDb.list decodes existing stream names through the key schema; raw encoded-prefix...
      Then the storage-primitives contract includes:
        """
        StreamDb.list decodes existing stream names through the key schema; raw
        encoded-prefix filtering is intentionally out of scope.
        """

    @spec-only @requirement:IDENTITY.3
    Scenario: Table identity is the schema-owned tableName plus schema-owned primary key, not...
      Then the storage-primitives contract includes:
        """
        Table identity is the schema-owned tableName plus schema-owned primary key,
        not caller-composed delimiter strings.
        """

  @constraint:SINGLE_WRITER
  Rule: Opened instance ownership

    @spec-only @requirement:SINGLE_WRITER.1
    Scenario: One opened StreamDb instance serializes writes through its local lock and CAS tail.
      Then the storage-primitives contract includes:
        """
        One opened StreamDb instance serializes writes through its local lock and
        CAS tail.
        """

    @spec-only @requirement:SINGLE_WRITER.2
    Scenario: Cross-process or multi-owner writes to the same stream are outside the StreamDb...
      Then the storage-primitives contract includes:
        """
        Cross-process or multi-owner writes to the same stream are outside the
        StreamDb guarantee unless a higher-level owner/fencing protocol serializes
        them.
        """

  @constraint:OBSERVABILITY
  Rule: Production tracing

    @spec-only @requirement:OBSERVABILITY.1
    Scenario: Public operations emit effect-s2-stream-db spans with enough attributes to identify the...
      Then the storage-primitives contract includes:
        """
        Public operations emit effect-s2-stream-db spans with enough attributes to
        identify the operation, stream, and table where applicable.
        """

    @spec-only @requirement:OBSERVABILITY.2
    Scenario: S2-backed behavior is proven by executable Cucumber specs over production spans;...
      Then the storage-primitives contract includes:
        """
        S2-backed behavior is proven by executable Cucumber specs over production
        spans; package-local tests remain pure and type-level.
        """

  @constraint:COMPAT
  Rule: COMPAT

    @spec-only @requirement:COMPAT.1
    Scenario: All additions are backward compatible — existing open/get/query/transact/compact/drop...
      Then the storage-primitives contract includes:
        """
        All additions are backward compatible — existing
        open/get/query/transact/compact/drop behaviour is unchanged when the new
        options/primitives are not used.
        """
