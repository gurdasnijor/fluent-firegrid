# Handoff: building the S2 durable actor model (spec → code → firelab)

Date: 2026-06-16 · Branch: `main` (work committed directly; see "Workflow conventions")
Audience: a fresh session picking up implementation of the actor-model redesign.

You are at the **start of implementation**. The design is done and committed as SDDs +
`.feature.yaml` contracts; a validation harness (`firelab`) exists and passes a baseline. Your job
is to build the code bottom-up, each slice gated by a firelab validation against its feature spec.

---

## 1. Read these first (the design is settled — don't relitigate)

Authoritative specs (each `.feature.yaml` is the **normative contract**; the SDD is the narrative):

- **Engine (top layer):** `docs/sdds/object-actor-model-sdd.md`
  → contract `features/effect-s2-durable/object-actor-model.feature.yaml` (14 groups, 60 reqs)
- **Storage primitives (mid layer):** `docs/sdds/s2-resource-provisioning-sdd.md` §2
  → contract `features/effect-s2-stream-db/storage-primitives.feature.yaml` (6 groups, 16 reqs)
- **Resource provisioning (low layer):** `docs/sdds/s2-resource-provisioning-sdd.md` §1
  → contract `features/effect-s2/resource-spec.feature.yaml` (2 groups, 7 reqs)
- **Design input (reference only, not contract):** `docs/sdds/effect-encore-informed-actor-proposal.md`

Reference id format is `<feature.name>.<GROUP>.<n>` (e.g. `storage-primitives.CHECKPOINT.1`,
`object-actor-model.CHECKPOINTING.5`). Firelab gate ids are `<feature.name>.<req.id>`.

### The one-paragraph "what we're building"
Virtual `object`s become **per-key actor streams**: ONE durable S2 stream per object key is the
single system of record (state + an append-only **accept-log** of exclusive calls + per-call
journals + ingress rows + results), read as an **ordered `ActorEvent` log by S2 `seq_num`**, with
the latest-value table fold reserved for the user-state projection. A **pure transition**
`(snapshot, event) -> (snapshot, action[])` decides ordering/parking/completion/checkpointing; an
**interpreter** runs the emitted actions (the only place effects happen). This deletes the current
two-stream (`obj` inbox ↔ `wf` journal) seam, the roster, the window-2 double-apply, and
residency-dependent ingress. `service` stays ephemeral (one stream per call, dropped on
completion). `workflow` = an object specialization (run-once `run` + shared handlers), no separate
engine. Exclusive handlers are single-writer per key via the accept-log + serial drainer; **shared**
handlers run concurrently read-only (may append signal rows, never user-state writes).

### Decisions already made (do not re-open without strong reason)
- **No mutable `status` field.** Done = a `Completed`/result event exists; pending = accepted ∧
  ¬completed. Completion is a single append; "advance" is re-derived → window-2 is *structurally*
  impossible (`COMPLETION.2-3`). (We deliberately diverged from a `status`+atomic-transact agent
  proposal — it reintroduces an avoidable atomicity obligation.)
- **S2 `seq_num` is the admission order** — no app-level seq field (`ADMISSION.3`). The ordered
  ActorEvent log is read via **`effect-s2.readDecoded`** (typed decode preserving `seq_num`/metadata)
  as a schema-owned actor-log in `effect-s2-durable` — NOT a stream-db `readLog`. Stream-db owns the
  latest-value table projection only (`storage-primitives.PROJECTION`).
- **callId self-routes** via a reversible Effect Schema codec → `StreamDb.open(owner)`; no roster,
  no delimiter parsing (`ROUTING`).
- **Ingress + dispatch are appends** (residency-independent); only execution needs the single owner
  (`INGRESS`, `ADMISSION.6`). This is the multi-process hook.
- **GC = checkpoint + trim, NOT age-retention** on object streams (they hold permanent state);
  explicit `CHECKPOINTING` (drainer-owned, watermark, idempotency horizon → `Expired`).
- **Layering is load-bearing:** engine (`effect-s2-durable`) → primitives (`effect-s2-stream-db`) →
  `effect-s2`/S2, all behind a planned `DurableStore` port. Do NOT pull S2 specifics up into the
  engine; do NOT bury reusable storage primitives in the engine.

---

## 2. The validation workflow (firelab) — how we prove each slice

Read `packages/firelab/README.md` in full. The model: a validation maps to a feature file and
runs **one isolated claim per requirement**; the verdict needs BOTH the behavioral assertion AND
**corroborating OpenTelemetry spans** from the system under test ("production-path coverage"). A
claim that passes only in memory without the expected spans does **not** count.

Validations live at `packages/firelab/src/validations/<id>/index.ts`, default-exporting
`defineValidation({ id, feature: {product, name}, backend: S2LiteLive, component, requirements })`.
Each requirement = `{ id: "<GROUP>.<n>", description, evidence (CEL over spans), claim }`.

Commands (run from repo root):
```bash
pnpm --filter firelab validate:list
pnpm --filter firelab validate:run <id> --timeout-ms 120000     # writes a run dir, exits non-zero on gate fail
pnpm --filter firelab validate:show     # render trace tree (latest run)
pnpm --filter firelab validate:gaps <run-id>    # instrumentation coverage gaps
pnpm --filter firelab typecheck && pnpm --filter firelab diagnostics
```
Note: `timeout` is NOT on PATH here; just run the pnpm command directly (s2 lite launches via
`S2LiteLive`, ~tens of seconds).

**Baseline confirmed green this session:** `pnpm --filter firelab validate:run
effect-s2-stream-db-storage-primitives` → 5 gates pass, verdict `production-path-covered`
(81 spans). So the harness, s2-lite, and the span pipeline all work today.

**Critical evidence-gate rule:** `evidence` is CEL over scoped OTel spans and must reference spans
actually emitted by the package under test (e.g. `effect-s2-stream-db.open`, `S2.checkTail`,
`S2.append`). The oracle rejects vacuous gates. So when you add a primitive, you must **instrument
it with a span in the package** (not in the validation) — mirror the existing
`Effect.withSpan("effect-s2-stream-db.<op>", { attributes: { stream } })` pattern in
`packages/effect-s2-stream-db/src/StreamDb.ts`. Existing emitted spans:
`effect-s2-stream-db.{open,table.insert,table.upsert,table.delete,table.insertOrGet,table.get,table.query,transact,commit,compact,drop}`
and `effect-s2` `S2.{append,checkTail,createStream,readBatch,deleteStream,...}`.

---

## 3. Current code state (what exists vs. what to build)

`packages/effect-s2-stream-db/src/StreamDb.ts` today exposes:
- `StreamDb<Self>(basePath)(tables, key?)` → class with `static open(key)`, `static basePath/key/tables`.
- Instance (`StreamDbInstance`): declared-table facades + `table(AnyTable)`, `transact`, `compact`,
  `drop`. (`compact` already does the snapshot+trim pattern internally — `MAX_BATCH_RECORDS = 1000`.)
- `open` already calls `client.createStream({ stream })` (catching `S2Conflict` → effectively an
  ensure) but passes **no config**.
- All ops are instrumented with `withSpan`.

NOT yet implemented (the Slice-1 gaps the feature spec defines):
- `StreamDb.open(key, { config })` — pass `StreamConfig` into `createStream` (`STREAM_CONFIG.1`).
- `StreamDb.list({ keyPrefix? })` — static; `listStreams({ prefix: basePath })` → strip
  `${basePath}/` → `Schema.decodeUnknownEffect(keySchema)` each suffix → keys (`ENUMERATE`).
- `StreamDb.exists(key)` / `StreamDb.openExisting(key)` — static; `checkTail`, never create
  (`EXISTENCE`).
- Instance `checkpoint()` + `trim(cursor)` — surface the snapshot+trim as first-class (`CHECKPOINT`).
- NOTE: ordered (`seq_num`) event-log replay is **not** a stream-db primitive. It is read via
  `effect-s2.readDecoded` (typed decode preserving `seq_num`) as a schema-owned actor-log in
  `effect-s2-durable` (`PROJECTION.1` disclaims it; object-actor-model `LAYERING.6`). Do not add
  `StreamDb.readLog`.

Key implementation facts gathered this session (so you don't re-discover):
- Factory at `StreamDb.ts:156`; `keySchema` (line 162) + `encodeKey` are in scope for static
  methods. Static `open` at line 169 shows the `encodeKey → openStream(\`${basePath}/${segment}\`)`
  pattern. Add `list/exists/openExisting` as `static readonly` on `StreamDbImpl`, and extend the
  `StreamDbClass` interface (line ~135) with their signatures.
- `effect-s2` `S2Client.listStreams(args?, opts?)` returns `Stream.Stream<StreamInfo, ...>`;
  `listAllStreams` returns a paginated `Stream`. `StreamInfo.name` is the full stream name. Both
  accept `{ prefix }`. `checkTail(name)` → `Tail` (throws/`S2NotFound` if absent — use to implement
  `exists`).
- The existing storage-primitives validation (`packages/firelab/src/validations/
  effect-s2-stream-db-storage-primitives/index.ts`) currently maps requirement ids to *existing*
  behavior (e.g. `CHECKPOINT.1`→`compact`, `PROJECTION.1`→latest-value reads). As you implement
  the real primitives, ADD requirement claims for `ENUMERATE.*`, `EXISTENCE.*`, `PROJECTION.1`,
  `CHECKPOINT.1-2` (surfaced API), `STREAM_CONFIG.1`, each with a span-backed `evidence` gate.
  Add new spans for the new ops (e.g. `effect-s2-stream-db.list`, `.exists`, `.checkpoint`, `.trim`).

---

## 4. Build plan (bottom-up; each slice ends green in firelab)

**Slice 1 — `effect-s2-stream-db` storage primitives.** Implement the gaps above + instrument +
extend the storage-primitives validation to cover all reqs with span evidence. Smallest first:
`list` + `exists`/`openExisting` (static, self-contained, central to recovery), then `open({config})`,
then surfaced `checkpoint`/`trim`. Stream-db stays the latest-value table-projection layer — no
`readLog`; ordered event replay is `effect-s2.readDecoded` at the actor layer. Keep changes
backward compatible (`COMPAT.1`).

**Slice 2 — `effect-s2` `S2Spec`.** `plan(spec)`/`apply(spec)` reconcile (ensureBasin +
reconfigureBasin diff + ensureStream/reconfigureStream) over existing client ops; idempotent,
partial-update. New validation `effect-s2-resource-spec` against `resource-spec.feature.yaml`.
Bonus: it replaces the s2-lite init-file hack in tests.

**Slice 3 — `effect-s2-durable` actor engine** (the big one; consumes Slices 1–2). Suggested
sub-slices, each its own firelab requirement(s):
1. `ActorEvent` schema + reversible `CallId`/owner codecs (`ROUTING`).
2. Pure `transition(snapshot, event) -> [snapshot, action[]]` + `ActorSnapshot` — unit-testable
   with no S2 (this is where the `snapshot+event→snapshot+actions` tests live; `PLANNING.7-8`).
3. The effectful drainer/interpreter folding the actor-log (`effect-s2.readDecoded` over
   `ActorEvent`, preserving `seq_num`) — admission, serial exclusive execution, completion-derived
   advance.
4. Ingress-as-append + `attach`/`poll` as projection views (`INGRESS`, `COMPLETION`).
5. Recovery via `StreamDb.list` + pending-head check; `CHECKPOINTING` (checkpoint+trim+horizon).
6. Shared handlers (concurrent read-only) + `workflow` run-once specialization.
   Then migrate the existing `object(...)` impl onto the actor runtime and delete the old
   `ObjectStateDb`/inbox/roster path. See the illustrative code in the actor SDD ("Illustrative
   runtime shape") — it sketches the exact `ActorEvent`/`transition`/`drain`/`interpret`/routing/
   checkpoint shapes; treat as a guide, not a required module layout.

**Later (own SDDs, not now):** cross-process leasing/fencing (per-key lease + fence — the actor
model is the prerequisite); object lifecycle (`clearAll`/destroy); framed/chunked snapshots for
large object state; OTel tracing pass across the engine seams.

---

## 5. Workflow conventions (this repo)

- **CI gates:** `pnpm preflight` (lint = eslint + knip + jscpd[threshold 0] + depcruise; typecheck;
  test; effect diagnostics). Keep all green. Lint landmines hit repeatedly this project:
  `@stylistic/comma-dangle` (run `pnpm exec eslint --fix <file>`), `local/no-launder-cast` (escape
  with `// eslint-disable-next-line local/no-launder-cast -- <justification>`),
  `@typescript-eslint/no-explicit-any` (error), `no-restricted-syntax` flags `Effect.die`/`orDie`,
  `local/no-for-of-in-source` (use `Effect.forEach`/`reduce`).
- **Effect 4.0.0-beta.78 landmines** (mainline, not effect-smol): `Schema.Literals([...])`,
  `Option.fromNullishOr`, `Effect.catch` (not catchAll), no `Effect.zipRight` (use
  `flatMap`/`andThen`), `Context.Reference` (no `FiberRef`), `Effect.fnUntraced`,
  `Cause.findErrorOption`, an Effect is `yield*`-able. `Semaphore.make(1)` + `lock.withPermits(1)`.
- **Commits:** branch off `main` (PRs were used for the engine slices — #17, #18 merged). End commit
  messages with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. Note: this
  session's doc/spec work landed on `main` directly (commit `2c5369d` bundled the firelab harness +
  feature specs).
- **feature.yaml gotcha:** a requirement value that *starts* with `"` truncates the YAML scalar —
  wrap the whole value in single quotes. (Hit twice this session.) Validate with the repo's yaml
  lib: `node -e '...YAML.parse(fs.readFileSync(f))'` (yaml@1.10.3 under `.pnpm`; exports `{ YAML }`).

---

## 6. Memory pointers (auto-loaded context)

`~/.claude/.../memory/MEMORY.md` indexes: `durable-execution-api-source` (authoring surface mirrors
the SDD/restate-sdk-gen), `object-exclusivity-admission-control` (exclusivity MUST be a durable
FIFO/admission record, never an in-process lock — the actor accept-log is its successor),
`streamdb-engine-followups` (carry-overs incl. the cross-process leasing frontier).

## 7. Suggested first action for the next session
`pnpm --filter firelab validate:run effect-s2-stream-db-storage-primitives` to re-confirm baseline,
then implement `StreamDb.list` + `exists`/`openExisting` in `packages/effect-s2-stream-db/src/
StreamDb.ts` (static methods, new spans `effect-s2-stream-db.list`/`.exists`), extend the validation
with `ENUMERATE.*` + `EXISTENCE.*` requirement claims (evidence on the new spans + `S2.listStreams`/
`S2.checkTail`), and run it green. That's Slice 1a — small, self-contained, proves the loop.
