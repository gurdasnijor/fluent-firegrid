# Dispatch Brief: Phase C / C2 — session spine (first T2 green-making packet)

Doc-Class: dispatch-brief
Date: 2026-07-09
Packet: PHASE C green-making #1 (C1 #129 ratified by the human; T2 law
bodies now FROZEN)
Branch: NEW `c2/session-spine` from post-#129 main
Architect: this session — escalate on anything marked GATE
Merge gate: architect review (laws already ratified; standard promotion)

## Mission

Make the core of the substrate real: sessions, turns, tools, and turn
handles — greening THREE t2 laws:

1. `t2.converse-across-crashes` — the flagship: a session's turn
   survives SIGKILL of its host; tool calls exactly-once; watch replays
   after the fact.
2. `t2.cancel-live-turn` — durable cancel observed at the next durable
   operation; no later moves; ends Cancelled; visible in the watch.
3. `t2.reserved-segment-admission` — typed `DurableReservedSegment` at
   Grid addressing/admission for `/gen/` and `/child/` (segment-wise;
   `team/generalist` admits). The L2 validation exists (G2) — surface
   it at L3.

Target scoreboard: **49 registered — 41 green, 8 expected-red, 0
errors.**

## What to implement (the lowerings, per the contract annotations)

In `src/Firegrid.Grid/` (packet file: `InternalSessions.fs` + contract
section bodies only):

- `Grid.connect` → `Client.connect`. `Grid.Session` → entity addressing
  (naming is addressing; creation on first use) + reserved-segment
  check at admission.
- `Session.Prompt` → session entity `.Call` (single-live-turn policy)
  → turn workflow `.Start`; returns on durable acceptance; duplicate
  promptId delivered once (entity admission dedup).
- The **turn workflow**: the loop that drives the model-turn step
  (harness), executes tool calls as journaled `Step.Call`s, and emits
  `AgentEvent`s (Thinking/Said/CalledTool/ToolReturned/TurnEnded) to
  the turn's durable out-log (`DurableLog.Append`, sealed with the
  terminal). `Tool.define` → `Step.define`. (`Tool.gated` bodies may
  remain notYet — approval is a later packet's law.)
- `TurnHandle.Watch` → `client.Logs(...).Attach` (recorded prefix →
  live tail → terminal; the L2 law for this is green).
  `TurnHandle.Outcome` → `run.Result`. `TurnHandle.Cancel` and
  `Session.CancelLiveTurn` → `run.Cancel` via the entity.
- Harness binding: the turn workflow calls the model-turn step through
  the same mechanic C1's corpus laws use to inject the scripted
  ModelSays harness (the ratified examples' stand-in). Make the product
  path real; the public `Harness` constructor amendment stays deferred
  (T3 surface finding — do not amend the surface).

## Freezes and scope guards

- ALL law bodies (t1 + t2 + foundation + canary): FROZEN. The t2 bodies
  were ratified in #129 — they are now the spec.
- `Firegrid.Grid.fs` signatures: FROZEN (ratified surface); section
  bodies only.
- The platform (`src/Firegrid.Durable/` contract AND implementation,
  kernel, harness infra): FROZEN. These three laws lower to L2
  capabilities that are already green — if you hit a genuine platform
  gap, GATE with specifics; do not edit the platform.
- Consumer purity: `Firegrid.Grid.fsproj` keeps its single
  ProjectReference (Firegrid.Durable). Any second reference is a GATE.
- `targets.json`: exactly 3 status flips.

## Milestones (push after FIRST commit; draft PR immediately; push per milestone)

- **M1** — session entity + turn workflow + prompt path: the converse
  law progresses past admission (partial checks moving from
  workload-crash to check-level failures is progress evidence).
- **M2** — tool steps + AgentEvent out-log + TurnHandle
  (Watch/Outcome): `t2.converse-across-crashes` green via
  `proof run`.
- **M3** — cancel path + reserved-segment surfacing: remaining two laws
  green; 3 manifest flips.
- **M4** — full `pnpm run check` (blocking foreground, ~4.5 min; expect
  49/41/8/0) + regression attention on the t1 suite (this packet
  exercises entities/workflows/logs heavily). PR body: lowering
  summary per law, scoreboard tail. Mark ready.

## Operating rules

Fresh worktree; `git fetch` first; `SKIP_SIMPLE_GIT_HOOKS=1`; never
`git add -A`; Fable traps; parked-signal flake → re-run before blaming;
full checks BLOCKING FOREGROUND; never stop with a run in flight.

## Exit criteria

1. Full check green: 49 — 41 green, 8 expected-red, 0 errors (t1 stays
   22/22).
2. Three laws green via frozen bodies; consumer purity intact.
3. PR ready. NOT merged (architect merges).
