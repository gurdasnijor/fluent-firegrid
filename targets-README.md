# targets.json — the red/green ratchet

Doc-Class: execution
Owner: Firegrid Architecture

Mechanics of the top-down red/green loop in
[`docs/sdds/api-layering-sdd.md`](docs/sdds/api-layering-sdd.md)
("Execution: the top-down red/green loop"). The manifest drives a target
suite beside the blocking proof suite: ratified red corpora live in CI from
day one, and every status change is an explicit, reviewed commit.

Run it: `pnpm run check:targets` (part of `pnpm run check`, so it blocks CI).
Runner: [`scripts/targets/run-targets.mjs`](scripts/targets/run-targets.mjs).

## Manifest schema

```jsonc
{
  "suites": [
    // One entry per corpus suite. `command` runs from the repo root.
    { "corpus": "t0-wiring", "command": ["node", "scripts/targets/t0-wiring-suite.mjs"] }
  ],
  "targets": [
    // One entry per ratified target test.
    { "id": "t0.wiring-red", "wp": "T0", "corpus": "t0-wiring", "status": "red" }
  ]
}
```

- `id` — globally unique test id (convention: `<wp-lowercase>.<test-name>`).
- `wp` — the owning work packet (`T1`, `T2`, ...).
- `corpus` — which suite reports this test; must match a `suites[].corpus`.
- `status` — `"red"` (ratified, not yet implemented; expected to fail) or
  `"green"` (implemented; must pass forever after).

## Suite protocol (TS and F# runners speak the same thing)

A suite is any command that:

1. runs its tests to completion regardless of individual failures,
2. prints exactly one JSON line per test on **stdout**:
   `{ "id": "...", "pass": true|false }` (extra keys allowed, ignored),
3. writes all diagnostics to **stderr** (stdout is reserved for result lines;
   any other stdout line is a protocol error), and
4. exits `0` when the suite itself ran (per-test failures are carried in the
   result lines, not the exit code). A nonzero exit means the suite crashed
   and fails the run.

To register a corpus suite: add its `suites[]` entry plus one `targets[]`
entry per test, in the same PR that adds the tests. F# corpora live in the
`apps/proofs` harness: proofs register in its suite-tagged `Registry` and
the suite command is `node apps/proofs/dist/Main.js proof targets <suite>`
(one `{ "id", "pass" }` line per registered proof, diagnostics on stderr,
exit 0 when the suite ran — e.g. the T1 `t1-durable` suite of migrated
`Firegrid.Durable` corpus laws). The suite command must be self-sufficient
or rely only on what `pnpm run check` has already built before
`check:targets` runs (Fable build + proofs run first).

## Strict rules (both ways — any violation exits nonzero)

| Manifest | Outcome | Result |
| --- | --- | --- |
| `red` | fails | OK — expected red |
| `red` | passes | **FAIL** — unexpected pass; promote explicitly (see below) |
| `green` | passes | OK |
| `green` | fails | **FAIL** — regression |
| registered | never reported | **FAIL** — drift |
| not registered | reported | **FAIL** — drift |

Also enforced: duplicate ids (manifest or output), a result reported by a
suite other than the target's `corpus`, malformed entries, suite crash.
There is no "skip" status and no tolerance window — the manifest is the
single source of truth for what red and green mean today.

## Promotion-commit protocol

- **Ratification freezes test bodies.** A corpus merges with every test
  registered `red` after human ratification. From that point the test bodies
  are frozen — editing one (or deleting/renaming a target) is an architect
  gate, not a worker decision.
- **Promotion is one PR.** A worker greens a target by shipping, in a single
  PR: the implementation, the manifest flip `red` → `green`, and the ledger
  flip for the owning WP row (`docs/execution/`). Zero edits to test bodies.
  The runner enforces the mechanical half: a red test that starts passing
  fails CI until the manifest is flipped in the same change.
- **Demotion does not exist.** A green target that fails is a regression to
  fix, never a status to edit back. Reverting a promotion reverts the whole
  promotion PR.

## Proof of wiring (T0)

`t0.wiring-red` (asserts false, registered `red`) and `t0.wiring-green`
(trivial pass, registered `green`) in
[`scripts/targets/t0-wiring-suite.mjs`](scripts/targets/t0-wiring-suite.mjs)
demonstrate the mechanics end to end. They stay in place as a canary for the
runner itself.
