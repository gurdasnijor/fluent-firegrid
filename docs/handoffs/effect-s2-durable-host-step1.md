# Handoff: effect-s2-durable host — build step 1

Implementation brief for the next session. **Design + rationale live in
`docs/sdds/effect-s2-durable-host-process-model-sdd.md`** (PR #47); this is the
operational "how to start coding in this repo" companion.

## Where things stand

- **Branch:** `claude/durable-host-process-model` (PR #47, docs-only: the SDD +
  this handoff). Build step 1 commits go **on this branch** (stacked on the SDD).
- **Architecture context** is in auto-memory `durable-engine-topology` (brokerless
  distributed peers over S2; bespoke engine is deliberate for the restate-sdk-gen
  surface; namespace = S2 basin; Model A compile-time catalog; multi-host via the
  S2 fence/lease/claim pattern). Read it first.
- `effect-s2-durable` is a **library only** today — no `bin`, no host composition;
  the engine is assembled+run only in `test/ingress-support.ts`. Step 1 promotes
  that assembly to a real, runnable surface.

## Step 1 scope (SDD §10.1–10.2) — and explicitly NOT more

**Goal:** make the engine runnable as a single host (N=1). Do **not** build fenced
ownership / claim-sweep (step 4), service-path unification (step 5), the timer
driver, the CLI interaction binary, or anything multi-host. Single host =
boot-recover (the engine already does this on layer build) + optionally serve
ingress + run forever.

**Files to add:**
- `src/host.ts` — exports `DurableHostLive(opts)`, `DurableHostFromConfig(catalog)`,
  `startHost(opts)`. The **only** module importing `@effect/platform-node`.
  - `DurableHostOptions = { catalog: ReadonlyArray<AnyDef>, namespace: string, ingress?: { port: number } }`
    (`AnyDef` is exported from `service.ts`).
  - `DurableHostLive` = merge of `serviceLayer(...catalog)` (engine + boot-recovery
    seed) + (if `ingress`) `durableIngress(catalog)` over `NodeHttpServer.layer` +
    `S2Client.layerConfig`. **Headless** = omit `ingress` (no NodeHttpServer).
  - `startHost` = `Layer.launch(DurableHostLive(opts)).pipe(Effect.zipRight(Effect.never))`
    (the predecessor's run-forever shape).
  - `DurableHostFromConfig(catalog)` reads namespace from `S2_BASIN` (= namespace,
    §3.4/§3.5) and ingress port from env (e.g. `INGRESS_PORT`); `S2Client.layerConfig`
    already reads `S2_ACCESS_TOKEN` + `S2_BASIN`.
- `src/bin/host.ts` — `NodeRuntime.runMain(startHost(...).pipe(Effect.provide(NodeContext.layer)))`,
  with the `isDirectRun` guard (see predecessor `bin/host.ts`). Wire `@effect/cli`
  for the `host` subcommand + flags (`--namespace`, `--ingress-port`) if cheap;
  otherwise env-only is fine for step 1.

**package.json changes:**
- Add `"./host"` to `exports` (→ `./src/host.ts`).
- Add a `"bin"` entry for `src/bin/host.ts`.
- **Promote `@effect/platform-node` from devDependencies → dependencies** (option A,
  SDD §8). The engine core (`.` import graph) must stay Node-free; only `./host`
  and `bin` import it.

**Mirror the proven wiring:** `test/ingress-support.ts` already composes
`durableIngress(catalog)` ← `serviceLayer(...catalog)` ← `NodeHttpServer.layer(() =>
createServer(), { port })` ← `NodeFileSystem`/`NodePath` ← S2 (s2lite in tests, real
`S2Client.layerConfig` in the host). The host is that stack, minus the test's
`connect`/client side, plus `Layer.launch + Effect.never`. Copy the provide order.

## Verification (this env can't smoke-run; rely on these + CI)

From `packages/effect-s2-durable/`:
- `pnpm exec tsc --noEmit`
- `pnpm exec eslint . --max-warnings 0`
- `pnpm exec effect-language-service diagnostics --project tsconfig.json --strict`
- From repo root: `pnpm run lint:dead` (knip v6 scans `test/**` AND `src` — new
  exports must be reachable; `bin`/host exports are reached from the bin entry).

A real run needs the `s2` binary + S2 creds (absent locally). The existing ingress
tests exercise the same wiring under CI (which installs `s2`). Build-plan step 3
(re-point `test/ingress-support.ts` onto `DurableHostLive`) is the dogfood that
proves the host composition end-to-end — consider doing it as part of step 1 so
there's a live test, not just typecheck.

**Env quirk (bit us):** the local `node_modules` can land in a partially-pruned
state where `@effect/platform-node` / `@types/node` don't resolve (tsc errors on
`Cannot find type 'node'` / `Cannot find module '@effect/platform-node/...'`). Fix:
`rm -rf node_modules packages/*/node_modules && pnpm install` (relinks from the
pnpm store; ~seconds, offline-safe).

## References gathered this session (don't re-fetch)

- **Process-model precedent — `gurdasnijor/firegrid` `packages/runtime`:**
  `src/bin/host.ts` (run-forever shape), `src/node.ts` (`firegridNodeHost`),
  `composition/host-live.ts` (`FiregridRuntimeHostLive`/`...FromConfig`),
  `composition/host-public.ts` (`startRuntime`). Read via
  `gh api "repos/gurdasnijor/firegrid/contents/<path>?ref=main" --jq .content | base64 -d`.
  Layering rule: runtime stays binding-free; adapters bind at the edge. Pitfall:
  forked daemons must `Effect.context()`-capture + re-`provide` or fail "Service
  not found."
- **S2 concurrency for LATER (step 4, not step 1):**
  `s2-streamstore/s2-sdk-typescript` `packages/resumable-stream/src/shared.ts`
  `claimSharedGeneration` (fence + lease-expiry takeover + CAS handoff) and
  `packages/patterns` (dedupe/framing). `effect-s2`'s `AppendOptions` already
  exposes `fencingToken` + `matchSeqNum`; the object path's `actor/log.ts` already
  uses `casAppend`/`checkTail`.
- **effect-s2 config:** `S2Client.layerConfig` = `S2_ACCESS_TOKEN` + `S2_BASIN`;
  streams are path-like within the one basin (`obj/<object>/<key>`, execution-id
  streams); per-op `basinName` override exists.
- **Restate (what we do NOT replicate):** `restate-sdk` `node.ts` `serve()/listen()`
  is the broker→SDK callback we eliminate; we keep only the ingress surface
  (`durableIngress`/`connect`). Authoring north star: `@restatedev/restate-sdk-gen`.

## After step 1

Build plan §10 steps 4 (fenced ownership + claim-sweep — the multi-host enabler,
subsumes recovery + timer re-arm) and 5 (service-path unification). Then the next
project is the **distributed cucumber runtime** built on this engine.
