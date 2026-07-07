# Agent Instructions

These instructions apply to the whole repository.

## Read First

- Read this file before making changes.
- For Effect-specific code style, also read [`LLMS.md`](LLMS.md).
- Prefer existing package-local patterns over inventing new abstractions.
- Keep generated or external reference material out of commits unless explicitly
  requested.

## Language Zones

This repo is two-zone by rule — see
[`docs/canon/architecture/fluent/language-and-targets.md`](docs/canon/architecture/fluent/language-and-targets.md):

- **F# owns everything that writes S2** (substrate, kernel semantics, folds,
  lifecycle, wakes, timers) and follows the sans-IO core rule: pure semantics
  modules, I/O in shells, time/randomness passed in as data. Use the ported
  eff-firegrid modules as the style exemplar. **No new EffSharp** — use
  `Result`, DUs, `Async`/`task`/`promise` CEs, records for dependencies, and
  pull-cursors for streams (EffSharp is deprecated; see the canon decision
  record).
- **TypeScript owns TS-ecosystem edges** (proof harness, harness adapters,
  client SDK). Cross zones only via the kernel protocol or the single
  Fable-emitted package seam — never deep imports.

## Effect Code

TS-zone Effect code must follow the local guidance in [`LLMS.md`](LLMS.md),
adapted from the Effect team guidance in
<https://github.com/Effect-TS/effect-smol/blob/main/LLMS.md>.

In short:

- Prefer `Effect.gen` for imperative Effect programs.
- Prefer `Effect.fn("name")` for exported functions that return an `Effect`.
- Do not wrap function bodies in `Effect.gen` when `Effect.fn` is the better
  shape.
- Model services with `Context.Service` and layers.
- Model expected domain failures as typed tagged errors.
- Manage resources through Effect scopes, layers, and `acquireRelease`.

## Proof-Driven Development

Proofs verify surfaces; they do not replace them. The known failure mode is
code that regresses to "whatever makes the proof pass." These rules prevent it:

- **Surface before proofs.** Capability work starts from a Target Surface
  section in the owning SDD: module placement, exported types and signatures,
  typed errors, and the laws the surface obeys. If the section does not exist
  or your work changes it, write/update it and get architect sign-off (gate G6
  in the execution ledger) before writing proof or implementation code.
- **Proofs consume only public exports.** No deep imports into module
  internals, no test-only hooks, no proof-only branches or flags in production
  code. If a proof cannot be written against the public surface, the surface is
  wrong — escalate, do not tunnel.
- **The consumer test.** A production module must be usable by a consumer who
  has never read its proofs. If correct setup requires proof-harness knowledge,
  the surface is incomplete.
- **Primitive + combinator.** New behavior should be expressible as an existing
  primitive plus a combinator over the surface. If it cannot be, that is a
  design smell to escalate, not to code around.
- **Passing is half done.** The deliverable of a capability work packet is the
  surface *and* its proof. Widening types, leaking internals, or weakening
  signatures to get green is a gate violation, not progress.

## Repository Workflow

- Use `pnpm` for repository commands.
- Run the narrowest relevant validation for a change. For broad or shared
  changes, run `pnpm preflight`.
- Do not revert unrelated user changes in the worktree.
- Keep docs and SDDs aligned with the current implementation plan; remove stale
  decisions rather than adding contradictory notes.
