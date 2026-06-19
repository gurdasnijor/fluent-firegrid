# Agent Instructions

These instructions apply to the whole repository.

## Read First

- Read this file before making changes.
- For Effect-specific code style, also read [`LLMS.md`](LLMS.md).
- Prefer existing package-local patterns over inventing new abstractions.
- Keep generated or external reference material out of commits unless explicitly
  requested.

## Effect Code

This repo is Effect-native. New or modified Effect code must follow the local
guidance in [`LLMS.md`](LLMS.md), adapted from the Effect team guidance in
<https://github.com/Effect-TS/effect-smol/blob/main/LLMS.md>.

In short:

- Prefer `Effect.gen` for imperative Effect programs.
- Prefer `Effect.fn("name")` for exported functions that return an `Effect`.
- Do not wrap function bodies in `Effect.gen` when `Effect.fn` is the better
  shape.
- Model services with `Context.Service` and layers.
- Model expected domain failures as typed tagged errors.
- Manage resources through Effect scopes, layers, and `acquireRelease`.

## Repository Workflow

- Use `pnpm` for repository commands.
- Run the narrowest relevant validation for a change. For broad or shared
  changes, run `pnpm preflight`.
- Do not revert unrelated user changes in the worktree.
- Keep docs and SDDs aligned with the current implementation plan; remove stale
  decisions rather than adding contradictory notes.

