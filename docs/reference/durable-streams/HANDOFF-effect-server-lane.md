# Handoff — Durable Streams `effect-durable-streams` Server Lane

Repo: `gurdasnijor/durable-streams`.

## Status

- **PR #10 merged** into `main` on 2026-06-09.
- Merge commit: `52c543636a1525a26900c0d0a00f609f7f103a47`.
- Final PR head: `afa49f486e651733d76a17dd75d3e803469c9f2d`.
- Agent2 completed a scoped review. GitHub could not record a formal approval
  because the authenticated account was the PR author, so the approval-intent
  comment is here:
  `https://github.com/gurdasnijor/durable-streams/pull/10#issuecomment-4659422914`.

## Local Gates Run Before Merge

- `pnpm --filter effect-durable-streams typecheck`
- `pnpm --filter effect-durable-streams lint`
- `pnpm --filter effect-durable-streams test`
- `pnpm format:check`

CI status at merge time:

- Core CI build, test, conformance, autofix, changeset, and client conformance
  checks were green.
- `publish` failed because `pkg-pr-new` is not installed for the repository
  GitHub app. This is infrastructure/configuration, not a server-slice failure.
- A late benchmark job was still running when the merge was performed; benchmark
  jobs are not correctness gates for this slice.

## Implemented Slice

This is the memory-backed Effect-native server slice. It implements the base
stream data plane over the domain-shaped `Store` algebra:

- `PUT /v1/stream/*`
- `POST /v1/stream/*`
- `HEAD /v1/stream/*`
- `GET /v1/stream/*`
- `DELETE /v1/stream/*`

Implemented behavior includes:

- Slash-containing stream paths round-trip through the wildcard route without
  truncation.
- Reserved `/v1/stream/__ds/*` paths are rejected before wildcard stream
  creation.
- Append decisions distinguish plain accepted appends from producer accepted
  appends so `204` vs `200` status mapping is preserved.
- Producer duplicate, fencing, sequence gap, idempotent close retry, and
  epoch-advance-with-nonzero-seq are explicit `AppendDecision` variants.
- In-memory store uses STM and per-stream byte offsets.
- Protocol numeric/header decoding goes through Effect Schema.
- Protocol/domain errors implement `HttpServerRespondable`, so route handlers
  can fail through the framework instead of locally reimplementing error
  response lowering.

## Current Module Shape

`packages/effect-durable-streams/src`:

- `Store.ts` — protocol/domain store algebra and decision types.
- `MemoryStore.ts` — STM-backed memory implementation.
- `schema.ts` — Store-free wire schemas and content-type normalization.
- `headers.ts` — Store-free protocol header constants.
- `ProtocolError.ts` — typed protocol/domain errors that are also HTTP
  respondable.
- `http/StreamRequest.ts` — Effect/platform request decoding:
  `schemaPathParams`, `schemaHeaders`, `schemaSearchParams`, raw byte body.
- `http/StreamResponse.ts` — store decision and read/head result to HTTP
  response mapping.
- `StreamHttp.ts` — service boundary that owns route-to-store actions and route
  spans.
- `routes/Stream.ts` — thin `HttpRouter` composition only.
- `Server.ts` — server layer and launch surface; no exported router.
- `test/support/start-server.ts` — boots the real production `Server.layer` with
  an injected Node HTTP server for observable ephemeral-port tests.

## Scope Still Out

Do not claim these from PR #10:

- Durable SQL/PGlite/Postgres store.
- Wake/subscriptions/pull-wake/webhooks/schedules/filters.
- Long-poll and SSE live reads.
- Fork stitching, retention, full ETag and TTL behavior.
- JSON append normalization and one-level array flattening.
- Shared `HttpApi` control plane implementation.

## Open Follow-Up Work

- PR #7 remains open for execution conformance work.
- PR #8 remains open for producer observability placement documentation.
- Draft PR #11 tracks the stricter tooling/diagnostics follow-up from
  `codex/strict-tooling-followup`. It is visible for morning review and is not a
  stale PR #10 branch.

Suggested next server milestones:

1. Build the SQL/PGlite store behind the existing `Store` algebra and add
   durable-backend conformance for restart/flush and producer isolation.
2. Add notification transport and live read semantics before advertising
   long-poll/SSE.
3. Implement the shared `HttpApi` control plane from
   `packages/durable-streams-protocol` when touching subscription/schedule/JWKS
   routes.
