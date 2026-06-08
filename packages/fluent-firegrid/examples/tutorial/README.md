# Fluent Firegrid Tutorial Examples

This folder tracks the shape of Restate sdk-gen's tutorial examples while
staying honest to the current fluent-firegrid slice.

Spec refs: `fluent-firegrid-keystone.EXAMPLES.1`,
`fluent-firegrid-keystone.EXAMPLES.2`,
`fluent-firegrid-keystone.EXAMPLES.4`.

Implemented:

- `src/01-basics.ts` — a Firegrid-shaped durable step pipeline using
  generator handlers plus free `run`, `all`, `race`, and `select`.
- `src/02-spawn.ts` — Restate-shaped local `spawn` affordance composed with
  `all`, `race`, and `select`; durable child sessions are separate.
- `src/03-timeout.ts` — timeout branching with local `Effect.race` and
  `Effect.sleep`.
- `src/07-state.ts` — deferred placeholder for the later state surface.
- `src/08-clients.ts` — typed call/send clients derived from definition
  descriptors.
- `src/09-workflows.ts` — a workflow-shaped surface using
  `workflow({ name, handlers })`.
- `src/10-ifaces.ts` — descriptor-only contract plus typed implementation.

Deferred until the package exposes the matching primitives:

| family | missing substrate |
|---|---|
| durable sleep | timer intent, subscription wake, and redrive |
| durable wait | CEL predicate wait intent, match wake, and redrive |
| state | state collections and concurrency semantics |
| retry | journaled retry policy and attempt classification |
| saga | durable compensation steps and compensation ordering helpers |
| cancellation | durable cancellation events and AbortSignal fanout |
| workflow promises | workflowPromise, attach, key, and shared workflow handler semantics |
| serdes | runtime input/output serde hooks |

`src/server.ts` exports a registry instead of starting an HTTP endpoint because
fluent-firegrid does not have an endpoint/server package yet.

The workflow tier is intentionally narrower than Restate's workflow tutorial:
it models workflow handlers over one caller-supplied journal endpoint. Workflow
promises, workflow keys, attach/cancel, and shared workflow handlers are still
deferred.

## API Affordance Parity

| Restate tutorial affordance | Fluent Part 1 shape |
|---|---|
| generator service handlers | generator handlers accepted by `service` |
| `run(action, { name })` | same authoring shape, backed by named journal steps |
| `all([...])` | free helper over Effect concurrency |
| `race([...])` | free helper over Effect race semantics |
| `select({ tag })` | free tagged race helper returning `{ tag, future }` |
| `spawn(op)` | local Effect fiber affordance; durable child-session spawn is deferred |
| typed clients | `client` / `sendClient` over a runtime-provided ingress |
| interface descriptors | `iface.service` plus `implement` |
