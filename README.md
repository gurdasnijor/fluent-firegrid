# fluent-firegrid

The standalone **fluent-firegrid** monorepo: an Effect-native substrate for durable agent coordination built over [Durable Streams](https://github.com/gurdasnijor/durable-streams). It provides a fluent, Schema-first API for composing named, replay-safe durable steps (spawn, retry, timeout, saga, cancel, state) on top of a Stream-shaped read / Sink-shaped write durable log — extracted, lean, from the larger firegrid workspace. See the canonical architecture notes in [`docs/cannon/architecture/fluent/README.md`](docs/cannon/architecture/fluent/README.md).

## Packages

- **`@firegrid/fluent-firegrid`** — Effect-native Firegrid primitives with named durable steps; the substrate-free Operation/Future scheduler.
- **`@firegrid/fluent-acp-process`** — ACP harness process owner: spawn/kill an ACP agent and expose its `acp.Stream`.

## Legacy Reference

The pre-fluent `effect-durable-*`, `firelab`, and `fluent-runtime` packages were
archived on [`archive/legacy-effect-firelab`](https://github.com/gurdasnijor/fluent-firegrid/tree/archive/legacy-effect-firelab).
They are reference material only; replacement packages should be rebuilt on top
of the fluent store/protocol/client/server modules.
