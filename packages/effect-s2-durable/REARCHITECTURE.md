# effect-s2-durable Rearchitecture Index

This is the entrypoint for the `effect-s2-durable` rearchitecture handoff.

The previous monolithic document mixed state-machine design, storage substrate
choices, naming cleanup, Effect service composition, migration ordering, and test
strategy. Those are now split into focused documents under
[`docs/rearchitecture`](./docs/rearchitecture/).

## Read Order

1. [`00-overview.md`](./docs/rearchitecture/00-overview.md)
   - product invariants;
   - what is changing structurally;
   - non-goals and acceptance criteria.
2. [`01-state-machine.md`](./docs/rearchitecture/01-state-machine.md)
   - the primary design axis;
   - command / decision / event / action model;
   - Restate command-processing inspiration;
   - why `ObjectOwnerDriver` should become a driver.
3. [`02-storage-substrate.md`](./docs/rearchitecture/02-storage-substrate.md)
   - `effect-s2-stream-db` stream/table split;
   - `EventStream<ActorEvent>`;
   - snapshots, trimming, and recovery direction.
4. [`03-engine-boundaries.md`](./docs/rearchitecture/03-engine-boundaries.md)
   - public engine API;
   - `engine/api.ts` public service tag and API types;
   - `engine/live.ts` live engine layer and assembly;
   - semantic internal boundaries and Effect layer composition.
5. [`04-dependency-graph-and-naming.md`](./docs/rearchitecture/04-dependency-graph-and-naming.md)
   - what the PR dependency graph is showing;
   - why "runtime" is muddy;
   - target package graph and entrypoint split.
6. [`05-migration-and-tests.md`](./docs/rearchitecture/05-migration-and-tests.md)
   - ordered build plan;
   - testing strategy;
   - validation gates.
7. [`06-architecture-diagram.md`](./docs/rearchitecture/06-architecture-diagram.md)
   - reviewable Mermaid diagrams for the current proposed module layout;
   - object owner command loop;
   - open review questions.

## One-Sentence Architecture

The durable protocol should be centered in a pure state machine; Effect services
and modules around it should be ports, drivers, and adapters for S2, handler
execution, timers, waiters, ingress, and host lifecycle.

## Product Invariants

This rearchitecture does not change the product model:

- one public engine service, exported as `DurableEngine`;
- user code remains plain `Effect.gen` plus free primitives;
- object executions remain backed by S2 owner streams;
- S2 fencing remains the cross-host write-correctness mechanism;
- snapshots and trimming are the default recovery-cost strategy;
- lease / heartbeat / claim-sweep remain optional, only for prompt peer takeover.

## Canonical References

- [`DESIGN.md`](./DESIGN.md): compact product and build-agent handoff.
- [`docs/sdds/effect-durable-execution-sdd.md`](../../docs/sdds/effect-durable-execution-sdd.md)
- [`docs/sdds/effect-s2-durable-consolidation-sdd.md`](../../docs/sdds/effect-s2-durable-consolidation-sdd.md)
- [`docs/sdds/effect-s2-durable-host-process-model-sdd.md`](../../docs/sdds/effect-s2-durable-host-process-model-sdd.md)
- [`docs/sdds/effect-s2-stream-db-relational-ivm-sdd.md`](../../docs/sdds/effect-s2-stream-db-relational-ivm-sdd.md)
