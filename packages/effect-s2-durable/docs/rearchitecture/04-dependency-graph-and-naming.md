# Dependency Graph And Naming Axis

## What The Graph Shows

The PR dependency graph shows "changed files plus everything that can reach
them." It is not a correctness failure by itself, but it makes the comprehension
problem visible:

- `engine/live.ts` is still the largest implementation node. Public modules now
  reach only `engine/api.ts` for the public service tag and API types, while
  live engine assembly reaches most engine/object implementation modules because
  it implements execution.
- The root barrel `index.ts` is now intended to be the authoring surface. Keep
  server/client/host adapters behind subpath exports so the default package
  surface does not become another mixed drawer.
- `DurableStores` is a mixed storage service: it opens service workflow DBs and
  the global roster, but also owns `ObjectOwnerDriver`. Completion, resolution, and
  primitive modules then appear to depend on all storage.
- `object/log.ts` and `object/drive-session.ts` reach directly into `effect-s2`,
  while service state reaches `effect-s2-stream-db`. That is conceptually a
  stream/table split, but it currently looks like two unrelated storage stacks.

## Vocabulary Reset

The word `runtime` did too much work. In the old layout it meant:

- public Effect service tag;
- ambient API that primitives call from handlers;
- S2-backed engine layer;
- in-process state;
- durable storage openers;
- handler execution and recovery logic;
- deployable host process in some prose.

Use semantic names instead:

| Concept | Preferred internal name |
| --- | --- |
| public engine API | `DurableEngine` / `DurableEngineApi` |
| engine assembly | `EngineLive` |
| active handler context | `ActiveInvocation` |
| handler authoring capabilities | `CurrentInvocationScope` / `InvocationScope` |
| primitive implementation | capability modules under `invocation/` |
| service execution lifecycle | `ServiceExecutor` |
| object execution lifecycle | `ObjectExecutor` |
| child call dispatch | `ChildCallCoordinator` |
| shared read-only calls | `SharedObjectRunner` |
| completion reads | `ResultReader` |
| external resolutions | `ResolutionRouter` |
| storage access | `S2Access`, `ServiceStores`, `ObjectStores` |
| process shell | `Host` |

New implementation modules should not be placed under a generic `src/runtime`
namespace.

## Target Graph

```text
index.ts / subpath exports
  -> public authoring types, primitives, clients

authoring/primitives.ts / handler-scoped invocation clients
  -> invocation/scope.ts

root clients / service-layer.ts / ingress adapters
  -> engine/api.ts

engine/live.ts
  -> engine/* semantic modules
  -> invocation/scope.ts
  -> ServiceStores / ObjectStores

object/owner-driver.ts
  -> object/machine/index.ts
  -> object/log.ts
  -> effect-s2-stream-db EventStream<ActorEvent>
  -> effect-s2
```

The target is not a graph with fewer files. The target is a graph where each edge
answers a clear question: public API, state-machine decision, driver action,
storage substrate, or edge adapter.

## Entrypoints

The package has explicit subpath exports by audience:

```json
{
  "./engine": "./src/engine/index.ts",
  "./ingress": "./src/ingress/index.ts",
  "./client": "./src/ingress/client.ts",
  "./host": "./src/host.ts"
}
```

`./client`, `./engine`, `./host`, and `./ingress` give consumers clear targets.
The root barrel is the authoring surface, not the server/client grab bag.

The root should read as the authoring surface:

- definitions;
- primitives;
- handler helpers;
- schemas/types;
- public engine service tag.

Server, client, and host adapters live behind subpaths.

## Mixed Stores

Replace `DurableStores` with narrower services:

```text
ServiceStores
  roster
  openWf

ObjectStores
  objectDriver

S2Access
  client
  provideClient
```

This avoids making every engine helper that needs one store appear to depend on
all storage. `ResultReader`, `ResolutionRouter`, and `HandlerPrimitives` should
depend on the smallest service that matches the method they implement.
