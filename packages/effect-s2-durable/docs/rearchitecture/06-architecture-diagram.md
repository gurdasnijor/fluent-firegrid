# Architecture Diagram

This diagram reflects the consolidated layout after moving engine internals under
`src/engine/` and co-locating object protocol semantics under
`src/object/machine/`.

## Component View

```mermaid
flowchart TB
  subgraph Public["Public authoring and host surface"]
    Definition["definition.ts\nservice/object/workflow definitions"]
    InvocationClient["invocation-client.ts\nin-process clients + workflow helpers"]
    ServiceLayer["service-layer.ts\ncatalog -> live engine layer"]
    Primitives["primitives.ts\nrun/sleep/state/signal/attach/poll"]
    Ingress["ingress/*\nHTTP contract, server, client"]
    Host["host.ts / bin/host.ts"]
    EngineApi["engine/api.ts\nDurableEngine\npublic engine API"]
  end

  subgraph Engine["engine/*\nco-located engine internals"]
    EngineLive["live.ts\nlive engine layer"]
    EngineState["state.ts\nEngineState\nscope, registries, running fibers, waiters"]
    Context["context.ts\nActiveInvocation\nservice/object/shared invocation context"]
    HandlerPrimitives["handler-primitives.ts\nrun/sleep/state/deferred primitives"]
    ResultReader["result-reader.ts\nattach/poll"]
    ResolutionRouter["resolution-router.ts\nexternal signal resolution"]
    DurableStores["durable-stores.ts\nS2 client, roster, WorkflowDb opener,\nobject driver"]
    EngineHelpers["helpers.ts\ncodec/error/retry helpers"]
    Address["address.ts\nservice vs object id routing"]
    ServiceDeferreds["service-deferreds.ts\nlocal service waiter poke + durable deferred row"]
  end

  subgraph Object["object/*\nobject owner vertical"]
    OwnerDriver["owner-driver.ts\nread/fold/decide/append/run action"]
    DriveSession["drive-session.ts\nS2 fence claim + fenced appends"]
    ObjectLog["log.ts\nowner stream read/append adapter"]

    subgraph Machine["object/machine/*\npure durable protocol"]
      MachineIndex["index.ts\nmachine import surface"]
      MachineModel["model.ts\nActorEvent, ActorSnapshot,\ntransition, replay, call-id codecs"]
      MachineCommands["commands.ts\nObjectCommand, decide,\nObjectDecision, ObjectDriverAction"]
    end
  end

  subgraph S2["S2 substrate"]
    S2Client["effect-s2\nclient, append, guarded append, read"]
    StreamDb["effect-s2-stream-db\nWorkflowDb, roster tables\nfuture EventStream<ActorEvent>"]
  end

  InvocationClient --> EngineApi
  ServiceLayer --> EngineLive
  Primitives --> EngineApi
  Ingress --> EngineApi
  Host --> EngineApi
  Host --> EngineLive

  EngineLive --> EngineApi
  EngineLive --> EngineState
  EngineLive --> HandlerPrimitives
  EngineLive --> ResultReader
  EngineLive --> ResolutionRouter
  EngineLive --> DurableStores

  HandlerPrimitives --> Context
  HandlerPrimitives --> EngineState
  HandlerPrimitives --> DurableStores
  HandlerPrimitives --> ServiceDeferreds
  HandlerPrimitives --> MachineModel

  ResultReader --> Address
  ResultReader --> EngineState
  ResultReader --> DurableStores

  ResolutionRouter --> Address
  ResolutionRouter --> EngineState
  ResolutionRouter --> DurableStores
  ResolutionRouter --> ServiceDeferreds

  DurableStores --> OwnerDriver
  DurableStores --> S2Client
  DurableStores --> StreamDb

  OwnerDriver --> MachineIndex
  OwnerDriver --> ObjectLog
  OwnerDriver --> DriveSession

  MachineIndex --> MachineModel
  MachineIndex --> MachineCommands
  MachineCommands --> MachineModel

  ObjectLog --> S2Client
  DriveSession --> S2Client
```

## Object Owner Command Loop

The object path is the clearest expression of the intended architecture:

```mermaid
sequenceDiagram
  autonumber
  participant Engine as DurableEngine
  participant Driver as object/owner-driver.ts
  participant Log as object/log.ts
  participant Machine as object/machine
  participant Session as object/drive-session.ts
  participant Handler as user handler
  participant S2 as S2 owner stream

  Engine->>Driver: admit(callId, object/key/method, input)
  Driver->>Log: read owner stream
  Log->>S2: read records
  S2-->>Log: ActorEvent records
  Log-->>Driver: LogEntry[]
  Driver->>Machine: replay(entries)
  Machine-->>Driver: ActorSnapshot
  Driver->>Machine: decide(Admit)
  Machine-->>Driver: Accepted event or Already*
  Driver->>Log: CAS append Accepted
  Log->>S2: conditional append

  Engine->>Driver: drain(object, key, runHead)
  Driver->>Log: read owner stream
  Log->>S2: read records
  S2-->>Log: ActorEvent records
  Driver->>Machine: replay + decide(SelectNextHead)
  Machine-->>Driver: RunHead action or none

  alt has pending head
    Driver->>Session: open owner drive session
    Session->>S2: fence stream with host token
    Driver->>Handler: run selected head with ObjectStateBackend
    Handler->>Driver: state/journal/signal primitive calls
    Driver->>Machine: decide(StateGet/StateSet/JournalPut/AwaitSignal/ResolveSignal)
    Machine-->>Driver: result + ActorEvent[] + ObjectDriverAction[]
    Driver->>Session: append emitted events under fence
    Session->>S2: guarded append
    Handler-->>Driver: ActorExit
    Driver->>Machine: decide(Complete)
    Machine-->>Driver: Completed event
    Driver->>Session: append Completed under fence
    Session->>S2: guarded append
  else no pending head
    Driver-->>Engine: quiescent
  end
```

## Folder View

```mermaid
flowchart LR
  Root["src/"] --> EngineApiFile["engine/api.ts\npublic engine API"]
  Root --> EngineDir["engine/\nengine-local modules"]
  Root --> ObjectDir["object/\nobject owner vertical"]
  Root --> IngressDir["ingress/\nHTTP/client adapters"]
  Root --> Authoring["definition.ts\ninvocation-client.ts\nservice-layer.ts\nprimitives.ts\nhandler.ts\ntypes.ts\nschema.ts"]

  EngineDir --> EngineFiles["live.ts\naddress.ts\ncontext.ts\ndurable-stores.ts\nhandler-primitives.ts\nhelpers.ts\nresolution-router.ts\nresult-reader.ts\nservice-deferreds.ts\nstate.ts"]

  ObjectDir --> MachineDir["machine/\nmodel.ts\ncommands.ts\nindex.ts"]
  ObjectDir --> ObjectFiles["owner-driver.ts\nlog.ts\ndrive-session.ts"]

  IngressDir --> IngressFiles["client.ts\ncontract.ts\nserver.ts"]
```

## Review Questions

- Should the next pass split `engine/live.ts` into executor modules, or should
  storage substrate work land first?
- Is `engine/durable-stores.ts` still too broad, or is that acceptable until the
  storage substrate work introduces `S2Access`, `ServiceStores`, and
  `ObjectStores`?
- Should `ObjectOwnerDriver` call only `decide(...)`, or is it acceptable for it
  to keep using typed helpers like `stateGet(...)` where that preserves better
  return types?
- Should `object/log.ts` move behind `effect-s2-stream-db` `EventStream` before
  snapshot/trimming work, or at the same time?
