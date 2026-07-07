# 6. System Overview

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A stream-first agent substrate is organized around the six managed-agent primitives described by Anthropic's Managed Agents framework:

```txt
Session
Orchestration
Harness
Sandbox
Resources
Tools
```

This RFC does not copy Anthropic's implementation. It adopts the primitive selection because those interfaces describe the stable boundary of a managed-agent harness. The stream-first substrate adds durable logs, replayable projections, claim-first execution, restart recovery, and observability around those primitives.

A stream-first agent substrate contains these planes.

```txt
Application Client Plane
  append intents
  observe projections

Durable Log Plane
  ordered durable facts
  replay
  live tail

Operator Plane
  projection operators
  claimed work operators
  subscriber operators

Live Runtime Plane
  sessions
  agent protocol adapters
  provider handles
  sandbox resources
  conductor/proxy chains

Projection Plane
  materialized state
  live subscriptions
  snapshots

External Integration Plane
  agents
  tools
  human approval UIs
  webhooks
  timers
  audit consumers
```

A common flow:

```txt
client appends launch intent
runtime operator claims launch
provider provisions live resource
session adapter creates agent session
runtime appends session-ready facts
client observes session projection
client appends prompt intent
prompt operator claims prompt
session adapter sends prompt through live session
agent emits updates
runtime appends update/chunk records
runtime appends terminal prompt record
client observes prompt completion
```

## 6.1 The Six Managed-Agent Primitives

Each primitive below has four layers:

```txt
Anthropic-style interface
substrate invariants
streams-as-truth contribution
implementer-facing interface
```

The conformance tests listed here are summarized again in §29.6.

### 6.1.1 Session

Anthropic-style interface:

```txt
getSession(session_id) -> (Session, Event[])
getEvents(session_id, cursor) -> Event[]
emitEvent(session_id, event, options) -> AppendResult
```

Substrate invariants:

```txt
Events are append-only facts.
Events are consumed in durable order from any retained cursor.
Appends are idempotent within a declared producer/key scope.
Session state is reconstructable from events and materializers.
The log, not a live runtime, is the source of session truth.
```

Streams-as-truth contribution:

```txt
durable event log per session or session namespace
opaque cursor/offset semantics
snapshot-first projection reads
materializers as deterministic folds over events
audit and replay from retained events
```

Implementer interface:

```txt
SessionStore.getSession(id, options) -> SessionSnapshot | SessionError
SessionStore.getEvents(id, cursor, options) -> EventBatch | SessionError
SessionStore.emitEvent(id, event, options) -> AppendResult | SessionError
SessionStore.subscribe(id, cursor, options) -> EventBatch | EOF | SessionError
```

Primitive conformance tests:

```txt
append event, replay from beginning, observe same event order
replay from captured cursor, observe only later events
retry emitEvent with same idempotency key, receive original append result
rebuild session materializer from events, match snapshot
start waiter after terminal event, resolve from snapshot before live subscription
```

Detailed log and projection requirements are in §7 and §10. Detailed session promptability requirements are in §14.

### 6.1.2 Orchestration

Anthropic-style interface:

```txt
wake(session_id) -> void
```

The implementation may be a scheduler, queue consumer, subscriber loop, cron job, webhook handler, or control-plane service. The primitive is the ability to resume work for a durable session id with retry semantics.

This RFC uses "Orchestration" here only in Anthropic's primitive sense: `wake(session_id)`. It is not a workflow definition language. Workflow orchestration in the sense of `createFunction`, `step.run`, `step.waitForEvent`, or YAML DAGs is explicitly not a substrate requirement and is rejected as the default application model in §6.3.

Substrate invariants:

```txt
Wake inputs are durable identities, not live process handles.
Wake is idempotent for the same session and wake cause.
Wake does not prove live session ownership; it triggers recovery or progress.
Failures are retryable according to explicit policy.
```

Streams-as-truth contribution:

```txt
wake causes can be durable events
subscriber offsets make scheduler restart safe
claims can fence multiple schedulers
session replay lets a new harness resume after failure
```

Implementer interface:

```txt
Orchestrator.wake(session_id, cause, options) -> WakeReceipt | OrchestrationError
Orchestrator.subscribeWakeSources(cursor, options) -> WakeEventBatch | OrchestrationError
Orchestrator.claimWake(wake_key, owner, options) -> ClaimResult | OrchestrationError
```

Primitive conformance tests:

```txt
append wake-causing event, subscriber observes it and calls wake
wake against live runtime is idempotent no-op or progress signal
concurrent wake attempts create one effective owner or one effective runtime
wake after crash replays session before side effects resume
wake failure can be retried without duplicating terminal state
```

Restart and owner reacquisition rules are in §25.

### 6.1.3 Harness

Anthropic-style interface:

```txt
yield Effect<T> -> EffectResult<T>
```

The harness is the loop that turns session context into effects and records progress. It may run in a model host, local process, browser worker, serverless function, provider-hosted service, or in-process runtime.

Substrate invariants:

```txt
Effectful work is preceded by durable intent or event context.
Multi-worker externally visible effects use durable claims.
Harness progress is appended to the Session.
Replay does not execute side effects.
Suspension and resume are represented by durable events when they affect application state.
```

Streams-as-truth contribution:

```txt
effects and results can be audited as events
claim-first execution fences duplicate workers
durable required actions suspend and resume through the log
materializers expose current harness state without making it truth
```

Implementer interface:

```txt
Harness.run(session_snapshot, effect_context) -> HarnessResult | HarnessError
Harness.yield(effect) -> EffectResult | HarnessError
Harness.resume(session_id, cursor, options) -> HarnessResult | HarnessError
Harness.cancel(session_id, reason) -> CancelResult | HarnessError
```

Primitive conformance tests:

```txt
prompt/effect dispatch claims before external side effect
replay does not re-run tools or prompts
required action suspends through a durable pending event
resolution resumes waiter through projection/log observation
runtime crash loses in-memory continuation but not durable suspension state
```

Operator and claim mechanics are detailed in §13. Required-action suspension is detailed in §21.

### 6.1.4 Sandbox

Anthropic-style interface:

```txt
provision({ resources }) -> SandboxHandle
execute(name, input) -> String
```

The Sandbox is an execution environment configured once and called many times as a tool or adapter target. It may be a local process, container, VM, remote API, browser worker, phone, simulator, or in-process executor.

Substrate invariants:

```txt
Provisioning facts that affect application behavior are durable.
Sandbox handles are live resources and are not durable truth.
Ready resource state does not imply session promptability.
Provider execute is adapter behavior, not the generic prompt dispatch seam.
Cleanup policy is explicit and durable when application-visible.
```

Streams-as-truth contribution:

```txt
resource lifecycle events are replayable
provider readiness can be projected
provider failures are typed durable facts
resource handles can be reacquired or classified lost after restart
```

Implementer interface:

```txt
SandboxProvider.provision(spec, resources, options) -> SandboxHandle | ProviderError
SandboxProvider.ready_check(handle) -> ReadyState | ProviderError
SandboxProvider.execute(handle, name, input, options) -> ExecutionResult | ProviderError
SandboxProvider.stop(handle, reason) -> StopResult | ProviderError
SandboxProvider.cleanup(handle, policy) -> CleanupResult | ProviderError
```

Primitive conformance tests:

```txt
provision appends requested/ready or requested/failed lifecycle records
ready sandbox can execute a named tool or adapter command
provider handle is not treated as durable after restart
cleanup emits durable terminal resource state when visible to clients
provider execute does not bypass prompt/session invariants unless the provider API is the adapter
```

Provider and resource lifecycle details are in §18.

### 6.1.5 Resources

Anthropic-style interface:

```txt
[{ source_ref, mount_path }]
```

Resources describe inputs made available to a Sandbox or Tool by reference. A source reference may point to an object store, git repository, file bundle, secret reference, mounted volume, generated artifact, or provider-native resource.

Substrate invariants:

```txt
Resource specs are serializable.
Resource content or authority is referenced, not smuggled through process memory.
Mount/provision results that affect application behavior are durable.
Secrets are referenced or vaulted, not logged in plaintext.
Physical mounts and virtual file backends have explicit boundaries.
Artifact evidence records reference content by digest or object reference instead of embedding large or sensitive payloads.
```

Streams-as-truth contribution:

```txt
resource mount lifecycle can be audited
resource projections expose what was made available
artifact refs can be durable without embedding payloads
cross-runtime resource observations can be reconstructed from event refs
```

Implementer interface:

```txt
ResourceResolver.resolve(source_ref, context) -> ResourceDescriptor | ResourceError
ResourceMounter.mount(resource, mount_path, sandbox) -> MountResult | ResourceError
ResourceMounter.unmount(resource, sandbox, policy) -> CleanupResult | ResourceError
```

Primitive conformance tests:

```txt
resource spec serializes and replays
mount result is durable or explicitly ephemeral
agent can access mounted resource through the declared path or virtual backend
secret resource writes only references to durable records
generated artifact writes emit durable metadata without inline content when payload retention belongs to a resource store
cleanup or retention policy is observable
```

Resource and sandbox details are in §18 and §19.

### 6.1.6 Tools

Anthropic-style interface:

```txt
{ name, description, inputSchema }
```

Tools are capabilities available to a Harness. A tool may be backed by sandbox execution, MCP, HTTP, gRPC, local functions, vendor APIs, peer agents, or protocol-native actions.

Tool catalogs are declared by topology/tool components. A component declaration
names the component identity, descriptor set, handler or transport binding,
credential references, ordering, and policy before the session is initialized.
Durable descriptor rows, when emitted, are projection/materialization artifacts
derived from that topology declaration. They are not an author-facing side
channel for registering tools.

The abstract descriptor exposed to an agent **MUST** contain only:

```txt
name
description
inputSchema
```

The descriptor **MUST NOT** expose transport handles, credential material, runtime host ids, node ids, provider internals, or implementation object types. A capability reference may pair the descriptor with transport and credential references for host use:

```txt
CapabilityRef = descriptor + transportRef + credentialRef?
```

`transportRef` and `credentialRef` are launch/session plumbing, not agent-visible descriptor fields. Live dispatch through a transport reference is an adapter/profile concern; this RFC requires the declaration boundary, not a specific transport invocation protocol.

Substrate invariants:

```txt
Tool descriptors are serializable and protocol-neutral.
Tool names are stable within their declared scope.
Tool descriptors are computed, validated, and frozen before session initialization completes.
Tool catalogs are derived from declared topology/tool components, not arbitrary runtime publication.
The visible tool set remains stable for the session lifetime.
Tool invocation effects follow Harness claim/replay rules when externally visible.
Credentials are resolved by reference and policy, not embedded in descriptors.
Same-name tool collisions resolve deterministically.
```

If multiple topology/conductor components attach a tool with the same visible name, the implementation **MUST** define a deterministic collision rule before session initialization. Ordered attachment systems SHOULD use first-valid-attach-wins; other policies are allowed only if they are stable under replay and surface conflicts in diagnostics or projection state. The losing descriptor **MUST NOT** appear nondeterministically. Within a single descriptor publication batch, duplicate visible names **SHOULD** be rejected before agent exposure rather than silently folded.

Streams-as-truth contribution:

```txt
tool registration can be represented as init/session events
tool descriptor rows are derived from topology materialization
tool calls can be audited as effects
approval gates can suspend concrete tool calls durably
tool result projections can be rebuilt from session events
```

Implementer interface:

```txt
ToolTopology.declare(component_spec, scope) -> ComponentRegistration | ToolError
ToolCatalog.materialize(topology, session_context) -> FrozenToolCatalog | ToolError
ToolCatalog.list(scope) -> ToolSpec[]
ToolInvoker.invoke(name, input, context) -> ToolResult | ToolError
```

Primitive conformance tests:

```txt
tool descriptor exposes name/description/inputSchema without transport leakage
capability ref separates descriptor from transportRef and credentialRef
tool catalog materializes from declared topology components before session initialization
same-name collision policy is deterministic
tool call can be approved/denied through required-action rows
tool invocation result is appended or represented in session events
credential resolution happens at call time through references
descriptor validation fails before agent exposure
runtime context binding does not add, remove, or rename visible tools
```

Tool-call approval and middleware lowering are detailed in §17 and §21.
