# 6.2 Stream-First Additions Over Anthropic Managed Agents

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

The six primitives above are sufficient to describe a managed-agent harness. This RFC adds stream-first substrate guarantees around them:

| Addition | What it contributes |
| --- | --- |
| Streams-as-truth | Durable event logs are the source of truth; projections are derivable caches. |
| Identity vs live ownership | A durable id names a thing that happened or exists; it does not prove the current runtime owns a live handle. |
| Claim-first execution | Multi-worker externally visible side effects require durable claim records before execution. |
| Restart-safe replay | Operators replay to a live boundary before side effects and reconstruct waits, claims, timers, and projections from records. |
| Live promptability gate | A session id in durable state does not prove the runtime owns a live promptable session. |
| Materializer pipeline | Derived query state is a fold over the Session event log. |
| Conductor/proxy chain | Middleware and topology are Component combinators over the Harness primitive. |

These additions are not assumptions of Anthropic's managed-agents framework itself. They are the durability, observability, and recovery constraints this RFC adds for stream-first managed-agent systems.

# 6.3 Choreography-First Application Layer

On top of the six managed-agent primitives and streams-as-truth semantics, this RFC defines a choreography-first application layer. The LLM owns sequence, branching, parallelism, and recovery at runtime. The prompt describes the goal in natural language; durability comes from the tools the model can call.

This follows the choreography-vs-orchestration reasoning cited in §37: hand-crafted workflow control flow is the agent-era version of the Bitter Lesson mistake. A developer-authored DAG, language-specific `step.run` function, or YAML workflow freezes assumptions about ordering, branching, timeouts, and parallelism. A choreography-first system gives the model durable primitives and makes the resulting dynamic schedule observable.

The canonical agent-facing choreography tool surface is:

| Tool | Contract | Backing primitive(s) | Substrate semantics |
| --- | --- | --- | --- |
| `wait_for(event, prompt?)` | Durably suspend until an event/projection matches (optional timeout). With no `prompt`, resolve inline; with `prompt`, wake the session with it as a new turn. | Session + Orchestration + Projection (+ Timer) | Append wait intent, evaluate snapshot-first, subscribe after cursor, optionally arm timeout, resolve from durable match/timeout record, append the prompt as input on resolve. |
| `wait_until(time, prompt?)` | Durably suspend until an absolute or relative time. Same `prompt?` semantics — subsumes scheduled self-prompts. | Session + Orchestration + Timer | Append timer intent, arm timer operator, resolve a durable completion when time is reached, append the prompt as input on resolve (with live-promptability checks). |
| `sleep(duration)` | Thin alias for `wait_until("+duration")` with no prompt. | Session + Orchestration + Timer | As `wait_until`, relative-only, prompt-free. |
| `spawn(agent, prompt)` | Run a child agent and durably await completion. | Session + Harness + Orchestration | Append child-session intent, claim child work before launch side effect, project child terminal state, resolve parent wait from projection. |
| `spawn_all(tasks)` | Fan out child agents and durably await all completions. | Session + Harness + Orchestration | Append N child intents or one fanout intent that expands durably, track each child by semantic key, resolve aggregate when all terminal keys resolve. |
| `execute(sandbox, input)` | Execute against a named lazily provisioned sandbox/tool target. | Sandbox + Tools + Harness | Resolve/provision sandbox, claim externally visible tool work when multi-worker, dispatch sandbox tool call, append result or failure as durable records. |

The suspension family (`wait_for`, `wait_until`, `sleep`) shares one shape:
`wait_<axis>(target, prompt?)` where `axis` is `for` (event/projection) or
`until` (time). The optional `prompt` is the proactivity lever — without it the
wait resolves inline; with it the session suspends durably and wakes with the
prompt as a new turn (a scheduled or event-triggered self-nudge). This replaces
the earlier separate `schedule_me`.

Each choreography primitive **MUST** append durable trace/session records before it suspends, fans out, or invokes externally visible work. Each **MUST** be observable by humans and by agents through the same stream-derived observation plane. The model may choose to call these tools in any order allowed by policy; the substrate provides durability, claims, projection, and recovery, not a pre-authored workflow.

Workflow orchestration remains an allowed external integration. A team MAY drive a conforming substrate from Temporal, Inngest, Trigger.dev, cron, a queue consumer, or a for-loop. A conforming substrate **MUST NOT** require such a workflow engine for normal agent progress, and it **SHOULD NOT** ship its own workflow orchestration SDK as the primary application model.

# 6.4 Component Combinator Algebra

Conductor components, middleware, tool registration, resources, and many product conveniences are not new primitives. They are combinators over the six primitives.

A conductor component is a wrapper around a Harness:

```txt
type Harness = Effect -> EffectResult
type Component = Harness -> Harness
compose(components...) = reduceRight(wrap, base_harness)
```

The seven base combinators are:

| Combinator | Type signature | Touches primitive(s) | Typical use |
| --- | --- | --- | --- |
| `observe(sink)` | `(Effect) -> void` -> `Component` | Harness, external sink | logging, metrics |
| `mapEffect(fn)` | `(Effect) -> Effect` -> `Component` | Harness | context injection, prompt rewrite |
| `appendToSession(mk)` | `(Effect) -> Event` -> `Component` | Session + Harness | audit, durable trace |
| `filter(pred, reject)` | `(Effect) -> bool`, `() -> EffectResult` -> `Component` | Harness | budget/policy gate |
| `substitute(rewrite)` | `(Effect) -> Effect` -> `Component` | Harness, Tools/Sandbox as needed | peer call routing, tool dispatch |
| `suspend(reason)` | `(Effect) -> SuspendReason?` -> `Component` | Session + Orchestration + Harness | approval gate, durable wait |
| `fanout(split, merge)` | `(Effect) -> Effect[]`, `(EffectResult[]) -> EffectResult` -> `Component` | Harness, optionally Session/Orchestration | parallel tools, multi-peer dispatch |

Component composition rule:

```txt
compose(a, b, c)(base_harness) = a(b(c(base_harness)))
```

Components **MUST** be describable by serializable specs when they are part of launch/session topology. Runtime closures may implement the component locally, but the durable topology is data.

# 6.5 Tools, Resources, and Middleware as Components

Tools registration is a special case of `mapEffect` on the initialization effect:

```txt
registerTool(tool_spec): Component
  = mapEffect(effect.kind == "init" ? add tool_spec to available_tools : effect)
```

Resources are init-time components with a single-fire mount/provision constraint:

```txt
provisionResources(resource_refs): Component
  = on init, resolve and mount resources before calling next harness
```

Middleware is a serializable spec lowered into combinators:

```txt
approve({ scope: "tool_calls" })
  -> { kind: "approval_gate", scope: "tool_calls", ... }
  -> suspend(tool_call_requires_approval) + appendToSession(permission_event)
```

This prevents approval, policy, or tool routing from becoming hidden runtime callbacks. They remain topology data lowered into operators/adapters over the log.

# 6.6 Materializers as Folds

The stream-first substrate has two pure composition layers over the six primitives.

Materializer pipeline:

```txt
type Materializer<S> = (Event, S) -> S
fold Session event log -> derived query state
```

Conductor proxy chain:

```txt
type Component = Harness -> Harness
fold Component list over base Harness -> wrapped Harness
```

Both layers are pure composition. Both are replayable when their inputs are durable specs/events. Both decompose into operations over the six primitives instead of creating new substrate primitives.

# 6.7 Round-Trip Principle

Anyone reading Anthropic's managed-agents post should be able to point at a stream-first managed-agent feature and ask, "which primitive plus which combinator?" A conforming substrate should answer in one sentence.

| Feature | Primitive(s) | Combinator / operation |
| --- | --- | --- |
| Audit trace | Session + Harness | `appendToSession(effect/result event)`. |
| Context injection | Harness | `mapEffect(add context)`. |
| Budget or policy block | Harness | `filter(policy, reject)`. |
| Approval gate | Session + Orchestration + Harness + Tools | `suspend(tool call)`, append permission row, wake on resolution. |
| Tool registration | Tools + Harness | `mapEffect(init -> add tool spec)`. |
| Resource mount | Resources + Sandbox + Harness | init-time resource component before base harness. |
| Peer or child agent dispatch | Harness + Orchestration + Session | `substitute(peer effect)` plus durable wake/session events. |
| Parallel tool calls | Harness + Tools | `fanout(split calls, merge results)`. |
| Prompt state projection | Session | materializer fold over prompt/chunk/terminal events. |
| Runtime/session dashboard | Session + Sandbox + Orchestration | materializer products over lifecycle, claim, and resource events. |
| ACP adapter | Session + Harness | adapter maps ACP `session/*` wire events to neutral session/prompt/tool events. |

If a feature cannot be expressed as one of the six primitives plus the combinator/fold vocabulary, that is a design smell. It may be a product object above the substrate, or it may justify a future primitive only after the existing primitives fail to model it.
