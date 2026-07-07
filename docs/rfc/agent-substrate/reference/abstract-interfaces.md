# 6.8 Abstract Component Interfaces

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

This section defines the minimum abstract interfaces used by the rest of this RFC. Implementations MAY expose different concrete APIs, but a conforming implementation MUST provide equivalent semantics.

```txt
DurableLog.append(stream, records, options) -> AppendResult | LogError
DurableLog.read(stream, cursor, options) -> RecordBatch | LogError
DurableLog.tail(stream, cursor, options) -> RecordBatch | EOF | LogError
DurableLog.close(stream, options) -> CloseResult | LogError
DurableLog.ensure_stream(stream, options) -> StreamHandle | LogError
```

`AppendResult` MUST identify the durable position of every accepted record. `RecordBatch` MUST preserve log order. `LogError` SHOULD distinguish conflict, unavailable, permission denied, invalid record, retention gap, and closed stream.

```txt
ProjectionEngine.register(name, fold, schema) -> ProjectionHandle
ProjectionEngine.rebuild(name, cursor) -> Snapshot | ProjectionError
ProjectionEngine.snapshot(query, options) -> Snapshot | ProjectionError
ProjectionEngine.subscribe(query, cursor, options) -> ProjectionEvents | ProjectionError
```

Projection folds MUST be deterministic over the retained log. Snapshot results MUST identify the log position they include.

```txt
Operator.start(spec, cursor) -> OperatorHandle | OperatorError
Operator.replay_until_live() -> LiveCursor | OperatorError
Operator.claim(work_key, options) -> ClaimResult | OperatorError
Operator.execute_owned(work_key, claim_id) -> TerminalRecord | OperatorError
Operator.stop(reason) -> StopResult
```

Operators that perform externally visible side effects MUST implement the claimed-work lifecycle in §13.

```txt
AgentAdapter.initialize(config) -> Capabilities | AdapterError
AgentAdapter.create_session(spec) -> AdapterSession | AdapterError
AgentAdapter.load_session(session_id, options) -> AdapterSession | AdapterError
AgentAdapter.send_prompt(session, prompt) -> PromptHandle | AdapterError
AgentAdapter.receive_updates(session, cursor) -> AdapterUpdate | AdapterError
AgentAdapter.handle_required_action(action, resolution) -> ActionResult | AdapterError
AgentAdapter.cancel(session, request_id) -> CancelResult | AdapterError
AgentAdapter.close(session, reason) -> CloseResult | AdapterError
```

Adapters translate protocol behavior into durable records; they MUST NOT define substrate semantics.

```txt
Provider.provision(spec) -> ResourceHandle | ProviderError
Provider.ready_check(handle) -> ReadyState | ProviderError
Provider.describe(handle) -> ResourceDescriptor | ProviderError
Provider.stop(handle, reason) -> StopResult | ProviderError
Provider.cleanup(handle, policy) -> CleanupResult | ProviderError
```

Providers own live resource lifecycle. Durable records describe provider facts, but provider handles are not durable truth.

```txt
Conductor.build(chain_spec) -> ConductorHandle | ConductorError
Conductor.initialize(direction, message) -> InitResult | ConductorError
Conductor.route(direction, envelope) -> RoutedEnvelope | ConductorError
Conductor.mutate_capabilities(capabilities, context) -> Capabilities | ConductorError
Conductor.close(reason) -> CloseResult
```

Conductor chains are optional. When present, they MUST preserve ordering and causality rules in §17.

```txt
Client.append_intent(intent, options) -> IntentReceipt | ClientError
Client.await_projection(query, predicate, options) -> Snapshot | ClientError
Client.subscribe(query, cursor, options) -> ProjectionEvents | ClientError
Client.resolve_required_action(action_id, resolution, options) -> IntentReceipt | ClientError
```

Clients operate through durable intents and projections. Normal application clients MUST NOT require direct protocol transport access.

```txt
Runtime.boot(config) -> RuntimeHandle | RuntimeError
Runtime.register_adapter(protocol, adapter) -> RegistrationResult | RuntimeError
Runtime.register_provider(kind, provider) -> RegistrationResult | RuntimeError
Runtime.own_session(session_id) -> OwnershipState | RuntimeError
Runtime.reattach(session_id, options) -> OwnershipState | RuntimeError
Runtime.shutdown(reason) -> ShutdownResult
```

The runtime owns live resources and executes operators. It MUST prove live promptability before emitting ready session facts.

---
