# 18. Provider and Resource Model

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A provider provisions live resources.

Examples:

```txt
local process
container
VM
remote hosted runtime
local filesystem sandbox
vendor API session
browser worker
in-process function
```

Provider lifecycle events SHOULD be durable:

```txt
provision requested
provision started
resource mounted
runtime ready
runtime failed
runtime stopped
```

Provider handles are live resources and MUST NOT be treated as durable truth.

Implementations that transfer live resource handles between parent/child
contexts **MUST** declare ownership, shutdown, and reattach/reprovision
semantics; durable resource identity remains distinct from the live handle
instance.

## 18.1 Provider Interface

A provider SHOULD expose semantics equivalent to:

```txt
provision(resource_spec, context) -> resource_handle | ProviderError
ready_check(resource_handle) -> ready_state | ProviderError
describe(resource_handle) -> resource_descriptor | ProviderError
execute(resource_handle, command) -> execution_result | ProviderError
stop(resource_handle, reason) -> stop_result | ProviderError
cleanup(resource_handle, policy) -> cleanup_result | ProviderError
```

`execute` is OPTIONAL. If provided, it is provider-adapter behavior, not a generic prompt dispatch seam. Promptable agent sessions SHOULD use the session/prompt model unless the provider API is itself the protocol adapter for that agent.

Provider errors SHOULD distinguish:

```txt
invalid spec
capacity unavailable
permission denied
provision failed
not ready
resource not found
resource not owned
cleanup failed
provider unavailable
```

## 18.2 Resource Lifecycle States

Provider-backed resources SHOULD use a lifecycle compatible with:

```txt
requested
provisioning
ready
failed
stopping
stopped
cleaning
cleaned
lost
```

State transitions that affect application behavior **MUST** be durable. A provider may maintain richer process-local state, but projections should be able to answer whether a resource is requested, ready, failed, stopped, or cleaned from durable records.

Ready means the provider has completed its readiness contract. It does not necessarily mean an agent session is promptable; session promptability also requires adapter/session ownership under §14.

## 18.3 Sandbox

A sandbox is a scoped environment for an agent.

It MAY include:

```txt
filesystem root
resource mounts
environment variables
network policy
secrets
tool permissions
CPU/memory limits
container image
```

A sandbox may be local or remote.

A local filesystem sandbox SHOULD be represented as:

```txt
durable resource facts
live scoped filesystem state
cleanup finalizer
```

The durable log records what was mounted or made available; it does not replace the live filesystem.

Sandbox cleanup **SHOULD** be explicit. If a sandbox contains durable artifacts, the cleanup policy **MUST** define whether artifacts are retained, exported, deleted, or redacted.

When a sandbox or resource backend captures generated artifacts, it **SHOULD** append durable evidence that identifies the path or logical artifact, content digest or object reference, size, and producer context. The evidence record **SHOULD NOT** inline the full artifact payload unless the resource policy explicitly allows inline storage.

---
