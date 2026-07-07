# 24. Idempotency

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

Idempotency is required for reliable retries.

A system SHOULD support idempotency for:

```txt
launch/session requests
prompt requests
claim appends
required-action resolutions
timer schedules
provider provisioning
resource mounts
```

Idempotency keys SHOULD identify logical operations.

On duplicate idempotent operations:

```txt
same payload, in progress -> observe existing operation
same payload, terminal -> return current terminal result
conflicting payload -> conflict error
stale live resource -> fail or reattach explicitly without appending new side-effect intent
```

A system MUST NOT return a stale durable identity as if it proves live ownership.

A transport-level idempotent producer model is one possible append dedupe mechanism.

## 24.1 Keyspace Allocation

Every idempotent operation **MUST** define its keyspace:

```txt
operation kind
tenant or namespace
logical subject
client or producer identity, if relevant
dedupe window
payload comparison rule
```

Examples:

```txt
launch: tenant + launch idempotency key
prompt: tenant + session id + request id or prompt idempotency key
permission resolution: tenant + permission id
timer schedule: tenant + timer id or completion key
provider provision: tenant + resource request id
```

Keys **SHOULD** be stable across client retries and runtime restarts. Random retry keys defeat idempotency.

## 24.2 Conflict Resolution

For duplicate idempotent operations:

```txt
same payload, in progress -> observe existing operation
same payload, terminal -> return or project completed result
same payload, failed retryable -> domain policy decides retry or stable failure
different payload -> IdempotencyConflict
stale live resource -> NotLive or explicit reattach/reprovision path
```

Conflict comparison **MUST** use canonicalized payload semantics, not incidental JSON field order or transport encoding. The implementation **SHOULD** record enough metadata to explain conflicts during audit.

Launch and prompt idempotency **MUST** follow these rules:

```txt
same payload, in progress -> observe existing durable operation
same payload, terminal -> return the existing terminal result
different payload for same idempotency key -> conflict
stale session or not-live session -> no new prompt intent is appended
```

The stale-session rule is mandatory: if a duplicate idempotent launch or prompt resolves to a durable session id that is not currently promptable, the client/runtime **MUST NOT** append a new prompt intent for that stale session id. It must return a typed not-live result, attempt a declared reattach/reprovision path, or require the caller to start a new logical operation.

Projection layers **SHOULD** fold duplicate launch/session requests by the logical idempotency key, not by incidental attempt id. Multiple attempts for the same logical idempotency key **MUST** materialize as one logical operation row or a documented conflict, so subscribers do not treat retries as independent launches.

---
