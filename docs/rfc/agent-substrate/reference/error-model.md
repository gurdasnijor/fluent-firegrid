# 26. Error Model

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A conforming system SHOULD distinguish:

```txt
expected domain errors
unexpected runtime defects
transport errors
projection errors
operator failures
provider failures
protocol failures
```

Expected domain errors SHOULD be typed and durable when they affect application-observable state.

Examples:

```txt
LaunchNotLive
SessionNotFound
PromptFailed
PromptCancelled
PermissionDenied
ProviderUnavailable
ProtocolUnsupported
ResourceUnavailable
IdempotencyConflict
```

Local timeouts SHOULD NOT be the primary way to signal known runtime-unavailable states. If the runtime can know that a session is not live, it SHOULD append or return a typed failure promptly.

---
