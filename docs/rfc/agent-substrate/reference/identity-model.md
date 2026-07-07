# 9. Identity Model

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A stream-first agent substrate SHOULD distinguish durable identity from live ownership.

## 9.1 Durable Identity

Durable identities include:

```txt
session id
prompt/request id
tool call id
runtime id
launch id
resource id
claim id
awaitable/completion key
```

When the underlying agent wire format provides canonical identifiers, implementations SHOULD preserve them.

For ACP-like protocols:

```txt
SessionId = conversation identity
RequestId = prompt/request identity within a session
ToolCallId = tool invocation identity
```

ACP defines session creation/loading, `sessionId`, prompt requests, prompt updates, tool call ids, permission requests, and cancellation in its protocol documentation. These are useful reference identities, not substrate requirements. See §37 for ACP citations.

For non-ACP adapters, canonical identifiers may come from other places:

```txt
stdio framed protocol: process launch id + protocol request id
HTTP request/response agent: vendor session token + HTTP request id
gRPC streaming agent: stream id + message sequence id
vendor API: provider conversation id + provider message id
in-process agent: runtime session id + function invocation id
```

An ACP adapter can treat ACP session identity as canonical and prompt identity as `(SessionId, RequestId)`, not as a substrate-minted prompt id. A non-ACP adapter uses that adapter's canonical identifiers instead.

## 9.2 Live Ownership

A durable session row does not prove the current process owns a live session connection.

A conforming runtime **MUST NOT** treat durable session identity as sufficient for prompt dispatch unless one of the following is true:

```txt
1. the current runtime owns a live promptable session resource;
2. the runtime successfully reattached or reloaded the session;
3. the runtime explicitly reprovisioned a compatible live session;
4. the agent wire format permits stateless prompt execution for that session.
```

If none is true, prompt dispatch **MUST** fail durably and promptly rather than relying on a local timeout.

This is a core correctness rule.

---
