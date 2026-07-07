# 27. Security Considerations

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A stream-first substrate has several security boundaries.

## 27.1 Authorization

Append and read permissions MUST be enforced.

A system SHOULD distinguish:

```txt
who may append intents
who may observe projections
who may resolve approvals
who may provision resources
who may access session updates
```

## 27.2 Secrets

Secrets MUST NOT be written to durable logs unless explicitly encrypted and intended for durable storage.

Secret references SHOULD be used instead of raw secret values.

Credential references **MUST** be resolved by the host or provider boundary, not by pure authoring helpers. Durable records may include the reference name or policy summary when safe, but **MUST NOT** include resolved secret material.

## 27.3 Tenant Isolation

Multi-tenant systems MUST isolate durable logs, projections, resources, and live sessions by tenant or equivalent namespace.

## 27.4 Local Agents

Local stdio agents can access local machine resources.

A local provider SHOULD constrain:

```txt
working directory
environment
resource mounts
network
process lifetime
tool permissions
```

## 27.5 Replay Privacy

Because logs are replayable, record contents must be treated as durable data. Sensitive payloads SHOULD be minimized or redacted.

---
