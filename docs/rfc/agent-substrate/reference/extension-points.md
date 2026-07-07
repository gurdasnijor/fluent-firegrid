# 28. Extension Points

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

Implementations MAY extend the substrate with:

```txt
additional record types
additional agent protocols
additional providers
custom projection families
custom durable promise domains
custom operator types
custom policy/middleware components
custom trace/audit sinks
```

Extensions SHOULD preserve the central invariant:

```txt
application-observable facts are durable log records
```

Extensions MUST NOT require application clients to bypass the stream-first model for normal operation.

Projection/query extensions **MUST** remain consumers of the durable log. Adding SQL, search, warehouse, archive, or cache backends is allowed, but extension APIs **MUST NOT** let those sinks become a second write path for substrate truth.

## 28.1 Versioning and Migration

This RFC versions the architecture model, not one wire protocol. Implementations **MUST** separately version:

```txt
record envelope schema
record payload schemas
projection schemas
operator fold versions
adapter protocol versions
provider spec versions
middleware spec versions
```

Schema evolution rules:

```txt
Adding optional fields is compatible when old consumers ignore them.
Adding required fields requires a new schema version or migration.
Renaming or changing field meaning is a breaking change.
Changing projection fold semantics requires a fold version bump.
Changing idempotency comparison rules requires a migration plan.
Changing claim winner rules is breaking unless old claims are fenced by version.
```

Migrations **SHOULD** be replayable from the log. A migration MAY create a new projection, backfill rows, append migration marker records, or dual-run old and new operators during cutover. It **MUST** document rollback behavior and the cursor or epoch at which the new semantics become active.

Breaking changes to this RFC SHOULD be published as a new RFC version. A conforming implementation **MAY** support multiple RFC versions concurrently if record schemas and projection namespaces are isolated.

---
