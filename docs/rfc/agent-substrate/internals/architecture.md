# Architecture

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

The architecture is decomposed into the following subcomponents. Each later section specifies interfaces, failure model, and rationale for one or more of these components.

| Subcomponent | Primary sections | Interface | Failure model | Rationale |
| --- | --- | --- | --- | --- |
| Transport and adapter boundary | §9, §14, §15, §16 | Adapter session/prompt/update/load/cancel interfaces. | Live handles die on restart; durable ids remain but do not prove promptability. | Keep ACP, stdio, HTTP, gRPC, vendor APIs, and in-process agents as peers. |
| Durable log and channel substrate | §7, §8, §10.6, §20, §22 | Append/read/replay/live-tail, typed channel keys, completion keys. | Duplicates, retries, EOF/live-tail gaps, and terminal races are resolved by log order and idempotency rules. | Make coordination durable, replayable, and observable without hidden callbacks. |
| Projection and query plane | §10, §11, §19, §23 | Snapshot-at-cursor, subscribe-after-cursor, rebuild, read-side sinks. | Projection lag or sink failure never changes log authority. | Separate durability from queryability and make waits restart-safe. |
| Session lifecycle and runtime ownership | §12, §13, §14, §15, §25 | Runtime ownership, reattach profile, promptability checks, claimed work. | Restart drops live ownership unless reattach/reprovision is declared and succeeds. | Prevent stale durable session ids from being treated as live resources. |
| Gateway / client surface | §11, §17, §21, §27 | Append intents, observe projections, resolve required actions, serializable middleware specs. | Clients may disconnect; correctness derives from durable intents and terminal rows. | Hide agent protocol transport from normal application code while preserving auditability. |
| Provider, sandbox, resources, and tools | §6.1.4-§6.1.6, §18 | Provision/ready/cleanup, resource refs, frozen tool descriptors, tool invocation. | Provider handles and tool handlers are live resources; descriptors and lifecycle facts are durable. | Keep sandbox/tool capability explicit and stable across a session. |

The remaining architecture sections retain numbered references because downstream reviews refer to them by section id.

---
