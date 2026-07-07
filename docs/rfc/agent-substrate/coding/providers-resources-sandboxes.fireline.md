# Fireline Providers, Resources, And Sandboxes Profile

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

> Fireline-specific companion to the neutral Stream-First Agent Substrate RFC.
>
> **Frozen / historical (Rust Fireline).** This profile describes the legacy
> Rust `crates/fireline-*` implementation, not the current `fluent-firegrid`
> system; it is retained as reference archaeology. Current implementation
> profiles use the `.fluent.md` suffix — see
> [Implementation Profiles](../README.md#implementation-profiles).

## Provider And Sandbox Roles

Fireline providers select and own execution boundaries. The host owns platform
behavior, lifecycle, and policy; `Sandbox` is a single tool-call executor and is
distinct from a host or session. [evidence:
`crates/fireline-runtime/src/sandbox/primitive.rs:1-14`;
`crates/fireline-runtime/src/sandbox/primitive.rs:81-88`;
`vault/canon/concepts/providers-and-sandboxes.md:54-103`;
`vault/canon/concepts/providers-and-sandboxes.md:174-188`]

## Sandbox Channel Handle Lifecycle

Fireline's sandbox channel may lazily provision handles, reuse them, evict idle
handles, and reprovision from durable resource facts. Its current child-session
handoff path can inherit a parent-provisioned handle, and parent shutdown does
not break a child-owned handle when the child has durable ownership or an
independent live supervisor. These are Fireline sandbox-channel profile
semantics, not universal substrate requirements. [executable contract:
`crates/fireline-runtime/src/sandbox_channel.rs:859`;
`crates/fireline-runtime/src/sandbox_channel.rs:919`;
`crates/fireline-runtime/src/sandbox_channel.rs:955`;
`crates/fireline-runtime/src/sandbox_channel.rs:998`]

## Sandbox Lifecycle Evidence Rows

Fireline records provider-owned sandbox live-resource lifecycle evidence with a
narrow `fireline.sandbox.lifecycle` row family. This row family is
Fireline-profile-specific unless the RFC owner later promotes a neutral
live-resource lifecycle primitive.

The row family MUST NOT be folded as proof that the current process owns a live
sandbox handle. Rows are durable recovery evidence only. After restart, a
runtime MUST fold these rows, run the provider-specific `get` / `list` /
reattach check allowed by the provider profile, and append
`reattach_attempted` followed by `reattached` or a terminal `lost`, `failed`, or
`stopped` row before treating the sandbox as promptable.

`fireline.sandbox.lifecycle` rows use append-only event keys:

```text
sandbox:{sandboxId}:event:{eventId}
```

The materialized projection primary key is `sandboxId` and folds the latest
valid lifecycle for that sandbox.

| Field | Requirement | Notes |
|---|---|---|
| `eventId` | Required. | Stable id for one runtime-owned operation attempt. |
| `correlationId` | Optional. | May carry the launch id, session id, or channel invocation id that caused the lifecycle operation. |
| `runtimeHostId` | Required. | Writer identity for the runtime participant. |
| `runtimeHostKey` | Required. | Stable host key from the runtime participant. |
| `provider` | Required. | Provider name selected by the runtime. |
| `sandboxId` | Required once known. | Provision attempts may start from `sandboxName`; `ready` and terminal rows must include `sandboxId`. |
| `sandboxName` | Optional. | Topology/channel name for the sandbox. |
| `operation` | Required. | One of `provision_requested`, `provision_started`, `ready`, `failed`, `stopping`, `stopped`, `lost`, `reattach_attempted`, `reattached`, `reprovision_started`, `cleaned`. |
| `status` | Required. | One of `requested`, `provisioning`, `ready`, `failed`, `stopping`, `stopped`, `lost`, `reattached`, `cleaned`. |
| `createdAtMs` | Required. | Row creation time in Fireline `...Ms` spelling. |
| `observedAtMs` | Required. | Time the runtime observed the provider/sandbox lifecycle fact. |
| `reason` | Optional. | Human-readable reason for terminal or cleanup rows. |
| `error` | Optional. | Structured or string error detail for `failed` / `lost`. |
| `descriptor` | Optional. | Snapshot of the provider `SandboxDescriptor` endpoints/status. This snapshot is explicitly not live ownership proof. |

Only the runtime participant / provider-dispatcher lane may write this row
family. Provider adapters such as Local, Docker, and Anthropic may return
descriptors, but they MUST NOT write lifecycle rows directly unless a future
profile update makes them explicit row-family owners.

Fold rules:

- `stopped`, `failed`, `lost`, and `cleaned` are terminal for the current event
  lineage.
- A later explicit `reprovision_started` followed by `ready` may start a new
  event lineage for the same `sandboxId`.
- `ready` is only a live candidate when the current runtime host has
  successfully provisioned or reattached the handle and provider freshness /
  reattach checks pass.
- Deployment discovery rows such as `HostProvisioned` and `HostStopped` keep
  their existing host/runtime fold rules and MUST NOT be overloaded for
  per-sandbox lifecycle state. [evidence:
  `crates/fireline-runtime/src/sandbox/deployment.rs:17-54`;
  `crates/fireline-runtime/src/sandbox/deployment.rs:111-193`;
  `crates/fireline-runtime/src/sandbox/deployment.rs:204-260`]

Implementation points:

- Define row DTOs and constants near the sandbox provider model or launch wire,
  not behind a generic envelope sink.
- Append `provision_started`, `ready`, and `failed` around
  `ProviderDispatcher::create`. [evidence:
  `crates/fireline-runtime/src/sandbox/provider_dispatcher.rs:98-102`]
- Append `stopping`, `stopped`, and `failed` around
  `ProviderDispatcher::stop`. [evidence:
  `crates/fireline-runtime/src/sandbox/provider_dispatcher.rs:130-152`]
- Append sandbox-channel `reprovision_started` and `lost` rows around idle
  eviction, reprovision, and destroy paths. [evidence:
  `crates/fireline-runtime/src/sandbox_channel.rs:272-355`;
  `crates/fireline-runtime/src/sandbox_channel.rs:393-408`;
  `crates/fireline-runtime/src/sandbox_channel.rs:534-541`]
- Tests must prove that folding a prior `ready` row after restart does not grant
  live ownership until a successful reattach/provision row is appended.

## Resource Mount Security

Fireline resource mounts reject path traversal, Docker volume subpaths, durable
blob traversal, and writable StreamFs snapshots before realization. Level 4
provider/resource conformance must preserve those checks before a mount becomes
visible to a sandbox or provider. [executable contract:
`crates/fireline-resources/src/mounter.rs:641`;
`crates/fireline-resources/src/mounter.rs:653`;
`crates/fireline-resources/src/mounter.rs:672`;
`crates/fireline-resources/src/mounter.rs:688`;
`crates/fireline-resources/src/mounter.rs:707`]
