# Agent UI Stress Test Requirements

Doc-Class: requirement
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: S2

These requirements describe the consumer-visible behavior a Firegrid-backed
agent UI must survive under crash, restart, duplicate delivery, and concurrent
ownership pressure. Each requirement should trace to an RFC invariant and either
an existing proof or a named proof gap.

| Requirement | RFC invariant | Evidence |
| --- | --- | --- |
| A completed prompt or tool result remains visible after host restart. | INV-002, INV-006 | `store.runtime-end-to-end`, `store.host-crash-restart` |
| Duplicate prompt/tool completion attempts do not reorder or overwrite the committed result. | INV-003, INV-005 | `effect-s2.capability-b.match-seq-num-contention`, `store.event-log-cas` |
| A deposed object/session owner cannot commit new visible UI state after takeover. | INV-004 | `store.object-live-fencing`, `store.object-stale-owner` |
| Timers and delayed sends appear once after restart, including when overdue at recovery time. | INV-007, INV-011 | `store.runtime-timer-sweep`, `store.object-delayed-send`, `store.service-delayed-send` |
| UI state can be rebuilt from durable facts without private process memory. | INV-008, INV-010 | `store.object-replay-state`, `store.object-cross-host` |
| Wait-driven UI transitions resolve from durable state/index facts, not callbacks lost on restart. | INV-009 | `store.object-state-wait`, `store.object-index-wait` |

Open proof gaps should be added here before they become implementation work.
Do not add UI-only requirements that bypass the durable fact model; add the RFC
invariant first, then add the proof.
