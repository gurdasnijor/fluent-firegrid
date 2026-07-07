# Documentation Map

This repository keeps architecture documents in separate lanes because each lane
ages at a different speed.

| Lane | Directory | Lifecycle | Use for |
| --- | --- | --- | --- |
| RFC | [`rfc/`](./rfc/) | Changes slowly; revisions are compatibility events. | Implementation-neutral contracts and conformance invariants. |
| Canon | [`canon/`](./canon/) | Revised when the implementation direction changes. | The architecture this repository has chosen. |
| SDD | [`sdds/`](./sdds/) | Fast-moving; frozen or deleted when superseded. | Package-local design plans and implementation scaffolding. |
| Guides | [`guides/`](./guides/) | User-facing; updated with product behavior. | How-to material for current workflows. |
| Requirements | [`requirements/`](./requirements/) | Consumer-driven; traced to proofs or RFC invariants. | Falsifiable product or stress-test requirements. |
| Execution | [`execution/`](./execution/) | Living ledgers; edited as work is claimed and lands. | Lane/work-packet tracking for an active SDD; agents claim and update rows. |

## Status Header

Every RFC, canon, and SDD page should start with a short status header after
the H1:

```md
Doc-Class: RFC | canon | SDD | guide | requirement
Status: draft | active | frozen | superseded
Date: YYYY-MM-DD
Owner: Firegrid Architecture
Substrate: idealized | S2 | neutral
```

Use `Substrate: idealized` for pages that describe Durable Streams capabilities
not currently supplied by S2. Use `Substrate: S2` for pages that describe the
current EffSharp/S2 implementation. Pages that are purely organizational can use
`Substrate: neutral`.

## Drift Policy

When a package, substrate, or implementation direction changes, update the canon
and re-status the affected SDDs in the same change. Do not keep superseded build
plans as active-looking reference material. Prefer a short `Status: frozen` or
`Status: superseded` note over contradictory active docs.
