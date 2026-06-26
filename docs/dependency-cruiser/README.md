# Dependency Cruiser Diagrams

Generated from `.dependency-cruiser.cjs` on 2026-06-25.

The configured dependency-cruiser gate currently reports:

- 91 modules cruised;
- 184 dependencies cruised;
- 0 errors;
- 0 warnings.

The rule set now also enforces the intended layer direction: fluent authoring
cannot import transport/runtime/substrate packages, transport cannot import Node
or S2 bindings, lower TanStack/S2 packages cannot import fluent product APIs,
production code cannot import verification, and `effect-s2` remains a raw
substrate package.

## Diagrams

| Diagram | Purpose |
| --- | --- |
| [`workspace-packages.svg`](workspace-packages.svg) | Whole workspace collapsed by package. Use this to spot surprising package-level edges. |
| [`production-packages.svg`](production-packages.svg) | Production runtime stack only, collapsed by package. Use this to keep authoring, transport, runtime, store, and S2 substrate boundaries visible. |
| [`fluent-firegrid-focus.svg`](fluent-firegrid-focus.svg) | `@firegrid/fluent-firegrid` neighborhood. Use this to catch the authoring package drifting into transport or S2 substrate concerns. |
| [`fluent-firegrid-s2-focus.svg`](fluent-firegrid-s2-focus.svg) | S2 fluent runtime binding neighborhood. Use this to check that S2 concerns stay below the fluent authoring surface. |
| [`tanstack-workflow-s2-focus.svg`](tanstack-workflow-s2-focus.svg) | TanStack Workflow S2 store neighborhood. Use this to keep the store package below fluent product APIs. |
| [`verification-runtime-focus.svg`](verification-runtime-focus.svg) | Verification runtime neighborhood. Use this to ensure reusable verification infrastructure does not import concrete proofs. |

The `.dot` files are the Graphviz source for the SVGs. `workspace-packages.mmd`
is a Mermaid version of the package-level graph. `dependency-cruiser-report.json`
is the machine-readable report used to audit violations and cross-package edges.

## Regenerate

```sh
pnpm exec depcruise --config .dependency-cruiser.cjs -T dot --collapse '^packages/[^/]+' --output-to docs/dependency-cruiser/workspace-packages.dot packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T dot --include-only '^packages/(fluent-firegrid|fluent-firegrid-http|fluent-firegrid-node|fluent-firegrid-s2|tanstack-workflow-core|tanstack-workflow-runtime|tanstack-workflow-s2|effect-s2)/' --collapse '^packages/[^/]+' --output-to docs/dependency-cruiser/production-packages.dot packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T dot --focus '^packages/fluent-firegrid/src/' --focus-depth 2 --output-to docs/dependency-cruiser/fluent-firegrid-focus.dot packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T dot --focus '^packages/fluent-firegrid-s2/src/' --focus-depth 2 --output-to docs/dependency-cruiser/fluent-firegrid-s2-focus.dot packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T dot --focus '^packages/tanstack-workflow-s2/src/' --focus-depth 2 --output-to docs/dependency-cruiser/tanstack-workflow-s2-focus.dot packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T dot --focus '^packages/verification/src/' --focus-depth 2 --output-to docs/dependency-cruiser/verification-runtime-focus.dot packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T json --output-to docs/dependency-cruiser/dependency-cruiser-report.json packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T mermaid --collapse '^packages/[^/]+' --output-to docs/dependency-cruiser/workspace-packages.mmd packages
for file in docs/dependency-cruiser/*.dot; do dot -Tsvg "$file" -o "${file%.dot}.svg"; done
```
