# Dependency Cruiser Diagrams

Generated from `.dependency-cruiser.cjs` on 2026-06-25.

These diagrams show the production package graph only. Proofs and examples live
under `apps/` and are intentionally excluded from the runtime diagrams.

The package-only dependency-cruiser report currently includes:

- 47 modules cruised;
- 70 dependencies cruised;
- 0 errors;
- 0 warnings.

## Diagrams

| Diagram | Purpose |
| --- | --- |
| [`production-packages.svg`](production-packages.svg) | Production packages collapsed by package. Use this to spot surprising package-level edges. |
| [`core-focus.svg`](core-focus.svg) | `@firegrid/core` neighborhood. This should remain the lowest shared contract package. |
| [`fluent-focus.svg`](fluent-focus.svg) | `@firegrid/fluent` neighborhood. Use this to catch the authoring surface drifting into concrete stores, log substrate, or trace sinks. |
| [`runtime-focus.svg`](runtime-focus.svg) | `@firegrid/runtime` neighborhood. This should depend on core and stay store-agnostic. |
| [`store-focus.svg`](store-focus.svg) | `@firegrid/store` neighborhood. Use this to keep S2-backed storage/binding concerns below fluent/runtime contracts. |

The `.dot` files are the Graphviz source for the SVGs. `production-packages.mmd`
is a Mermaid version of the package-level graph. `dependency-cruiser-report.json`
is the machine-readable package-only report.

## Regenerate

```sh
pnpm exec depcruise --config .dependency-cruiser.cjs -T dot --collapse '^packages/[^/]+' --output-to docs/dependency-cruiser/production-packages.dot packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T dot --focus '^packages/core/src/' --focus-depth 2 --output-to docs/dependency-cruiser/core-focus.dot packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T dot --focus '^packages/fluent/src/' --focus-depth 2 --output-to docs/dependency-cruiser/fluent-focus.dot packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T dot --focus '^packages/runtime/src/' --focus-depth 2 --output-to docs/dependency-cruiser/runtime-focus.dot packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T dot --focus '^packages/store/src/' --focus-depth 2 --output-to docs/dependency-cruiser/store-focus.dot packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T json --output-to docs/dependency-cruiser/dependency-cruiser-report.json packages
pnpm exec depcruise --config .dependency-cruiser.cjs -T mermaid --collapse '^packages/[^/]+' --output-to docs/dependency-cruiser/production-packages.mmd packages
for file in docs/dependency-cruiser/*.dot; do dot -Tsvg "$file" -o "${file%.dot}.svg"; done
```
