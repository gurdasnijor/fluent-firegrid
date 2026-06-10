// Advisory burn-down lane for @codeforbreakfast/eslint-effect + eslint-plugin-functional.
//
// Run via `pnpm run lint:effect`. NON-BLOCKING: every rule here is a WARNING, so
// eslint exits 0 regardless of count — it surfaces the adopted-but-not-yet-clean
// rules so they can be driven to zero and graduated into the blocking set in
// eslint.config.js (effectBlockingRules).
//
// Disposition (agreed with the team):
//   - functional immutability suite + effect/prefer-effect-platform: ADOPTED,
//     burning down here until zero.
//   - moderate effect/* rules: tracked here too (cheap to clear).
//   - prefer-andThen / prefer-as-some: here for MANUAL fixing — the plugin's
//     autofixers for these are broken (see eslint.config.js note).
//   - no-gen / no-if-statement / no-method-pipe: NOT adopted (fight this repo's
//     Effect.gen-first / imperative-control-flow idioms) — absent on purpose.
//
// Do NOT run `eslint --fix` against these rules; the plugin's fixers are unsafe.
//
// ESLint's `--config` REPLACES eslint.config.js (it does not merge), so this
// extends the base config (spread below) to keep the `local` plugin, parser
// setup, and ignores — then layers the advisory warnings on top.
import base from "./eslint.config.js"
import functional from "eslint-plugin-functional"

export default [
  ...base,
  {
    // Parser, project, and the `local` plugin come from the spread base config.
    files: ["packages/**/src/**/*.ts"],
    ignores: [
      "packages/**/src/__tests__/**/*.ts",
      "packages/**/*.test.ts",
      "**/*.d.ts",
    ],
    // `effect` is already registered by the base config's blocking block;
    // redefining it would collide. Only add `functional` here.
    plugins: {
      functional,
    },
    rules: {
      // --- Agreed opinionated adoptions (high blast radius, burning down) ---
      "effect/prefer-effect-platform": "warn",
      "functional/prefer-readonly-type": "warn",
      "functional/prefer-immutable-types": [
        "warn",
        { enforcement: "ReadonlyShallow", ignoreInferredTypes: true },
      ],
      "functional/immutable-data": [
        "warn",
        { ignoreImmediateMutation: true, ignoreClasses: true },
      ],
      "functional/no-let": "warn",
      "functional/no-loop-statements": "warn",
      "functional/type-declaration-immutability": "warn",

      // --- Moderate effect/* (cheap to clear, then graduate to blocking) ---
      "effect/no-direct-tag-access": "warn",
      "effect/no-curried-calls": "warn",
      "effect/no-switch-statement": "warn",
      "effect/no-classes": "warn",
      "effect/prefer-match-over-ternary": "warn",
      "effect/no-unnecessary-function-alias": "warn",
      "effect/no-eta-expansion": "warn",
      "effect/no-identity-transform": "warn",
      "effect/no-intermediate-effect-variables": "warn",
      "effect/prefer-schema-validation-over-assertions": "warn",
      "effect/suggest-currying-opportunity": "warn",

      // --- Manual-fix targets (autofixers broken; fix by hand, then graduate) ---
      "effect/prefer-andThen": "warn",
      "effect/prefer-as-some": "warn",

      // no-gen / no-if-statement / no-method-pipe intentionally omitted.
    },
  },
]
