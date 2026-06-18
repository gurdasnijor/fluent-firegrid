export default {
  paths: ["features/**/*.feature"],
  import: [
    "packages/spec-harness/src/**/*.ts",
    "features/support/**/*.ts",
    "features/step_definitions/**/*.ts",
  ],
  format: [
    "summary",
    "./packages/spec-harness/src/trace-formatter.ts",
  ],
  tags: "not @spec-only",
}
