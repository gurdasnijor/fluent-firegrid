const common = {
  paths: ["features/**/*.feature"],
  import: [
    "./cucumber-tsx-register.mjs",
    "features/**/*.ts",
  ],
  format: [
    "summary",
    "./packages/spec-harness/src/trace-formatter.ts",
  ],
}

export default common

export const proofs = {
  ...common,
  tags: "@proof",
}
