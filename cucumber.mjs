const tagFilter = process.env.CUCUMBER_TAGS ?? "not @spec-only"

const config = {
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

if (tagFilter !== "") {
  config.tags = tagFilter
}

export default config
