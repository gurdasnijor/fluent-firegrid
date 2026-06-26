import path from "node:path"
import { defineConfig, defineProject } from "vitest/config"

const alias = [
  { find: "@firegrid/fluent/clients", replacement: path.resolve(__dirname, "./packages/fluent/src/clients/index.ts") },
  { find: "@firegrid/fluent/http", replacement: path.resolve(__dirname, "./packages/fluent/src/http.ts") },
  { find: "@firegrid/fluent/runtime", replacement: path.resolve(__dirname, "./packages/fluent/src/runtime/index.ts") },
  { find: "@firegrid/fluent/s2", replacement: path.resolve(__dirname, "./packages/fluent/src/adapters/s2/index.ts") },
  { find: "@firegrid/fluent/state", replacement: path.resolve(__dirname, "./packages/fluent/src/state.ts") },
  { find: "@firegrid/fluent/testing", replacement: path.resolve(__dirname, "./packages/fluent/src/testing/index.ts") },
  {
    find: "@firegrid/core/statePredicate",
    replacement: path.resolve(__dirname, "./packages/core/src/statePredicate.ts")
  },
  { find: "@firegrid/core/state", replacement: path.resolve(__dirname, "./packages/core/src/state.ts") },
  { find: "@firegrid/clients", replacement: path.resolve(__dirname, "./packages/clients/src/index.ts") },
  { find: "@firegrid/core", replacement: path.resolve(__dirname, "./packages/core/src/index.ts") },
  { find: "@firegrid/fluent", replacement: path.resolve(__dirname, "./packages/fluent/src/index.ts") },
  { find: "@firegrid/log", replacement: path.resolve(__dirname, "./packages/log/src/index.ts") },
  { find: "@firegrid/proofs", replacement: path.resolve(__dirname, "./apps/proofs/src/index.ts") },
  { find: "@firegrid/runtime", replacement: path.resolve(__dirname, "./packages/runtime/src/index.ts") },
  { find: "@firegrid/store", replacement: path.resolve(__dirname, "./packages/store/src/index.ts") },
  { find: "@firegrid/trace", replacement: path.resolve(__dirname, "./packages/trace/src/index.ts") }
]

const testProject = (
  name: string,
  include: ReadonlyArray<string>,
  passWithNoTests = false
) =>
  defineProject({
    resolve: { alias },
    test: {
      name,
      include: [...include],
      exclude: ["**/node_modules/**"],
      passWithNoTests
    }
  })

export default defineConfig({
  root: __dirname,
  resolve: { alias },
  test: {
    projects: [
      testProject("acp-process", ["apps/acp-process/test/**/*.test.ts"]),
      testProject("clients", ["packages/clients/test/**/*.test.ts"], true),
      testProject("core", ["packages/core/test/**/*.test.ts"], true),
      testProject("fluent", ["packages/fluent/test/**/*.test.ts"], true),
      testProject("log", ["packages/log/test/**/*.test.ts"]),
      testProject("runtime", ["packages/runtime/test/**/*.test.ts"], true),
      testProject("store", ["packages/store/test/**/*.test.ts"], true),
      testProject("trace", ["packages/trace/test/**/*.test.ts"]),
      testProject("proofs", ["apps/proofs/test/**/*.test.ts"]),
      testProject("example-full-stack-service", ["apps/examples/full-stack-service/test/**/*.test.ts"], true)
    ]
  }
})
