import { defineConfig, defineProject } from "vitest/config"
import path from "node:path"

const alias = {
  "@durable-streams/conformance-tests/server": path.resolve(
    __dirname,
    "./packages/conformance/src/server"
  ),
  "@durable-streams/conformance-tests/client": path.resolve(
    __dirname,
    "./packages/conformance/src/client"
  ),
  "durable-streams-protocol": path.resolve(
    __dirname,
    "./packages/durable-streams-protocol/src"
  ),
  "effect-durable-client": path.resolve(
    __dirname,
    "./packages/effect-durable-client/src"
  ),
  "effect-durable-execution": path.resolve(
    __dirname,
    "./packages/effect-durable-execution/src"
  ),
  "effect-durable-streams": path.resolve(
    __dirname,
    "./packages/effect-durable-streams/src"
  ),
}

export default defineConfig({
  root: __dirname,
  test: {
    projects: [
      defineProject({
        test: {
          name: "fluent-acp-process",
          include: ["packages/fluent-acp-process/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: { alias },
      }),
      defineProject({
        test: {
          name: "fluent-runtime",
          include: ["packages/fluent-runtime/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: { alias },
      }),
      defineProject({
        test: {
          name: "fluent-store",
          include: ["packages/fluent-store/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "@firegrid/fluent-store/testing": path.resolve(
              __dirname,
              "./packages/fluent-store/src/testing/durable-stream-log-test-suite.ts"
            ),
            "@firegrid/fluent-store": path.resolve(__dirname, "./packages/fluent-store/src"),
          },
        },
      }),
      defineProject({
        test: {
          name: "fluent-store-inmemory",
          include: ["packages/fluent-store-inmemory/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "@firegrid/fluent-store/testing": path.resolve(
              __dirname,
              "./packages/fluent-store/src/testing/durable-stream-log-test-suite.ts"
            ),
            "@firegrid/fluent-store": path.resolve(__dirname, "./packages/fluent-store/src"),
            "@firegrid/fluent-store-inmemory": path.resolve(
              __dirname,
              "./packages/fluent-store-inmemory/src"
            ),
          },
        },
      }),
      defineProject({
        test: {
          name: "effect-durable-execution",
          include: ["packages/effect-durable-execution/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: { alias },
      }),
      defineProject({
        test: {
          name: "effect-durable-streams",
          include: ["packages/effect-durable-streams/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: { alias },
      }),
    ],
  },
})
