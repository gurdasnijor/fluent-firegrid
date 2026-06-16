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
          name: "fluent-stream-log",
          include: ["packages/fluent-stream-log/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "@firegrid/fluent-stream-log/testing": path.resolve(
              __dirname,
              "./packages/fluent-stream-log/src/testing/durable-stream-log-test-suite.ts"
            ),
            "@firegrid/fluent-stream-log": path.resolve(__dirname, "./packages/fluent-stream-log/src"),
          },
        },
      }),
      defineProject({
        test: {
          name: "fluent-stream-log-inmemory",
          include: ["packages/fluent-stream-log-inmemory/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "@firegrid/fluent-stream-log/testing": path.resolve(
              __dirname,
              "./packages/fluent-stream-log/src/testing/durable-stream-log-test-suite.ts"
            ),
            "@firegrid/fluent-stream-log": path.resolve(__dirname, "./packages/fluent-stream-log/src"),
            "@firegrid/fluent-stream-log-inmemory": path.resolve(
              __dirname,
              "./packages/fluent-stream-log-inmemory/src"
            ),
          },
        },
      }),
      defineProject({
        test: {
          name: "fluent-transport",
          include: ["packages/fluent-transport/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "@firegrid/fluent-transport": path.resolve(
              __dirname,
              "./packages/fluent-transport/src"
            ),
          },
        },
      }),
      defineProject({
        test: {
          name: "fluent-transport-inmemory",
          include: ["packages/fluent-transport-inmemory/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "@firegrid/fluent-transport/testing": path.resolve(
              __dirname,
              "./packages/fluent-transport/src/testing/client-server-contract-test-suite.ts"
            ),
            "@firegrid/fluent-transport": path.resolve(
              __dirname,
              "./packages/fluent-transport/src"
            ),
            "@firegrid/fluent-transport-inmemory": path.resolve(
              __dirname,
              "./packages/fluent-transport-inmemory/src"
            ),
          },
        },
      }),
      defineProject({
        test: {
          name: "fluent-protocol",
          include: ["packages/fluent-protocol/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "@firegrid/fluent-stream-log": path.resolve(__dirname, "./packages/fluent-stream-log/src"),
            "@firegrid/fluent-stream-log-inmemory": path.resolve(
              __dirname,
              "./packages/fluent-stream-log-inmemory/src"
            ),
            "@firegrid/fluent-transport": path.resolve(
              __dirname,
              "./packages/fluent-transport/src"
            ),
            "@firegrid/fluent-protocol": path.resolve(
              __dirname,
              "./packages/fluent-protocol/src"
            ),
          },
        },
      }),
      defineProject({
        test: {
          name: "fluent-client",
          include: ["packages/fluent-client/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "@firegrid/fluent-stream-log": path.resolve(__dirname, "./packages/fluent-stream-log/src"),
            "@firegrid/fluent-stream-log-inmemory": path.resolve(
              __dirname,
              "./packages/fluent-stream-log-inmemory/src"
            ),
            "@firegrid/fluent-protocol": path.resolve(
              __dirname,
              "./packages/fluent-protocol/src"
            ),
            "@firegrid/fluent-client": path.resolve(__dirname, "./packages/fluent-client/src"),
          },
        },
      }),
      defineProject({
        test: {
          name: "effect-s2",
          include: ["packages/effect-s2/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "effect-s2/testing": path.resolve(__dirname, "./packages/effect-s2/src/TestS2.ts"),
            "effect-s2": path.resolve(__dirname, "./packages/effect-s2/src/index.ts"),
          },
        },
      }),
      defineProject({
        test: {
          name: "effect-s2-stream-db",
          include: ["packages/effect-s2-stream-db/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "effect-s2": path.resolve(__dirname, "./packages/effect-s2/src/index.ts"),
            "effect-s2-stream-db": path.resolve(
              __dirname,
              "./packages/effect-s2-stream-db/src/index.ts",
            ),
          },
        },
      }),
      defineProject({
        test: {
          name: "effect-s2-durable",
          include: ["packages/effect-s2-durable/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "effect-s2": path.resolve(__dirname, "./packages/effect-s2/src/index.ts"),
            "effect-s2-stream-db": path.resolve(
              __dirname,
              "./packages/effect-s2-stream-db/src/index.ts",
            ),
            "effect-s2-durable": path.resolve(
              __dirname,
              "./packages/effect-s2-durable/src/index.ts",
            ),
          },
        },
      }),
    ],
  },
})
