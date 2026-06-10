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
            "@firegrid/fluent-store": path.resolve(__dirname, "./packages/fluent-store/src"),
            "@firegrid/fluent-store-inmemory": path.resolve(
              __dirname,
              "./packages/fluent-store-inmemory/src"
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
            "@firegrid/fluent-store": path.resolve(__dirname, "./packages/fluent-store/src"),
            "@firegrid/fluent-store-inmemory": path.resolve(
              __dirname,
              "./packages/fluent-store-inmemory/src"
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
          name: "fluent-server",
          include: ["packages/fluent-server/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
        },
        resolve: {
          alias: {
            ...alias,
            "@firegrid/fluent-store": path.resolve(__dirname, "./packages/fluent-store/src"),
            "@firegrid/fluent-store-inmemory": path.resolve(
              __dirname,
              "./packages/fluent-store-inmemory/src"
            ),
            "@firegrid/fluent-server": path.resolve(__dirname, "./packages/fluent-server/src"),
          },
        },
      }),
    ],
  },
})
