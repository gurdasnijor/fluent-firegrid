import { defineConfig, defineProject } from "vitest/config"
import path from "node:path"

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
      }),
      defineProject({
        test: {
          name: "observability",
          include: ["packages/observability/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
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
