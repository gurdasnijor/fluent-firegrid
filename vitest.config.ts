import path from "node:path"
import { defineConfig, defineProject } from "vitest/config"

export default defineConfig({
  root: __dirname,
  test: {
    projects: [
      defineProject({
        test: {
          name: "fluent-acp-process",
          include: ["packages/fluent-acp-process/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"]
        }
      }),
      defineProject({
        test: {
          name: "observability",
          include: ["packages/observability/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"]
        }
      }),
      defineProject({
        test: {
          name: "verification",
          include: ["packages/verification/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"]
        },
        resolve: {
          alias: {
            "@firegrid/verification": path.resolve(__dirname, "./packages/verification/src/index.ts")
          }
        }
      }),
      defineProject({
        test: {
          name: "effect-s2",
          include: ["packages/effect-s2/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"]
        },
        resolve: {
          alias: {
            "effect-s2": path.resolve(__dirname, "./packages/effect-s2/src/index.ts")
          }
        }
      }),
      defineProject({
        test: {
          name: "effect-s2-flow",
          include: ["packages/effect-s2-flow/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
          passWithNoTests: true
        },
        resolve: {
          alias: {
            "effect-s2-flow": path.resolve(__dirname, "./packages/effect-s2-flow/src/index.ts")
          }
        }
      }),
      defineProject({
        test: {
          name: "fluent-firegrid",
          include: ["packages/fluent-firegrid/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
          passWithNoTests: true
        },
        resolve: {
          alias: {
            "@firegrid/fluent-firegrid": path.resolve(__dirname, "./packages/fluent-firegrid/src/index.ts"),
            "@firegrid/tanstack-workflow-s2": path.resolve(
              __dirname,
              "./packages/tanstack-workflow-s2/src/index.ts"
            ),
            "@tanstack/workflow-core": path.resolve(__dirname, "./packages/tanstack-workflow-core/src/index.ts"),
            "@tanstack/workflow-runtime": path.resolve(__dirname, "./packages/tanstack-workflow-runtime/src/index.ts")
          }
        }
      }),
      defineProject({
        test: {
          name: "fluent-firegrid-s2",
          include: ["packages/fluent-firegrid-s2/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
          passWithNoTests: true
        },
        resolve: {
          alias: {
            "@firegrid/fluent-firegrid": path.resolve(__dirname, "./packages/fluent-firegrid/src/index.ts"),
            "@firegrid/fluent-firegrid/state": path.resolve(__dirname, "./packages/fluent-firegrid/src/state.ts"),
            "@firegrid/fluent-firegrid-s2": path.resolve(
              __dirname,
              "./packages/fluent-firegrid-s2/src/index.ts"
            ),
            "effect-s2": path.resolve(__dirname, "./packages/effect-s2/src/index.ts")
          }
        }
      }),
      defineProject({
        test: {
          name: "fluent-firegrid-http",
          include: ["packages/fluent-firegrid-http/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
          passWithNoTests: true
        },
        resolve: {
          alias: {
            "@firegrid/fluent-firegrid": path.resolve(__dirname, "./packages/fluent-firegrid/src/index.ts"),
            "@firegrid/fluent-firegrid-http": path.resolve(
              __dirname,
              "./packages/fluent-firegrid-http/src/index.ts"
            )
          }
        }
      }),
      defineProject({
        test: {
          name: "tanstack-workflow-core",
          include: ["packages/tanstack-workflow-core/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
          passWithNoTests: true
        },
        resolve: {
          alias: {
            "@tanstack/workflow-core": path.resolve(__dirname, "./packages/tanstack-workflow-core/src/index.ts")
          }
        }
      }),
      defineProject({
        test: {
          name: "tanstack-workflow-runtime",
          include: ["packages/tanstack-workflow-runtime/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
          passWithNoTests: true
        },
        resolve: {
          alias: {
            "@tanstack/workflow-core": path.resolve(__dirname, "./packages/tanstack-workflow-core/src/index.ts"),
            "@tanstack/workflow-runtime": path.resolve(__dirname, "./packages/tanstack-workflow-runtime/src/index.ts")
          }
        }
      }),
      defineProject({
        test: {
          name: "tanstack-workflow-s2",
          include: ["packages/tanstack-workflow-s2/test/**/*.test.ts"],
          exclude: ["**/node_modules/**"],
          passWithNoTests: true
        },
        resolve: {
          alias: {
            "@tanstack/workflow-core": path.resolve(__dirname, "./packages/tanstack-workflow-core/src/index.ts"),
            "@tanstack/workflow-runtime": path.resolve(__dirname, "./packages/tanstack-workflow-runtime/src/index.ts"),
            "@firegrid/tanstack-workflow-s2": path.resolve(
              __dirname,
              "./packages/tanstack-workflow-s2/src/index.ts"
            )
          }
        }
      })
    ]
  }
})
