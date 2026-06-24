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
      })
    ]
  }
})
