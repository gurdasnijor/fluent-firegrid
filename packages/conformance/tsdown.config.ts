import type { Options } from "tsdown"

const config: Options = {
  entry: [
    // Unified dispatcher CLI + root barrel
    "src/cli.ts",
    "src/index.ts",
    // Client conformance engine
    "src/client/index.ts",
    "src/client/cli.ts",
    "src/client/protocol.ts",
    "src/client/adapters/typescript-adapter.ts",
    // Server conformance engine
    "src/server/index.ts",
    "src/server/cli.ts",
    "src/server/test-runner.ts",
  ],
  format: ["esm", "cjs"],
  platform: "node",
  dts: true,
  clean: true,
}

export default config
