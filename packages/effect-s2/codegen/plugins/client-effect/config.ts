import { definePluginConfig } from "@hey-api/openapi-ts"

import { clientEffectImports } from "./imports"
import { handler } from "./plugin"
export const defaultConfig = {
  config: {
    includeInEntry: true
  },
  dependencies: ["effect-schema"],
  handler,
  imports: clientEffectImports,
  name: "client-effect",
  symbolMeta() {
    return {
      artifact: "client-effect"
    }
  },
  tags: ["client"]
} as any

export const defineConfig = definePluginConfig(defaultConfig)
