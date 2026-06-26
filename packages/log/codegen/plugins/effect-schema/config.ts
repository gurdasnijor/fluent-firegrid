import { definePluginConfig } from "@hey-api/openapi-ts"

import { effectSchemaImports } from "./imports"
import { handler } from "./plugin"
export const defaultConfig = {
  config: {
    includeInEntry: true
  },
  handler,
  imports: effectSchemaImports,
  name: "effect-schema",
  symbolMeta() {
    return {
      artifact: "effect-schema"
    }
  },
  tags: ["validator"]
} as any

export const defineConfig = definePluginConfig(defaultConfig)
