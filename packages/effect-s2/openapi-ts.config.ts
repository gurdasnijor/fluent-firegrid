import { defineConfig, type UserConfig } from "@hey-api/openapi-ts"

import { defaultConfig as clientEffect } from "./codegen/plugins/client-effect/config"
import { defaultConfig as effectSchema } from "./codegen/plugins/effect-schema/config"

export const S2_OPENAPI_URL =
  "https://raw.githubusercontent.com/s2-streamstore/s2-specs/329de93f7b240a4daef9edbeb98ced0699aab7d0/s2/v1/openapi.json"

export default defineConfig({
  input: S2_OPENAPI_URL,
  logs: {
    level: "silent"
  },
  output: {
    clean: true,
    path: "src/generated"
  },
  plugins: [
    effectSchema,
    clientEffect
  ]
} as UserConfig)
