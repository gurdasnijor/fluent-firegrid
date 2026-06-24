import type { EffectSchemaImports } from "./imports"

export interface UserConfig {
  readonly name: "effect-schema"
  readonly includeInEntry?: boolean
}

export interface EffectSchemaPlugin {
  readonly Config: any
  readonly Handler: (args: { readonly plugin: any }) => void
  readonly Instance: any & { readonly imports: EffectSchemaImports }
}
