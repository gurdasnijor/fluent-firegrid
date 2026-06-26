import type { ClientEffectImports } from "./imports"

export interface UserConfig {
  readonly name: "client-effect"
  readonly includeInEntry?: boolean
}

export interface ClientEffectPlugin {
  readonly Config: any
  readonly Handler: (args: { readonly plugin: any }) => void
  readonly Instance: {
    readonly imports: ClientEffectImports
    readonly [key: string]: any
  }
}
