export const effectSchemaImports = (plugin: any) => ({
  Schema: plugin.symbolFactory.register("Schema", {
    external: "effect/Schema",
    importKind: "namespace"
  }),
  SchemaGetter: plugin.symbolFactory.register("SchemaGetter", {
    external: "effect/SchemaGetter",
    importKind: "namespace"
  })
})

export type EffectSchemaImports = ReturnType<typeof effectSchemaImports>
