export const clientEffectImports = (plugin: any) => ({
  Context: plugin.symbolFactory.register("Context", {
    external: "effect/Context",
    importKind: "namespace"
  }),
  Effect: plugin.symbolFactory.register("Effect", {
    external: "effect/Effect",
    importKind: "namespace"
  }),
  Layer: plugin.symbolFactory.register("Layer", {
    external: "effect/Layer",
    importKind: "namespace"
  }),
  Schema: plugin.symbolFactory.register("Schema", {
    external: "effect/Schema",
    importKind: "namespace"
  }),
  HttpApi: plugin.symbolFactory.register("HttpApi", {
    external: "effect/unstable/httpapi/HttpApi",
    importKind: "namespace"
  }),
  HttpApiClient: plugin.symbolFactory.register("HttpApiClient", {
    external: "effect/unstable/httpapi/HttpApiClient",
    importKind: "namespace"
  }),
  HttpApiEndpoint: plugin.symbolFactory.register("HttpApiEndpoint", {
    external: "effect/unstable/httpapi/HttpApiEndpoint",
    importKind: "namespace"
  }),
  HttpApiGroup: plugin.symbolFactory.register("HttpApiGroup", {
    external: "effect/unstable/httpapi/HttpApiGroup",
    importKind: "namespace"
  }),
  HttpApiSchema: plugin.symbolFactory.register("HttpApiSchema", {
    external: "effect/unstable/httpapi/HttpApiSchema",
    importKind: "namespace"
  }),
  HttpClient: plugin.symbolFactory.register("HttpClient", {
    external: "effect/unstable/http/HttpClient",
    importKind: "namespace",
    kind: "type"
  }),
  Schemas: plugin.symbolFactory.register("Schemas", {
    external: "./effect-schema.gen",
    importKind: "namespace"
  })
})

export type ClientEffectImports = ReturnType<typeof clientEffectImports>
