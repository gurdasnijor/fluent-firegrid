import type { IR } from "@hey-api/openapi-ts"
import { $, toCase } from "@hey-api/openapi-ts"

import type { ClientEffectPlugin } from "./types"

const schemaName = (name: string): string => `${toCase(name, "PascalCase")}Schema`

const refName = (ref: string): string => decodeURIComponent(ref.split("/").at(-1) || ref)

const schemaRef = (plugin: ClientEffectPlugin["Instance"], name: string) =>
  $(plugin.imports.Schemas).attr(schemaName(name))

const schemaCall = (
  plugin: ClientEffectPlugin["Instance"],
  name: string,
  args: ReadonlyArray<any> = []
) => $(plugin.imports.Schema).attr(name).call(...args)

const literal = (value: unknown) => $.literal(value as string | number | boolean | null)

const schemaExpression = (
  plugin: ClientEffectPlugin["Instance"],
  schema: IR.SchemaObject | undefined,
  seen: ReadonlySet<IR.SchemaObject> = new Set()
): any => {
  if (!schema) {
    return $(plugin.imports.Schema).attr("Unknown")
  }
  if (schema.$ref) {
    return schemaRef(plugin, refName(schema.$ref))
  }
  if (schema.symbolRef) {
    return $(schema.symbolRef)
  }
  if (seen.has(schema)) {
    return $(plugin.imports.Schema).attr("Unknown")
  }

  const nextSeen = new Set(seen).add(schema)

  if (schema.const !== undefined) {
    return schemaCall(plugin, "Literal", [literal(schema.const)])
  }

  if (schema.type === "enum" && schema.items?.length) {
    const values = schema.items
      .map((item) => item.const)
      .filter((value): value is string | number | boolean => value !== undefined && value !== null)
    return values.length
      ? schemaCall(plugin, "Literals", [$.array(...values.map((value) => literal(value)))])
      : $(plugin.imports.Schema).attr("Unknown")
  }

  switch (schema.type) {
    case "array":
      return schemaCall(plugin, "Array", [schemaExpression(plugin, schema.items?.[0], nextSeen)])
    case "boolean":
      return $(plugin.imports.Schema).attr("Boolean")
    case "integer":
    case "number":
      return $(plugin.imports.Schema).attr("Number")
    case "null":
      return $(plugin.imports.Schema).attr("Null")
    case "object": {
      const object = $.object()
      const required = new Set(schema.required ?? [])
      Object.entries(schema.properties ?? {}).forEach(([key, value]) => {
        const propertySchema = schemaExpression(plugin, value, nextSeen)
        object.prop(
          key,
          required.has(key) ? propertySchema : schemaCall(plugin, "optionalKey", [propertySchema])
        )
      })
      if (schema.additionalProperties && typeof schema.additionalProperties === "object") {
        return schemaCall(plugin, "Record", [
          $(plugin.imports.Schema).attr("String"),
          schemaExpression(plugin, schema.additionalProperties, nextSeen)
        ])
      }
      return schemaCall(plugin, "Struct", [object])
    }
    case "string":
      return $(plugin.imports.Schema).attr("String")
    case "tuple":
      return schemaCall(plugin, "Tuple", [
        $.array(...(schema.items ?? []).map((item) => schemaExpression(plugin, item, nextSeen)))
      ])
    case "void":
      return $(plugin.imports.Schema).attr("Void")
    case "never":
      return $(plugin.imports.Schema).attr("Never")
    case "undefined":
      return $(plugin.imports.Schema).attr("Undefined")
    case "unknown":
    default:
      if (schema.items?.length && schema.logicalOperator !== "and") {
        return schemaCall(plugin, "Union", [
          $.array(...schema.items.map((item) => schemaExpression(plugin, item, nextSeen)))
        ])
      }
      return $(plugin.imports.Schema).attr("Unknown")
  }
}

const parametersFields = (
  plugin: ClientEffectPlugin["Instance"],
  parameters: Record<string, any> | undefined
): any => {
  if (!parameters || !Object.keys(parameters).length) {
    return undefined
  }
  const object = $.object()
  Object.values(parameters).forEach((parameter) => {
    const schema = schemaExpression(plugin, parameter.schema)
    object.prop(
      parameter.name,
      parameter.required ? schema : schemaCall(plugin, "optionalKey", [schema])
    )
  })
  return object
}

const rawComponentSchemas = (plugin: ClientEffectPlugin["Instance"]): Record<string, any> => {
  const spec = plugin.context?.spec
  return spec?.components?.schemas ?? {}
}

const rawRefName = (schema: any): string | undefined => schema?.$ref ? refName(schema.$ref) : undefined

const resolveRawSchema = (
  plugin: ClientEffectPlugin["Instance"],
  schema: any
): any => {
  const name = rawRefName(schema)
  return name ? rawComponentSchemas(plugin)[name] ?? schema : schema
}

const rawSchemaExpression = (
  plugin: ClientEffectPlugin["Instance"],
  schema: any
): any => {
  const name = rawRefName(schema)
  if (name) {
    return schemaRef(plugin, name)
  }
  if (schema?.enum?.length) {
    return schemaCall(plugin, "Literals", [$.array(...schema.enum.map((value: unknown) => literal(value)))])
  }
  if (schema?.oneOf?.length || schema?.anyOf?.length) {
    const items = schema.oneOf ?? schema.anyOf
    return schemaCall(plugin, "Union", [$.array(...items.map((item: any) => rawSchemaExpression(plugin, item)))])
  }
  switch (schema?.type) {
    case "array":
      return schemaCall(plugin, "Array", [rawSchemaExpression(plugin, schema.items)])
    case "boolean":
      return $(plugin.imports.Schema).attr("Boolean")
    case "integer":
    case "number":
      return $(plugin.imports.Schema).attr("Number")
    case "object": {
      const object = $.object()
      const required = new Set(schema.required ?? [])
      Object.entries(schema.properties ?? {}).forEach(([key, value]) => {
        const propertySchema = rawSchemaExpression(plugin, value)
        object.prop(
          key,
          required.has(key) ? propertySchema : schemaCall(plugin, "optionalKey", [propertySchema])
        )
      })
      return schemaCall(plugin, "Struct", [object])
    }
    case "string":
      return $(plugin.imports.Schema).attr("String")
    default:
      return $(plugin.imports.Schema).attr("Unknown")
  }
}

const sseEventVariantExpression = (
  plugin: ClientEffectPlugin["Instance"],
  schema: any
): any => {
  const resolved = resolveRawSchema(plugin, schema)
  const object = $.object()
  const required = new Set(resolved.required ?? [])
  Object.entries(resolved.properties ?? {}).forEach(([key, value]: [string, any]) => {
    const propertySchema = key === "data" && value?.type !== "string"
      ? $(plugin.imports.Schema).attr("fromJsonString").call(rawSchemaExpression(plugin, value))
      : rawSchemaExpression(plugin, value)
    object.prop(
      key,
      required.has(key) ? propertySchema : schemaCall(plugin, "optionalKey", [propertySchema])
    )
  })
  return schemaCall(plugin, "Struct", [object])
}

const sseEventCodecExpression = (
  plugin: ClientEffectPlugin["Instance"],
  schema: any
): any => {
  const resolved = resolveRawSchema(plugin, schema)
  const variants = resolved.oneOf ?? resolved.anyOf
  if (variants?.length) {
    return schemaCall(plugin, "Union", [
      $.array(...variants.map((variant: any) => sseEventVariantExpression(plugin, variant)))
    ])
  }
  return sseEventVariantExpression(plugin, resolved)
}

const statusCodeFor = (status: string): number => Number.parseInt(status, 10)

const isSuccessStatus = (status: string): boolean => {
  const statusCode = statusCodeFor(status)
  return status === "default" || (statusCode >= 200 && statusCode < 400)
}

const statusSymbolNames: Readonly<Record<string, string>> = {
  "400": "BadRequest",
  "401": "Unauthorized",
  "403": "Forbidden",
  "404": "NotFound",
  "408": "RequestTimeout",
  "409": "Conflict",
  "412": "PreconditionFailed",
  "416": "RangeNotSatisfiable"
}

const responseConstName = (schema: IR.SchemaObject | undefined, status: string): string | undefined => {
  if (!schema?.$ref) {
    return undefined
  }
  const name = refName(schema.$ref)
  if (name === "ErrorInfo") {
    return statusSymbolNames[status] ?? `${toCase(name, "PascalCase")}${status}`
  }
  return `${toCase(name, "PascalCase")}${status}`
}

const responseKey = (status: string, response: any, variant?: "sse"): string | undefined => {
  if (variant === "sse" || response?.mediaType === "text/event-stream") {
    return `sse:${status}`
  }
  if (!response?.schema?.$ref) {
    return undefined
  }
  return `ref:${refName(response.schema.$ref)}:${status}`
}

const responseSymbolName = (entry: ResponseEntry): string | undefined =>
  entry.variant === "sse" || entry.response?.mediaType === "text/event-stream"
    ? undefined
    : responseConstName(entry.response?.schema, entry.status)

const streamSseResponseSchema = (
  plugin: ClientEffectPlugin["Instance"],
  status: string,
  schema: IR.SchemaObject | undefined
): any => {
  const statusCode = statusCodeFor(status)
  const options = $.object()
  options.prop("events", sseEventCodecExpression(plugin, schema))

  const stream = $(plugin.imports.HttpApiSchema).attr("StreamSse").call(options)

  return Number.isFinite(statusCode) && statusCode !== 200
    ? $(plugin.imports.HttpApiSchema).attr("status").call($.literal(statusCode)).call(stream)
    : stream
}

const responseSchema = (
  plugin: ClientEffectPlugin["Instance"],
  status: string,
  response: any,
  variant?: "sse"
): any => {
  if (variant === "sse" || response?.mediaType === "text/event-stream") {
    return streamSseResponseSchema(plugin, status, response?.schema)
  }

  const statusCode = statusCodeFor(status)
  const isEmptyResponse = !response?.mediaType &&
    (response?.schema?.type === "unknown" || response?.schema?.type === "void")
  const base = !isEmptyResponse && response?.schema
    ? schemaExpression(plugin, response.schema)
    : Number.isFinite(statusCode)
    ? $(plugin.imports.HttpApiSchema).attr("Empty").call($.literal(statusCode))
    : $(plugin.imports.HttpApiSchema).attr("NoContent")

  return Number.isFinite(statusCode) && !isEmptyResponse && response?.schema
    ? base
      .attr("pipe")
      .call($(plugin.imports.HttpApiSchema).attr("status").call($.literal(statusCode)))
    : base
}

interface ResponseEntry {
  readonly status: string
  readonly response: any
  readonly variant?: "sse"
}

interface ResponseArtifacts {
  readonly responseSymbols: ReadonlyMap<string, any>
  readonly responseSetSymbols: ReadonlyMap<string, any>
}

const rawResponsesFor = (
  plugin: ClientEffectPlugin["Instance"],
  operation: IR.OperationObject
): Record<string, any> => {
  const spec = plugin.context?.spec
  return spec?.paths?.[operation.path]?.[operation.method.toLowerCase()]?.responses ?? {}
}

const responseEntriesFor = (
  plugin: ClientEffectPlugin["Instance"],
  operation: IR.OperationObject,
  kind: "error" | "success"
): ReadonlyArray<ResponseEntry> => {
  const out: Array<ResponseEntry> = []
  Object.entries(operation.responses ?? {}).forEach(([status, response]) => {
    const isSuccess = isSuccessStatus(status)
    if ((kind === "success" && isSuccess) || (kind === "error" && !isSuccess)) {
      out.push({ response, status })
    }
  })

  if (kind === "success") {
    Object.entries(rawResponsesFor(plugin, operation)).forEach(([status, rawResponse]) => {
      const hasTextEventStream = Boolean(rawResponse?.content?.["text/event-stream"]?.schema)
      const hasResponseEntry = out.some((entry) =>
        entry.status === status &&
        (entry.variant === "sse" || entry.response?.mediaType === "text/event-stream")
      )
      if (hasTextEventStream && isSuccessStatus(status) && !hasResponseEntry) {
        out.push({
          response: {
            mediaType: "text/event-stream",
            schema: rawResponse.content["text/event-stream"].schema
          },
          status,
          variant: "sse"
        })
      }
    })
  }

  return out
}

const responseExpression = (
  plugin: ClientEffectPlugin["Instance"],
  entry: ResponseEntry,
  artifacts: ResponseArtifacts
): any => {
  const key = responseKey(entry.status, entry.response, entry.variant)
  const symbol = key ? artifacts.responseSymbols.get(key) : undefined
  return symbol ? $(symbol) : responseSchema(plugin, entry.status, entry.response, entry.variant)
}

const errorSetKeyFor = (entries: ReadonlyArray<ResponseEntry>): string | undefined => {
  const keys = entries.map((entry) => responseKey(entry.status, entry.response, entry.variant))
  return keys.every((key): key is string => Boolean(key)) ? keys.join("|") : undefined
}

const errorSetSymbolName = (entries: ReadonlyArray<ResponseEntry>): string => {
  const statuses = entries.map((entry) => entry.status).join("")
  const refs = entries
    .map((entry) => entry.response?.schema?.$ref ? refName(entry.response.schema.$ref) : undefined)
    .filter((name): name is string => Boolean(name))
  if (refs.length && refs.every((name) => name === "ErrorInfo")) {
    switch (statuses) {
      case "400403408":
        return "CommonErrorInfo"
      case "400403408409":
        return "ConflictErrorInfo"
      case "400403404408":
        return "ResourceErrorInfo"
      case "400403404408409":
        return "ResourceConflictErrorInfo"
      default:
        break
    }
  }
  const schemaPrefix = refs.length && refs.every((name) => name === refs[0])
    ? `${toCase(refs[0]!, "PascalCase")}Errors`
    : "ErrorResponses"
  return `${schemaPrefix}${statuses}`
}

const emitResponseArtifacts = (
  plugin: ClientEffectPlugin["Instance"],
  operations: ReadonlyArray<IR.OperationObject>
): ResponseArtifacts => {
  const responseCounts = new Map<string, number>()
  const responseEntriesByKey = new Map<string, ResponseEntry>()
  const errorSetCounts = new Map<string, number>()
  const errorSetEntriesByKey = new Map<string, ReadonlyArray<ResponseEntry>>()

  operations.forEach((operation) => {
    ;(["success", "error"] as const).forEach((kind) => {
      const entries = responseEntriesFor(plugin, operation, kind)
      entries.forEach((entry) => {
        const key = responseKey(entry.status, entry.response, entry.variant)
        if (key && kind === "error") {
          responseCounts.set(key, (responseCounts.get(key) ?? 0) + 1)
          responseEntriesByKey.set(key, entry)
        }
      })
      if (kind === "error" && entries.length > 1) {
        const setKey = errorSetKeyFor(entries)
        if (setKey) {
          errorSetCounts.set(setKey, (errorSetCounts.get(setKey) ?? 0) + 1)
          errorSetEntriesByKey.set(setKey, entries)
        }
      }
    })
  })

  const responseSymbols = new Map<string, any>()
  responseCounts.forEach((count, key) => {
    const entry = responseEntriesByKey.get(key)
    const symbolName = entry ? responseSymbolName(entry) : undefined
    if (entry && symbolName && entry.response?.schema?.$ref && refName(entry.response.schema.$ref) === "ErrorInfo") {
      const symbol = plugin.symbol(symbolName, {
        meta: {
          category: "client",
          resource: "response",
          resourceId: key
        }
      })
      responseSymbols.set(key, symbol)
      plugin.node($.const(symbol).assign(responseSchema(plugin, entry.status, entry.response, entry.variant)))
    }
  })

  const responseSetSymbols = new Map<string, any>()
  errorSetCounts.forEach((count, key) => {
    const entries = errorSetEntriesByKey.get(key)
    const allResponsesHaveSymbols = entries?.every((entry) => {
      const itemKey = responseKey(entry.status, entry.response, entry.variant)
      return Boolean(itemKey && responseSymbols.has(itemKey))
    }) ?? false
    if (entries && count > 1 && allResponsesHaveSymbols) {
      const symbol = plugin.symbol(errorSetSymbolName(entries), {
        meta: {
          category: "client",
          resource: "response-set",
          resourceId: key
        }
      })
      responseSetSymbols.set(key, symbol)
      plugin.node(
        $.const(symbol)
          .assign(
            $.array(
              ...entries.map((entry) => {
                const itemKey = responseKey(entry.status, entry.response, entry.variant)!
                return $(responseSymbols.get(itemKey))
              })
            )
          )
      )
    }
  })

  return { responseSetSymbols, responseSymbols }
}

const responsesFor = (
  plugin: ClientEffectPlugin["Instance"],
  operation: IR.OperationObject,
  kind: "error" | "success",
  artifacts: ResponseArtifacts
): any => {
  const entries = responseEntriesFor(plugin, operation, kind)
  const setKey = kind === "error" ? errorSetKeyFor(entries) : undefined
  const setSymbol = setKey ? artifacts.responseSetSymbols.get(setKey) : undefined
  if (setSymbol) {
    return $(setSymbol)
  }
  if (entries.length === 1) {
    return responseExpression(plugin, entries[0]!, artifacts)
  }
  if (entries.length > 1) {
    return $.array(...entries.map((entry) => responseExpression(plugin, entry, artifacts)))
  }
  return undefined
}

const routePath = (path: string): string => path.replace(/\{([^}]+)\}/g, ":$1")

const methodName = (method: string): string => method.toLowerCase()

interface OperationRenderModel {
  readonly endpoint: any
  readonly groupIdentifier: string
}

const addEndpoint = (
  plugin: ClientEffectPlugin["Instance"],
  operation: IR.OperationObject,
  artifacts: ResponseArtifacts
): OperationRenderModel => {
  const options = $.object()
  const params = parametersFields(plugin, operation.parameters?.path)
  const query = parametersFields(plugin, operation.parameters?.query)
  const headers = parametersFields(plugin, operation.parameters?.header)
  const payload = operation.body?.schema ? schemaExpression(plugin, operation.body.schema) : undefined
  const success = responsesFor(plugin, operation, "success", artifacts)
  const error = responsesFor(plugin, operation, "error", artifacts)

  if (params) {
    options.prop("params", params)
  }
  if (query) {
    options.prop("query", query)
  }
  if (headers) {
    options.prop("headers", headers)
  }
  if (payload) {
    options.prop("payload", payload)
  }
  if (success) {
    options.prop("success", success)
  }
  if (error) {
    options.prop("error", error)
  }

  const endpoint = $(plugin.imports.HttpApiEndpoint)
    .attr(methodName(operation.method))
    .call($.literal(operation.id), $.literal(routePath(operation.path)), options)

  return {
    endpoint,
    groupIdentifier: operation.tags?.[0] ?? "S2"
  }
}

export const handler: ClientEffectPlugin["Handler"] = ({ plugin }) => {
  const apiSymbol = plugin.symbol("S2Api", {
    meta: {
      category: "client",
      resource: "api"
    }
  })
  const apiTypeSymbol = plugin.symbol("S2ProtocolClientApi", {
    meta: {
      category: "client",
      resource: "api-type"
    }
  })
  const optionsSymbol = plugin.symbol("S2ProtocolClientOptions", {
    meta: {
      category: "client",
      resource: "options"
    }
  })
  const serviceSymbol = plugin.symbol("S2ProtocolClient", {
    meta: {
      category: "client",
      resource: "service"
    }
  })
  const makeSymbol = plugin.symbol("make", {
    meta: {
      category: "client",
      resource: "make"
    }
  })
  const layerSymbol = plugin.symbol("layer", {
    meta: {
      category: "client",
      resource: "layer"
    }
  })

  const operations: Array<IR.OperationObject> = []
  plugin.forEach(
    "operation",
    ({ operation }: any) => {
      operations.push(operation)
    },
    { order: "declarations" }
  )

  const responseArtifacts = emitResponseArtifacts(plugin, operations)
  const renders = operations.map((operation) => addEndpoint(plugin, operation, responseArtifacts))
  const groupSymbols = new Map<string, any>()
  const operationsByGroup = new Map<string, Array<OperationRenderModel>>()
  renders.forEach((render) => {
    const items = operationsByGroup.get(render.groupIdentifier) ?? []
    items.push(render)
    operationsByGroup.set(render.groupIdentifier, items)
  })

  operationsByGroup.forEach((renders, identifier) => {
    const groupSymbol = plugin.symbol(`${toCase(identifier, "PascalCase")}Group`, {
      meta: {
        category: "client",
        resource: "group",
        resourceId: identifier
      }
    })
    groupSymbols.set(identifier, groupSymbol)
    plugin.node(
      $.const(groupSymbol)
        .assign(
          $(plugin.imports.HttpApiGroup)
            .attr("make")
            .call($.literal(identifier))
            .attr("add")
            .call(...renders.map((render) => render.endpoint))
        )
    )
  })

  plugin.node(
    $.const(apiSymbol)
      .export()
      .assign(
        $(plugin.imports.HttpApi)
          .attr("make")
          .call($.literal("S2Api"))
          .attr("add")
          .call(...[...groupSymbols.values()].map((symbol) => $(symbol)))
      )
  )

  plugin.node(
    $.type
      .alias(apiTypeSymbol)
      .export()
      .type($.type(plugin.imports.HttpApiClient).attr("ForApi").generic($.type.query(apiSymbol)))
  )

  const optionsType = $.type.object()
  optionsType.prop("baseUrl", (prop) =>
    prop
      .readonly()
      .optional()
      .type($.type.or($.type("URL"), $.type("string"))))
  optionsType.prop("transformClient", (prop) => {
    const fn = $.type.func()
    fn.param("client", (param) => param.type($.type(plugin.imports.HttpClient).attr("HttpClient")))
    fn.returns($.type(plugin.imports.HttpClient).attr("HttpClient"))
    prop.readonly().optional().type(fn)
  })
  optionsType.prop("transformResponse", (prop) => {
    const fn = $.type.func()
    fn.param("effect", (param) =>
      param.type(
        $.type(plugin.imports.Effect).attr("Effect").generics("unknown", "unknown", "unknown")
      ))
    fn.returns($.type(plugin.imports.Effect).attr("Effect").generics("unknown", "unknown", "unknown"))
    prop.readonly().optional().type(fn)
  })

  plugin.node($.type.alias(optionsSymbol).export().type(optionsType))

  const serviceBase = $(plugin.imports.Context)
    .attr("Service")
    .call()
    .generics(serviceSymbol, apiTypeSymbol)
    .call($.literal("@firegrid/log/generated/client-effect.gen/S2ProtocolClient"))

  plugin.node(
    $.class(serviceSymbol)
      .export()
      .extends(serviceBase as any)
  )

  plugin.node(
    $.const(makeSymbol)
      .export()
      .assign(
        $(plugin.imports.Effect)
          .attr("fn")
          .call($.literal("S2ProtocolClient.make"))
          .call(
            $.func((fn) => {
              fn.param("options", (param) => {
                param.optional()
                param.type($.type(optionsSymbol))
              })
              fn.do($.return($(plugin.imports.HttpApiClient).attr("make").call($(apiSymbol), $("options"))))
            })
          )
      )
  )

  plugin.node(
    $.const(layerSymbol)
      .export()
      .assign(
        $.func((fn) => {
          fn.param("options", (param) => {
            param.optional()
            param.type($.type(optionsSymbol))
          })
          fn.do(
            $.return(
              $(plugin.imports.Layer)
                .attr("effect")
                .call($(serviceSymbol), $(makeSymbol).call($("options")))
            )
          )
        })
      )
  )
}
