import type { IR } from "@hey-api/openapi-ts"
import { $, toCase } from "@hey-api/openapi-ts"

import type { EffectSchemaPlugin } from "./types"

const schemaName = (name: string): string => `${toCase(name, "PascalCase")}Schema`

const refName = (ref: string): string => decodeURIComponent(ref.split("/").at(-1) || ref)

const schemaSymbol = (plugin: EffectSchemaPlugin["Instance"], name: string) =>
  plugin.symbolOnce(schemaName(name), {
    meta: {
      category: "schema",
      resource: "definition",
      resourceId: name
    }
  })

const schemaCall = (
  plugin: EffectSchemaPlugin["Instance"],
  name: string,
  args: ReadonlyArray<any> = []
) => $(plugin.imports.Schema).attr(name).call(...args)

const literal = (value: unknown) => $.literal(value as string | number | boolean | null)

const schemaGetterTransform = (plugin: EffectSchemaPlugin["Instance"], paramName: string, expression: any) =>
  $(plugin.imports.SchemaGetter).attr("transform").call(
    $.func((fn) => {
      fn.param(paramName)
      fn.do($.return(expression))
    })
  )

const u64Expression = (plugin: EffectSchemaPlugin["Instance"]): any => {
  const encoded = $(plugin.imports.Schema)
    .attr("Number")
    .attr("check")
    .call(
      $(plugin.imports.Schema).attr("isInt").call(),
      $(plugin.imports.Schema).attr("isGreaterThanOrEqualTo").call($.literal(0))
    )

  const decoded = $(plugin.imports.Schema)
    .attr("BigInt")
    .attr("check")
    .call(
      $(plugin.imports.Schema).attr("isGreaterThanOrEqualToBigInt").call($("BigInt").call($.literal(0))),
      $(plugin.imports.Schema)
        .attr("isLessThanOrEqualToBigInt")
        .call($("BigInt").call($("Number").attr("MAX_SAFE_INTEGER")))
    )

  return encoded.attr("pipe").call(
    $(plugin.imports.Schema).attr("decodeTo").call(
      decoded,
      $.object()
        .prop("decode", schemaGetterTransform(plugin, "value", $("BigInt").call($("value"))))
        .prop("encode", schemaGetterTransform(plugin, "value", $("Number").call($("value"))))
    )
  )
}

const isInt64 = (schema: IR.SchemaObject): boolean => schema.type === "integer" && (schema as any).format === "int64"

const stringExpression = (
  plugin: EffectSchemaPlugin["Instance"],
  schema: IR.SchemaObject
): any => {
  if ((schema as any).format === "date-time") {
    return $(plugin.imports.Schema).attr("DateFromString")
  }

  const checks: Array<any> = []
  const minLength = (schema as any).minLength
  const maxLength = (schema as any).maxLength
  const pattern = (schema as any).pattern
  if (typeof minLength === "number") {
    checks.push($(plugin.imports.Schema).attr("isMinLength").call($.literal(minLength)))
  }
  if (typeof maxLength === "number") {
    checks.push($(plugin.imports.Schema).attr("isMaxLength").call($.literal(maxLength)))
  }
  if (typeof pattern === "string" && pattern.length > 0) {
    checks.push($(plugin.imports.Schema).attr("isPattern").call($("RegExp").call($.literal(pattern))))
  }

  const base = $(plugin.imports.Schema).attr("String")
  return checks.length ? base.attr("check").call(...checks) : base
}

const schemaExpression = (
  plugin: EffectSchemaPlugin["Instance"],
  schema: IR.SchemaObject | undefined,
  seen: ReadonlySet<IR.SchemaObject> = new Set()
): any => {
  if (!schema) {
    return $(plugin.imports.Schema).attr("Unknown")
  }
  if (schema.$ref) {
    const name = refName(schema.$ref)
    return schemaCall(plugin, "suspend", [
      $.func((fn) => fn.do($.return($(schemaSymbol(plugin, name)))))
    ])
  }
  if (schema.symbolRef) {
    return schemaCall(plugin, "suspend", [
      $.func((fn) => fn.do($.return($(schema.symbolRef))))
    ])
  }
  if (seen.has(schema)) {
    return schemaCall(plugin, "suspend", [
      $.func((fn) => fn.do($.return($(plugin.imports.Schema).attr("Unknown"))))
    ])
  }

  const nextSeen = new Set(seen).add(schema)

  if (isInt64(schema)) {
    return u64Expression(plugin)
  }

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
      return $(plugin.imports.Schema)
        .attr("Number")
        .attr("check")
        .call(
          $(plugin.imports.Schema).attr("isInt").call(),
          ...(
            typeof (schema as any).minimum === "number"
              ? [$(plugin.imports.Schema).attr("isGreaterThanOrEqualTo").call($.literal((schema as any).minimum))]
              : []
          )
        )
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
      let result = schemaCall(plugin, "Struct", [object])
      if (schema.additionalProperties && typeof schema.additionalProperties === "object") {
        result = schemaCall(plugin, "Record", [
          $(plugin.imports.Schema).attr("String"),
          schemaExpression(plugin, schema.additionalProperties, nextSeen)
        ])
      }
      return result
    }
    case "string":
      return stringExpression(plugin, schema)
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

export const handler: EffectSchemaPlugin["Handler"] = ({ plugin }) => {
  plugin.forEach(
    "schema",
    ({ name, schema }: any) => {
      const symbol = schemaSymbol(plugin, name)
      plugin.node($.const(symbol).export().assign(schemaExpression(plugin, schema)))
    },
    { order: "declarations" }
  )
}

export const effectSchemaNames = {
  schemaName
}
