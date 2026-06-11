import { Data, Effect, Schema } from "effect"

export type JsonValue = null | boolean | number | string | readonly JsonValue[] | { readonly [key: string]: JsonValue }

export type StreamBody = Uint8Array | string | JsonValue

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

export class InvalidContent extends Data.TaggedError("InvalidContent")<
  Readonly<{
    readonly message: string
    readonly cause?: unknown
  }>
> {}

export const contentTypeEssence = (contentType: string): string =>
  contentType.split(";")[0]?.trim().toLowerCase() ?? ""

export const isJsonContentType = (contentType: string): boolean =>
  contentTypeEssence(contentType) === "application/json"

export const isUtf8ReadableContentType = (contentType: string): boolean => {
  const essence = contentTypeEssence(contentType)
  return essence === "application/json" || essence.startsWith("text/")
}

const isJsonArray = (value: JsonValue): value is readonly JsonValue[] =>
  Array.isArray(value)

const parseJsonString = (input: string): Effect.Effect<JsonValue, InvalidContent> =>
  Effect.try({
    try: () => JSON.parse(input) as unknown,
    catch: (cause) =>
      new InvalidContent({
        message: "application/json body is not valid JSON",
        cause,
      }),
  }).pipe(
    Effect.flatMap((parsed) =>
      Schema.decodeUnknownEffect(Schema.Json)(parsed).pipe(
        Effect.map((value): JsonValue => value),
        Effect.mapError((cause) =>
          new InvalidContent({
            message: "application/json body is not JSON-serializable",
            cause,
          }),
        ),
      ),
    ),
  )

const stringifyJson = (value: JsonValue) =>
  Effect.try({
    try: () => textEncoder.encode(JSON.stringify(value)),
    catch: (cause) =>
      new InvalidContent({
        message: "application/json value cannot be encoded",
        cause,
      }),
  })

const bodyToJson = (body: StreamBody): Effect.Effect<JsonValue, InvalidContent> => {
  if (body instanceof Uint8Array) {
    return parseJsonString(textDecoder.decode(body))
  }
  if (typeof body === "string") {
    return parseJsonString(body)
  }
  return Schema.decodeUnknownEffect(Schema.Json)(body).pipe(
    Effect.map((value) => value),
    Effect.mapError((cause) =>
      new InvalidContent({
        message: "application/json body is not JSON-serializable",
        cause,
      }),
    ),
  )
}

const opaqueBody = (body: StreamBody): Effect.Effect<readonly Uint8Array[], InvalidContent> => {
  if (body instanceof Uint8Array) {
    return Effect.succeed(body.length === 0 ? [] : [body])
  }
  if (typeof body === "string") {
    const bytes = textEncoder.encode(body)
    return Effect.succeed(bytes.length === 0 ? [] : [bytes])
  }
  return Effect.fail(
    new InvalidContent({
      message: "opaque stream bodies must be Uint8Array or string",
    }),
  )
}

export const encodeBody = (
  contentType: string,
  body: StreamBody | undefined,
  options?: { readonly rejectEmptyJsonArray?: boolean },
): Effect.Effect<readonly Uint8Array[], InvalidContent> => {
  if (body === undefined) {
    return Effect.succeed([])
  }

  if (!isJsonContentType(contentType)) {
    return opaqueBody(body)
  }

  return bodyToJson(body).pipe(
    Effect.flatMap((json) => {
      const items = isJsonArray(json) ? json : undefined
      if (items !== undefined) {
        if (items.length === 0) {
          return options?.rejectEmptyJsonArray === true
            ? Effect.fail(
              new InvalidContent({
                message: "application/json append bodies must not be an empty array",
              }),
            )
            : Effect.succeed([])
        }
        return Effect.all(items.map((item) => stringifyJson(item)), { concurrency: "unbounded" })
      }
      return stringifyJson(json).pipe(Effect.map((bytes) => [bytes]))
    }),
  )
}

export const decodeJsonRecords = (records: readonly { readonly bytes: Uint8Array }[]) =>
  Effect.all(
    records.map((record) => parseJsonString(textDecoder.decode(record.bytes))),
    { concurrency: "unbounded" },
  )
