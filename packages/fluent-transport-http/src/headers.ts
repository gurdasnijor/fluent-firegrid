export const CONTENT_TYPE = "content-type"
export const ACCEPT = "accept"
export const CACHE_CONTROL = "cache-control"
export const ETAG = "etag"
export const STREAM_CURSOR = "stream-cursor"
export const STREAM_NEXT_OFFSET = "stream-next-offset"
export const STREAM_UP_TO_DATE = "stream-up-to-date"
export const STREAM_CLOSED = "stream-closed"
export const STREAM_SSE_DATA_ENCODING = "stream-sse-data-encoding"

export const JSON_CONTENT_TYPE = "application/json"
export const SSE_CONTENT_TYPE = "text/event-stream"
export const BASE64_ENCODING = "base64"

export const browserSafetyHeaders = {
  "x-content-type-options": "nosniff",
  "referrer-policy": "no-referrer",
} as const

export const sseHeaders = {
  [CONTENT_TYPE]: SSE_CONTENT_TYPE,
  [CACHE_CONTROL]: "no-cache",
  connection: "keep-alive",
  ...browserSafetyHeaders,
} as const

export const readHeaders = {
  [ACCEPT]: "*/*",
} as const

export const sseReadHeaders = {
  [ACCEPT]: SSE_CONTENT_TYPE,
} as const

export const appendHeaders = (
  contentType: string,
  options: { readonly closed?: boolean } = {},
): Readonly<Record<string, string>> => ({
  [CONTENT_TYPE]: contentType,
  ...(options.closed === true && { [STREAM_CLOSED]: "true" }),
})

export const sseDataEncodingHeaders = (
  encoding: typeof BASE64_ENCODING | undefined,
): Readonly<Record<string, string>> =>
  encoding === undefined ? {} : { [STREAM_SSE_DATA_ENCODING]: encoding }
