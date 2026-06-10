export type LiveReadMode = "long-poll" | "sse"

export interface ReadStreamUrlOptions {
  readonly offset?: string
  readonly live?: LiveReadMode
  readonly cursor?: string
}

export const reservedControlSegment = "__ds"

export const trimTrailingSlash = (url: string): string => url.replace(/\/+$/u, "")

export const joinUrlPath = (baseUrl: string, path: string): string =>
  `${trimTrailingSlash(baseUrl)}${path.startsWith("/") ? path : `/${path}`}`

export const normalizeStreamPath = (path: string): string =>
  path.replace(/^\/+/u, "").replace(/\/+$/u, "")

export const encodeStreamPath = (path: string): string =>
  normalizeStreamPath(path)
    .split("/")
    .map(encodeURIComponent)
    .join("/")

export const streamUrl = (baseUrl: string, path: string): string =>
  joinUrlPath(baseUrl, encodeStreamPath(path))

export const readStreamUrl = (
  baseUrl: string,
  path: string,
  options: ReadStreamUrlOptions = {},
): string => {
  const url = new URL(streamUrl(baseUrl, path))
  if (options.offset !== undefined) {
    url.searchParams.set("offset", options.offset)
  }
  if (options.live !== undefined) {
    url.searchParams.set("live", options.live)
  }
  if (options.cursor !== undefined) {
    url.searchParams.set("cursor", options.cursor)
  }
  return url.toString()
}

export const subscriptionUrl = (baseUrl: string, streamPath: string, id: string): string =>
  joinUrlPath(streamUrl(baseUrl, streamPath), `${reservedControlSegment}/subscriptions/${encodeURIComponent(id)}`)

export const isReservedControlPath = (path: string): boolean =>
  normalizeStreamPath(path).split("/")[0] === reservedControlSegment
