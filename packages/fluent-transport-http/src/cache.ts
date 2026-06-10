import { CACHE_CONTROL, ETAG, STREAM_CURSOR } from "./headers.ts"

export interface CacheMetadata {
  readonly etag?: string
  readonly cursor?: string
  readonly maxAgeSeconds?: number
  readonly private?: boolean
}

export const cacheHeaders = (metadata: CacheMetadata): Readonly<Record<string, string>> => ({
  ...(metadata.etag !== undefined && { [ETAG]: metadata.etag }),
  ...(metadata.cursor !== undefined && { [STREAM_CURSOR]: metadata.cursor }),
  [CACHE_CONTROL]:
    metadata.maxAgeSeconds === undefined
      ? "no-store"
      : `${metadata.private === true ? "private" : "public"}, max-age=${metadata.maxAgeSeconds}`,
})
