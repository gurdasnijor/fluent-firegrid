import { HttpClientRequest } from "@effect/platform"
import { appendHeaders, readHeaders, sseReadHeaders } from "./headers.ts"
import { readStreamUrl, streamUrl, type ReadStreamUrlOptions } from "./routes.ts"

export interface StreamRequest {
  readonly baseUrl: string
  readonly path: string
}

export interface CreateStreamRequest extends StreamRequest {
  readonly contentType: string
  readonly closed?: boolean
}

export interface AppendStreamRequest extends StreamRequest {
  readonly contentType: string
  readonly bytes: Uint8Array
  readonly closed?: boolean
}

export interface ReadStreamRequest extends StreamRequest, ReadStreamUrlOptions {}

const closedOption = (closed: boolean | undefined): { readonly closed?: boolean } =>
  closed === undefined ? {} : { closed }

export const putStreamRequest = (request: CreateStreamRequest): HttpClientRequest.HttpClientRequest =>
  HttpClientRequest.put(streamUrl(request.baseUrl, request.path)).pipe(
    HttpClientRequest.setHeaders(appendHeaders(request.contentType, closedOption(request.closed))),
  )

export const appendStreamRequest = (request: AppendStreamRequest): HttpClientRequest.HttpClientRequest =>
  HttpClientRequest.post(streamUrl(request.baseUrl, request.path)).pipe(
    HttpClientRequest.setHeaders(appendHeaders(request.contentType, closedOption(request.closed))),
    HttpClientRequest.bodyUint8Array(request.bytes),
  )

export const headStreamRequest = (request: StreamRequest): HttpClientRequest.HttpClientRequest =>
  HttpClientRequest.head(streamUrl(request.baseUrl, request.path))

export const readStreamRequest = (request: ReadStreamRequest): HttpClientRequest.HttpClientRequest =>
  HttpClientRequest.get(readStreamUrl(request.baseUrl, request.path, request)).pipe(
    HttpClientRequest.setHeaders(request.live === "sse" ? sseReadHeaders : readHeaders),
  )

export const deleteStreamRequest = (request: StreamRequest): HttpClientRequest.HttpClientRequest =>
  HttpClientRequest.del(streamUrl(request.baseUrl, request.path))
