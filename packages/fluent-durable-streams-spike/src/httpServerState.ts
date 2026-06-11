/* eslint-disable local/no-date-now */
import type { StreamProblem } from "./model.ts"
import {
  badRequest,
  STREAM_EXPIRES_AT,
  STREAM_TTL,
} from "./httpShared.ts"

export interface Lifetime {
  readonly ttl?: string
  readonly expiresAt?: string
  readonly deadline: number
}

type HeaderMap = Record<string, string | readonly string[] | undefined>

const readonlyHeaderOptional = (value: string | readonly string[] | undefined): string | undefined =>
  typeof value === "string" ? value : value?.[0]

export interface HttpServerState {
  readonly isExpired: (path: string) => boolean
  readonly touchLifetime: (path: string) => void
  readonly lifetimeHeaders: (path: string) => Record<string, string>
  readonly parseLifetime: (headers: HeaderMap) => Lifetime | StreamProblem | undefined
  readonly getLifetime: (path: string) => Lifetime | undefined
  readonly setLifetime: (path: string, lifetime: Lifetime) => void
  readonly deleteLifetime: (path: string) => void
  readonly getForkSpec: (path: string) => string | undefined
  readonly setForkSpec: (path: string, spec: string) => void
}

export const makeHttpServerState = (): HttpServerState => {
  const lifetimes = new Map<string, Lifetime>()
  const forkSpecs = new Map<string, string>()

  const lifetimeHeaders = (path: string): Record<string, string> => {
    const lifetime = lifetimes.get(path)
    if (lifetime === undefined) {
      return {}
    }
    return {
      ...(lifetime.ttl !== undefined && { [STREAM_TTL]: lifetime.ttl }),
      ...(lifetime.expiresAt !== undefined && { [STREAM_EXPIRES_AT]: lifetime.expiresAt }),
    }
  }

  const parseLifetime = (headers: HeaderMap): Lifetime | StreamProblem | undefined => {
    const ttl = readonlyHeaderOptional(headers[STREAM_TTL])
    const expiresAt = readonlyHeaderOptional(headers[STREAM_EXPIRES_AT])
    if (ttl !== undefined && expiresAt !== undefined) {
      return badRequest("stream-ttl and stream-expires-at are mutually exclusive")
    }
    if (ttl !== undefined) {
      if (!/^[1-9][0-9]*$/.test(ttl)) {
        return badRequest("stream-ttl must be a positive integer without leading zeroes")
      }
      return { ttl, deadline: Date.now() + Number(ttl) * 1000 }
    }
    if (expiresAt !== undefined) {
      const deadline = Date.parse(expiresAt)
      if (!Number.isFinite(deadline)) {
        return badRequest("stream-expires-at must be a valid timestamp")
      }
      return { expiresAt, deadline }
    }
    return undefined
  }

  return {
    isExpired: (path) => {
      const lifetime = lifetimes.get(path)
      return lifetime !== undefined && Date.now() >= lifetime.deadline
    },
    touchLifetime: (path) => {
      const lifetime = lifetimes.get(path)
      if (lifetime?.ttl !== undefined) {
        lifetimes.set(path, { ...lifetime, deadline: Date.now() + Number(lifetime.ttl) * 1000 })
      }
    },
    lifetimeHeaders,
    parseLifetime,
    getLifetime: (path) => lifetimes.get(path),
    setLifetime: (path, lifetime) => {
      lifetimes.set(path, lifetime)
    },
    deleteLifetime: (path) => {
      lifetimes.delete(path)
    },
    getForkSpec: (path) => forkSpecs.get(path),
    setForkSpec: (path, spec) => {
      forkSpecs.set(path, spec)
    },
  }
}
