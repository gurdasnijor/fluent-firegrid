import type { Effect, Scope, Stream } from "effect"
import type { ReadOffset, StreamPath } from "@firegrid/fluent-stream-log"
import type { StreamBody } from "./content.ts"
import { makeProducer, type Producer, type ProducerConfig } from "./producer.ts"
import type {
  AppendStreamCommand,
  AppendStreamOutcome,
  CreateStreamOutcome,
  DeleteStreamOutcome,
  DurableStreamsChannel,
  HeadStreamOutcome,
  ReadStreamOutcome,
  StreamEvent,
  StreamProblem,
} from "./model.ts"

export interface DurableStreamHandle {
  readonly path: StreamPath
  readonly contentType: string
  readonly create: (options?: {
    readonly body?: StreamBody
    readonly closed?: boolean
  }) => Effect.Effect<CreateStreamOutcome>
  readonly append: (body: StreamBody, options?: {
    readonly close?: boolean
    readonly expectedTailOffset?: AppendStreamCommand["expectedTailOffset"]
  }) => Effect.Effect<AppendStreamOutcome>
  readonly close: () => Effect.Effect<AppendStreamOutcome>
  readonly read: (offset?: ReadOffset) => Effect.Effect<ReadStreamOutcome>
  readonly readJson: (offset?: ReadOffset) => Effect.Effect<ReadStreamOutcome>
  readonly follow: (offset?: ReadOffset) => Effect.Effect<Stream.Stream<StreamEvent, StreamProblem>, StreamProblem, Scope.Scope>
  readonly head: () => Effect.Effect<HeadStreamOutcome>
  readonly delete: () => Effect.Effect<DeleteStreamOutcome>
  readonly producer: (config: Omit<ProducerConfig, "path" | "contentType">) => Effect.Effect<Producer>
}

export interface DurableStreamsClient {
  readonly stream: (path: StreamPath, contentType: string) => DurableStreamHandle
  readonly create: (
    path: StreamPath,
    contentType: string,
    options?: { readonly body?: StreamBody; readonly closed?: boolean },
  ) => Effect.Effect<CreateStreamOutcome>
  readonly append: (
    path: StreamPath,
    contentType: string,
    body: StreamBody,
    options?: {
      readonly close?: boolean
      readonly expectedTailOffset?: AppendStreamCommand["expectedTailOffset"]
    },
  ) => Effect.Effect<AppendStreamOutcome>
  readonly close: (path: StreamPath, contentType: string) => Effect.Effect<AppendStreamOutcome>
  readonly read: (path: StreamPath, offset?: ReadOffset) => Effect.Effect<ReadStreamOutcome>
  readonly readJson: (path: StreamPath, offset?: ReadOffset) => Effect.Effect<ReadStreamOutcome>
  readonly follow: (
    path: StreamPath,
    offset?: ReadOffset,
  ) => Effect.Effect<Stream.Stream<StreamEvent, StreamProblem>, StreamProblem, Scope.Scope>
  readonly head: (path: StreamPath) => Effect.Effect<HeadStreamOutcome>
  readonly delete: (path: StreamPath) => Effect.Effect<DeleteStreamOutcome>
  readonly producer: (config: ProducerConfig) => Effect.Effect<Producer>
}

export const makeClient = (channel: DurableStreamsChannel): DurableStreamsClient => {
  const client: DurableStreamsClient = {
    stream: (path, contentType) => ({
      path,
      contentType,
      create: (options) => client.create(path, contentType, options),
      append: (body, options) => client.append(path, contentType, body, options),
      close: () => client.close(path, contentType),
      read: (offset) => client.read(path, offset),
      readJson: (offset) => client.readJson(path, offset),
      follow: (offset) => client.follow(path, offset),
      head: () => client.head(path),
      delete: () => client.delete(path),
      producer: (config) => client.producer({ ...config, path, contentType }),
    }),
    create: (path, contentType, options) =>
      channel.create({
        path,
        contentType,
        ...(options?.body !== undefined && { body: options.body }),
        ...(options?.closed !== undefined && { closed: options.closed }),
      }),
    append: (path, contentType, body, options) =>
      channel.append({
        path,
        contentType,
        body,
        ...(options?.close !== undefined && { close: options.close }),
        ...(options?.expectedTailOffset !== undefined && {
          expectedTailOffset: options.expectedTailOffset,
        }),
      }),
    close: (path, contentType) =>
      channel.append({
        path,
        contentType,
        close: true,
      }),
    read: (path, offset) => channel.read({ path, ...(offset !== undefined && { offset }) }),
    readJson: (path, offset) => channel.readJson({ path, ...(offset !== undefined && { offset }) }),
    follow: (path, offset) => channel.follow({ path, ...(offset !== undefined && { offset }) }),
    head: (path) => channel.head(path),
    delete: (path) => channel.delete(path),
    producer: (config) => makeProducer(channel, config),
  }

  return client
}
