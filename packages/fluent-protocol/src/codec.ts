import { Effect, Schema, pipe } from "effect"
import { makeTransportMessage, type TransportMessage } from "@firegrid/fluent-transport"
import {
  ProtocolCodecError,
  ProtocolEnvelope,
  ProtocolValidationError,
  type ProtocolEnvelope as ProtocolEnvelopeType,
} from "./protocol.ts"

export const protocolTransportType = "fluent.protocol"

const stringify = (input: unknown) =>
  Effect.try({
    try: () => JSON.stringify(input),
    catch: (cause) =>
      new ProtocolCodecError({
        message: "Failed to encode protocol envelope",
        cause,
      }),
  })

const parse = (input: string) =>
  Effect.try({
    try: () => JSON.parse(input) as unknown,
    catch: (cause) =>
      new ProtocolValidationError({
        message: "Failed to parse protocol envelope",
        rawData: input,
        cause,
      }),
  })

export const encodeProtocolEnvelope = (envelope: ProtocolEnvelopeType) =>
  pipe(
    envelope,
    Schema.encode(ProtocolEnvelope),
    Effect.flatMap(stringify),
    Effect.map((payload) => makeTransportMessage(envelope.id, protocolTransportType, payload)),
  )

export const decodeProtocolEnvelope = (message: TransportMessage) =>
  parse(message.payload).pipe(
    Effect.flatMap((rawData) =>
      pipe(
        rawData,
        Schema.decodeUnknown(ProtocolEnvelope),
        Effect.mapError(
          (cause) =>
            new ProtocolValidationError({
              message: "Invalid protocol envelope",
              rawData,
              cause,
            }),
        ),
      ),
    ),
  )
