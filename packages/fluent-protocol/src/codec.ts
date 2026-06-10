import { Effect, Schema, pipe } from "effect"
import { makeTransportMessage, type TransportMessage } from "@firegrid/fluent-transport"
import {
  ProtocolCodecError,
  ProtocolEnvelope,
  ProtocolValidationError,
  type ProtocolEnvelope as ProtocolEnvelopeType,
} from "./protocol.ts"

export const protocolTransportType = "fluent.protocol"

const ProtocolEnvelopeJson = Schema.parseJson(ProtocolEnvelope)

export const encodeProtocolEnvelope = (envelope: ProtocolEnvelopeType) =>
  pipe(
    envelope,
    Schema.encode(ProtocolEnvelopeJson),
    Effect.mapError(
      (cause) =>
        new ProtocolCodecError({
          message: "Failed to encode protocol envelope",
          cause,
        }),
    ),
    Effect.map((payload) => makeTransportMessage(envelope.id, protocolTransportType, payload)),
  )

export const decodeProtocolEnvelope = (message: TransportMessage) =>
  pipe(
    message.payload,
    Schema.decodeUnknown(ProtocolEnvelopeJson),
    Effect.mapError(
      (cause) =>
        new ProtocolValidationError({
          message: "Invalid protocol envelope",
          rawData: message.payload,
          cause,
        }),
    ),
  )
