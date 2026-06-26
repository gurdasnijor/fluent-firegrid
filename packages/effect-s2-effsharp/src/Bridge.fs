namespace Firegrid.EffectS2.EffSharp

open Effect

type AppendStringCommand =
    { Basin: string
      Stream: string
      Body: string
      MatchSeqNum: float option }

type StreamTarget = { Basin: string; Stream: string }

[<RequireQualifiedAccess>]
module EffectS2 =

    let config (accessToken: string) : S2Config = S2Config.Create accessToken

    let layer (accessToken: string) : Layer<S2Error, 'RIn> = S2.layer (config accessToken)

    let appendString (command: AppendStringCommand) : Effect<S2AppendAck, S2Error, Context> =
        S2.Stream.append
            { Target =
                { Basin = command.Basin
                  Stream = command.Stream }
              Records = [ S2.AppendRecord.string command.Body ]
              Options =
                Some
                    { S2AppendOptions.Empty with
                        MatchSeqNum = command.MatchSeqNum }
              RequestOptions = None }

    let checkTail (target: StreamTarget) : Effect<S2TailResponse, S2Error, Context> =
        S2.Stream.checkTail { Basin = target.Basin; Stream = target.Stream }

    let listBasins (prefix: string option) : Effect<S2ListBasinsResponse, S2Error, Context> =
        S2.Basins.list (Some { S2PageRequest.Empty with Prefix = prefix })
