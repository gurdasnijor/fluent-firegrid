namespace Firegrid.FluentFiregrid.S2

open Effect

[<RequireQualifiedAccess>]
module Runtime =

    let create (config: S2ObjectStateBackendConfig) : S2Runtime =
        let accessToken = config.AccessToken |> Option.defaultValue "s2_access_token"

        let s2Config =
            S2.configWithEndpoint accessToken config.S2Endpoint

        { Basin = config.Basin |> Option.defaultValue "fluent-firegrid"
          Namespace = config.Namespace |> Option.defaultValue "default"
          Layer = S2.layer s2Config }

    let provide (runtime: S2Runtime) (effect: Effect<'A, S2Error, Context>) : Effect<'A, S2Error, unit> =
        Layer.provide runtime.Layer effect

    let streamTarget (runtime: S2Runtime) (streamName: string) : S2StreamRef =
        S2.streamRef runtime.Basin streamName
