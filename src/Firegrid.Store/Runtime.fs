namespace Firegrid.Store

open Firegrid.Log

[<RequireQualifiedAccess>]
module Runtime =

    let create (config: S2ObjectStateBackendConfig) : S2Runtime =
        let accessToken = config.AccessToken |> Option.defaultValue "s2_access_token"

        let s2Config =
            { S2.ConnectOptions.create accessToken with
                AccountEndpoint = Some config.S2Endpoint
                BasinEndpoint = Some config.S2Endpoint }

        { Basin = config.Basin |> Option.defaultValue "fluent-firegrid"
          Namespace = config.Namespace |> Option.defaultValue "default"
          Client = S2.connectWith s2Config }

    let streamTarget (runtime: S2Runtime) (streamName: string) : S2StreamRef =
        { Basin = runtime.Basin; Stream = streamName }

    let basin (runtime: S2Runtime) : S2.Basin =
        runtime.Client |> S2.basin runtime.Basin

    let stream (runtime: S2Runtime) (target: S2StreamRef) : S2.Stream =
        runtime.Client |> S2.basin target.Basin |> S2.stream target.Stream

    let ensureTarget (runtime: S2Runtime) (target: S2StreamRef) : Async<S2StreamRef> =
        async {
            let basin = runtime.Client |> S2.basin target.Basin
            do! runtime.Client |> S2.ensureBasin target.Basin |> Async.Ignore
            do! basin |> S2.ensureStream target.Stream
            return target
        }

    let ensureStream (runtime: S2Runtime) (streamName: string) : Async<S2StreamRef> =
        ensureTarget runtime (streamTarget runtime streamName)
