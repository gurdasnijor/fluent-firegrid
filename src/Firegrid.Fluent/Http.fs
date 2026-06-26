namespace Firegrid.Fluent

type FluentHttpHandler =
    { Handle: obj -> obj }

[<RequireQualifiedAccess>]
module Http =
    let createFluentHttpHandler handle = { Handle = handle }
