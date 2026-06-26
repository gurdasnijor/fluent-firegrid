namespace Firegrid.Core

/// `createMiddleware()` builder (`middleware/create-middleware.ts`).
/// `ModuleSuffix` lets this module share the `Middleware` name with the type.
[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Middleware =

    type CreateMiddlewareBuilder = { Server: MiddlewareServerFn -> Middleware }

    /// `createMiddleware()` — returns a builder whose `.server(fn)` produces a
    /// `{ __kind: "middleware", server }` middleware.
    let createMiddleware () : CreateMiddlewareBuilder =
        { Server = fun fn -> { Kind = "middleware"; Server = fn } }
