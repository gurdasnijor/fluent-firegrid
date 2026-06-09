/**
 * Durable Streams conformance test suite.
 *
 * Bundles both the client and server conformance engines in a single package.
 * The two engines have distinct `runConformanceTests` entry points, so they are
 * exposed as namespaces here and as `./client` / `./server` subpath exports.
 *
 * @packageDocumentation
 */

export * as client from "./client/index.ts"
export * as server from "./server/index.ts"
