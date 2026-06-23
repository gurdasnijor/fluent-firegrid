import * as NodeServices from "@effect/platform-node/NodeServices"
import * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import { describe, expect, it } from "vitest"
import { spawnAcpProcess } from "../src/process-owner.ts"
import { resolveAgent } from "../src/resolve-agent.ts"

// A fake ACP harness: emits one session/update notification on start and replies
// to any JSON-RPC request with `{ result: { ok: true } }`. UNIT aid only (F-A10)
// — not a real ACP process and not acceptance proof for the binding.
const FAKE_HARNESS = [
  "process.stdout.write(JSON.stringify({jsonrpc:'2.0',method:'session/update',params:{sessionId:'x',update:{sessionUpdate:'agent_message_chunk',content:{type:'text',text:'hi'}}}})+'\\n');",
  "let buf='';process.stdin.on('data',d=>{buf+=d;let i;while((i=buf.indexOf('\\n'))>=0){const line=buf.slice(0,i);buf=buf.slice(i+1);if(!line.trim())continue;const m=JSON.parse(line);if(m.id!==undefined&&m.method){process.stdout.write(JSON.stringify({jsonrpc:'2.0',id:m.id,result:{ok:true}})+'\\n')}}});"
].join("")

const runScoped = <A, E>(
  eff: Effect.Effect<A, E, NodeServices.NodeServices | Scope.Scope>
): Promise<A> => Effect.runPromise(Effect.scoped(eff).pipe(Effect.provide(NodeServices.layer)))

describe("resolveAgent", () => {
  it("maps known agents to their ACP adapters", () =>
    Effect.runPromise(
      Effect.gen(function*() {
        expect(yield* resolveAgent("claude")).toEqual({
          command: "npx",
          args: ["-y", "@zed-industries/claude-code-acp"]
        })
        expect(yield* resolveAgent("codex")).toEqual({
          command: "npx",
          args: ["-y", "@zed-industries/codex-acp"]
        })
        expect(yield* resolveAgent({ command: "my-acp", args: ["--x"] })).toEqual(
          { command: "my-acp", args: ["--x"] }
        )
      })
    ))

  it("fails with AcpProcessError on an unknown agent key", async () => {
    const exit = await Effect.runPromiseExit(resolveAgent("nope"))
    expect(exit._tag).toBe("Failure")
  })
})

describe("spawnAcpProcess (fake harness, unit-only)", () => {
  it("exposes the process stdio as a bidirectional acp.Stream", () =>
    runScoped(
      Effect.gen(function*() {
        const handle = yield* spawnAcpProcess({
          agent: { command: "node", args: ["-e", FAKE_HARNESS] },
          cwd: "."
        })

        const reader = handle.stream.readable.getReader()
        // Agent -> client: the harness notification is parsed off the stream.
        const first = yield* Effect.promise(() => reader.read())
        expect((first.value as { method?: string }).method).toBe("session/update")

        // Client -> agent: a request written to the stream gets a reply back.
        const writer = handle.stream.writable.getWriter()
        yield* Effect.promise(() => writer.write({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} }))
        const second = yield* Effect.promise(() => reader.read())
        const value = second.value as { id?: number; result?: { ok?: boolean } }
        expect(value.id).toBe(1)
        expect(value.result?.ok).toBe(true)

        reader.releaseLock()
      })
    ))
})
