/// Node interop for the corpus harness (fs side-channels, child processes,
/// timers). Test INFRASTRUCTURE only — the system under test is reached
/// exclusively through Firegrid.Durable's public surface.
namespace Firegrid.Durable.Corpus

open Fable.Core
open Fable.Core.JsInterop

module Node =
    type ChildProcess =
        abstract kill: signal: string -> bool
        abstract pid: int

    [<Import("spawn", "node:child_process")>]
    let spawn (_command: string) (_args: string array) (_options: obj) : ChildProcess = jsNative

    let private fs: obj = importAll "node:fs"
    let private path: obj = importAll "node:path"

    [<Emit("$0.mkdirSync($1, { recursive: true })")>]
    let private mkdirp (_fs: obj) (_path: string) : unit = jsNative

    [<Emit("$0.writeFileSync($1, $2, 'utf8')")>]
    let private writeFileWith (_fs: obj) (_path: string) (_content: string) : unit = jsNative

    [<Emit("$0.readFileSync($1, 'utf8')")>]
    let private readFileWith (_fs: obj) (_path: string) : string = jsNative

    [<Emit("$0.appendFileSync($1, $2, 'utf8')")>]
    let private appendFileWith (_fs: obj) (_path: string) (_content: string) : unit = jsNative

    [<Emit("$0.existsSync($1)")>]
    let private existsWith (_fs: obj) (_path: string) : bool = jsNative

    [<Emit("$0.join(...$1)")>]
    let private joinWith (_path: obj) (_parts: string array) : string = jsNative

    let ensureDir dir = mkdirp fs dir
    let writeFile p content = writeFileWith fs p content
    let readFile p = readFileWith fs p
    let appendFile p content = appendFileWith fs p content
    let exists p = existsWith fs p
    let join (parts: string list) = joinWith path (List.toArray parts)

    [<Emit("process.argv.slice(2)")>]
    let argv () : string array = jsNative

    [<Emit("process.argv[1]")>]
    let scriptPath () : string = jsNative

    [<Emit("process.execPath")>]
    let nodePath () : string = jsNative

    [<Emit("process.cwd()")>]
    let cwd () : string = jsNative

    [<Emit("process.env[$0] || ''")>]
    let env (_name: string) : string = jsNative

    [<Emit("Object.assign({}, process.env, $0)")>]
    let withProcessEnv (_extra: obj) : obj = jsNative

    [<Emit("process.exitCode = $0")>]
    let setExitCode (_code: int) : unit = jsNative

    [<Emit("console.log($0)")>]
    let stdout (_message: string) : unit = jsNative

    [<Emit("console.error($0)")>]
    let stderr (_message: string) : unit = jsNative

    [<Emit("new Promise(resolve => setTimeout(resolve, $0))")>]
    let private sleepPromise (_millis: int) : JS.Promise<unit> = jsNative

    let sleep (millis: int) = sleepPromise millis |> Async.AwaitPromise

    [<Emit("Date.now()")>]
    let nowMillis () : float = jsNative

    [<Emit("Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8)")>]
    let entropy () : string = jsNative

    [<Emit("20000 + Math.floor(Math.random() * 20000)")>]
    let randomPort () : int = jsNative

    [<Emit("process.env.S2_BIN || (process.env.HOME ? process.env.HOME + '/.s2/bin/s2' : 's2')")>]
    let s2Bin () : string = jsNative

    [<Emit("fetch($0).then(() => true).catch(() => false)")>]
    let fetchReady (_url: string) : JS.Promise<bool> = jsNative

    /// Race a unit of work against a wall-clock deadline. Resolves null on
    /// completion, the marker string on timeout; work rejections propagate.
    [<Emit("Promise.race([$0.then(() => null), new Promise(resolve => setTimeout(() => resolve('timeout'), $1))])")>]
    let raceTimeout (_work: JS.Promise<unit>) (_millis: int) : JS.Promise<obj> = jsNative
