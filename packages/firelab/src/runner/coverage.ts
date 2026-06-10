/**
 * The trace-coverage oracle — the verdict is computed HERE, from the run's OTel
 * spans, not asserted by the driver. Ported from flamelab (flamecast-v5) and
 * retargeted at firegrid's host-substrate span vocabulary. A coverage spec is
 * DATA: each claim is a CEL string (Common Expression Language,
 * `@marcbachmann/cel-js`) over the run's spans, evaluated against a small,
 * fixed vocabulary:
 *
 *   named(s, "x")          — the span is named "x"            (host-substrate name)
 *   namedPrefix(s, "x/")   — the span's name starts with "x/" (dynamic-suffix host span)
 *   hasChild(s, "x")       — s has a direct child named "x"   (structural, forge-proof)
 *   hasDescendant(s, "x")  — s has any descendant named "x"
 *   errored(s)             — the span ended in error (status.code === 2)
 *   attr(s, "k")           — s.attributes["k"] as a string ("" if absent)
 *   statusMessage(s)       — s.status.message as a string ("" if none)
 *   startMs(s) / endMs(s)  — span start/end as int ms (for ordering + correlation:
 *                            startMs(t) <= startMs(d), same-process trace only)
 *   spans.exists/all/exists_one/filter/size + &&/!/==/>= + .startsWith(...) — CEL built-ins
 *
 * Two buckets, and the forge-proof discipline is STRUCTURAL, not a comment:
 *   - gates          — gate the verdict. A LINT walks each gate's parsed AST and
 *                      asserts every span NAME it references is host-substrate
 *                      (emitted server-side by the real runtime — the driver
 *                      cannot forge it). A gate that names an edge span fails it.
 *   - corroborations — never gate; may reference any span (driver-side ok).
 *
 * firegrid advantage over flamelab: the harness annotates every host-side span
 * with `firegrid.side` (runner/side.ts), so beyond the static name allowlist a
 * gate is held to a RUNTIME lock — a passing gate must have a referenced span
 * that actually fired with `firegrid.side != "driver"`. A driver echoing a
 * host span NAME on its own side cannot satisfy a gate.
 *
 * Span names enter a claim ONLY through named/hasChild/hasDescendant, so the
 * lint is capture-complete: attribute VALUES are not names and are never linted.
 * Reads normalized `SpanRecord`s (trace.ts); pure, no I/O.
 */
import { Console, Effect } from "effect"
import { Environment, parse } from "@marcbachmann/cel-js"
import { endNs, type SpanRecord, startNs } from "./trace.ts"

const SIDE_ATTR = "firegrid.side"
const isErrorSpan = (s: SpanRecord): boolean => s.status.code === 2
const sideOf = (s: SpanRecord): string => {
  const v = s.attributes[SIDE_ATTR]
  return typeof v === "string" ? v : ""
}

// ── the contract — a claim is data (a CEL string), not a closure ─────────────
interface ClaimDef {
  readonly id: string
  readonly description: string
  /** A CEL boolean expression over `spans` (see the vocabulary above). */
  readonly claim: string
}

export interface CoverageSpec {
  /** Gate the verdict. Lint-enforced to reference only host-substrate spans. */
  readonly gates: ReadonlyArray<ClaimDef>
  /** Never gate. May reference any span (driver-side corroboration). */
  readonly corroborations?: ReadonlyArray<ClaimDef>
}

// ── the forge-proof core spans — a gate may name only these ──────────────────
// The canonical firegrid host-side span vocabulary (the names the runtime emits
// server-side, the driver cannot forge). Exact names plus dynamic-suffix
// prefixes (`unified.tool.execute/<id>` etc.). `gaps` on a real run surfaces any
// host span absent here as `unknown` — that is the signal to add it, not to gate
// around it.
const HOST_SUBSTRATE_NAMES: ReadonlySet<string> = new Set([
  // channel + signal
  "firegrid.channel.dispatch",
  "firegrid.unified.signal.send",
  "firegrid.unified.signal.record",
  // session + adapter
  "firegrid.unified.session.body",
  "firegrid.unified.session.terminal_signal",
  "firegrid.unified.adapter.start_or_attach",
  "firegrid.unified.adapter.send",
  "firegrid.unified.adapter.deregister",
  "firegrid.unified.adapter.resolve_effective_mcp_servers",
  // permission + tool roundtrips (fixed-name entrypoints; bodies are prefixed)
  "unified.permission-roundtrip.execute",
  "unified.tool-dispatch.execute",
  // journal + observation
  "firegrid.unified.journal_observer.daemon",
  "firegrid.runtime_output.journal.events",
  "firegrid.runtime_output.journal.agent_output",
  // session + tool dispatch (fixed-name entrypoints; bodies are prefixed)
  "unified.runtime-context-session.execute",
  "unified.mcp-tool-dispatch.execute",
  // workflow engine
  "firegrid.workflow_engine.execution.execute",
  "firegrid.workflow_engine.execution.resume.body",
  "firegrid.workflow_engine.execution.poll",
  "firegrid.workflow_engine.execution.interrupt",
  "firegrid.workflow_engine.activity.execute",
  "firegrid.workflow_engine.activity.claim",
  "firegrid.workflow_engine.clock.schedule",
  "firegrid.workflow_engine.clock.fire",
  "firegrid.workflow_engine.deferred.done",
  "firegrid.workflow_engine.deferred.result",
  "firegrid.workflow_engine.recover_pending_deferreds",
  "firegrid.workflow_engine.workflow.register",
  // ACP codec
  "firegrid.agent_event_pipeline.acp.initialize",
  "firegrid.agent_event_pipeline.acp.prompt",
  "firegrid.agent_event_pipeline.acp.session_update",
  "firegrid.agent_event_pipeline.acp.exit",
  "firegrid.agent_event_pipeline.acp.cancel",
  "firegrid.agent_event_pipeline.acp.permission_request",
  "firegrid.agent_event_pipeline.acp.permission_response",
  "firegrid.agent_event_pipeline.acp.tool_result",
  "firegrid.agent_event_pipeline.acp.output_queue",
  "firegrid.codec.sdk.call",
  // local-process sandbox (real subprocess + its byte pipes)
  "firegrid.agent_event_pipeline.source.local_process.open_byte_pipe",
  "firegrid.agent_event_pipeline.source.local_process.execute",
  "firegrid.agent_event_pipeline.source.local_process.exit",
  "firegrid.agent_event_pipeline.source.local_process.byte_stream",
  "firegrid.agent_event_pipeline.source.local_process.stdout_bytes",
  "firegrid.agent_event_pipeline.source.local_process.stdin_bytes",
  "firegrid.agent_event_pipeline.source.local_process.stderr_bytes",
  // durable table + streams transport (host-initiated, driver cannot forge)
  "firegrid.durable_table.action",
  "firegrid.durable_table.producer_append",
  "firegrid.durable_table.insert_or_get",
  "firegrid.durable_table.get",
  "firegrid.durable_table.query",
  "firegrid.durable_table.subscribe",
  "firegrid.durable_table.layer.acquire",
  "firegrid.durable_streams.http.request",
  // host-side MCP surfacing
  "firegrid.mcp.durable_streams_context.resolve",
  "firegrid.mcp.register_toolkit",
])

// Dynamic-suffix host spans: `unified.tool.execute/<contextId>` etc. A gate that
// names the stable prefix-form is host-substrate; the run emits the suffixed name.
const HOST_SUBSTRATE_PREFIXES: ReadonlyArray<string> = [
  "firegrid.workflow_engine.execution.resume",
  // sim host-side probe spans (emitted by a sim's host composition; forge-proof
  // because they fire host-side, side != "driver"). Lets a sim gate on a
  // behavior-specific probe without polluting the global production allowlist.
  "firegrid.sim.",
  // fluent-runtime managed-agent store: emitted host-side by FluentStore when it
  // drives durable streams (session/turn create, append-and-close, read-back).
  // Forge-proof because the store runs in the launched host (the served HTTP
  // surface), never in the driver — `firegrid.side != "driver"`.
  "fluent_runtime.event_ingress.",
  "fluent_runtime.sources.",
  "fluent_runtime.store.",
  "fluent_runtime.worker_redrive.",
  // dynamic-suffix host spans: <verb>/<sessionId|contextId>
  "unified.permission.request/",
  "unified.permission.relay/",
  "unified.tool.execute/",
  "unified.tool.relay/",
  "unified.mcp-tool.execute/",
  "unified.session.spawn/",
  "unified.session.send/",
  "unified.session.deregister/",
]

const isHostSubstrate = (name: string): boolean =>
  HOST_SUBSTRATE_NAMES.has(name) ||
  HOST_SUBSTRATE_PREFIXES.some((p) => name.startsWith(p))

// the ONLY functions that name a span — so the lint/witness is capture-complete.
// `namedPrefix` matches dynamic-suffix host spans (`unified.session.spawn/<id>`)
// by their stable prefix; the prefix string is still a NAME the lint captures.
const NAME_FNS: ReadonlySet<string> = new Set([
  "named",
  "namedPrefix",
  "hasChild",
  "hasDescendant",
])

// ── the CEL environment over a run's spans (parent index built once; the spans
//    themselves are never mutated) ──────────────────────────────────────────
const buildEnv = (spans: ReadonlyArray<SpanRecord>): Environment => {
  const childIdx = new Map<string, SpanRecord[]>()
  spans.forEach((s) => {
    if (s.parentSpanId === undefined) return
    const arr = childIdx.get(s.parentSpanId) ?? []
    arr.push(s)
    childIdx.set(s.parentSpanId, arr)
  })
  const kids = (id: unknown): ReadonlyArray<SpanRecord> =>
    typeof id === "string" ? childIdx.get(id) ?? [] : []
  const desc = (id: unknown): ReadonlyArray<SpanRecord> =>
    kids(id).flatMap((c) => [c, ...desc(c.spanId)])
  const spanId = (s: unknown): unknown =>
    s !== null && typeof s === "object" ? (s as { spanId?: unknown }).spanId : undefined
  const nameOf = (s: unknown): unknown =>
    s !== null && typeof s === "object" ? (s as { name?: unknown }).name : undefined

  return new Environment({ unlistedVariablesAreDyn: true })
    .registerFunction("named(dyn, string): bool", (s: unknown, n: unknown) => nameOf(s) === n)
    .registerFunction("namedPrefix(dyn, string): bool", (s: unknown, p: unknown) => {
      const name = nameOf(s)
      return typeof name === "string" && typeof p === "string" && name.startsWith(p)
    })
    .registerFunction("hasChild(dyn, string): bool", (s: unknown, n: unknown) =>
      kids(spanId(s)).some((c) => c.name === n),
    )
    .registerFunction("hasDescendant(dyn, string): bool", (s: unknown, n: unknown) =>
      desc(spanId(s)).some((d) => d.name === n),
    )
    .registerFunction("errored(dyn): bool", (s: unknown) =>
      s !== null && typeof s === "object" ? isErrorSpan(s as SpanRecord) : false,
    )
    .registerFunction("attr(dyn, string): string", (s: unknown, k: unknown) => {
      const attrs =
        s !== null && typeof s === "object" ? (s as { attributes?: Record<string, unknown> }).attributes : undefined
      const v = attrs && typeof k === "string" ? attrs[k] : undefined
      // OTel attribute values are string | number | boolean | array of those.
      if (typeof v === "string") return v
      if (typeof v === "number" || typeof v === "boolean" || typeof v === "bigint") {
        return String(v)
      }
      return v === undefined || v === null ? "" : JSON.stringify(v)
    })
    // The span's OTel status message ("" if none) — lets a gate assert the
    // ABSENCE of a specific host-side error (e.g. "codec send failed"), the
    // forge-proof analog of seam-coverage's *_absent assertions.
    .registerFunction("statusMessage(dyn): string", (s: unknown) => {
      const status =
        s !== null && typeof s === "object" ? (s as { status?: { message?: unknown } }).status : undefined
      const m = status?.message
      return typeof m === "string" ? m : ""
    })
    // Span start/end as integer milliseconds (BigInt — no 53-bit overflow), so a
    // gate can express temporal ORDERING and correlation in stock CEL:
    //   startMs(t) <= startMs(d), endMs(t) < startMs(d), startMs(d) - endMs(t) < 5000
    // (cel-js@7.6.1 has no registrable `timestamp` type — smoke-tested — so ms
    //  int is the route.) Times are OTel hrtime from a SINGLE process: ordering is
    // sound WITHIN one run's trace (the oracle's only scope), NOT across merged
    // multi-process traces. start/end are span-DERIVED, not span NAMES, so the
    // capture-complete name-allowlist lint is untouched and forge-proofing holds.
    .registerFunction("startMs(dyn): int", (s: unknown) =>
      s !== null && typeof s === "object" ? startNs(s as SpanRecord) / 1_000_000n : 0n,
    )
    .registerFunction("endMs(dyn): int", (s: unknown) =>
      s !== null && typeof s === "object" ? endNs(s as SpanRecord) / 1_000_000n : 0n,
    )
}

// ── the lint/witness — walk the public typed AST, collect the span names a
//    claim references via NAME_FNS. The forge-proof rule becomes a check. ─────
interface AstNode {
  readonly op: string
  readonly args: unknown
}
const asNode = (x: unknown): AstNode | undefined =>
  x !== null && typeof x === "object" && typeof (x as { op?: unknown }).op === "string"
    ? (x as AstNode)
    : undefined
const stringLiteral = (x: unknown): string | undefined => {
  const n = asNode(x)
  return n?.op === "value" && typeof n.args === "string" ? n.args : undefined
}

/** Span names a claim references through named/hasChild/hasDescendant. */
const referencedSpanNames = (claim: string): ReadonlyArray<string> => {
  const out = new Set<string>()
  const visit = (x: unknown): void => {
    const n = asNode(x)
    if (n === undefined) return
    if (n.op === "call") {
      // call: [fnName, argNodes[]]
      const [fn, argNodes] = n.args as [string, ReadonlyArray<unknown>]
      if (NAME_FNS.has(fn)) {
        argNodes.forEach((a) => {
          const lit = stringLiteral(a)
          if (lit !== undefined) out.add(lit)
        })
      }
      argNodes.forEach(visit)
    } else if (n.op === "rcall") {
      // receiver-call (method/macro): [fnName, receiver, argNodes[]]
      const [, recv, argNodes] = n.args as [string, unknown, ReadonlyArray<unknown>]
      visit(recv)
      argNodes.forEach(visit)
    } else if (n.op === "." || n.op === ".?") {
      visit((n.args as ReadonlyArray<unknown>)[0])
    } else if (n.op === "!_" || n.op === "-_") {
      visit(n.args)
    } else if (Array.isArray(n.args)) {
      n.args.forEach(visit)
    }
  }
  visit(parse(claim).ast)
  return [...out]
}

// ── results ──────────────────────────────────────────────────────────────────
interface ClaimResult {
  readonly id: string
  readonly description: string
  readonly gating: boolean
  readonly status: "pass" | "fail"
  readonly refs: ReadonlyArray<string>
  /** non-host-substrate names a GATE referenced (the lint violation; [] if ok). */
  readonly illegal: ReadonlyArray<string>
  /** the gate evaluated true but NO host-substrate span it names fired host-side
   *  (`firegrid.side != "driver"`), so it proves nothing (driver echo or empty
   *  set). A vacuous pass is treated as not-covered — the oracle's forge-proof gap. */
  readonly vacuous: boolean
  readonly error?: string
}

type SpanClass = "host-substrate" | "edge" | "unknown"

interface ObservedSpan {
  readonly name: string
  readonly count: number
  readonly cls: SpanClass
}

/** What the run's trace reveals about the instrumented surface — the seam for
 *  spotting gaps the forge-proof oracle can only verify indirectly. */
interface TraceGaps {
  /** every distinct span name the run emitted, classified + counted. */
  readonly observed: ReadonlyArray<ObservedSpan>
  /** known host-substrate spans this run did NOT exercise (per-run blind spots). */
  readonly hostSubstrateUnfired: ReadonlyArray<string>
  /** observed spans neither host-substrate nor driver-edge — new instrumentation
   *  to classify (a host-side span fired the oracle doesn't know how to weigh). */
  readonly unknown: ReadonlyArray<string>
  /** gate ids that passed on no host-side firing evidence (see ClaimResult.vacuous). */
  readonly vacuousGates: ReadonlyArray<string>
}

interface CoverageReport {
  readonly totalSpans: number
  readonly gates: ReadonlyArray<ClaimResult>
  readonly corroborations: ReadonlyArray<ClaimResult>
  readonly gatingFailing: number
  readonly gaps: TraceGaps
  readonly verdict: "production-path-covered" | "production-path-not-covered"
}

// Edge = driver-reachable (annotated `firegrid.side == "driver"`); never gate on
// it. host-substrate = in the forge-proof vocabulary. Anything else is an
// unclassified host span — the instrumentation-gap signal.
const classify = (name: string, sides: ReadonlyMap<string, ReadonlySet<string>>): SpanClass => {
  if (isHostSubstrate(name)) return "host-substrate"
  const seen = sides.get(name) ?? new Set<string>()
  return seen.has("driver") && seen.size === 1 ? "edge" : "unknown"
}

const judge = (
  env: Environment,
  spans: ReadonlyArray<SpanRecord>,
  counts: ReadonlyMap<string, number>,
  hostCounts: ReadonlyMap<string, number>,
  c: ClaimDef,
  gating: boolean,
): ClaimResult => {
  try {
    const refs = referencedSpanNames(c.claim)
    const illegal = gating ? refs.filter((r) => !isHostSubstrate(r)) : []
    const passed = illegal.length === 0 && env.evaluate(c.claim, { spans }) === true
    // Forge-proof only if a span the gate NAMES actually fired HOST-SIDE; a
    // passing gate whose referenced spans are all absent (or only driver-side)
    // is vacuously true and proves nothing. A ref may be an exact name or a
    // namedPrefix — both satisfied by a host span whose name starts with it.
    const firedHostSide = (r: string): boolean =>
      [...hostCounts.entries()].some(([name, count]) => count > 0 && name.startsWith(r))
    const vacuous = gating && passed && !refs.some(firedHostSide)
    return {
      id: c.id,
      description: c.description,
      gating,
      status: passed ? "pass" : "fail",
      refs,
      illegal,
      vacuous,
    }
  } catch (e) {
    // A malformed claim (parse/eval error) is a failure, surfaced in the report.
    return {
      id: c.id,
      description: c.description,
      gating,
      status: "fail",
      refs: [],
      illegal: [],
      vacuous: false,
      error: e instanceof Error ? e.message : String(e),
    }
  }
}

export const analyzeCoverage = (
  spec: CoverageSpec,
  spans: ReadonlyArray<SpanRecord>,
): CoverageReport => {
  const env = buildEnv(spans)
  const counts = new Map<string, number>()
  const hostCounts = new Map<string, number>()
  const sides = new Map<string, Set<string>>()
  spans.forEach((s) => {
    counts.set(s.name, (counts.get(s.name) ?? 0) + 1)
    const side = sideOf(s)
    if (side !== "driver") hostCounts.set(s.name, (hostCounts.get(s.name) ?? 0) + 1)
    const seen = sides.get(s.name) ?? new Set<string>()
    seen.add(side)
    sides.set(s.name, seen)
  })

  const gates = spec.gates.map((c) => judge(env, spans, counts, hostCounts, c, true))
  const corroborations = (spec.corroborations ?? []).map((c) =>
    judge(env, spans, counts, hostCounts, c, false),
  )
  // A vacuous gate is not real coverage, so it fails the verdict like any other gap.
  const gatingFailing = gates.filter((g) => g.status === "fail" || g.vacuous).length

  const observed = [...counts.entries()]
    .map(([name, count]): ObservedSpan => ({ name, count, cls: classify(name, sides) }))
    .sort((a, b) => b.count - a.count)
  const gaps: TraceGaps = {
    observed,
    hostSubstrateUnfired: [...HOST_SUBSTRATE_NAMES].filter((n) => !counts.has(n)).sort(),
    unknown: observed.filter((o) => o.cls === "unknown").map((o) => o.name),
    vacuousGates: gates.filter((g) => g.vacuous).map((g) => g.id),
  }

  return {
    totalSpans: spans.length,
    gates,
    corroborations,
    gatingFailing,
    gaps,
    verdict: gatingFailing === 0 ? "production-path-covered" : "production-path-not-covered",
  }
}

const mark = (c: ClaimResult): string => (c.vacuous ? "⚠" : c.status === "pass" ? "✓" : "✗")

const printClaim = (c: ClaimResult): Effect.Effect<void> =>
  Console.log(
    `  ${mark(c)} ${c.id.padEnd(32)} — ${c.description}` +
      (c.vacuous ? "  [VACUOUS: no referenced span fired host-side — proves nothing]" : "") +
      (c.illegal.length > 0 ? `  [LINT: names non-host-substrate ${JSON.stringify(c.illegal)}]` : "") +
      (c.error !== undefined ? `  [error: ${c.error}]` : ""),
  )

// The trace-gap report: what the run reveals about the instrumented surface, so a
// behavior the oracle can only verify INDIRECTLY (or not at all) is visible.
export const printGaps = (gaps: TraceGaps): Effect.Effect<void> =>
  Effect.gen(function* () {
    yield* Console.log("\nInstrumentation map (spans observed):")
    yield* Effect.forEach(gaps.observed, (o) =>
      Console.log(`  ${String(o.count).padStart(3)}× ${o.name.padEnd(52)} (${o.cls})`),
    )
    if (gaps.unknown.length > 0) {
      yield* Console.log(`\n  ⚠ unclassified host spans (classify in coverage.ts): ${gaps.unknown.join(", ")}`)
    }
    if (gaps.hostSubstrateUnfired.length > 0) {
      yield* Console.log(`  host-substrate spans not exercised here: ${gaps.hostSubstrateUnfired.join(", ")}`)
    }
    if (gaps.vacuousGates.length > 0) {
      yield* Console.log(`  ⚠ vacuous gates (passed on no host-side evidence): ${gaps.vacuousGates.join(", ")}`)
    }
  })

export const printSummary = (summary: CoverageReport): Effect.Effect<void> =>
  Effect.gen(function* () {
    yield* Console.log(`\nOTel trace coverage — ${summary.totalSpans} spans`)
    yield* Console.log("\nGates (host-substrate, forge-proof — gate the verdict):")
    yield* Effect.forEach(summary.gates, printClaim)
    if (summary.corroborations.length > 0) {
      yield* Console.log("\nCorroborations (report-only):")
      yield* Effect.forEach(summary.corroborations, printClaim)
    }
    yield* printGaps(summary.gaps)
    yield* Console.log(
      `\nverdict: ${summary.verdict}` +
        (summary.gatingFailing > 0 ? ` — ${summary.gatingFailing} gating claim(s) failed` : ""),
    )
  })
