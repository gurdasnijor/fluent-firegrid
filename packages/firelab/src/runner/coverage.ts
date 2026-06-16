/**
 * The trace-coverage oracle — the verdict is computed HERE, from the run's OTel
 * spans, not asserted by the driver. Ported from flamelab (flamecast-v5) and
 * retargeted at the effect-s2 validation evidence-span vocabulary. A coverage spec is
 * DATA: each claim is a CEL string (Common Expression Language,
 * `@marcbachmann/cel-js`) over the run's spans and trusted observations,
 * evaluated against a small, fixed vocabulary:
 *
 *   named(s, "x")          — the span is named "x"            (evidence name)
 *   namedPrefix(s, "x/")   — the span's name starts with "x/"
 *   hasChild(s, "x")       — s has a direct child named "x"
 *   hasDescendant(s, "x")  — s has any descendant named "x"
 *   errored(s)             — the span ended in error (status.code === 2)
 *   attr(s, "k")           — s.attributes["k"] as a string ("" if absent)
 *   statusMessage(s)       — s.status.message as a string ("" if none)
 *   startMs(s) / endMs(s)  — span start/end as int ms (for ordering + correlation:
 *                            startMs(t) <= startMs(d), same-process trace only)
 *   observation("id")      — a trusted observation captured by the validation
 *   spans.exists/all/exists_one/filter/size + &&/!/==/>= + .startsWith(...) — CEL built-ins
 *
 * Two buckets, and the evidence discipline is structural:
 *   - gates          — gate the verdict. A LINT walks each gate's parsed AST and
 *                      asserts every span NAME it references is evidence
 *                      (`S2.*`, `effect-s2-stream-db.*`, etc.). A gate that names
 *                      only arbitrary driver spans fails lint.
 *   - corroborations — never gate; may reference any span.
 *
 * Span names enter a claim ONLY through named/hasChild/hasDescendant, so the
 * lint is capture-complete: attribute VALUES are not names and are never linted.
 * A passing gate that names no fired evidence span is vacuous and fails; this
 * keeps observation claims tied to SUT/runtime trace evidence.
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
const attrValue = (s: SpanRecord, name: string): string => {
  const value = s.attributes[name]
  return typeof value === "string" ? value : value === undefined || value === null ? "" : String(value)
}

// ── the contract — a claim is data (a CEL string), not a closure ─────────────
export interface ClaimDef {
  readonly id: string
  readonly description: string
  /** A CEL boolean expression over `spans` (see the vocabulary above). */
  readonly claim: string
  /** Optional per-claim span scope; used to bind a requirement to its own run. */
  readonly scope?: {
    readonly attribute: string
    readonly value: string
  }
}

export type CoverageObservations = Readonly<Record<string, unknown>>

export interface CoverageSpec {
  /** Gate the verdict. Lint-enforced to reference only evidence spans. */
  readonly gates: ReadonlyArray<ClaimDef>
  /** Never gate. May reference any span (driver-side corroboration). */
  readonly corroborations?: ReadonlyArray<ClaimDef>
}

// ── the evidence spans — a gate may name only these ──────────────────────────
// The canonical effect-s2/firelab evidence span vocabulary. `gaps` on a real run
// surfaces any unclassified span as `unknown`; that is the signal to classify
// new instrumentation rather than silently gating on arbitrary driver spans.
const EVIDENCE_SPAN_NAMES: ReadonlySet<string> = new Set([
  "S2.append",
  "S2.appendSession",
  "S2.appendSession.submit",
  "S2.checkTail",
  "S2.createStream",
  "S2.deleteStream",
  "S2.ensureStream",
  "S2.producer",
  "S2.producer.submit",
  "S2.read",
  "S2.readBatch",
  "S2.readBatchBytes",
  "S2.readBytes",
  "firegrid.validation.run",
])

const EVIDENCE_SPAN_PREFIXES: ReadonlyArray<string> = [
  "S2.",
  "effect-s2-stream-db.",
  "firelab.s2.",
  "firelab.s2lite.",
]

const isEvidenceSpan = (name: string): boolean =>
  EVIDENCE_SPAN_NAMES.has(name) ||
  EVIDENCE_SPAN_PREFIXES.some((p) => name.startsWith(p))

// the ONLY functions that name a span — so the lint/witness is capture-complete.
// `namedPrefix` matches dynamic-suffix spans by their stable prefix; the prefix
// string is still a NAME the lint captures.
const NAME_FNS: ReadonlySet<string> = new Set([
  "named",
  "namedPrefix",
  "hasChild",
  "hasDescendant",
])

// ── the CEL environment over a run's spans (parent index built once; the spans
//    themselves are never mutated) ──────────────────────────────────────────
const MissingObservation = { present: false, missing: true }

const buildEnv = (
  spans: ReadonlyArray<SpanRecord>,
  observations: CoverageObservations,
): Environment => {
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
      const v = attrs !== undefined && typeof k === "string" ? attrs[k] : undefined
      // OTel attribute values are string | number | boolean | array of those.
      if (typeof v === "string") return v
      if (typeof v === "number" || typeof v === "boolean" || typeof v === "bigint") {
        return String(v)
      }
      return v === undefined || v === null ? "" : JSON.stringify(v)
    })
    // The span's OTel status message ("" if none), useful for gates that need
    // to assert the absence of a specific evidence error.
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
    // capture-complete name-allowlist lint is untouched.
    .registerFunction("startMs(dyn): int", (s: unknown) =>
      s !== null && typeof s === "object" ? startNs(s as SpanRecord) / 1_000_000n : 0n,
    )
    .registerFunction("endMs(dyn): int", (s: unknown) =>
      s !== null && typeof s === "object" ? endNs(s as SpanRecord) / 1_000_000n : 0n,
    )
    .registerFunction("observation(string): dyn", (id: unknown) =>
      typeof id === "string" && Object.prototype.hasOwnProperty.call(observations, id)
        ? observations[id]
        : MissingObservation,
    )
}

// ── the lint/witness — walk the public typed AST and collect the span names a
//    claim references via NAME_FNS. Evidence discipline becomes a check. ──────
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
  /** non-evidence names a GATE referenced (the lint violation; [] if ok). */
  readonly illegal: ReadonlyArray<string>
  /** The gate evaluated true but none of its referenced evidence spans fired. */
  readonly vacuous: boolean
  readonly error?: string
}

type SpanClass = "evidence" | "edge" | "unknown"

interface ObservedSpan {
  readonly name: string
  readonly count: number
  readonly cls: SpanClass
}

/** What the run's trace reveals about the instrumented surface, including gaps
 *  the coverage oracle can only verify indirectly. */
interface TraceGaps {
  /** every distinct span name the run emitted, classified + counted. */
  readonly observed: ReadonlyArray<ObservedSpan>
  /** Evidence named by this coverage spec but not observed in this run. */
  readonly evidenceUnfired: ReadonlyArray<string>
  /** Observed spans neither evidence nor driver-edge — new instrumentation to classify. */
  readonly unknown: ReadonlyArray<string>
  /** Gate ids that passed on no referenced evidence span firing. */
  readonly vacuousGates: ReadonlyArray<string>
}

interface CoverageReport {
  readonly totalSpans: number
  readonly totalObservations: number
  readonly gates: ReadonlyArray<ClaimResult>
  readonly corroborations: ReadonlyArray<ClaimResult>
  readonly gatingFailing: number
  readonly gaps: TraceGaps
  readonly verdict: "production-path-covered" | "production-path-not-covered"
}

// Edge = arbitrary driver span; evidence = allowlisted effect-s2/firelab span.
// Anything else is an instrumentation-gap signal.
const classify = (name: string, sides: ReadonlyMap<string, ReadonlySet<string>>): SpanClass => {
  if (isEvidenceSpan(name)) return "evidence"
  const seen = sides.get(name) ?? new Set<string>()
  return seen.has("driver") && seen.size === 1 ? "edge" : "unknown"
}

const evidenceFired = (
  counts: ReadonlyMap<string, number>,
  ref: string,
): boolean =>
  [...counts.entries()].some(([name, count]) => count > 0 && name.startsWith(ref))

const referencedEvidence = (
  claims: ReadonlyArray<ClaimDef>,
): ReadonlyArray<string> =>
  [...new Set(
    claims.flatMap((claim) =>
      referencedSpanNames(claim.claim).filter(isEvidenceSpan)),
  )].sort()

const judge = (
  spans: ReadonlyArray<SpanRecord>,
  observations: CoverageObservations,
  c: ClaimDef,
  gating: boolean,
): ClaimResult => {
  try {
    const scope = c.scope
    const scopedSpans = scope === undefined
      ? spans
      : spans.filter((span) => attrValue(span, scope.attribute) === scope.value)
    const env = buildEnv(scopedSpans, observations)
    const evidenceCounts = new Map<string, number>()
    scopedSpans.forEach((s) => {
      if (isEvidenceSpan(s.name)) evidenceCounts.set(s.name, (evidenceCounts.get(s.name) ?? 0) + 1)
    })
    const refs = referencedSpanNames(c.claim)
    const illegal = gating ? refs.filter((r) => !isEvidenceSpan(r)) : []
    const passed = illegal.length === 0 && env.evaluate(c.claim, { spans: scopedSpans }) === true
    // A passing gate whose referenced evidence spans are all absent is vacuous
    // and proves nothing. A ref may be an exact name or a namedPrefix.
    const firedEvidence = (r: string): boolean =>
      [...evidenceCounts.entries()].some(([name, count]) => count > 0 && name.startsWith(r))
    const vacuous = gating && passed && !refs.some(firedEvidence)
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
  observations: CoverageObservations = {},
): CoverageReport => {
  const counts = new Map<string, number>()
  const evidenceCounts = new Map<string, number>()
  const sides = new Map<string, Set<string>>()
  spans.forEach((s) => {
    counts.set(s.name, (counts.get(s.name) ?? 0) + 1)
    const side = sideOf(s)
    if (isEvidenceSpan(s.name)) evidenceCounts.set(s.name, (evidenceCounts.get(s.name) ?? 0) + 1)
    const seen = sides.get(s.name) ?? new Set<string>()
    seen.add(side)
    sides.set(s.name, seen)
  })

  const gates = spec.gates.map((c) => judge(spans, observations, c, true))
  const corroborations = (spec.corroborations ?? []).map((c) =>
    judge(spans, observations, c, false),
  )
  // A vacuous gate is not real coverage, so it fails the verdict like any other gap.
  const gatingFailing = gates.filter((g) => g.status === "fail" || g.vacuous).length

  const observed = [...counts.entries()]
    .map(([name, count]): ObservedSpan => ({ name, count, cls: classify(name, sides) }))
    .sort((a, b) => b.count - a.count)
  const expectedEvidence = referencedEvidence([
    ...spec.gates,
    ...(spec.corroborations ?? []),
  ])
  const gaps: TraceGaps = {
    observed,
    evidenceUnfired: expectedEvidence.filter((ref) => !evidenceFired(counts, ref)),
    unknown: observed.filter((o) => o.cls === "unknown").map((o) => o.name),
    vacuousGates: gates.filter((g) => g.vacuous).map((g) => g.id),
  }

  return {
    totalSpans: spans.length,
    totalObservations: Object.keys(observations).length,
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
      (c.vacuous ? "  [VACUOUS: no referenced evidence span fired — proves nothing]" : "") +
      (c.illegal.length > 0 ? `  [LINT: names non-evidence ${JSON.stringify(c.illegal)}]` : "") +
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
      yield* Console.log(`\n  ⚠ unclassified spans (classify in coverage.ts): ${gaps.unknown.join(", ")}`)
    }
    if (gaps.evidenceUnfired.length > 0) {
      yield* Console.log(`  evidence spans not exercised here: ${gaps.evidenceUnfired.join(", ")}`)
    }
    if (gaps.vacuousGates.length > 0) {
      yield* Console.log(`  ⚠ vacuous gates (passed on no referenced evidence): ${gaps.vacuousGates.join(", ")}`)
    }
  })

export const printSummary = (summary: CoverageReport): Effect.Effect<void> =>
  Effect.gen(function* () {
    yield* Console.log(
      `\nOTel trace coverage — ${summary.totalSpans} spans, ${summary.totalObservations} observations`,
    )
    yield* Console.log("\nGates (evidence spans — gate the verdict):")
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
