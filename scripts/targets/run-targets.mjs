#!/usr/bin/env node
// Target runner for the red/green ratchet (T0).
//
// Reads targets.json at the repo root, executes each registered suite, and
// compares per-test outcomes against the manifest with STRICT semantics both
// ways. See targets-README.md for the manifest schema, the suite protocol,
// and the promotion-commit protocol.
//
// Exit code 0 only when every manifest target reported exactly once and every
// outcome matched its manifest status (red => fail, green => pass).

import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const manifestPath = join(repoRoot, "targets.json");

const errors = [];
const fail = (message) => errors.push(message);

// --- Load and validate the manifest -----------------------------------------

let manifest;
try {
  manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
} catch (cause) {
  console.error(`targets: cannot read ${manifestPath}: ${cause.message}`);
  process.exit(1);
}

const suites = Array.isArray(manifest.suites) ? manifest.suites : [];
const targets = Array.isArray(manifest.targets) ? manifest.targets : [];

const targetById = new Map();
for (const target of targets) {
  const ok =
    target !== null &&
    typeof target === "object" &&
    typeof target.id === "string" &&
    target.id.length > 0 &&
    typeof target.wp === "string" &&
    typeof target.corpus === "string" &&
    (target.status === "red" || target.status === "green");
  if (!ok) {
    fail(`manifest: malformed target entry ${JSON.stringify(target)} — expected { id, wp, corpus, status: "red" | "green" }`);
    continue;
  }
  if (targetById.has(target.id)) {
    fail(`manifest: duplicate target id "${target.id}"`);
    continue;
  }
  targetById.set(target.id, target);
}

const suiteCorpora = new Set();
for (const suite of suites) {
  const ok =
    suite !== null &&
    typeof suite === "object" &&
    typeof suite.corpus === "string" &&
    suite.corpus.length > 0 &&
    Array.isArray(suite.command) &&
    suite.command.length > 0 &&
    suite.command.every((part) => typeof part === "string");
  if (!ok) {
    fail(`manifest: malformed suite entry ${JSON.stringify(suite)} — expected { corpus, command: [string, ...] }`);
    continue;
  }
  if (suiteCorpora.has(suite.corpus)) {
    fail(`manifest: duplicate suite for corpus "${suite.corpus}"`);
  }
  suiteCorpora.add(suite.corpus);
}

for (const target of targetById.values()) {
  if (!suiteCorpora.has(target.corpus)) {
    fail(`drift: target "${target.id}" registered under corpus "${target.corpus}" but no suite is registered for that corpus`);
  }
}

// --- Run suites and collect results (JSON lines on stdout) ------------------

/** id -> { pass, corpus } */
const results = new Map();

for (const suite of suites) {
  if (!suite || typeof suite.corpus !== "string" || !Array.isArray(suite.command)) continue;
  const [cmd, ...args] = suite.command;
  const run = spawnSync(cmd, args, { cwd: repoRoot, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });

  if (run.stderr) process.stderr.write(run.stderr);
  if (run.error) {
    fail(`suite "${suite.corpus}": failed to launch (${run.error.message})`);
    continue;
  }
  if (run.status !== 0) {
    fail(`suite "${suite.corpus}": exited ${run.status ?? `signal ${run.signal}`} — suites must exit 0 and report per-test outcomes as JSON lines`);
    continue;
  }

  for (const line of run.stdout.split("\n")) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;

    let result;
    try {
      result = JSON.parse(trimmed);
    } catch {
      fail(`suite "${suite.corpus}": non-protocol stdout line ${JSON.stringify(trimmed)} — stdout is reserved for result lines { "id": ..., "pass": ... }; log to stderr`);
      continue;
    }
    if (result === null || typeof result !== "object" || typeof result.id !== "string" || typeof result.pass !== "boolean") {
      fail(`suite "${suite.corpus}": malformed result line ${JSON.stringify(trimmed)} — expected { "id": string, "pass": boolean }`);
      continue;
    }
    if (results.has(result.id)) {
      fail(`drift: test "${result.id}" reported more than once`);
      continue;
    }
    results.set(result.id, { pass: result.pass, corpus: suite.corpus });
  }
}

// --- Strict comparison, both ways --------------------------------------------

let expectedRed = 0;
let green = 0;

for (const [id, result] of results) {
  const target = targetById.get(id);
  if (target === undefined) {
    fail(`drift: test "${id}" reported by suite "${result.corpus}" but is not in targets.json — register it (with status) in the same PR that adds it`);
    continue;
  }
  if (target.corpus !== result.corpus) {
    fail(`drift: target "${id}" is registered under corpus "${target.corpus}" but was reported by suite "${result.corpus}"`);
    continue;
  }
  if (target.status === "green" && result.pass) {
    green += 1;
  } else if (target.status === "green" && !result.pass) {
    fail(`regression: green target "${id}" FAILED`);
  } else if (target.status === "red" && !result.pass) {
    expectedRed += 1;
  } else {
    fail(`unexpected pass: red target "${id}" PASSED — promote explicitly: flip manifest + ledger in the same PR`);
  }
}

for (const id of targetById.keys()) {
  if (!results.has(id)) {
    fail(`drift: target "${id}" is in targets.json but no suite reported it`);
  }
}

// --- Report ------------------------------------------------------------------

console.error(
  `targets: ${targetById.size} registered, ${results.size} reported — ${green} green, ${expectedRed} expected-red, ${errors.length} error(s)`
);

if (errors.length > 0) {
  for (const message of errors) console.error(`targets: ${message}`);
  process.exit(1);
}
