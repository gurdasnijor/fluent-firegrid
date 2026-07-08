#!/usr/bin/env node
// T0 proof-of-wiring stub suite for the red/green ratchet.
//
// Speaks the target-suite protocol (targets-README.md): one JSON line
// { "id": "...", "pass": true|false } per test on stdout; diagnostics on
// stderr; exit 0 when the suite itself ran to completion.
//
// t0.wiring-red is deliberately failing (registered "red" in targets.json) to
// prove that an expected-fail passes CI. t0.wiring-green is a trivial pass.

import assert from "node:assert/strict";

const tests = [
  {
    id: "t0.wiring-red",
    run: () => {
      assert.equal(true, false, "t0.wiring-red is red on purpose: it proves the ratchet tolerates registered reds");
    },
  },
  {
    id: "t0.wiring-green",
    run: () => {
      assert.equal(1 + 1, 2);
    },
  },
];

for (const test of tests) {
  let pass = true;
  try {
    test.run();
  } catch (cause) {
    pass = false;
    console.error(`t0-wiring: ${test.id} failed: ${cause.message}`);
  }
  console.log(JSON.stringify({ id: test.id, pass }));
}
