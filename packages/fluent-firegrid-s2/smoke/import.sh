#!/usr/bin/env bash
set -euo pipefail

node --input-type=module <<'EOF'
import {
  appendStateEventJson,
  createS2Runtime,
  delayedStartStreamName,
  objectInvocationStreamName,
  objectStateStreamName,
  stateStreamTarget
} from "./src/generated/src/FluentFiregrid.S2/Exports.js"

const exportsToCheck = [
  appendStateEventJson,
  createS2Runtime,
  delayedStartStreamName,
  objectInvocationStreamName,
  objectStateStreamName,
  stateStreamTarget
]

if (exportsToCheck.some((value) => typeof value !== "function")) {
  throw new Error("Expected generated fluent-firegrid-s2 exports to be functions")
}

process.stdout.write("fluent-firegrid-s2 Fable bridge imports cleanly\n")
EOF
