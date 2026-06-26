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
  throw new Error("Expected generated @firegrid/store exports to be functions")
}

process.stdout.write("@firegrid/store Fable bridge imports cleanly\n")
EOF
