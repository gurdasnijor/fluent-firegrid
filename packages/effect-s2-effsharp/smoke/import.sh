#!/usr/bin/env bash
set -euo pipefail

node --input-type=module <<'EOF'
import {
  EffectS2_appendString,
  EffectS2_checkTail,
  EffectS2_config,
  EffectS2_layer,
  EffectS2_listBasins
} from "./src/generated/src/Bridge.js"

const exportsToCheck = [
  EffectS2_appendString,
  EffectS2_checkTail,
  EffectS2_config,
  EffectS2_layer,
  EffectS2_listBasins
]

if (exportsToCheck.some((value) => typeof value !== "function")) {
  throw new Error("Expected generated EffSharp S2 bridge exports to be functions")
}

process.stdout.write("EffSharp S2 Fable bridge imports cleanly\n")
EOF
