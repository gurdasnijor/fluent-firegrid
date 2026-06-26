#!/usr/bin/env bash
set -euo pipefail

node --input-type=module <<'EOF'
import {
  S2_AppendRecord_string,
  S2_Basins_list,
  S2_Stream_appendString,
  S2_Stream_tail,
  S2_configWithEndpoint,
  S2_layer,
  S2_streamRef
} from "./src/generated/src/Effect.S2/S2.js"

const exportsToCheck = [
  S2_AppendRecord_string,
  S2_Basins_list,
  S2_Stream_appendString,
  S2_Stream_tail,
  S2_configWithEndpoint,
  S2_layer,
  S2_streamRef
]

if (exportsToCheck.some((value) => typeof value !== "function")) {
  throw new Error("Expected generated @firegrid/log bridge exports to be functions")
}

process.stdout.write("@firegrid/log Fable bridge imports cleanly\n")
EOF
