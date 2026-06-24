import * as NodeRuntime from "@effect/platform-node/NodeRuntime"

import { runCli } from "./CliApp.ts"

NodeRuntime.runMain(runCli)
