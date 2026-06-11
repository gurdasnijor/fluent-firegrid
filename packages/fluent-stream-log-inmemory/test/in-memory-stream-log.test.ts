import { runDurableStreamLogTestSuite } from "@firegrid/fluent-stream-log/testing"
import * as InMemoryStreamLog from "../src/index.ts"

runDurableStreamLogTestSuite("In-memory", InMemoryStreamLog.make)
