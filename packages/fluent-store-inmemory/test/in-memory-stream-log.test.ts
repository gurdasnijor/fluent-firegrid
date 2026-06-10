import { runDurableStreamLogTestSuite } from "@firegrid/fluent-store/testing"
import * as InMemoryStreamLog from "../src/inMemoryStreamLog.ts"

runDurableStreamLogTestSuite("In-memory", InMemoryStreamLog.make)
