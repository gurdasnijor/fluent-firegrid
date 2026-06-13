# @firegrid/fluent-s2-workflow-engine

S2-native `WorkflowEngine` implementation for Firegrid.

The engine persists workflow executions to one S2 stream per execution. The
public API is Effect's `WorkflowEngine` service; S2 is the durability substrate.

```ts
import { Layer } from "effect"
import {
  layerConfig,
  layerFromConfig,
} from "@firegrid/fluent-s2-workflow-engine"

const WorkflowEngineLive = layerFromConfig.pipe(
  Layer.provide(layerConfig({
    basin: "firegrid-workflows",
    accessToken: process.env.S2_ACCESS_TOKEN!,
  })),
)
```

Tests run against real S2 Lite through the official `@s2-dev/streamstore` SDK.
