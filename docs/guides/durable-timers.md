# Durable Timers And Schedules

Use `sleep` and `sleepUntil` when a running handler needs to pause and resume
with the same durable execution history.

```ts
import { sleep, sleepUntil } from "@firegrid/fluent-firegrid"

yield* sleep("10 seconds")
yield* sleepUntil(Date.now() + 60_000)
```

Use delayed sends when the current handler should finish now and another
invocation should start later.

```ts
import { rpc, serviceSendClient } from "@firegrid/fluent-firegrid"

yield* serviceSendClient(emails).sendReceipt(
  { orderId: "order-1" },
  rpc.sendOpts({ delay: { hours: 1 }, idempotencyKey: "order-1:receipt" })
)
```

Use workflow schedules for recurring work. Schedules target a specific fluent
workflow handler and are materialized by the S2 host loop.

```ts
import { cron, every, schedule, workflow } from "@firegrid/fluent-firegrid"

const jobs = workflow({
  name: "jobs",
  handlers: {
    *reconcile(input: { readonly tenantId: string }) {
      // durable work
      return { ok: true, tenantId: input.tenantId }
    }
  },
  schedules: [
    schedule({
      handler: "reconcile",
      id: "hourly-reconcile",
      input: { tenantId: "default" },
      overlapPolicy: "skip",
      schedule: every.hours(1)
    }),
    schedule({
      handler: "reconcile",
      id: "daily-reconcile",
      input: { tenantId: "default" },
      schedule: cron("0 0 * * *")
    })
  ]
})
```

Keep deployment versions that may still own sleeping or scheduled runs available
until those executions have finished or been intentionally abandoned.
