import { bench, describe } from "vitest"
import { BENCH_OPTS, BENCH_SIZES } from "./_helpers.ts"

for (const size of BENCH_SIZES) {
  describe(`baseline ${size} awaits`, () => {
    bench(
      `${size}x await Promise.resolve()`,
      async () => {
        for (let index = 0; index < size; index += 1) {
          await Promise.resolve(index)
        }
      },
      BENCH_OPTS,
    )
  })
}
