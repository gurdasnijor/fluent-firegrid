import { counter } from "./examples/Counter.ts"
import { greeter } from "./examples/Greeter.ts"
import { runHostMain } from "./runtime.ts"

runHostMain({ services: [greeter, counter] })
