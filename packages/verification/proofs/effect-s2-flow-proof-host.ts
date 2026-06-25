import { counter } from "effect-s2-flow/examples/counter"
import { greeter } from "effect-s2-flow/examples/greeter"
import { sleeper } from "effect-s2-flow/examples/sleeper"
import { runHostMain } from "effect-s2-flow"

runHostMain({ services: [greeter, counter, sleeper] })
