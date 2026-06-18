import { setDefinitionFunctionWrapper, type IWorld } from "@cucumber/cucumber"
import { Effect } from "effect"
import { runSpecEffect } from "../../packages/spec-harness/src/runtime.ts"

type Definition = (this: IWorld, ...args: Array<unknown>) => unknown

const wrapDefinition = (fn: Definition): Definition =>
  function(this: IWorld, ...args: Array<unknown>) {
    const result = fn.apply(this, args)
    return Effect.isEffect(result) ? runSpecEffect(this, result) : result
  }

setDefinitionFunctionWrapper(wrapDefinition)
