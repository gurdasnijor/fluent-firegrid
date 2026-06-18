import { Given, Then, When, type IWorld } from "@cucumber/cucumber"
import { strict as assert } from "node:assert"
import { Duration, Effect, Option, Schema } from "effect"
import {
  attach,
  client,
  deferred,
  object,
  objectClient,
  objectSendClient,
  poll,
  resolvePromise,
  resolveSignal,
  run,
  sendClient,
  service,
  serviceLayer,
  sharedClient,
  signal,
  sleep,
  state,
  workflow,
  workflowAttach,
  workflowRunId,
  workflowSubmit,
} from "effect-s2-durable"
import { primaryKey, Table } from "effect-s2-stream-db"
import { scenarioKey } from "../../packages/spec-harness/src/runtime.ts"

class CounterRow extends Table<CounterRow>("counter")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

const Calculator = service({
  name: "cucumber-calculator",
  handlers: {
    *double(input: number) {
      return yield* run("double", Effect.succeed(input * 2), { output: Schema.Number })
    },
    *deferredEcho(input: string) {
      const gate = deferred("echo", Schema.String)
      yield* gate.resolve(input)
      return yield* gate.get()
    },
  },
  schemas: {
    double: { input: Schema.Number, output: Schema.Number },
    deferredEcho: { input: Schema.String, output: Schema.String },
  },
})

const Waiter = service({
  name: "cucumber-waiter",
  handlers: {
    *wait(name: string) {
      const value = yield* signal(name, Schema.String)
      return `resolved:${value}`
    },
  },
  schemas: {
    wait: { input: Schema.String, output: Schema.String },
  },
})

const Counter = object({
  name: "cucumber-counter",
  handlers: {
    *add(amount: number) {
      yield* sleep("turn", Duration.millis(1))
      const cells = state(CounterRow)
      const current = yield* cells.get("value")
      const previous = Option.match(current, {
        onNone: () => 0,
        onSome: (row) => row.value,
      })
      const value = previous + amount
      yield* cells.set({ id: "value", value })
      return value
    },
  },
  shared: {
    *get() {
      const cells = state(CounterRow)
      const current = yield* cells.get("value")
      return Option.match(current, {
        onNone: () => 0,
        onSome: (row) => row.value,
      })
    },
  },
  schemas: {
    add: { input: Schema.Number, output: Schema.Number },
  },
  sharedSchemas: {
    get: { input: Schema.Void, output: Schema.Number },
  },
})

const Router = object({
  name: "cucumber-router",
  handlers: {
    *callChild(input: { readonly key: string; readonly amount: number }) {
      return yield* objectClient(Counter, input.key).add(input.amount)
    },
    *sendChild(input: { readonly key: string; readonly amount: number }) {
      return yield* objectSendClient(Counter, input.key).add(input.amount)
    },
  },
  schemas: {
    callChild: {
      input: Schema.Struct({ key: Schema.String, amount: Schema.Number }),
      output: Schema.Number,
    },
    sendChild: {
      input: Schema.Struct({ key: Schema.String, amount: Schema.Number }),
      output: Schema.String,
    },
  },
})

const Gate = object({
  name: "cucumber-gate",
  handlers: {
    *wait(input: { readonly signalName: string; readonly value: string }) {
      const opened = yield* signal(input.signalName, Schema.String)
      return `${input.value}:${opened}`
    },
  },
  schemas: {
    wait: {
      input: Schema.Struct({ signalName: Schema.String, value: Schema.String }),
      output: Schema.String,
    },
  },
})

const ApprovalWorkflow = workflow({
  name: "cucumber-approval-workflow",
  *run(input: number) {
    const approved = yield* signal("approved", Schema.Boolean)
    return approved ? input * 2 : 0
  },
  handlers: {
    *approve(value: boolean) {
      yield* resolvePromise("approved", Schema.Boolean, value)
      return "approved"
    },
  },
  runSchema: { input: Schema.Number, output: Schema.Number },
  sharedSchemas: {
    approve: { input: Schema.Boolean, output: Schema.String },
  },
})

interface DurableProofState {
  service: {
    direct?: number
    sentId?: string
    attached?: number
    deferred?: string
  }
  object: {
    counterKey?: string
    routerKey?: string
    gateKey?: string
    gateId?: string
    gateResult?: string
    first?: number
    second?: number
    shared?: number
    sent?: number
  }
  signal: {
    id?: string
    result?: string
  }
  workflow: {
    id?: string
    runCallId?: string
    firstStart?: string
    secondStart?: string
    signalResult?: string
    attached?: number
  }
}

const states = new WeakMap<IWorld, DurableProofState>()

const stateFor = (world: IWorld): DurableProofState => {
  const existing = states.get(world)
  if (existing !== undefined) return existing

  const created: DurableProofState = {
    service: {},
    object: {},
    signal: {},
    workflow: {},
  }
  states.set(world, created)
  return created
}

const requireValue = <A>(value: A | undefined, label: string): A => {
  if (value === undefined) {
    throw new Error(`${label} was not initialized`)
  }
  return value
}

const objectOwnerKeys = (world: IWorld): { readonly counterKey: string; readonly routerKey: string } => {
  const objectState = stateFor(world).object
  return {
    counterKey: requireValue(objectState.counterKey, "Counter owner key"),
    routerKey: requireValue(objectState.routerKey, "Router owner key"),
  }
}

const approvalWorkflowId = (world: IWorld): string =>
  requireValue(stateFor(world).workflow.id, "Approval workflow id")

const withCalculator = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(serviceLayer(Calculator)))

const withObjects = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(serviceLayer(Counter, Router, Gate)))

const withWaiter = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(serviceLayer(Waiter)))

const withApprovalWorkflow = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(serviceLayer(ApprovalWorkflow)))

const callCalculatorDouble = (world: IWorld, input: number) =>
  Effect.gen(function*() {
    const direct = yield* client(Calculator).double(input, {
      idempotencyKey: scenarioKey(world, `calculator-direct-${input}`),
    })
    stateFor(world).service.direct = direct
  })

const sendCalculatorDouble = (world: IWorld, input: number) =>
  Effect.gen(function*() {
    const sentId = yield* sendClient(Calculator).double(input, {
      idempotencyKey: scenarioKey(world, `calculator-send-${input}`),
    })
    stateFor(world).service.sentId = sentId
  })

const attachCalculatorExecution = (world: IWorld) =>
  Effect.gen(function*() {
    const sentId = requireValue(stateFor(world).service.sentId, "Calculator sent execution id")
    const attached = yield* attach(sentId, Schema.Number)
    stateFor(world).service.attached = attached
  })

const resolveCalculatorDeferredEcho = (world: IWorld, input: string) =>
  Effect.gen(function*() {
    const deferredResult = yield* client(Calculator).deferredEcho(input, {
      idempotencyKey: scenarioKey(world, `calculator-deferred-${input}`),
    })
    stateFor(world).service.deferred = deferredResult
  })

When("I call Calculator.double with {int} through the durable client", function(this: IWorld, input: number) {
  return withCalculator(callCalculatorDouble(this, input))
})

When("I send Calculator.double with {int} through the durable send client", function(this: IWorld, input: number) {
  return withCalculator(sendCalculatorDouble(this, input))
})

When("I attach the sent Calculator execution", function(this: IWorld) {
  return withCalculator(attachCalculatorExecution(this))
})

When("I resolve Calculator.deferredEcho with {string}", function(this: IWorld, input: string) {
  return withCalculator(resolveCalculatorDeferredEcho(this, input))
})

Then("Calculator observed direct {int}, attached {int}, and deferred {string}", function(
  this: IWorld,
  direct: number,
  attached: number,
  deferred: string,
) {
  const actual = stateFor(this).service
  assert.equal(actual.direct, direct)
  assert.equal(typeof actual.sentId, "string")
  assert.equal(actual.attached, attached)
  assert.equal(actual.deferred, deferred)
})

const initializeCounterOwners = (world: IWorld): void => {
  const objectState = stateFor(world).object
  objectState.counterKey = scenarioKey(world, "counter")
  objectState.routerKey = scenarioKey(world, "router")
}

const addCounterObject = (world: IWorld, amount: number) =>
  Effect.gen(function*() {
    const counterKey = requireValue(stateFor(world).object.counterKey, "Counter owner key")
    const first = yield* client(Counter, counterKey).add(amount, {
      idempotencyKey: scenarioKey(world, `counter-add-${amount}`),
    })
    stateFor(world).object.first = first
  })

const callCounterThroughRouter = (world: IWorld, amount: number) =>
  Effect.gen(function*() {
    const { counterKey, routerKey } = objectOwnerKeys(world)
    const second = yield* client(Router, routerKey).callChild({
      key: counterKey,
      amount,
    }, { idempotencyKey: scenarioKey(world, `router-call-${amount}`) })
    stateFor(world).object.second = second
  })

const readSharedCounter = (world: IWorld, expected: number) =>
  Effect.gen(function*() {
    const counterKey = requireValue(stateFor(world).object.counterKey, "Counter owner key")
    const shared = yield* sharedClient(Counter, counterKey).get()
    stateFor(world).object.shared = shared
    assert.equal(shared, expected)
  })

const sendCounterThroughRouter = (world: IWorld, amount: number) =>
  Effect.gen(function*() {
    const { counterKey, routerKey } = objectOwnerKeys(world)
    const sentId = yield* client(Router, routerKey).sendChild({
      key: counterKey,
      amount,
    }, { idempotencyKey: scenarioKey(world, `router-send-${amount}`) })
    const sent = yield* attach(sentId, Schema.Number)
    stateFor(world).object.sent = sent
  })

const initializeGateOwner = (world: IWorld): void => {
  stateFor(world).object.gateKey = scenarioKey(world, "gate")
}

const sendGateWait = (world: IWorld, signalName: string, value: string) =>
  Effect.gen(function*() {
    const gateKey = requireValue(stateFor(world).object.gateKey, "Gate owner key")
    const gateId = yield* sendClient(Gate, gateKey).wait(
      { signalName, value },
      { idempotencyKey: scenarioKey(world, `gate-wait-${signalName}`) },
    )
    stateFor(world).object.gateId = gateId
  })

const pollGateExecution = (world: IWorld) =>
  Effect.gen(function*() {
    const gateId = requireValue(stateFor(world).object.gateId, "Gate execution id")
    const result = yield* poll(gateId, Schema.String)
    assert.equal(Option.isNone(result), true)
  })

const resolveGateSignal = (world: IWorld, signalName: string, value: string) =>
  Effect.gen(function*() {
    const gateId = requireValue(stateFor(world).object.gateId, "Gate execution id")
    yield* resolveSignal(gateId, signalName, Schema.String, value)
  })

const attachGateExecution = (world: IWorld, expected: string) =>
  Effect.gen(function*() {
    const gateId = requireValue(stateFor(world).object.gateId, "Gate execution id")
    const result = yield* attach(gateId, Schema.String)
    stateFor(world).object.gateResult = result
    assert.equal(result, expected)
  })

Given("a durable Counter owner and Router owner", function(this: IWorld) {
  initializeCounterOwners(this)
})

When("I add {int} through the Counter object", function(this: IWorld, amount: number) {
  return withObjects(addCounterObject(this, amount))
})

When("I call the Counter object through Router with {int}", function(this: IWorld, amount: number) {
  return withObjects(callCounterThroughRouter(this, amount))
})

Then("the shared Counter read returns {int}", function(this: IWorld, expected: number) {
  return withObjects(readSharedCounter(this, expected))
})

When("I send the Counter object through Router with {int} and attach it", function(this: IWorld, amount: number) {
  return withObjects(sendCounterThroughRouter(this, amount))
})

Then("the Counter object observed first {int}, second {int}, shared {int}, and sent {int}", function(
  this: IWorld,
  first: number,
  second: number,
  shared: number,
  sent: number,
) {
  const actual = stateFor(this).object
  assert.equal(typeof actual.counterKey, "string")
  assert.equal(typeof actual.routerKey, "string")
  assert.equal(actual.first, first)
  assert.equal(actual.second, second)
  assert.equal(actual.shared, shared)
  assert.equal(actual.sent, sent)
})

Given("a durable Gate owner", function(this: IWorld) {
  initializeGateOwner(this)
})

When("I send Gate.wait for signal {string} and value {string}", function(
  this: IWorld,
  signalName: string,
  value: string,
) {
  return withObjects(sendGateWait(this, signalName, value))
})

Then("polling the Gate execution is pending", function(this: IWorld) {
  return withObjects(pollGateExecution(this))
})

When("I resolve signal {string} with {string} for the Gate execution after a fresh runtime boundary", function(
  this: IWorld,
  signalName: string,
  value: string,
) {
  return withObjects(resolveGateSignal(this, signalName, value))
})

Then("attaching the Gate execution returns {string}", function(this: IWorld, expected: string) {
  return withObjects(attachGateExecution(this, expected))
})

const sendWaitingExecution = (world: IWorld, name: string) =>
  Effect.gen(function*() {
    const id = yield* sendClient(Waiter).wait(name, { idempotencyKey: scenarioKey(world, `waiter-${name}`) })
    stateFor(world).signal.id = id
  })

const pollWaiterExecution = (world: IWorld) =>
  Effect.gen(function*() {
    const id = requireValue(stateFor(world).signal.id, "Waiter execution id")
    const result = yield* poll(id, Schema.String)
    assert.equal(Option.isNone(result), true)
  })

const resolveWaiterSignal = (world: IWorld, name: string, value: string) =>
  Effect.gen(function*() {
    const id = requireValue(stateFor(world).signal.id, "Waiter execution id")
    yield* resolveSignal(id, name, Schema.String, value)
  })

const attachWaiterExecution = (world: IWorld, expected: string) =>
  Effect.gen(function*() {
    const id = requireValue(stateFor(world).signal.id, "Waiter execution id")
    const result = yield* attach(id, Schema.String)
    stateFor(world).signal.result = result
    assert.equal(result, expected)
  })

When("I send a Waiter execution waiting on signal {string}", function(this: IWorld, name: string) {
  return withWaiter(sendWaitingExecution(this, name))
})

Then("polling the Waiter execution is pending", function(this: IWorld) {
  return withWaiter(pollWaiterExecution(this))
})

When("I resolve signal {string} with {string} for the Waiter execution", function(
  this: IWorld,
  name: string,
  value: string,
) {
  return withWaiter(resolveWaiterSignal(this, name, value))
})

Then("attaching the Waiter execution returns {string}", function(this: IWorld, expected: string) {
  return withWaiter(attachWaiterExecution(this, expected))
})

const deriveWorkflowRunId = (world: IWorld) =>
  Effect.gen(function*() {
    const workflowId = scenarioKey(world, "approval")
    const runCallId = yield* workflowRunId(ApprovalWorkflow, workflowId)
    assert.equal(runCallId.length > 0, true)
    const workflowState = stateFor(world).workflow
    workflowState.id = workflowId
    workflowState.runCallId = runCallId
  })

const submitApprovalWorkflow = (
  world: IWorld,
  input: number,
  field: "firstStart" | "secondStart",
) =>
  Effect.gen(function*() {
    const status = yield* workflowSubmit(ApprovalWorkflow, approvalWorkflowId(world), input)
    stateFor(world).workflow[field] = status
  })

const approveWorkflow = (world: IWorld) =>
  Effect.gen(function*() {
    const signalResult = yield* sharedClient(ApprovalWorkflow, approvalWorkflowId(world)).approve(true)
    stateFor(world).workflow.signalResult = signalResult
  })

const attachApprovalWorkflow = (world: IWorld, expected: number) =>
  Effect.gen(function*() {
    const attached = yield* workflowAttach(ApprovalWorkflow, approvalWorkflowId(world))
    stateFor(world).workflow.attached = attached
    assert.equal(attached, expected)
  })

When("I derive the approval workflow run id", function(this: IWorld) {
  return withApprovalWorkflow(deriveWorkflowRunId(this))
})

When("I submit the approval workflow with {int}", function(this: IWorld, input: number) {
  return withApprovalWorkflow(submitApprovalWorkflow(this, input, "firstStart"))
})

When("I submit the approval workflow again with {int}", function(this: IWorld, input: number) {
  return withApprovalWorkflow(submitApprovalWorkflow(this, input, "secondStart"))
})

When("I approve the workflow through its shared handler", function(this: IWorld) {
  return withApprovalWorkflow(approveWorkflow(this))
})

Then("the workflow starts are {string} and {string}", function(this: IWorld, firstStart: string, secondStart: string) {
  const actual = stateFor(this).workflow
  assert.equal(actual.firstStart, firstStart)
  assert.equal(actual.secondStart, secondStart)
  assert.equal(actual.signalResult, "approved")
  assert.equal(requireValue(actual.runCallId, "Approval workflow run call id").length > 0, true)
})

Then("attaching the approval workflow returns {int}", function(this: IWorld, expected: number) {
  return withApprovalWorkflow(attachApprovalWorkflow(this, expected))
})
