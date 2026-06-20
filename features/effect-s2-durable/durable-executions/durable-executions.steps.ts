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
import { defineSteps } from "../../../packages/spec-harness/src/durable/support.ts"
import { scenarioKey, type SpecWorld } from "../../../packages/spec-harness/src/firegrid/proofs.ts"

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
    input?: number
    echoPayload?: string
    direct?: number
    sentId?: string
    attached?: number
    deferred?: string
  }
  object: {
    increment?: number
    routedIncrement?: number
    counterKey?: string
    routerKey?: string
    gateKey?: string
    gateId?: string
    gateSignal?: string
    gatePending?: boolean
    gateResult?: string
    submittedId?: string
    submittedResult?: number
    first?: number
    second?: number
    shared?: number
    sent?: number
  }
  signal: {
    id?: string
    name?: string
    pending?: boolean
    result?: string
  }
  workflow: {
    id?: string
    input?: number
    runCallId?: string
    firstStart?: string
    secondStart?: string
    signalResult?: string
    attached?: number
  }
}

const states = new WeakMap<SpecWorld, DurableProofState>()

const stateFor = (world: SpecWorld): DurableProofState => {
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

const objectOwnerKeys = (world: SpecWorld): { readonly counterKey: string; readonly routerKey: string } => {
  const objectState = stateFor(world).object
  return {
    counterKey: requireValue(objectState.counterKey, "Counter owner key"),
    routerKey: requireValue(objectState.routerKey, "Router owner key"),
  }
}

const approvalWorkflowId = (world: SpecWorld): string =>
  requireValue(stateFor(world).workflow.id, "Approval workflow id")

const withCalculator = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(serviceLayer(Calculator)))

const withObjects = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(serviceLayer(Counter, Router, Gate)))

const withWaiter = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(serviceLayer(Waiter)))

const withApprovalWorkflow = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(serviceLayer(ApprovalWorkflow)))

const callCalculatorDouble = (world: SpecWorld, input: number) =>
  Effect.gen(function*() {
    const direct = yield* client(Calculator).double(input, {
      idempotencyKey: scenarioKey(world, `calculator-direct-${input}`),
    })
    stateFor(world).service.direct = direct
  })

const sendCalculatorDouble = (world: SpecWorld, input: number) =>
  Effect.gen(function*() {
    const sentId = yield* sendClient(Calculator).double(input, {
      idempotencyKey: scenarioKey(world, `calculator-send-${input}`),
    })
    stateFor(world).service.sentId = sentId
  })

const attachCalculatorExecution = (world: SpecWorld) =>
  Effect.gen(function*() {
    const sentId = requireValue(stateFor(world).service.sentId, "Calculator sent execution id")
    const attached = yield* attach(sentId, Schema.Number)
    stateFor(world).service.attached = attached
  })

const resolveCalculatorDeferredEcho = (world: SpecWorld, input: string) =>
  Effect.gen(function*() {
    const deferredResult = yield* client(Calculator).deferredEcho(input, {
      idempotencyKey: scenarioKey(world, `calculator-deferred-${input}`),
    })
    stateFor(world).service.deferred = deferredResult
  })

const initializeCounterOwners = (world: SpecWorld): void => {
  const objectState = stateFor(world).object
  objectState.counterKey = scenarioKey(world, "counter")
  objectState.routerKey = scenarioKey(world, "router")
}

const addCounterObject = (world: SpecWorld, amount: number) =>
  Effect.gen(function*() {
    const counterKey = requireValue(stateFor(world).object.counterKey, "Counter owner key")
    const first = yield* client(Counter, counterKey).add(amount, {
      idempotencyKey: scenarioKey(world, `counter-add-${amount}`),
    })
    stateFor(world).object.first = first
  })

const callCounterThroughRouter = (world: SpecWorld, amount: number) =>
  Effect.gen(function*() {
    const { counterKey, routerKey } = objectOwnerKeys(world)
    const second = yield* client(Router, routerKey).callChild({
      key: counterKey,
      amount,
    }, { idempotencyKey: scenarioKey(world, `router-call-${amount}`) })
    stateFor(world).object.second = second
  })

const readCounterSnapshot = (world: SpecWorld) =>
  Effect.gen(function*() {
    const counterKey = requireValue(stateFor(world).object.counterKey, "Counter owner key")
    stateFor(world).object.shared = yield* sharedClient(Counter, counterKey).get()
  })

const submitCounterThroughRouter = (world: SpecWorld, amount: number) =>
  Effect.gen(function*() {
    const { counterKey, routerKey } = objectOwnerKeys(world)
    const submittedId = yield* client(Router, routerKey).sendChild({
      key: counterKey,
      amount,
    }, { idempotencyKey: scenarioKey(world, `router-submit-${amount}`) })
    stateFor(world).object.submittedId = submittedId
  })

const attachSubmittedObjectCall = (world: SpecWorld) =>
  Effect.gen(function*() {
    const submittedId = requireValue(stateFor(world).object.submittedId, "Submitted object execution id")
    stateFor(world).object.submittedResult = yield* attach(submittedId, Schema.Number)
  })

const initializeGateOwner = (world: SpecWorld): void => {
  stateFor(world).object.gateKey = scenarioKey(world, "gate")
}

const sendGateWait = (world: SpecWorld, signalName: string, value: string) =>
  Effect.gen(function*() {
    const gateKey = requireValue(stateFor(world).object.gateKey, "Gate owner key")
    const gateId = yield* sendClient(Gate, gateKey).wait(
      { signalName, value },
      { idempotencyKey: scenarioKey(world, `gate-wait-${signalName}`) },
    )
    stateFor(world).object.gateId = gateId
  })

const resolveGateSignal = (world: SpecWorld, signalName: string, value: string) =>
  Effect.gen(function*() {
    const gateId = requireValue(stateFor(world).object.gateId, "Gate execution id")
    yield* resolveSignal(gateId, signalName, Schema.String, value)
  })

const attachGateExecution = (world: SpecWorld, expected: string) =>
  Effect.gen(function*() {
    const gateId = requireValue(stateFor(world).object.gateId, "Gate execution id")
    const result = yield* attach(gateId, Schema.String)
    stateFor(world).object.gateResult = result
    assert.equal(result, expected)
  })

const sendWaitingExecution = (world: SpecWorld, name: string) =>
  Effect.gen(function*() {
    const id = yield* sendClient(Waiter).wait(name, { idempotencyKey: scenarioKey(world, `waiter-${name}`) })
    stateFor(world).signal.id = id
  })

const pollWaiterExecution = (world: SpecWorld) =>
  Effect.gen(function*() {
    const id = requireValue(stateFor(world).signal.id, "Waiter execution id")
    const result = yield* poll(id, Schema.String)
    stateFor(world).signal.pending = Option.isNone(result)
  })

const resolveWaiterSignal = (world: SpecWorld, name: string, value: string) =>
  Effect.gen(function*() {
    const id = requireValue(stateFor(world).signal.id, "Waiter execution id")
    yield* resolveSignal(id, name, Schema.String, value)
  })

const attachWaiterExecution = (world: SpecWorld, expected: string) =>
  Effect.gen(function*() {
    const id = requireValue(stateFor(world).signal.id, "Waiter execution id")
    const result = yield* attach(id, Schema.String)
    stateFor(world).signal.result = result
    assert.equal(result, expected)
  })

const approveWorkflow = (world: SpecWorld) =>
  Effect.gen(function*() {
    const signalResult = yield* sharedClient(ApprovalWorkflow, approvalWorkflowId(world)).approve(true)
    stateFor(world).workflow.signalResult = signalResult
  })

const attachApprovalWorkflow = (world: SpecWorld, expected: number) =>
  Effect.gen(function*() {
    const attached = yield* workflowAttach(ApprovalWorkflow, approvalWorkflowId(world))
    stateFor(world).workflow.attached = attached
    assert.equal(attached, expected)
  })

const initializeApprovalWorkflow = (world: SpecWorld, input: number): void => {
  const workflowState = stateFor(world).workflow
  workflowState.id = scenarioKey(world, "approval")
  workflowState.input = input
}

const startApprovalWorkflowTwice = (world: SpecWorld) =>
  Effect.gen(function*() {
    const workflowState = stateFor(world).workflow
    const workflowId = approvalWorkflowId(world)
    workflowState.runCallId = yield* workflowRunId(ApprovalWorkflow, workflowId)
    workflowState.firstStart = yield* workflowSubmit(
      ApprovalWorkflow,
      workflowId,
      requireValue(workflowState.input, "Approval workflow input"),
    )
    workflowState.secondStart = yield* workflowSubmit(ApprovalWorkflow, workflowId, 99)
  })

const submitWaitingApprovalWorkflow = (world: SpecWorld) =>
  Effect.gen(function*() {
    const workflowState = stateFor(world).workflow
    workflowState.runCallId = yield* workflowRunId(ApprovalWorkflow, approvalWorkflowId(world))
    workflowState.firstStart = yield* workflowSubmit(
      ApprovalWorkflow,
      approvalWorkflowId(world),
      requireValue(workflowState.input, "Approval workflow input"),
    )
  })

export const durableExecutionsSteps = defineSteps(({ Given, When, Then }) => {
  Given("a service execution will double {int}", function(this: SpecWorld, input: number) {
    stateFor(this).service.input = input
  })

  When("the service execution starts", function(this: SpecWorld) {
    return withCalculator(callCalculatorDouble(this, requireValue(stateFor(this).service.input, "Calculation input")))
  })

  Then("the service execution result is {int}", function(this: SpecWorld, expected: number) {
    assert.equal(stateFor(this).service.direct, expected)
  })

  Given("a service execution was submitted without waiting for input {int}", function(this: SpecWorld, input: number) {
    return withCalculator(sendCalculatorDouble(this, input))
  })

  When("the caller attaches to the submitted service execution", function(this: SpecWorld) {
    return withCalculator(attachCalculatorExecution(this))
  })

  Then("the submitted service execution result is {int}", function(this: SpecWorld, expected: number) {
    assert.equal(stateFor(this).service.attached, expected)
  })

  Given("a service execution has local promise payload {string}", function(this: SpecWorld, payload: string) {
    stateFor(this).service.echoPayload = payload
  })

  When("the service execution resolves its local promise", function(this: SpecWorld) {
    return withCalculator(resolveCalculatorDeferredEcho(this, requireValue(stateFor(this).service.echoPayload, "Echo payload")))
  })

  Then("the local promise result is {string}", function(this: SpecWorld, expected: string) {
    assert.equal(stateFor(this).service.deferred, expected)
  })

  Given("a counter owner", function(this: SpecWorld) {
    initializeCounterOwners(this)
  })

  Given("the increment is {int}", function(this: SpecWorld, amount: number) {
    stateFor(this).object.increment = amount
  })

  When("the owner applies the increment", function(this: SpecWorld) {
    return withObjects(addCounterObject(this, requireValue(stateFor(this).object.increment, "Counter increment")))
  })

  Then("the owner value is {int}", function(this: SpecWorld, expected: number) {
    assert.equal(stateFor(this).object.first, expected)
  })

  Given("the routed increment is {int}", function(this: SpecWorld, amount: number) {
    stateFor(this).object.routedIncrement = amount
  })

  When("another owner applies the routed increment", function(this: SpecWorld) {
    return withObjects(callCounterThroughRouter(this, requireValue(stateFor(this).object.routedIncrement, "Routed increment")))
  })

  Then("the routed update result is {int}", function(this: SpecWorld, expected: number) {
    assert.equal(stateFor(this).object.second, expected)
  })

  When("the caller reads the owner snapshot", function(this: SpecWorld) {
    return withObjects(readCounterSnapshot(this))
  })

  Then("the owner snapshot value is {int}", function(this: SpecWorld, expected: number) {
    assert.equal(stateFor(this).object.shared, expected)
  })

  When("another owner submits the routed increment without waiting", function(this: SpecWorld) {
    return withObjects(submitCounterThroughRouter(this, requireValue(stateFor(this).object.routedIncrement, "Routed increment")))
  })

  Then("the submitted object result is {int}", function(this: SpecWorld, expected: number) {
    return withObjects(attachSubmittedObjectCall(this)).pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          assert.equal(stateFor(this).object.submittedResult, expected)
        })
      ),
    )
  })

  Given("a gate owner is waiting for signal {string} with value {string}", function(
    this: SpecWorld,
    signalName: string,
    value: string,
  ) {
    initializeGateOwner(this)
    stateFor(this).object.gateSignal = signalName
    return withObjects(sendGateWait(this, signalName, value))
  })

  When("signal {string} is resolved with {string} after runtime re-entry", function(
    this: SpecWorld,
    signalName: string,
    value: string,
  ) {
    return withObjects(resolveGateSignal(this, signalName, value))
  })

  Then("the gate result is {string}", function(this: SpecWorld, expected: string) {
    return withObjects(attachGateExecution(this, expected))
  })

  Given("a service execution is waiting for signal {string}", function(this: SpecWorld, name: string) {
    stateFor(this).signal.name = name
    return withWaiter(sendWaitingExecution(this, name))
  })

  When("the caller checks the execution status", function(this: SpecWorld) {
    return withWaiter(pollWaiterExecution(this))
  })

  Then("the execution is still pending", function(this: SpecWorld) {
    assert.equal(stateFor(this).signal.pending, true)
  })

  When("signal {string} is resolved with {string}", function(this: SpecWorld, name: string, value: string) {
    return withWaiter(resolveWaiterSignal(this, name, value))
  })

  Then("the service execution result is {string}", function(this: SpecWorld, expected: string) {
    return withWaiter(attachWaiterExecution(this, expected))
  })

  Given("the approval workflow input is {int}", function(this: SpecWorld, input: number) {
    initializeApprovalWorkflow(this, input)
  })

  When("the workflow is started twice", function(this: SpecWorld) {
    return withApprovalWorkflow(startApprovalWorkflowTwice(this))
  })

  Given("the approval workflow is waiting for approval of {int}", function(this: SpecWorld, input: number) {
    initializeApprovalWorkflow(this, input)
    return withApprovalWorkflow(submitWaitingApprovalWorkflow(this))
  })

  When("the run is approved", function(this: SpecWorld) {
    return withApprovalWorkflow(approveWorkflow(this))
  })

  Then("the workflow result is {int}", function(this: SpecWorld, expected: number) {
    return withApprovalWorkflow(attachApprovalWorkflow(this, expected))
  })

  Then("the workflow starts are {string} and {string}", function(this: SpecWorld, firstStart: string, secondStart: string) {
    const actual = stateFor(this).workflow
    assert.equal(actual.firstStart, firstStart)
    assert.equal(actual.secondStart, secondStart)
    assert.equal(requireValue(actual.runCallId, "Approval workflow run call id").length > 0, true)
  })
})
