// @ts-nocheck -- Vendored TanStack source targets a looser optional-property TypeScript policy.
export { FluentFiregridError } from "./error.ts"
export {
  type AnyGeneratorHandler,
  type AttachableReference,
  type CallOptions,
  type CallRequest,
  createTanStackRuntimeBinding,
  type DefinitionKind,
  duration,
  type DurationLike,
  type FluentRuntimeHost,
  type FluentWorkflowInput,
  type GenericInvocationRequest,
  type HandlerDescriptor,
  type HandlerDescriptors,
  type HandlerInput,
  type HandlerOutput,
  type InvocationBinding,
  type InvocationHandle,
  type InvocationOptions,
  normalizeDuration,
  type RuntimeInvocationHost,
  type RuntimeInvocationRunResult,
  type SendOptions,
  type SendReference,
  type SendRequest,
  workflowIdForRequest
} from "./invocation.ts"
export {
  cel,
  type CelExpressionBuilder,
  type CelExpressionInput,
  type CelExpressionNode,
  type CelFactory,
  type CelFieldExpression,
  type CelLiteral,
  type CelStatePredicate,
  ChangeMessage,
  evaluateStatePredicate,
  MaterializedState,
  type ObjectStateBackend,
  type StateChangeMessage,
  type StateControl,
  type StateControlMessage,
  stateIndexKey,
  type StateIndexWaitBackendOptions,
  type StateMessage,
  type StateOperation,
  type StatePredicate,
  type StatePredicateContext,
  type StateReadJournaledMessage,
  type StateWaitBackendOptions,
  validateStatePredicate
} from "./state.ts"
// ===== Workflow definition =====
export { createWorkflow } from "./define/define-workflow"
export type { AccumulateExtensions, CreateWorkflowConfig, WorkflowBuilder } from "./define/define-workflow"

// ===== Middleware =====
export { createMiddleware } from "./middleware/create-middleware"
export type { CreateMiddlewareBuilder } from "./middleware/create-middleware"

// ===== Result helpers =====
export { fail, succeed } from "./result"

// ===== Engine =====
export { handleWorkflowWebhook } from "./engine/handle-webhook"
export type { HandleWebhookOptions, WebhookPayload } from "./engine/handle-webhook"
export { runWorkflow } from "./engine/run-workflow"
export type { RunWorkflowOptions } from "./engine/run-workflow"
export type { Operation } from "./engine/state-diff"

// ===== Server helpers =====
export { parseWorkflowRequest, WorkflowRequestParseError } from "./server"
export type { WorkflowRequestParams } from "./server"

// ===== Cross-version registry =====
export { createWorkflowRegistry, selectWorkflowVersion } from "./registry/select-version"
export type { WorkflowRegistry } from "./registry/select-version"

// ===== Run store =====
export { inMemoryRunStore } from "./run-store/in-memory"
export type { InMemoryRunStore, InMemoryRunStoreOptions } from "./run-store/in-memory"

// ===== Errors =====
export { LogConflictError, StepTimeoutError } from "./types"

// ===== Public types =====
export type {
  AnyMiddleware,
  AnyWorkflowDefinition,
  ApprovalResult,
  ApproveOptions,
  AssertNonReservedExtension,
  BaseCtx,
  CheckpointEvent,
  Ctx,
  DeleteReason,
  DeterministicValueOptions,
  DurableOperationOptions,
  InferSchema,
  Middleware,
  MiddlewareServerFn,
  ReservedCtxFields,
  RunAwaitable,
  RunState,
  RunStatus,
  RunStore,
  SchemaInput,
  SerializedError,
  SignalDelivery,
  SleepOptions,
  StepAttempt,
  StepContext,
  StepOptions,
  StepRetryOptions,
  WaitForEventOptions,
  WorkflowCtx,
  WorkflowDefinition,
  WorkflowEvent,
  WorkflowInput,
  WorkflowMetadata,
  WorkflowOutput,
  WorkflowState
} from "./types"
