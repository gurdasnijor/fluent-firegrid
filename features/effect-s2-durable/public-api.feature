@unverified
Feature: Public API
  The ergonomic public authoring and invocation surface exposed by
  effect-s2-durable. Persistence semantics live in stateless-execution,
  stateful-execution, and workflow-execution; this feature pins the API
  boundary so implementations do not introduce parallel facades or stringly
  alternatives.

  Rule: Authoring definitions

    Scenario: service({ name, handlers }) defines stateless methods as generator functions receiving decoded input directly; authors do not write handlerRequest or Effect.gen wrappers for the common surface
      Then service({ name, handlers }) defines stateless methods as generator functions receiving decoded input directly; authors do not write handlerRequest or Effect.gen wrappers for the common surface.

    Scenario: object({ name, handlers, shared }) defines keyed virtual objects with exclusive handlers and optional shared read-only handlers
      Then object({ name, handlers, shared }) defines keyed virtual objects with exclusive handlers and optional shared read-only handlers.

    Scenario: workflow({ name, run, handlers }) defines a run-once workflow as a stateful-execution specialization; it is not a separate persistence boundary
      Then workflow({ name, run, handlers }) defines a run-once workflow as a stateful-execution specialization; it is not a separate persistence boundary.

    Scenario: Handler schemas are declared on the owning definition and are the source of input/output encoding boundaries for public calls, child object calls, shared calls, and workflow starts
      Then Handler schemas are declared on the owning definition and are the source of input/output encoding boundaries for public calls, child object calls, shared calls, and workflow starts.

  Rule: Invocation clients

    Scenario: client(def, key?).method(input) submits and attaches, returning the decoded method result
      Then client(def, key?).method(input) submits and attaches, returning the decoded method result.

    Scenario: sendClient(def, key?).method(input) submits and returns the execution/call id without waiting for completion
      Then sendClient(def, key?).method(input) submits and returns the execution/call id without waiting for completion.

    Scenario: objectClient(def, key).method(input) is the only public durable in-handler call surface to another object; it derives target identity from the object definition and method schema, never raw object/method strings
      Then objectClient(def, key).method(input) is the only public durable in-handler call surface to another object; it derives target identity from the object definition and method schema, never raw object/method strings.

    Scenario: objectSendClient(def, key).method(input) is the one-way in-handler send surface and shares the same definition-derived identity and input encoding boundary as objectClient
      Then objectSendClient(def, key).method(input) is the one-way in-handler send surface and shares the same definition-derived identity and input encoding boundary as objectClient.

    Scenario: sharedClient(def, key).method(input) invokes shared object/workflow handlers over a snapshot and cannot be used to mutate user state or issue durable child calls
      Then sharedClient(def, key).method(input) invokes shared object/workflow handlers over a snapshot and cannot be used to mutate user state or issue durable child calls.

  Rule: By-id resolution

    Scenario: attach(callId) waits for and returns a call's terminal result; poll(callId) returns the current status without blocking
      Then attach(callId) waits for and returns a call's terminal result; poll(callId) returns the current status without blocking.

    Scenario: Object call ids resolve through the owner-stream projection; service call ids resolve through the stateless service result path
      Then Object call ids resolve through the owner-stream projection; service call ids resolve through the stateless service result path.

    Scenario: A syntactically valid but unknown object call id resolves to Unknown/NotFound rather than polling forever
      Then A syntactically valid but unknown object call id resolves to Unknown/NotFound rather than polling forever.

  Rule: Call-id routing

    Scenario: A call id self-routes; it carries enough schema-decodable identity to locate its execution without a side index or residency
      Then A call id self-routes; it carries enough schema-decodable identity to locate its execution without a side index or residency.

    Scenario: Object call ids carry an explicit namespace/kind; a service id cannot decode as an object id, and an object id cannot decode as a service id
      Then Object call ids carry an explicit namespace/kind; a service id cannot decode as an object id, and an object id cannot decode as a service id.

    Scenario: By-id APIs branch on id kind before selecting the stateful owner projection or stateless service result path
      Then By-id APIs branch on id kind before selecting the stateful owner projection or stateless service result path.

  Rule: Workflow helpers

    Scenario: workflowRunId(def, id) deterministically derives the run call id for a workflow id and decodes to the workflow owner plus reserved run method
      Then workflowRunId(def, id) deterministically derives the run call id for a workflow id and decodes to the workflow owner plus reserved run method.

    Scenario: workflowSubmit(def, id, input) starts the workflow at most once and returns started or alreadyStarted
      Then workflowSubmit(def, id, input) starts the workflow at most once and returns started or alreadyStarted.

    Scenario: workflowAttach(def, id) attaches to the workflow run and decodes its result through the run output schema
      Then workflowAttach(def, id) attaches to the workflow run and decodes its result through the run output schema.

    Scenario: A workflow shared handler may resolve a run promise through resolvePromise, which appends ingress for the run owner; it may not mutate user state
      Then A workflow shared handler may resolve a run promise through resolvePromise, which appends ingress for the run owner; it may not mutate user state.

    Scenario: The method name run is reserved for the workflow entrypoint and is rejected for shared handlers at both the type boundary and definition-time guard
      Then The method name run is reserved for the workflow entrypoint and is rejected for shared handlers at both the type boundary and definition-time guard.

  Rule: Free durable primitives

    Scenario: run, sleep, state, signal, deferred, awakeable, attach, poll, resolveSignal, resolveAwakeable, and resolvePromise are free Effect primitives that delegate to the ambient DurableExecutionRuntime
      Then run, sleep, state, signal, deferred, awakeable, attach, poll, resolveSignal, resolveAwakeable, and resolvePromise are free Effect primitives that delegate to the ambient DurableExecutionRuntime.

    Scenario: Reusable internals remain behind DurableExecutionRuntime
      Then Reusable internals remain behind DurableExecutionRuntime.

    Scenario: Durable primitives fail clearly when called outside an active invocation or from an invocation kind that cannot support them
      Then Durable primitives fail clearly when called outside an active invocation or from an invocation kind that cannot support them.

    Scenario: resolvePromise is valid only inside a shared workflow/object invocation that has an owner run call to target; outside that scope it fails clearly
      Then resolvePromise is valid only inside a shared workflow/object invocation that has an owner run call to target; outside that scope it fails clearly.

  Rule: UNIFORM SEMANTICS

    Scenario: Users must not see partial or divergent primitive semantics depending on whether they chose service, object, or workflow
      Then Users must not see partial or divergent primitive semantics depending on whether they chose service, object, or workflow.

    Scenario: The runtime may keep different storage backends for services and objects, but the public primitive contract is identical across compatible surfaces
      Then The runtime may keep different storage backends for services and objects, but the public primitive contract is identical across compatible surfaces.

  Rule: NO PARALLEL SURFACE

    Scenario: Do not add public Actor.admit, Actor.drain, ActorObject, actorClient, validation-only facades, or raw { object, key, method } call APIs as product paths
      Then Do not add public Actor.admit, Actor.drain, ActorObject, actorClient, validation-only facades, or raw { object, key, method } call APIs as product paths.

    Scenario: There is no public ctx object and no public Actor namespace
      Then There is no public ctx object and no public Actor namespace.

  Rule: SCHEMA DERIVED IDENTITY

    Scenario: Public identity construction is schema/definition-derived
      Then Public identity construction is schema/definition-derived.

    Scenario: Callers must not hand-build stream paths, delimiter-encoded owner strings, or raw method target strings
      Then Callers must not hand-build stream paths, delimiter-encoded owner strings, or raw method target strings.
