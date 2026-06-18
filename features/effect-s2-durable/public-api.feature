@product:effect-s2-durable @feature:public-api @spec-only
Feature: Public API
  The ergonomic public authoring and invocation surface exposed by
  effect-s2-durable. Persistence semantics live in stateless-execution,
  stateful-execution, and workflow-execution; this feature pins the API
  boundary so implementations do not introduce parallel facades or stringly
  alternatives.

  @component:DEFINITIONS
  Rule: Authoring definitions

    @requirement:DEFINITIONS.1
    Scenario: service( name, handlers ) defines stateless methods as generator functions receiving...
      Then the public-api contract includes:
        """
        service({ name, handlers }) defines stateless methods as generator functions
        receiving decoded input directly; authors do not write handlerRequest or
        Effect.gen wrappers for the common surface.
        """

    @requirement:DEFINITIONS.2
    Scenario: object( name, handlers, shared ) defines keyed virtual objects with exclusive handlers...
      Then the public-api contract includes:
        """
        object({ name, handlers, shared }) defines keyed virtual objects with
        exclusive handlers and optional shared read-only handlers.
        """

    @requirement:DEFINITIONS.3
    Scenario: workflow( name, run, handlers ) defines a run-once workflow as a stateful-execution...
      Then the public-api contract includes:
        """
        workflow({ name, run, handlers }) defines a run-once workflow as a
        stateful-execution specialization; it is not a separate persistence
        boundary.
        """

    @requirement:DEFINITIONS.4
    Scenario: Handler schemas are declared on the owning definition and are the source of input/output...
      Then the public-api contract includes:
        """
        Handler schemas are declared on the owning definition and are the source of
        input/output encoding boundaries for public calls, child object calls,
        shared calls, and workflow starts.
        """

  @component:CLIENTS
  Rule: Invocation clients

    @requirement:CLIENTS.1
    Scenario: client(def, key?).method(input) submits and attaches, returning the decoded method result.
      Then the public-api contract includes:
        """
        client(def, key?).method(input) submits and attaches, returning the decoded
        method result.
        """

    @requirement:CLIENTS.2
    Scenario: sendClient(def, key?).method(input) submits and returns the execution/call id without...
      Then the public-api contract includes:
        """
        sendClient(def, key?).method(input) submits and returns the execution/call
        id without waiting for completion.
        """

    @requirement:CLIENTS.3
    Scenario: objectClient(def, key).method(input) is the only public durable in-handler call surface...
      Then the public-api contract includes:
        """
        objectClient(def, key).method(input) is the only public durable in-handler
        call surface to another object; it derives target identity from the object
        definition and method schema, never raw object/method strings.
        """

    @requirement:CLIENTS.4
    Scenario: objectSendClient(def, key).method(input) is the one-way in-handler send surface and...
      Then the public-api contract includes:
        """
        objectSendClient(def, key).method(input) is the one-way in-handler send
        surface and shares the same definition-derived identity and input encoding
        boundary as objectClient.
        """

    @requirement:CLIENTS.5
    Scenario: sharedClient(def, key).method(input) invokes shared object/workflow handlers over a...
      Then the public-api contract includes:
        """
        sharedClient(def, key).method(input) invokes shared object/workflow handlers
        over a snapshot and cannot be used to mutate user state or issue durable
        child calls.
        """

  @component:BY_ID
  Rule: By-id resolution

    @requirement:BY_ID.1
    Scenario: attach(callId) waits for and returns a call's terminal result; poll(callId) returns the...
      Then the public-api contract includes:
        """
        attach(callId) waits for and returns a call's terminal result; poll(callId)
        returns the current status without blocking.
        """

    @requirement:BY_ID.2
    Scenario: Object call ids resolve through the owner-stream projection; service call ids resolve...
      Then the public-api contract includes:
        """
        Object call ids resolve through the owner-stream projection; service call
        ids resolve through the stateless service result path.
        """

    @requirement:BY_ID.3
    Scenario: A syntactically valid but unknown object call id resolves to Unknown/NotFound rather...
      Then the public-api contract includes:
        """
        A syntactically valid but unknown object call id resolves to
        Unknown/NotFound rather than polling forever.
        """

  @component:CALL_ID
  Rule: Call-id routing

    @requirement:CALL_ID.1
    Scenario: A call id self-routes; it carries enough schema-decodable identity to locate its...
      Then the public-api contract includes:
        """
        A call id self-routes; it carries enough schema-decodable identity to locate
        its execution without a side index or residency.
        """

    @requirement:CALL_ID.2
    Scenario: Object call ids carry an explicit namespace/kind; a service id cannot decode as an...
      Then the public-api contract includes:
        """
        Object call ids carry an explicit namespace/kind; a service id cannot decode
        as an object id, and an object id cannot decode as a service id.
        """

    @requirement:CALL_ID.3
    Scenario: By-id APIs branch on id kind before selecting the stateful owner projection or stateless...
      Then the public-api contract includes:
        """
        By-id APIs branch on id kind before selecting the stateful owner projection
        or stateless service result path.
        """

  @component:WORKFLOW_API
  Rule: Workflow helpers

    @requirement:WORKFLOW_API.1
    Scenario: workflowRunId(def, id) deterministically derives the run call id for a workflow id and...
      Then the public-api contract includes:
        """
        workflowRunId(def, id) deterministically derives the run call id for a
        workflow id and decodes to the workflow owner plus reserved run method.
        """

    @requirement:WORKFLOW_API.2
    Scenario: workflowSubmit(def, id, input) starts the workflow at most once and returns started or...
      Then the public-api contract includes:
        """
        workflowSubmit(def, id, input) starts the workflow at most once and returns
        started or alreadyStarted.
        """

    @requirement:WORKFLOW_API.3
    Scenario: workflowAttach(def, id) attaches to the workflow run and decodes its result through the...
      Then the public-api contract includes:
        """
        workflowAttach(def, id) attaches to the workflow run and decodes its result
        through the run output schema.
        """

    @requirement:WORKFLOW_API.4
    Scenario: A workflow shared handler may resolve a run promise through resolvePromise, which...
      Then the public-api contract includes:
        """
        A workflow shared handler may resolve a run promise through resolvePromise,
        which appends ingress for the run owner; it may not mutate user state.
        """

    @requirement:WORKFLOW_API.5
    Scenario: The method name run is reserved for the workflow entrypoint and is rejected for shared...
      Then the public-api contract includes:
        """
        The method name run is reserved for the workflow entrypoint and is rejected
        for shared handlers at both the type boundary and definition-time guard.
        """

  @component:PRIMITIVES
  Rule: Free durable primitives

    @requirement:PRIMITIVES.1
    Scenario: run, sleep, state, signal, deferred, awakeable, attach, poll, resolveSignal,...
      Then the public-api contract includes:
        """
        run, sleep, state, signal, deferred, awakeable, attach, poll, resolveSignal,
        resolveAwakeable, and resolvePromise are free Effect primitives that
        delegate to the ambient DurableExecutionRuntime.
        """

    @requirement:PRIMITIVES.2
    Scenario: Reusable internals remain behind DurableExecutionRuntime.
      Then the public-api contract includes:
        """
        Reusable internals remain behind DurableExecutionRuntime.
        """

    @requirement:PRIMITIVES.3
    Scenario: Durable primitives fail clearly when called outside an active invocation or from an...
      Then the public-api contract includes:
        """
        Durable primitives fail clearly when called outside an active invocation or
        from an invocation kind that cannot support them.
        """

    @requirement:PRIMITIVES.4
    Scenario: resolvePromise is valid only inside a shared workflow/object invocation that has an...
      Then the public-api contract includes:
        """
        resolvePromise is valid only inside a shared workflow/object invocation that
        has an owner run call to target; outside that scope it fails clearly.
        """

  @constraint:UNIFORM_SEMANTICS
  Rule: UNIFORM SEMANTICS

    @requirement:UNIFORM_SEMANTICS.1
    Scenario: Users must not see partial or divergent primitive semantics depending on whether they...
      Then the public-api contract includes:
        """
        Users must not see partial or divergent primitive semantics depending on
        whether they chose service, object, or workflow.
        """

    @requirement:UNIFORM_SEMANTICS.2
    Scenario: The runtime may keep different storage backends for services and objects, but the public...
      Then the public-api contract includes:
        """
        The runtime may keep different storage backends for services and objects,
        but the public primitive contract is identical across compatible surfaces.
        """

  @constraint:NO_PARALLEL_SURFACE
  Rule: NO PARALLEL SURFACE

    @requirement:NO_PARALLEL_SURFACE.1
    Scenario: Do not add public Actor.admit, Actor.drain, ActorObject, actorClient, validation-only...
      Then the public-api contract includes:
        """
        Do not add public Actor.admit, Actor.drain, ActorObject, actorClient,
        validation-only facades, or raw { object, key, method } call APIs as product
        paths.
        """

    @requirement:NO_PARALLEL_SURFACE.2
    Scenario: There is no public ctx object and no public Actor namespace.
      Then the public-api contract includes:
        """
        There is no public ctx object and no public Actor namespace.
        """

  @constraint:SCHEMA_DERIVED_IDENTITY
  Rule: SCHEMA DERIVED IDENTITY

    @requirement:SCHEMA_DERIVED_IDENTITY.1
    Scenario: Public identity construction is schema/definition-derived.
      Then the public-api contract includes:
        """
        Public identity construction is schema/definition-derived.
        """

    @requirement:SCHEMA_DERIVED_IDENTITY.2
    Scenario: Callers must not hand-build stream paths, delimiter-encoded owner strings, or raw method...
      Then the public-api contract includes:
        """
        Callers must not hand-build stream paths, delimiter-encoded owner strings,
        or raw method target strings.
        """
