@product:effect-s2 @feature:api-fidelity @spec-only
Feature: API fidelity
  Effect-native facade for the upstream @s2-dev/streamstore SDK. The package
  preserves SDK request/response/config type fidelity while exposing S2
  operations as Effect Context services, Layers, Effects, Streams, scoped
  sessions, typed errors, and production evidence spans.

  The upstream source of truth is s2-sdk-typescript/packages/streamstore/src;
  this feature records the wrapper parity expected by fluent-firegrid packages.

  @component:CLIENT_SURFACE
  Rule: S2Client service surface

    @requirement:CLIENT_SURFACE.1
    Scenario: S2Client is available as a service and static accessor surface
      Then the effect-s2 API contract includes:
        """
        S2Client is an Effect Context.Service whose methods are available both
        through dependency injection and as static accessors requiring S2Client
        in the environment.
        """

    @requirement:CLIENT_SURFACE.2
    Scenario: Account and control-plane operations are covered
      Then the effect-s2 API contract includes:
        """
        Account/control-plane operations cover listBasins, listAllBasins,
        createBasin, getBasinConfig, deleteBasin, ensureBasin,
        reconfigureBasin, listLocations, getDefaultLocation, setDefaultLocation,
        listAccessTokens, listAllAccessTokens, issueAccessToken,
        revokeAccessToken, accountMetrics, basinMetrics, and streamMetrics.
        """

    @requirement:CLIENT_SURFACE.3
    Scenario: Stream-management operations are covered
      Then the effect-s2 API contract includes:
        """
        Stream-management operations cover listStreams, listAllStreams,
        createStream, getStreamConfig, deleteStream, ensureStream, and
        reconfigureStream.
        """

    @requirement:CLIENT_SURFACE.4
    Scenario: Stream data-plane operations are covered
      Then the effect-s2 API contract includes:
        """
        Stream data-plane operations cover checkTail, append, readBatch,
        readBatchBytes, read, readBytes, appendSession, and producer.
        """

    @requirement:CLIENT_SURFACE.5
    Scenario: SDK promise and iterable shapes are translated to Effect types
      Then the effect-s2 API contract includes:
        """
        Methods that return a single SDK promise are exposed as Effect.Effect;
        SDK async iterables and read sessions are exposed as Effect Stream.Stream.
        """

  @component:LAYERING
  Rule: Effect layer construction

    @requirement:LAYERING.1
    Scenario: S2Client.layer accepts config values or concrete values
      Then the effect-s2 API contract includes:
        """
        S2Client.layer accepts accessToken and basinName as either Effect Config
        values or concrete values, resolves them in Layer construction, and
        stores the resulting live SDK client behind S2Client.
        """

    @requirement:LAYERING.2
    Scenario: S2Client.layerConfig reads environment config safely
      Then the effect-s2 API contract includes:
        """
        S2Client.layerConfig reads S2_ACCESS_TOKEN and S2_BASIN from Effect
        Config and redacts the access token.
        """

    @requirement:LAYERING.3
    Scenario: Layer options preserve upstream option names
      Then the effect-s2 API contract includes:
        """
        Layer options pass through upstream endpoints, retry, and forceTransport
        settings without inventing parallel configuration names.
        """

    @requirement:LAYERING.4
    Scenario: Append retry defaults avoid unintended side effects
      Then the effect-s2 API contract includes:
        """
        The default append retry policy is noSideEffects unless the caller
        explicitly supplies a retry configuration.
        """

  @component:SDK_FIDELITY
  Rule: Upstream SDK type fidelity

    @requirement:SDK_FIDELITY.1
    Scenario: Public method signatures use upstream SDK types
      Then the effect-s2 API contract includes:
        """
        Public method arguments and return types use upstream
        @s2-dev/streamstore request, response, config, ack, metric, stream-info,
        and token-info types rather than handwritten structural duplicates.
        """

    @requirement:SDK_FIDELITY.2
    Scenario: Required upstream types are re-exported
      Then the effect-s2 API contract includes:
        """
        effect-s2 re-exports upstream AppendInput, AppendRecord, BatchTransform,
        Producer, S2Environment, low-level SDK errors, and generated SDK types
        needed by callers.
        """

    @requirement:SDK_FIDELITY.3
    Scenario: AppendRecord constructors remain public
      Then the effect-s2 API contract includes:
        """
        AppendRecord.string, AppendRecord.bytes, AppendRecord.fence, and
        AppendRecord.trim remain available through the effect-s2 public export
        surface.
        """

    @requirement:SDK_FIDELITY.4
    Scenario: AppendInput.create preserves conditional append fields
      Then the effect-s2 API contract includes:
        """
        AppendInput.create remains the constructor for validated append batches
        and preserves upstream conditional fields such as matchSeqNum and
        fencingToken.
        """

    @requirement:SDK_FIDELITY.5
    Scenario: Upstream pagination helpers are used
      Then the effect-s2 API contract includes:
        """
        Upstream pagination helpers are used for listAllBasins,
        listAllAccessTokens, and listAllStreams; the wrapper must not hand-roll
        hasMore/startAfter loops when the SDK already exposes listAll.
        """

  @component:BASIN_AND_STREAMS
  Rule: Basin and stream resource operations

    @requirement:BASIN_AND_STREAMS.1
    Scenario: Basin operations preserve upstream basin behavior
      Then the effect-s2 API contract includes:
        """
        Basin operations map to the upstream S2.basins helper and preserve basin
        request options.
        """

    @requirement:BASIN_AND_STREAMS.2
    Scenario: Stream operations support basin overrides
      Then the effect-s2 API contract includes:
        """
        Stream operations map to the selected basin's streams or stream handle
        and support overriding basinName per operation.
        """

    @requirement:BASIN_AND_STREAMS.3
    Scenario: Operation options separate basin, request, and transport concerns
      Then the effect-s2 API contract includes:
        """
        S2OperationOptions separates basinName, request options, and stream
        transport options.
        """

    @requirement:BASIN_AND_STREAMS.4
    Scenario: Stream handles apply forceTransport consistently
      Then the effect-s2 API contract includes:
        """
        Stream handle construction applies forceTransport from the layer unless
        overridden by operation stream options.
        """

    @requirement:BASIN_AND_STREAMS.5
    Scenario: Ensure operations preserve upstream idempotent responses
      Then the effect-s2 API contract includes:
        """
        Ensure operations preserve upstream idempotent semantics and return
        upstream ensure responses rather than collapsing created/updated/noop
        into boolean flags.
        """

  @component:APPENDS
  Rule: Append semantics

    @requirement:APPENDS.1
    Scenario: append delegates to upstream stream.append and returns AppendAck
      Then the effect-s2 API contract includes:
        """
        append(name, input, options) delegates to the upstream stream.append and
        returns the upstream AppendAck, including acknowledged start/tail
        sequence information.
        """

    @requirement:APPENDS.2
    Scenario: Conditional appends preserve S2 conflict semantics
      Then the effect-s2 API contract includes:
        """
        Conditional appends preserve matchSeqNum and fencingToken semantics and
        map SDK sequence/fencing conflicts into typed S2Conflict errors.
        """

    @requirement:APPENDS.3
    Scenario: publish encodes through Effect Schema and appends JSON
      Then the effect-s2 API contract includes:
        """
        publish encodes a value through an Effect Schema codec into a JSON
        string record and appends it as one record.
        """

    @requirement:APPENDS.4
    Scenario: conditionalAppend is publish plus matchSeqNum
      Then the effect-s2 API contract includes:
        """
        conditionalAppend is publish plus matchSeqNum and returns the same
        upstream AppendAck shape.
        """

    @requirement:APPENDS.5
    Scenario: Append evidence spans include safe operation metadata
      Then the effect-s2 API contract includes:
        """
        Append evidence spans include stream, basin, record count, matchSeqNum
        when present, and the acknowledged starting seqNum.
        """

  @component:READS
  Rule: Read semantics

    @requirement:READS.1
    Scenario: Batch reads preserve upstream string and byte batch shapes
      Then the effect-s2 API contract includes:
        """
        readBatch returns the upstream string ReadBatch and readBatchBytes
        returns the upstream bytes ReadBatch.
        """

    @requirement:READS.2
    Scenario: Read sessions are exposed as scoped Effect Streams
      Then the effect-s2 API contract includes:
        """
        read and readBytes open upstream read sessions and expose them as scoped
        Effect Streams of normalized S2Record and S2RecordBytes values.
        """

    @requirement:READS.3
    Scenario: Normalized read records preserve upstream metadata
      Then the effect-s2 API contract includes:
        """
        S2Record preserves seqNum, timestamp, headers, and body metadata from
        upstream reads; S2RecordBytes preserves byte headers and byte body.
        """

    @requirement:READS.4
    Scenario: readDecoded preserves source record metadata
      Then the effect-s2 API contract includes:
        """
        readDecoded reads the string stream, JSON-decodes each body, decodes
        through the supplied Effect Schema codec, and preserves the source
        record metadata alongside value.
        """

    @requirement:READS.5
    Scenario: Read options preserve upstream cursor semantics
      Then the effect-s2 API contract includes:
        """
        Read options preserve upstream start/stop/limit/clamp/
        ignoreCommandRecords behavior; effect-s2 does not reinterpret S2 read
        cursors.
        """

  @component:SESSIONS_AND_PRODUCER
  Rule: Append sessions and producer

    @requirement:SESSIONS_AND_PRODUCER.1
    Scenario: appendSession is scoped
      Then the effect-s2 API contract includes:
        """
        appendSession is scoped; acquiring it opens the upstream append session
        and finalization closes it.
        """

    @requirement:SESSIONS_AND_PRODUCER.2
    Scenario: appendSession.submit returns ticket acknowledgements as Effect
      Then the effect-s2 API contract includes:
        """
        appendSession.submit submits an AppendInput, waits for the upstream
        ticket acknowledgement, and returns AppendAck in Effect.
        """

    @requirement:SESSIONS_AND_PRODUCER.3
    Scenario: producer is scoped over upstream Producer and append session
      Then the effect-s2 API contract includes:
        """
        producer is scoped; acquiring it constructs the upstream Producer over
        BatchTransform and an upstream append session, and finalization closes it.
        """

    @requirement:SESSIONS_AND_PRODUCER.4
    Scenario: ProducerConfig maps to upstream batching options
      Then the effect-s2 API contract includes:
        """
        ProducerConfig maps to upstream BatchTransformOptions and
        AppendSessionOptions for linger, batch size, bytes, and inflight bounds.
        """

    @requirement:SESSIONS_AND_PRODUCER.5
    Scenario: S2Client.sink adapts producer submission into an Effect Sink
      Then the effect-s2 API contract includes:
        """
        S2Client.sink adapts an S2Producer into an Effect Sink that submits
        upstream append records.
        """

  @component:ERRORS
  Rule: Typed error mapping

    @requirement:ERRORS.1
    Scenario: Upstream failures map into typed S2ClientError variants
      Then the effect-s2 API contract includes:
        """
        All upstream thrown/rejected failures are mapped into S2ClientError
        variants with operation, message, status, and original cause.
        """

    @requirement:ERRORS.2
    Scenario: Common HTTP status failures map to specific variants
      Then the effect-s2 API contract includes:
        """
        HTTP 404 maps to S2NotFound, 409 and 412 map to S2Conflict, 416 maps to
        S2RangeNotSatisfiable, and 429 maps to S2Throttled.
        """

    @requirement:ERRORS.3
    Scenario: Sequence and fencing conflict details are retained
      Then the effect-s2 API contract includes:
        """
        SeqNumMismatchError and FencingTokenMismatchError details are retained
        on S2Conflict when the upstream error exposes them.
        """

    @requirement:ERRORS.4
    Scenario: Range not satisfiable tail metadata is retained
      Then the effect-s2 API contract includes:
        """
        RangeNotSatisfiableError tail metadata is retained on
        S2RangeNotSatisfiable when available.
        """

    @requirement:ERRORS.5
    Scenario: Unknown upstream failures stay typed
      Then the effect-s2 API contract includes:
        """
        Unknown upstream failures remain typed as S2Error rather than escaping as
        raw defects.
        """

  @component:OBSERVABILITY
  Rule: Centralized tracing

    @requirement:OBSERVABILITY.1
    Scenario: Every S2Client operation emits a stable evidence span
      Then the effect-s2 API contract includes:
        """
        Every S2Client operation emits one stable S2.<operation> evidence span
        from production code.
        """

    @requirement:OBSERVABILITY.2
    Scenario: Streaming operations are traced around the stream
      Then the effect-s2 API contract includes:
        """
        Streaming operations emit spans around the Effect Stream, not
        validation-only instrumentation.
        """

    @requirement:OBSERVABILITY.3
    Scenario: Append/session/producer spans annotate acknowledged seqNum
      Then the effect-s2 API contract includes:
        """
        Append/session/producer submit spans annotate the acknowledged seqNum
        when the acknowledgement is available.
        """

    @requirement:OBSERVABILITY.4
    Scenario: Trace attributes include safe operation arguments
      Then the effect-s2 API contract includes:
        """
        Trace attributes include operation arguments when safely serializable
        plus basin and stream where applicable.
        """

  @constraint:EFFECT_IDIOMS
  Rule: Effect-native API shape

    @requirement:EFFECT_IDIOMS.1
    Scenario: Public runtime APIs return Effect-native data types
      Then the effect-s2 API contract includes:
        """
        Public runtime APIs return Effect.Effect, Stream.Stream, Sink.Sink, or
        Layer.Layer; callers do not receive raw Promises or AsyncIterables from
        effect-s2 methods.
        """

    @requirement:EFFECT_IDIOMS.2
    Scenario: Resourceful SDK sessions use Scope-aware acquire and release
      Then the effect-s2 API contract includes:
        """
        Resourceful SDK sessions are represented with Scope-aware
        acquire/release, not manual close obligations at the call site.
        """

    @requirement:EFFECT_IDIOMS.3
    Scenario: Access tokens are redacted at layer boundaries
      Then the effect-s2 API contract includes:
        """
        Access tokens are represented as Redacted values at the layer boundary.
        """

  @constraint:NO_SHADOW_SDK
  Rule: No parallel SDK model

    @requirement:NO_SHADOW_SDK.1
    Scenario: effect-s2 does not redefine upstream SDK models
      Then the effect-s2 API contract includes:
        """
        effect-s2 does not redefine S2 resource/config/request/response data
        models except for small Effect-facing wrappers such as S2Record,
        S2RecordBytes, S2ClientError, S2Producer, and S2AppendSession.
        """

    @requirement:NO_SHADOW_SDK.2
    Scenario: New required upstream operations are exposed or explicitly unsupported
      Then the effect-s2 API contract includes:
        """
        When upstream adds a stable streamstore operation that fluent-firegrid
        needs, effect-s2 either exposes it with SDK types or explicitly records
        it as unsupported in this feature.
        """

    @requirement:NO_SHADOW_SDK.3
    Scenario: Higher packages consume S2 through effect-s2
      Then the effect-s2 API contract includes:
        """
        Higher packages must consume S2 through effect-s2 rather than importing
        @s2-dev/streamstore directly.
        """

  @constraint:COMMAND_RECORDS
  Rule: S2 command records remain first-class

    @requirement:COMMAND_RECORDS.1
    Scenario: Fence and trim command records use upstream constructors
      Then the effect-s2 API contract includes:
        """
        Fence and trim command records are exposed through upstream AppendRecord
        constructors, not reimplemented manually.
        """

    @requirement:COMMAND_RECORDS.2
    Scenario: Reads preserve command-record ordering and sequence effects
      Then the effect-s2 API contract includes:
        """
        Reads preserve command-record ordering and seqNum effects unless the
        caller explicitly uses an upstream ignoreCommandRecords option.
        """

    @requirement:COMMAND_RECORDS.3
    Scenario: Higher packages explicitly handle or filter command records
      Then the effect-s2 API contract includes:
        """
        Higher-level packages that interpret application records must explicitly
        handle or filter command records.
        """

  @constraint:VERSION_ALIGNMENT
  Rule: Upstream alignment checks

    @requirement:VERSION_ALIGNMENT.1
    Scenario: Installed upstream SDK version is the compile-time API truth
      Then the effect-s2 API contract includes:
        """
        The installed @s2-dev/streamstore version is the source of compile-time
        API truth for effect-s2.
        """

    @requirement:VERSION_ALIGNMENT.2
    Scenario: API coverage tests enumerate promised S2ClientApi methods
      Then the effect-s2 API contract includes:
        """
        API coverage tests enumerate every S2ClientApi method that effect-s2
        promises to expose.
        """

    @requirement:VERSION_ALIGNMENT.3
    Scenario: CI fails on wrapper type drift from upstream SDK types
      Then the effect-s2 API contract includes:
        """
        CI must fail when wrapper types drift from upstream SDK types in a way
        that breaks public effect-s2 consumers.
        """
