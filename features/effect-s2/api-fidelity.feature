Feature: API fidelity
  Effect-native facade for the upstream @s2-dev/streamstore SDK. The package
  preserves SDK request/response/config type fidelity while exposing S2
  operations as Effect Context services, Layers, Effects, Streams, scoped
  sessions, typed errors, and production evidence spans.

  The upstream source of truth is s2-sdk-typescript/packages/streamstore/src;
  this feature records the wrapper parity expected by fluent-firegrid packages.

  Rule: S2Client service surface

    Scenario: S2Client is an Effect Context.Service whose methods are available both through dependency injection and as static accessors requiring S2Client in the environment
      Then S2Client is an Effect Context.Service whose methods are available both through dependency injection and as static accessors requiring S2Client in the environment.

    Scenario: Account/control-plane operations cover listBasins, listAllBasins, createBasin, getBasinConfig, deleteBasin, ensureBasin, reconfigureBasin, listLocations, getDefaultLocation, setDefaultLocation, listAccessTokens, listAllAccessTokens, issueAccessToken, revokeAccessToken, accountMetrics, basinMetrics, and streamMetrics
      Then Account/control-plane operations cover listBasins, listAllBasins, createBasin, getBasinConfig, deleteBasin, ensureBasin, reconfigureBasin, listLocations, getDefaultLocation, setDefaultLocation, listAccessTokens, listAllAccessTokens, issueAccessToken, revokeAccessToken, accountMetrics, basinMetrics, and streamMetrics.

    Scenario: Stream-management operations cover listStreams, listAllStreams, createStream, getStreamConfig, deleteStream, ensureStream, and reconfigureStream
      Then Stream-management operations cover listStreams, listAllStreams, createStream, getStreamConfig, deleteStream, ensureStream, and reconfigureStream.

    Scenario: Stream data-plane operations cover checkTail, append, readBatch, readBatchBytes, read, readBytes, appendSession, and producer
      Then Stream data-plane operations cover checkTail, append, readBatch, readBatchBytes, read, readBytes, appendSession, and producer.

    Scenario: Methods that return a single SDK promise are exposed as Effect.Effect; SDK async iterables and read sessions are exposed as Effect Stream.Stream
      Then Methods that return a single SDK promise are exposed as Effect.Effect; SDK async iterables and read sessions are exposed as Effect Stream.Stream.

  Rule: Effect layer construction

    Scenario: S2Client.layer accepts accessToken and basinName as either Effect Config values or concrete values, resolves them in Layer construction, and stores the resulting live SDK client behind S2Client
      Then S2Client.layer accepts accessToken and basinName as either Effect Config values or concrete values, resolves them in Layer construction, and stores the resulting live SDK client behind S2Client.

    Scenario: S2Client.layerConfig reads S2_ACCESS_TOKEN and S2_BASIN from Effect Config and redacts the access token
      Then S2Client.layerConfig reads S2_ACCESS_TOKEN and S2_BASIN from Effect Config and redacts the access token.

    Scenario: Layer options pass through upstream endpoints, retry, and forceTransport settings without inventing parallel configuration names
      Then Layer options pass through upstream endpoints, retry, and forceTransport settings without inventing parallel configuration names.

    Scenario: The default append retry policy is noSideEffects unless the caller explicitly supplies a retry configuration
      Then The default append retry policy is noSideEffects unless the caller explicitly supplies a retry configuration.

  Rule: Upstream SDK type fidelity

    Scenario: Public method arguments and return types use upstream @s2-dev/streamstore request, response, config, ack, metric, stream-info, and token-info types rather than handwritten structural duplicates
      Then Public method arguments and return types use upstream @s2-dev/streamstore request, response, config, ack, metric, stream-info, and token-info types rather than handwritten structural duplicates.

    Scenario: effect-s2 re-exports upstream AppendInput, AppendRecord, BatchTransform, Producer, S2Environment, low-level SDK errors, and generated SDK types needed by callers
      Then effect-s2 re-exports upstream AppendInput, AppendRecord, BatchTransform, Producer, S2Environment, low-level SDK errors, and generated SDK types needed by callers.

    Scenario: AppendRecord.string, AppendRecord.bytes, AppendRecord.fence, and AppendRecord.trim remain available through the effect-s2 public export surface
      Then AppendRecord.string, AppendRecord.bytes, AppendRecord.fence, and AppendRecord.trim remain available through the effect-s2 public export surface.

    Scenario: AppendInput.create remains the constructor for validated append batches and preserves upstream conditional fields such as matchSeqNum and fencingToken
      Then AppendInput.create remains the constructor for validated append batches and preserves upstream conditional fields such as matchSeqNum and fencingToken.

    Scenario: Upstream pagination helpers are used for listAllBasins, listAllAccessTokens, and listAllStreams; the wrapper must not hand-roll hasMore/startAfter loops when the SDK already exposes listAll
      Then Upstream pagination helpers are used for listAllBasins, listAllAccessTokens, and listAllStreams; the wrapper must not hand-roll hasMore/startAfter loops when the SDK already exposes listAll.

  Rule: Basin and stream resource operations

    Scenario: Basin operations map to the upstream S2.basins helper and preserve basin request options
      Then Basin operations map to the upstream S2.basins helper and preserve basin request options.

    Scenario: Stream operations map to the selected basin's streams or stream handle and support overriding basinName per operation
      Then Stream operations map to the selected basin's streams or stream handle and support overriding basinName per operation.

    Scenario: S2OperationOptions separates basinName, request options, and stream transport options
      Then S2OperationOptions separates basinName, request options, and stream transport options.

    Scenario: Stream handle construction applies forceTransport from the layer unless overridden by operation stream options
      Then Stream handle construction applies forceTransport from the layer unless overridden by operation stream options.

    Scenario: Ensure operations preserve upstream idempotent semantics and return upstream ensure responses rather than collapsing created/updated/noop into boolean flags
      Then Ensure operations preserve upstream idempotent semantics and return upstream ensure responses rather than collapsing created/updated/noop into boolean flags.

  Rule: Append semantics

    Scenario: append(name, input, options) delegates to the upstream stream.append and returns the upstream AppendAck, including acknowledged start/tail sequence information
      Then append(name, input, options) delegates to the upstream stream.append and returns the upstream AppendAck, including acknowledged start/tail sequence information.

    Scenario: Conditional appends preserve matchSeqNum and fencingToken semantics and map SDK sequence/fencing conflicts into typed S2Conflict errors
      Then Conditional appends preserve matchSeqNum and fencingToken semantics and map SDK sequence/fencing conflicts into typed S2Conflict errors.

    Scenario: publish encodes a value through an Effect Schema codec into a JSON string record and appends it as one record
      Then publish encodes a value through an Effect Schema codec into a JSON string record and appends it as one record.

    Scenario: conditionalAppend is publish plus matchSeqNum and returns the same upstream AppendAck shape
      Then conditionalAppend is publish plus matchSeqNum and returns the same upstream AppendAck shape.

    Scenario: Append evidence spans include stream, basin, record count, matchSeqNum when present, and the acknowledged starting seqNum
      Then Append evidence spans include stream, basin, record count, matchSeqNum when present, and the acknowledged starting seqNum.

  Rule: Read semantics

    Scenario: readBatch returns the upstream string ReadBatch and readBatchBytes returns the upstream bytes ReadBatch
      Then readBatch returns the upstream string ReadBatch and readBatchBytes returns the upstream bytes ReadBatch.

    Scenario: read and readBytes open upstream read sessions and expose them as scoped Effect Streams of normalized S2Record and S2RecordBytes values
      Then read and readBytes open upstream read sessions and expose them as scoped Effect Streams of normalized S2Record and S2RecordBytes values.

    Scenario: S2Record preserves seqNum, timestamp, headers, and body metadata from upstream reads; S2RecordBytes preserves byte headers and byte body
      Then S2Record preserves seqNum, timestamp, headers, and body metadata from upstream reads; S2RecordBytes preserves byte headers and byte body.

    Scenario: readDecoded reads the string stream, JSON-decodes each body, decodes through the supplied Effect Schema codec, and preserves the source record metadata alongside value
      Then readDecoded reads the string stream, JSON-decodes each body, decodes through the supplied Effect Schema codec, and preserves the source record metadata alongside value.

    Scenario: Read options preserve upstream start/stop/limit/clamp/ ignoreCommandRecords behavior; effect-s2 does not reinterpret S2 read cursors
      Then Read options preserve upstream start/stop/limit/clamp/ ignoreCommandRecords behavior; effect-s2 does not reinterpret S2 read cursors.

  Rule: Append sessions and producer

    Scenario: appendSession is scoped; acquiring it opens the upstream append session and finalization closes it
      Then appendSession is scoped; acquiring it opens the upstream append session and finalization closes it.

    Scenario: appendSession.submit submits an AppendInput, waits for the upstream ticket acknowledgement, and returns AppendAck in Effect
      Then appendSession.submit submits an AppendInput, waits for the upstream ticket acknowledgement, and returns AppendAck in Effect.

    Scenario: producer is scoped; acquiring it constructs the upstream Producer over BatchTransform and an upstream append session, and finalization closes it
      Then producer is scoped; acquiring it constructs the upstream Producer over BatchTransform and an upstream append session, and finalization closes it.

    Scenario: ProducerConfig maps to upstream BatchTransformOptions and AppendSessionOptions for linger, batch size, bytes, and inflight bounds
      Then ProducerConfig maps to upstream BatchTransformOptions and AppendSessionOptions for linger, batch size, bytes, and inflight bounds.

    Scenario: S2Client.sink adapts an S2Producer into an Effect Sink that submits upstream append records
      Then S2Client.sink adapts an S2Producer into an Effect Sink that submits upstream append records.

  Rule: Typed error mapping

    Scenario: All upstream thrown/rejected failures are mapped into S2ClientError variants with operation, message, status, and original cause
      Then All upstream thrown/rejected failures are mapped into S2ClientError variants with operation, message, status, and original cause.

    Scenario: HTTP 404 maps to S2NotFound, 409 and 412 map to S2Conflict, 416 maps to S2RangeNotSatisfiable, and 429 maps to S2Throttled
      Then HTTP 404 maps to S2NotFound, 409 and 412 map to S2Conflict, 416 maps to S2RangeNotSatisfiable, and 429 maps to S2Throttled.

    Scenario: SeqNumMismatchError and FencingTokenMismatchError details are retained on S2Conflict when the upstream error exposes them
      Then SeqNumMismatchError and FencingTokenMismatchError details are retained on S2Conflict when the upstream error exposes them.

    Scenario: RangeNotSatisfiableError tail metadata is retained on S2RangeNotSatisfiable when available
      Then RangeNotSatisfiableError tail metadata is retained on S2RangeNotSatisfiable when available.

    Scenario: Unknown upstream failures remain typed as S2Error rather than escaping as raw defects
      Then Unknown upstream failures remain typed as S2Error rather than escaping as raw defects.

  Rule: Centralized tracing

    Scenario: Every S2Client operation emits one stable S2.<operation> evidence span from production code
      Then Every S2Client operation emits one stable S2.<operation> evidence span from production code.

    Scenario: Streaming operations emit spans around the Effect Stream, not validation-only instrumentation
      Then Streaming operations emit spans around the Effect Stream, not validation-only instrumentation.

    Scenario: Append/session/producer submit spans annotate the acknowledged seqNum when the acknowledgement is available
      Then Append/session/producer submit spans annotate the acknowledged seqNum when the acknowledgement is available.

    Scenario: Trace attributes include operation arguments when safely serializable plus basin and stream where applicable
      Then Trace attributes include operation arguments when safely serializable plus basin and stream where applicable.

  Rule: Effect-native API shape

    Scenario: Public runtime APIs return Effect.Effect, Stream.Stream, Sink.Sink, or Layer.Layer; callers do not receive raw Promises or AsyncIterables from effect-s2 methods
      Then Public runtime APIs return Effect.Effect, Stream.Stream, Sink.Sink, or Layer.Layer; callers do not receive raw Promises or AsyncIterables from effect-s2 methods.

    Scenario: Resourceful SDK sessions are represented with Scope-aware acquire/release, not manual close obligations at the call site
      Then Resourceful SDK sessions are represented with Scope-aware acquire/release, not manual close obligations at the call site.

    Scenario: Access tokens are represented as Redacted values at the layer boundary
      Then Access tokens are represented as Redacted values at the layer boundary.

  Rule: No parallel SDK model

    Scenario: effect-s2 does not redefine S2 resource/config/request/response data models except for small Effect-facing wrappers such as S2Record, S2RecordBytes, S2ClientError, S2Producer, and S2AppendSession
      Then effect-s2 does not redefine S2 resource/config/request/response data models except for small Effect-facing wrappers such as S2Record, S2RecordBytes, S2ClientError, S2Producer, and S2AppendSession.

    Scenario: When upstream adds a stable streamstore operation that fluent-firegrid needs, effect-s2 either exposes it with SDK types or explicitly records it as unsupported in this feature
      Then When upstream adds a stable streamstore operation that fluent-firegrid needs, effect-s2 either exposes it with SDK types or explicitly records it as unsupported in this feature.

    Scenario: Higher packages must consume S2 through effect-s2 rather than importing @s2-dev/streamstore directly
      Then Higher packages must consume S2 through effect-s2 rather than importing @s2-dev/streamstore directly.

  Rule: S2 command records remain first-class

    Scenario: Fence and trim command records are exposed through upstream AppendRecord constructors, not reimplemented manually
      Then Fence and trim command records are exposed through upstream AppendRecord constructors, not reimplemented manually.

    Scenario: Reads preserve command-record ordering and seqNum effects unless the caller explicitly uses an upstream ignoreCommandRecords option
      Then Reads preserve command-record ordering and seqNum effects unless the caller explicitly uses an upstream ignoreCommandRecords option.

    Scenario: Higher-level packages that interpret application records must explicitly handle or filter command records
      Then Higher-level packages that interpret application records must explicitly handle or filter command records.

  Rule: Upstream alignment checks

    Scenario: The installed @s2-dev/streamstore version is the source of compile-time API truth for effect-s2
      Then The installed @s2-dev/streamstore version is the source of compile-time API truth for effect-s2.

    Scenario: API coverage tests enumerate every S2ClientApi method that effect-s2 promises to expose
      Then API coverage tests enumerate every S2ClientApi method that effect-s2 promises to expose.

    Scenario: CI must fail when wrapper types drift from upstream SDK types in a way that breaks public effect-s2 consumers
      Then CI must fail when wrapper types drift from upstream SDK types in a way that breaks public effect-s2 consumers.

