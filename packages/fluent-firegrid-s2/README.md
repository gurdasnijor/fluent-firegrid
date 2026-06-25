# @firegrid/fluent-firegrid-s2

S2-backed adapters for `@firegrid/fluent-firegrid`.

This package keeps `effect-s2` out of fluent core. The first surface is an
`ObjectStateBackend` implementation for table/materialized virtual object state.
The object owner/drainer layer will build on the same owner stream model.
