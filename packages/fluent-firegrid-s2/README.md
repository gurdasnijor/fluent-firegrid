# @firegrid/fluent-firegrid-s2

S2-backed adapters for `@firegrid/fluent-firegrid`.

This package keeps `effect-s2` out of fluent core. It provides:

- an `ObjectStateBackend` implementation for table/materialized virtual object
  state;
- an S2 object owner/drainer runtime binding;
- `s2FluentDefinitionBindingOptions`, which wires S2 object state and can pass
  an invocation binding thunk through for ambient fluent clients.
