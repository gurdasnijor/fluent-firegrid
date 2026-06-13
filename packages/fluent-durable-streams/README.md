# fluent-durable-streams

S2-native Durable Streams HTTP API and coordination services.

This package uses the official `@s2-dev/streamstore` SDK directly. It does not
wrap S2 into a DurableStreamLog CRUD facade and does not define a client/server
transport abstraction. The public boundary is an Effect `HttpApi` definition
implemented by handlers over S2 basin and stream handles.
