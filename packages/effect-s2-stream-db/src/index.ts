export * as ChangeMessage from "./ChangeMessage.ts"
export { decodeEventRecord, encodeEventRecord, EventStream } from "./EventStream.ts"
export type { EventKeySchema, EventRecord, EventStreamClass, OpenEventStreamOptions, ReadFromOptions, TypedEventStreamInstance } from "./EventStream.ts"
export { MaterializedState } from "./MaterializedState.ts"
export { S2StreamDbError } from "./errors.ts"
export { primaryKey, StreamDb, Table } from "./StreamDb.ts"
export type {
  AnyTable,
  InsertOrGetResult,
  KeySchema,
  OpenOptions,
  RowOf,
  StreamDbClass,
  StreamDbInstance,
  TableClass,
  TableFacade,
  Tables,
  Transaction,
} from "./StreamDb.ts"
