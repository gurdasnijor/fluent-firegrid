import * as Data from "effect/Data"

export class FlowError extends Data.TaggedError("FlowError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

export class BatchTooLarge extends Data.TaggedError("BatchTooLarge")<{
  readonly bytes: number
  readonly maxBytes: number
  readonly maxRecords: number
  readonly records: number
}> {
  get message(): string {
    return `atomic journal commit exceeds S2 append limits (${this.records}/${this.maxRecords} records, ${this.bytes}/${this.maxBytes} bytes)`
  }
}
