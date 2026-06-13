import { defaultStreamPrefix } from "./config.ts"

const textEncoder = new TextEncoder()

const base64Url = (value: string): string => {
  const bytes = textEncoder.encode(value)
  const binary = Array.from(bytes, (byte) => String.fromCharCode(byte)).join("")
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "")
}

export const executionStreamName = (options: {
  readonly streamPrefix?: string
  readonly workflowName: string
  readonly executionId: string
}): string => {
  const prefix = options.streamPrefix ?? defaultStreamPrefix
  return [
    prefix,
    "executions",
    base64Url(options.workflowName),
    base64Url(options.executionId),
  ].join("/")
}
