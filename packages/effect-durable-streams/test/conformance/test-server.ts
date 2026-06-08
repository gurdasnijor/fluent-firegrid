import { DurableStreamTestServer } from "@durable-streams/server"
import type { TestServerOptions } from "@durable-streams/server"

export interface TestServerHandle {
  readonly url: string
  readonly streamUrl: (name?: string) => string
  readonly stop: () => Promise<void>
}

/**
 * Start the reference `@durable-streams/server` in-process on an ephemeral
 * port. Returns a handle with a helper to mint unique stream URLs.
 */
export const startTestServer = async (options?: {
  readonly webhooks?: boolean
}): Promise<TestServerHandle> => {
  const serverOptions: TestServerOptions = {
    port: 0,
    host: "127.0.0.1",
  }
  if (options?.webhooks !== undefined) {
    serverOptions.webhooks = options.webhooks
  }
  const server = new DurableStreamTestServer(serverOptions)
  const url = await server.start()
  return {
    url,
    streamUrl: (name = "test") =>
      `${url}/v1/stream/${name}-${crypto.randomUUID()}`,
    stop: () => server.stop(),
  }
}
