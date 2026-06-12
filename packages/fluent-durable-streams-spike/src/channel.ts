import type { DurableStreamsChannel, DurableStreamsServer } from "./model.ts"

export const makeInProcessChannel = (server: DurableStreamsServer): DurableStreamsChannel => ({
  create: (command) => server.create(command),
  append: (command) => server.append(command),
  head: (path) => server.head(path),
  read: (command) => server.read(command),
  readJson: (command) => server.readJson(command),
  follow: (command) => server.follow(command),
  delete: (path) => server.delete(path),
})

