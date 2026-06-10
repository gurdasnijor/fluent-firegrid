export interface SubscriptionServer {
  readonly _tag: "SubscriptionServer"
}

export const makeSubscriptionServer = (): SubscriptionServer => ({
  _tag: "SubscriptionServer",
})
