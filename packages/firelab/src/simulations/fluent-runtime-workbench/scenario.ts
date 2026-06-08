export const sessionId = "fluent-workbench-session"
export const turnId = "fluent-workbench-turn"
export const waitId = "review-wait"
export const deliveryId = "review-delivery-1"
export const reviewKey = "reviews/review-delivery-1"
export const reviewPredicate =
  "event.type == \"review.posted\" && event.value.issueId == self.issueId"
