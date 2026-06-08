// Host-side scenario constants. The driver is airgapped (firelab rule: drivers
// import only @firegrid/client-sdk + effect) and re-declares the contract it
// observes — so only the host imports this module.

export const SESSION_ID = "fluent-acp-conductor-session"
export const AGENT_LABEL = "firelab-fluent-acp-conductor"
