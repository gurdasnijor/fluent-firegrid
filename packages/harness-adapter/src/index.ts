/**
 * `@firegrid/harness-adapter` — the harness adapter contract (MS-C6, WP D2): the
 * reconstruction-model seam between one agent harness and the L1 observation
 * vocabulary (I2). See the ratified Target Surface in the managed-sessions SDD
 * §MS-C6.
 *
 * - `contract.ts` — the ratified types + service seams (`HarnessAdapter`,
 *   `L1Sink`, `ToolGate`, `HarnessLowering`, errors).
 * - `replay.ts` — the pure, Effect-free lowering core (`harness.fixture-replay`).
 * - `reconstruction.ts` — the generic `drive` shell + resume-suppression.
 * - `reference.ts` — the ACP-native reference lowering (not Claude; that is D3).
 */

export * from "./contract.ts"
export * from "./reconstruction.ts"
export * from "./reference.ts"
export * from "./replay.ts"
