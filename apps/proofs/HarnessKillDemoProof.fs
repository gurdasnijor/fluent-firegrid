/// P0.1 wiring placeholder for the `p0.harness-kill-demo` runner canary:
/// proves the targets-mode plumbing (registry -> runner -> JSONL protocol ->
/// ratchet) and the chdb evidence path end to end. Replaced at M4 by the
/// real ProcessHost + KillHost demonstration proof.
namespace Firegrid.Foundation.Proofs

module HarnessKillDemoProof =
    let private wiringProperty =
        property "p0.harness-kill-demo-proof" {
            workload (fun ctx ->
                async {
                    do!
                        ctx.EmitSpan
                            "verification.harness.wiring"
                            [ "proof.property", "p0.harness-kill-demo-proof" ]

                    return true
                })

            verify (fun v ->
                [ v.Expect.Workload "wiring self-check" id
                  v.Trace.SpanExists
                      "wiring span recorded"
                      "verification.harness.wiring"
                      [ "proof.property", "p0.harness-kill-demo-proof" ] ])
        }

    let proof =
        proof "p0.harness-kill-demo" {
            describedAs
                "Runner canary: the rebuilt harness runs a compiled self-proof through targets mode with chdb-backed trace evidence."

            property wiringProperty
        }
