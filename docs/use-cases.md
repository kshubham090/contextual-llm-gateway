# Use cases that can be tested

All organizations, systems, policies, identifiers, and operational values below are fictional. The packs demonstrate retrieval behavior; they are not expert-approved operating procedures. The canonical machine-readable source is [`examples/scenarios.json`](../examples/scenarios.json).

Each pack keeps its four seeds and two held-out questions in one user/feature scope. A unique run ID prevents previous evaluation answers from contaminating a new trial. The evaluator also asks in an unseeded user and an unseeded feature, expecting no retrieved calls. These controls supplement the dedicated tenant-boundary tests; they do not replace a complete access-control assessment.

## Orbit deployment incidents

**Situation:** An on-call engineer needs a recovery sequence for an unfamiliar release platform. Separate conversations established an eight-minute canary, a 20% budget gate, an image-only rollback command, and separate Ballast configuration recovery.

**Held-out task:** Reconstruct image/config recovery and the deployment audit reference; recover the bake interval and monitored signals. Useful memory connects the exception to the procedure, rather than merely matching “rollback.”

**Evidence to inspect:** `orbitctl revert`, `config revert`, and flight-number references appear when supplied by retrieved seeds. A Cedar support marker is a contamination probe. Reviewer checks should include whether the answer presents conditions clearly and invents any commands.

## Cedar support continuity

**Situation:** A fictional connector emits CDR-409 after a workspace migration. Prior exchanges describe cursor refresh, a dry-run reconciliation, ownership verification, and an integration-team escalation threshold.

**Held-out task:** Recover the sequence before resuming sync, then explain escalation evidence after retries.

**Evidence to inspect:** Cursor refresh and reconciliation are recovered without a destructive workspace reset. Source memories stay within the support user's scope. A lexical forbidden match can also occur in a warning such as “do not delete”; inspect the full answer before classifying it as a bad recommendation.

## Lantern experiment protocols

**Situation:** A team uses a fictional optical sensor protocol. Calibration, twelve reference tiles, seed 31415, drift acceptance, and review ownership appeared in separate interactions.

**Held-out task:** Prepare a reproducible run and handle a drift failure.

**Evidence to inspect:** The answer recovers the actual supplied parameters and retains failed data for review. Evaluate whether it separates a fictional internal protocol from general scientific advice. This is a nonclinical illustration, not experimental validation.

## Quartz data migrations

**Situation:** A data team migrates an analytics events table while maintaining per-tenant correctness. Prior calls established a 72-hour dual-write window, amount checksums, minor currency units, and a reader-view rollback.

**Held-out task:** Define cutover evidence and respond to a partition mismatch.

**Evidence to inspect:** The answer connects verification to `tenant_key`, retains the audit log, and does not suggest dropping source tables during the migration. This example tests recalled engineering constraints, not financial advice or accounting correctness.

## Harbor maintenance training

**Situation:** A fictional training conveyor has an alert whose history spans controlled stopping, qualified technician inspection, bearing checks, and supervisor release.

**Held-out task:** Retrieve the supplied inspection and return-to-service checklist.

**Evidence to inspect:** The answer preserves the qualification boundary and the ten-minute guarded test. This is a synthetic training scenario; real equipment requires its own manufacturer guidance and approved procedures. The gateway must never be treated as a safety controller.

## Meadow preview releases

**Situation:** A preview launch has synthetic-data requirements, a 48-hour export expiry, a named review group, and separate privacy/accessibility checks.

**Held-out task:** Recall data-handling requirements and launch reviewers.

**Evidence to inspect:** The answer recovers synthetic-only fixtures and keyboard-navigation review without importing facts from Quartz or Cedar. This is a retrieval example, not a privacy compliance certification.

## Extending a pack

Add a disjoint scope, concise fact-bearing seeds, and genuinely held-out questions. For every expected fact, supply an ID, a description, and accepted lexical aliases. Add forbidden facts for concrete contradictions or cross-scope markers. The validator requires expected aliases to appear in seed facts; it cannot establish semantic completeness.

Prefer narrow, measurable tasks over generic “answer better” prompts. Test missing information, superseded policies, contradictory memory, empty retrieval, high lexical overlap across tenants, cache bypass, and a poisoned memory separately. Record human judgments alongside the automated proxy instead of tuning aliases until every answer passes.
