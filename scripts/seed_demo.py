"""Seed the gateway with ~35 related calls about a fictional internal platform.

The domain facts live inside the prompts (the way real users leak context into
questions), so the LLM's responses absorb and restate them — and the graph
accumulates a working memory of the domain. Run demo_compare.py afterwards.

Usage:
    python scripts/seed_demo.py [--base-url http://localhost:8000]
"""
import argparse
import sys
import time

import httpx

# Fictional domain: "Orbit", the internal deployment platform at Nimbus Labs.
# Facts are deliberately specific so grounded answers are visibly different
# from generic ones.
SEED_PROMPTS = [
    # deploys / rollouts
    ("deploys", "Our Orbit platform uses blue-green deployments with a mandatory 8-minute bake time before traffic shift. Is 8 minutes reasonable, or should we tune it?"),
    ("deploys", "Orbit shifts traffic 5% -> 25% -> 100% during a rollout. When should we abort instead of proceeding to 25%?"),
    ("deploys", "In Orbit, a deploy is blocked if the error budget for the service is below 20%. How do error budgets normally interact with deploy gates?"),
    ("deploys", "Orbit tags every deploy with a 'flight number' like ORB-2291. What's a good convention for referencing flight numbers in incident reports?"),
    ("deploys", "Rollbacks in Orbit restore the previous container image but NOT config changes — those need a separate 'config revert'. How should we document this gotcha for new engineers?"),
    ("deploys", "Orbit's canary analysis compares p99 latency and 5xx rate against the baseline for 8 minutes. What other signals are worth adding to canary analysis?"),
    ("deploys", "We limit Orbit to 3 concurrent deploys per team to protect the build farm. Is per-team concurrency limiting a common pattern?"),
    ("deploys", "Orbit deploys are frozen every Friday after 2pm and during the last week of each quarter. What are the tradeoffs of deploy freezes?"),
    # incidents
    ("incidents", "At Nimbus Labs, SEV1 incidents page the on-call via Orbit's alert bridge within 60 seconds. What should our SEV1 acknowledgement SLA be?"),
    ("incidents", "Our incident review template requires a 'flight number' (Orbit deploy ID) for any deploy-related incident. What else belongs in a good incident template?"),
    ("incidents", "Orbit auto-creates an incident channel named #inc-<date>-<service>. How can we keep incident channels from becoming graveyards?"),
    ("incidents", "Nimbus Labs targets 30-minute mean time to mitigate for SEV2s. Is MTTM a better north-star than MTTR?"),
    ("incidents", "When Orbit's canary aborts a deploy, it files a 'flight incident' automatically. Should auto-filed incidents share the same review process as human-filed ones?"),
    # config service
    ("config", "Orbit's config service ('Ballast') versions every config change and requires two-person review for production namespaces. Is two-person review overkill for config?"),
    ("config", "Ballast config changes propagate to services within 30 seconds via long-polling. What are the failure modes of long-poll config distribution?"),
    ("config", "Ballast supports per-service 'config freezes' independent of deploy freezes. When would you freeze config but not deploys?"),
    ("config", "A Ballast misconfig caused our biggest outage last quarter — a fleet-wide flag flip with no canary. Should config changes get canary analysis like deploys do?"),
    # observability
    ("observability", "Every Orbit service exports RED metrics (rate, errors, duration) by default, scraped every 15 seconds. What dashboards should we build on top of RED metrics?"),
    ("observability", "Orbit attaches the flight number as a trace attribute on every request during a rollout. How can we exploit that in debugging?"),
    ("observability", "Nimbus Labs keeps 30 days of metrics at full resolution and 13 months downsampled. Is that a sensible retention policy?"),
    ("observability", "Orbit's SLO dashboard burns down error budget in real time and gates deploys below 20%. How should teams negotiate SLO targets?"),
    # cost
    ("cost", "Orbit bills teams internally per vCPU-hour, and idle preview environments auto-sleep after 48 hours. How else can we cut preview environment costs?"),
    ("cost", "Nimbus Labs' preview environments spin up per pull request via Orbit. What's a reasonable TTL policy for PR environments?"),
    ("cost", "Orbit shows a cost estimate before every deploy based on the resource diff. Does showing cost at deploy time actually change engineer behavior?"),
    # onboarding / docs
    ("onboarding", "New Nimbus engineers get deploy rights in Orbit only after completing the 'first flight' checklist (a supervised deploy). Is gated deploy access worth the friction?"),
    ("onboarding", "Orbit's CLI is 'orbitctl' — the most-used commands are 'orbitctl launch', 'orbitctl status', and 'orbitctl revert'. What should a cheat-sheet for new engineers cover?"),
    ("onboarding", "We keep Orbit runbooks in the repo next to the service code, not in a wiki. What are the pros and cons of runbooks-in-repo?"),
    # security / access
    ("security", "Orbit requires hardware-key MFA for production 'revert' and 'config revert' operations. Should read-only production access also require MFA?"),
    ("security", "Orbit service tokens rotate every 24 hours automatically via Ballast. What breaks most often with short-lived token rotation?"),
    ("security", "Only the on-call and the service owner can bypass Orbit's deploy freeze with a 'break-glass' flow that pages the platform team. Is that the right approval set?"),
    # architecture
    ("architecture", "Orbit's control plane is a Go monolith, but execution agents ('tugs') run per-cluster. Why do platform teams often split control plane from agents?"),
    ("architecture", "Each Orbit 'tug' agent polls the control plane rather than receiving pushes, so clusters behind NAT work without inbound firewall rules. What are the latency tradeoffs of poll-based agents?"),
    ("architecture", "Orbit stores deploy state in Postgres with an event-sourced history table. When does event sourcing pay off for infrastructure tooling?"),
    ("architecture", "Nimbus Labs runs Orbit tugs in 6 regions; a regional control-plane outage must not block deploys elsewhere. How do we make the control plane regional-failure tolerant?"),
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    args = parser.parse_args()

    client = httpx.Client(base_url=args.base_url, timeout=120.0)

    print(f"Seeding {len(SEED_PROMPTS)} calls into the gateway at {args.base_url} ...\n")
    ok, failed = 0, 0
    for i, (feature, prompt) in enumerate(SEED_PROMPTS, 1):
        try:
            resp = client.post(
                "/v1/chat",
                json={
                    "prompt": prompt,
                    "user_id": f"seed-user-{(i % 5) + 1}",  # spread across 5 users
                    "feature_tag": feature,
                    "max_tokens": 400,
                },
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", "60"))
                print(f"  rate limited, sleeping {wait}s ...")
                time.sleep(wait)
                resp = client.post(
                    "/v1/chat",
                    json={"prompt": prompt, "user_id": f"seed-user-{(i % 5) + 1}",
                          "feature_tag": feature, "max_tokens": 400},
                )
            resp.raise_for_status()
            meta = resp.json()["meta"]
            print(
                f"  [{i:2}/{len(SEED_PROMPTS)}] {feature:13} "
                f"model={meta.get('model') or 'CACHE':16} "
                f"ctx={len(meta.get('context_used', []))} "
                f"${meta.get('cost', 0):.4f}"
            )
            ok += 1
        except httpx.HTTPError as e:
            print(f"  [{i:2}] FAILED: {e}", file=sys.stderr)
            failed += 1

    stats = client.get("/v1/graph/stats").json()
    print(f"\nDone: {ok} seeded, {failed} failed.")
    print(f"Graph now holds: {stats}")
    print("\nNext: python scripts/demo_compare.py")


if __name__ == "__main__":
    main()
