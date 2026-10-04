"""Seed a synthetic scenario into a dedicated user/feature memory scope.

Set GATEWAY_API_KEY, then run demo_compare.py with the same run identifier.
Reusing a run identifier adds memory: use a fresh one for independent trials.
"""
import argparse
import os

import httpx
from evaluate import load_fixture, seed_scenario, selected_scenarios


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--scenario", default="orbit-incident")
    parser.add_argument("--run-id", default="demo")
    parser.add_argument("--graph-wait", type=float, default=60)
    args = parser.parse_args()
    token = os.environ.get("GATEWAY_API_KEY")
    if not token:
        parser.error("Set GATEWAY_API_KEY to an authorized gateway bearer token")
    scenario = selected_scenarios(load_fixture(), [args.scenario])[0]
    with httpx.Client(base_url=args.base_url, headers={"Authorization": f"Bearer {token}"},
                      timeout=120) as client:
        calls = seed_scenario(client, scenario, args.run_id, args.graph_wait)
    print(f"Indexed {len(calls)} memories for {scenario['title']} (run {args.run_id}).")
    print(f"Next: python scripts/demo_compare.py --scenario {args.scenario} --run-id {args.run_id}")


if __name__ == "__main__":
    main()
