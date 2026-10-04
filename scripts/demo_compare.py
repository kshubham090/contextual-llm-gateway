"""Compare a held-out prompt with graph memory disabled and enabled.

Seeds and questions use the same tenant, user, and feature. Neither held-out
answer enters future memory. Provider calls incur charges; inspect provenance.
"""
import argparse
import json
import os
from pathlib import Path

import httpx
from evaluate import (
    SCORE_LIMITATION,
    chat,
    load_fixture,
    resolve_scope,
    score_answer,
    selected_scenarios,
    wait_for_graph,
    write_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--scenario", default="orbit-incident")
    parser.add_argument("--run-id", default="demo")
    parser.add_argument("--prompt", help="Override question; disables lexical scoring")
    parser.add_argument("--output", type=Path, default=Path("artifacts/demo-comparison.json"))
    args = parser.parse_args()
    token = os.environ.get("GATEWAY_API_KEY")
    if not token:
        parser.error("Set GATEWAY_API_KEY to an authorized gateway bearer token")
    scenario = selected_scenarios(load_fixture(), [args.scenario])[0]
    scope = resolve_scope(scenario, args.run_id)
    question = scenario["questions"][0]
    prompt = args.prompt or question["prompt"]
    report = {"scenario_id": scenario["id"], "scope": scope, "prompt": prompt,
              "scoring_limitation": SCORE_LIMITATION, "answers": []}
    with httpx.Client(base_url=args.base_url, headers={"Authorization": f"Bearer {token}"},
                      timeout=120) as client:
        wait_for_graph(client, scope, len(scenario["seeds"]), timeout=60)
        for use_graph in (False, True):
            result = chat(client, prompt, scope, graph=use_graph, store=False)
            answer = {"mode": "graph_on" if use_graph else "graph_off", **result}
            if not args.prompt:
                answer["score"] = score_answer(result["response"], question)
            report["answers"].append(answer)
            print(f"\n{answer['mode'].upper()}\n{result['response']}\n")
            print(json.dumps(result["meta"], indent=2))
    write_report(report, args.output)
    print(f"\nSaved comparison: {args.output.resolve()}\n{SCORE_LIMITATION}")


if __name__ == "__main__":
    main()
