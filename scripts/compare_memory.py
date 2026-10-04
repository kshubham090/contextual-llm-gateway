"""Three-way, repeated, counterbalanced memory evaluation through the public HTTP API.

Curated fixture ingestion avoids model-generated seed contamination. Live inference
costs money. Lexical scores are review aids, never a substitute for blinded judgment.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import statistics
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.evaluate import (  # noqa: E402
    DEFAULT_FIXTURES,
    SCORE_LIMITATION,
    load_fixture,
    resolve_scope,
    score_answer,
    selected_scenarios,
)

MODES = ("none", "vector", "graph")


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    point = (len(ordered) - 1) * fraction
    left = int(point)
    return ordered[left] + (ordered[min(left + 1, len(ordered) - 1)] - ordered[left]) * (point - left)


def summarize(rows):
    result = {}
    for mode in MODES:
        group = [row for row in rows if row["mode"] == mode]
        costs = [row["meta"].get("cost") for row in group]
        result[mode] = {
            "answers": len(group),
            "mean_fact_coverage": statistics.mean(row["score"]["expected_fact_recall"] for row in group)
            if group
            else None,
            "forbidden_phrase_matches": sum(len(row["score"]["forbidden_fact_matches"]) for row in group),
            "p50_http_ms": percentile([r["http_ms"] for r in group], 0.5),
            "p95_http_ms": percentile([r["http_ms"] for r in group], 0.95),
            "mean_context_count": statistics.mean(len(r["meta"].get("context_used", [])) for r in group)
            if group
            else None,
            "generation_cost_usd": sum(cost for cost in costs if cost is not None),
            "unpriced_answers": costs.count(None),
            "tokens": sum(r["meta"].get("tokens_in", 0) + r["meta"].get("tokens_out", 0) for r in group),
        }
    pairs = {}
    for row in rows:
        pairs.setdefault((row["trial"], row["scenario"], row["question"]), {})[row["mode"]] = row
    complete = [group for group in pairs.values() if all(mode in group for mode in MODES)]
    result["complete_triplets"] = len(complete)
    matched = []
    for group in complete:
        identities = {(r["meta"].get("provider"), r["meta"].get("model")) for r in group.values()}
        if len(identities) == 1 and all(next(iter(identities))):
            matched.append(group)
    result["model_matched_triplets"] = len(matched)
    result["confounded_or_missing_model_triplets"] = len(complete) - len(matched)
    result["paired_graph_minus_vector_coverage"] = (
        statistics.mean(
            group["graph"]["score"]["expected_fact_recall"] - group["vector"]["score"]["expected_fact_recall"]
            for group in matched
        )
        if matched
        else None
    )
    return result


async def request(client, method, path, **kwargs):
    result = await client.request(method, path, **kwargs)
    result.raise_for_status()
    data = result.json()
    if not isinstance(data, dict):
        raise ValueError("Gateway response must be a JSON object")
    if path == "/v1/chat" and (
        not isinstance(data.get("response"), str)
        or not isinstance(data.get("meta"), dict)
        or not isinstance(data["meta"].get("context_used"), list)
    ):
        raise ValueError("Gateway returned an invalid completion")
    return data


async def wait_graph(client, scope, count, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        data = await request(client, "GET", "/v1/graph/stats", params=scope)
        if data.get("calls", 0) >= count:
            return
        await asyncio.sleep(0.25)
    raise TimeoutError("Graph projection did not finish before the evaluation deadline")


async def evaluate(
    client,
    scenarios,
    *,
    trials=3,
    seed=42,
    graph_wait=60,
    max_tokens=500,
    run_id=None,
    fixture_hash="",
    label="unverified-provider",
):
    rng = random.Random(seed)
    report = {
        "schema_version": 1,
        "started_at": datetime.now(UTC).isoformat(),
        "run_id": run_id or uuid.uuid4().hex[:12],
        "fixture_sha256": fixture_hash,
        "label": label,
        "seed": seed,
        "trials": trials,
        "max_tokens": max_tokens,
        "score_limitation": SCORE_LIMITATION,
        "results": [],
        "errors": [],
        "isolation_controls": [],
        "seeds": [],
        "scopes": [],
        "cost_scope": ("Scored-answer generation only; isolation-call costs remain in control metadata. "
                       "Embeddings, failed calls and infrastructure excluded."),
    }
    for trial in range(trials):
        for scenario in scenarios:
            suffix = f"{report['run_id']}-{trial}"
            scope = resolve_scope(scenario, suffix)
            report["scopes"].append(scope)
            try:
                current = await request(client, "GET", "/v1/memories", params={**scope, "limit": 1})
                if current["items"] or current["scope_revision"] != 0:
                    raise ValueError("Evaluation scope already exists; choose a fresh run ID")
                revision = current["scope_revision"]
                for item in scenario["seeds"]:
                    result = await request(
                        client,
                        "POST",
                        "/v1/memories",
                        json={
                            **scope,
                            "prompt": item["prompt"],
                            "response": item["prompt"],
                            "expected_scope_revision": revision,
                        },
                    )
                    revision = result["scope_revision"]
                    report["seeds"].append(
                        {
                            "trial": trial,
                            "scenario": scenario["id"],
                            "seed_id": item["id"],
                            "memory_id": result["memory"]["id"],
                        }
                    )
                await wait_graph(client, scope, len(scenario["seeds"]), graph_wait)
            except (httpx.HTTPError, ValueError, KeyError, TimeoutError) as exc:
                report["errors"].append(
                    {
                        "stage": "seed",
                        "trial": trial,
                        "scenario": scenario["id"],
                        "error_type": type(exc).__name__,
                    }
                )
                continue
            for index, question in enumerate(scenario["questions"]):
                # Every mode occupies each position across three consecutive trials.
                base = list(MODES)
                random.Random(seed + index).shuffle(base)
                order = base[trial % 3 :] + base[: trial % 3]
                for mode in order:
                    try:
                        start = time.perf_counter()
                        result = await request(
                            client,
                            "POST",
                            "/v1/chat",
                            json={
                                **scope,
                                "prompt": question["prompt"],
                                "retrieval_mode": mode,
                                "use_cache": False,
                                "store": False,
                                "max_tokens": max_tokens,
                            },
                        )
                        elapsed = (time.perf_counter() - start) * 1000
                        report["results"].append(
                            {
                                "trial": trial,
                                "scenario": scenario["id"],
                                "question": question["id"],
                                "prompt": question["prompt"],
                                "mode": mode,
                                "order": order,
                                "http_ms": elapsed,
                                "response": result["response"],
                                "meta": result["meta"],
                                "score": score_answer(result["response"], question),
                            }
                        )
                    except (httpx.HTTPError, ValueError, KeyError) as exc:
                        report["errors"].append(
                            {
                                "stage": "answer",
                                "trial": trial,
                                "scenario": scenario["id"],
                                "question": question["id"],
                                "mode": mode,
                                "error_type": type(exc).__name__,
                            }
                        )
            for dimension in ("user_id", "feature_tag"):
                isolated = {**scope, dimension: f"empty-{uuid.uuid4().hex[:12]}"}
                try:
                    result = await request(
                        client,
                        "POST",
                        "/v1/chat",
                        json={
                            **isolated,
                            "prompt": scenario["questions"][0]["prompt"],
                            "retrieval_mode": "graph",
                            "store": False,
                            "use_cache": False,
                            "max_tokens": max_tokens,
                        },
                    )
                    report["isolation_controls"].append(
                        {
                            "trial": trial,
                            "scenario": scenario["id"],
                            "dimension": dimension,
                            "passed": result["meta"].get("context_used") == [],
                            "meta": result["meta"],
                        }
                    )
                except (httpx.HTTPError, ValueError, KeyError) as exc:
                    report["errors"].append({"stage": "isolation", "error_type": type(exc).__name__})
    report["summary"] = summarize(report["results"])
    report["finished_at"] = datetime.now(UTC).isoformat()
    blinded = list(report["results"])
    rng.shuffle(blinded)
    report["review_key"] = [
        {
            "review_id": f"R{i:04}",
            "trial": row["trial"],
            "scenario": row["scenario"],
            "question": row["question"],
            "mode": row["mode"],
        }
        for i, row in enumerate(blinded)
    ]
    review = [
        {
            "review_id": f"R{i:04}",
            "prompt": row["prompt"],
            "answer": row["response"],
            "factual_correctness_0_to_2": None,
            "irrelevant_or_stale_facts": None,
            "notes": "",
        }
        for i, row in enumerate(blinded)
    ]
    return report, review


async def main_async(args):
    fixture = load_fixture(args.fixtures)
    token = os.environ.get("GATEWAY_API_KEY")
    if not token:
        raise ValueError("Set GATEWAY_API_KEY; do not pass credentials in command arguments")
    async with httpx.AsyncClient(
        base_url=args.url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=args.timeout,
        follow_redirects=False,
    ) as client:
        report, review = await evaluate(
            client,
            selected_scenarios(fixture, args.scenario),
            trials=args.trials,
            seed=args.seed,
            graph_wait=args.graph_wait,
            max_tokens=args.max_tokens,
            label=args.label,
            fixture_hash=hashlib.sha256(args.fixtures.read_bytes()).hexdigest(),
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    review_path = args.output.with_name(args.output.stem + "-blind-review.json")
    review_path.write_text(json.dumps(review, indent=2) + "\n")
    print(json.dumps(report["summary"], indent=2))
    return 1 if report["errors"] or not all(c["passed"] for c in report["isolation_controls"]) else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURES)
    parser.add_argument("--scenario", action="append")
    parser.add_argument("--trials", type=int, choices=range(1, 31), default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--graph-wait", type=float, default=60)
    parser.add_argument("--max-tokens", type=int, choices=range(1, 8193), default=500)
    parser.add_argument(
        "--label", required=True, help="Inference/model/hardware description; label synthetic runs"
    )
    parser.add_argument("--output", type=Path, default=Path("artifacts/memory-comparison.json"))
    args = parser.parse_args()
    if args.timeout <= 0 or args.graph_wait <= 0:
        parser.error("Timeouts must be positive")
    try:
        return asyncio.run(main_async(args))
    except (ValueError, OSError, httpx.HTTPError) as exc:
        print(f"Evaluation failed: {type(exc).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
