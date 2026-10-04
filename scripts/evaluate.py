"""Validate synthetic fixtures or measure graph-off/on behavior on a running gateway.

The lexical scorer is a transparent quality proxy. It cannot judge correctness,
negation, contradiction, citations, or safety. Live runs incur provider charges.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import unicodedata
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_FIXTURES = Path(__file__).resolve().parents[1] / "examples" / "scenarios.json"
SCORE_LIMITATION = (
    "Lexical expected-fact coverage and forbidden-phrase matches are quality proxies, "
    "not expert correctness or safety judgments. Negated mentions can be false positives; "
    "paraphrases can be false negatives. Review answers and retrieved provenance manually."
)


def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKC", text).casefold()
    return " ".join(re.findall(r"\w+", text, flags=re.UNICODE))


def phrase_present(text: str, phrase: str) -> bool:
    return f" {normalize(phrase)} " in f" {normalize(text)} "


def score_answer(answer: str, question: dict[str, Any]) -> dict[str, Any]:
    expected = question["expected_facts"]
    forbidden = question["forbidden_facts"]
    found = [f["id"] for f in expected if any(phrase_present(answer, p) for p in f["any_of"])]
    hits = [f["id"] for f in forbidden if any(phrase_present(answer, p) for p in f["any_of"])]
    return {
        "expected_fact_recall": len(found) / len(expected),
        "expected_facts_matched": found,
        "expected_facts_missing": [f["id"] for f in expected if f["id"] not in found],
        "forbidden_fact_matches": hits,
        "forbidden_fact_match_rate": len(hits) / len(forbidden) if forbidden else 0.0,
    }


def validate_fixture(data: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if not isinstance(data, dict):
        return ["fixture must be a JSON object"]
    if data.get("schema_version") != 1 or data.get("synthetic") is not True:
        errors.append("schema_version must be 1 and synthetic must be true")
    scenarios = data.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        return errors + ["scenarios must be a nonempty list"]
    scenario_ids: set[str] = set()
    scopes: set[tuple[str, str]] = set()
    for index, s in enumerate(scenarios):
        prefix = f"scenario[{index}]"
        if not isinstance(s, dict):
            errors.append(f"{prefix}: expected an object")
            continue
        sid = s.get("id")
        if not isinstance(sid, str) or not re.fullmatch(r"[a-z0-9-]{1,48}", sid):
            errors.append(f"{prefix}: id must be a lowercase slug up to 48 characters")
        elif sid in scenario_ids:
            errors.append(f"{prefix}: duplicate scenario id {sid}")
        else:
            scenario_ids.add(sid)
        for key in ("title", "task", "why_graph"):
            if not isinstance(s.get(key), str) or not s[key].strip():
                errors.append(f"{prefix}: missing {key}")
        scope = s.get("scope", {})
        if not isinstance(scope, dict):
            scope = {}
        values = tuple(scope.get(k) for k in ("user_id", "feature_tag"))
        if not all(isinstance(v, str) and re.fullmatch(r"[a-z0-9-]{1,64}", v) for v in values):
            errors.append(f"{prefix}: scope requires user_id and feature_tag slugs")
        elif values in scopes:
            errors.append(f"{prefix}: scopes must be disjoint")
        else:
            scopes.add(values)
        seeds, questions = s.get("seeds"), s.get("questions")
        if not isinstance(seeds, list) or len(seeds) < 2:
            errors.append(f"{prefix}: at least two seed memories are required")
            seeds = []
        if not isinstance(questions, list) or not questions:
            errors.append(f"{prefix}: at least one held-out question is required")
            questions = []
        known_prompts: set[str] = set()
        for group, records in (("seeds", seeds), ("questions", questions)):
            record_ids: set[str] = set()
            for i, record in enumerate(records):
                loc = f"{prefix}.{group}[{i}]"
                if not isinstance(record, dict):
                    errors.append(f"{loc}: expected an object")
                    continue
                rid = record.get("id")
                if not isinstance(rid, str) or not rid or rid in record_ids:
                    errors.append(f"{loc}: id must be a unique nonempty string")
                else:
                    record_ids.add(rid)
                prompt = record.get("prompt")
                if not isinstance(prompt, str) or not prompt.strip():
                    errors.append(f"{loc}: prompt must be nonempty")
                elif normalize(prompt) in known_prompts:
                    errors.append(f"{loc}: duplicate prompt; held-out questions must differ from seeds")
                else:
                    known_prompts.add(normalize(prompt))
                if group == "questions":
                    fact_ids: set[str] = set()
                    for fact_type in ("expected_facts", "forbidden_facts"):
                        facts = record.get(fact_type)
                        if not isinstance(facts, list) or not facts:
                            errors.append(f"{loc}.{fact_type}: at least one fact is required")
                            continue
                        for fact in facts:
                            if not isinstance(fact, dict):
                                errors.append(f"{loc}.{fact_type}: facts must be objects")
                                continue
                            fid, aliases = fact.get("id"), fact.get("any_of")
                            if not isinstance(fid, str) or not fid or fid in fact_ids:
                                errors.append(f"{loc}: fact ids must be unique nonempty strings")
                            else:
                                fact_ids.add(fid)
                            if not isinstance(aliases, list) or not aliases or not all(
                                isinstance(a, str) and normalize(a) for a in aliases
                            ):
                                errors.append(f"{loc}.{fact_type}: aliases must be nonempty phrases")
                            elif fact_type == "expected_facts" and not any(
                                phrase_present(seed.get("prompt", ""), alias)
                                for seed in seeds
                                if isinstance(seed, dict) and isinstance(seed.get("prompt"), str)
                                for alias in aliases
                            ):
                                errors.append(f"{loc}: expected fact {fid} is unsupported by seed facts")
    return errors


def load_fixture(path: Path = DEFAULT_FIXTURES) -> dict[str, Any]:
    data = json.loads(path.read_text())
    errors = validate_fixture(data)
    if errors:
        raise ValueError("Invalid fixtures:\n" + "\n".join(errors))
    return data


def selected_scenarios(data: dict[str, Any], selected: list[str] | None) -> list[dict[str, Any]]:
    known = {s["id"] for s in data["scenarios"]}
    unknown = set(selected or []) - known
    if unknown:
        raise ValueError(f"Unknown scenario(s): {', '.join(sorted(unknown))}")
    return [s for s in data["scenarios"] if not selected or s["id"] in selected]


def resolve_scope(scenario: dict[str, Any], run_id: str) -> dict[str, str]:
    if not re.fullmatch(r"[a-zA-Z0-9-]{1,32}", run_id):
        raise ValueError("run_id must contain 1–32 letters, digits, or hyphens")
    return {**scenario["scope"], "user_id": f"{scenario['scope']['user_id']}-{run_id}"}


def chat(client: Any, prompt: str, scope: dict[str, str], *, graph: bool, store: bool,
         max_tokens: int = 500) -> dict[str, Any]:
    response = client.post("/v1/chat", json={
        "prompt": prompt, **scope, "use_graph": graph,
        "use_cache": False, "store": store, "max_tokens": max_tokens,
    })
    response.raise_for_status()
    data = response.json()
    if not isinstance(data.get("response"), str) or not isinstance(data.get("meta"), dict):
        raise ValueError("Gateway returned an invalid chat response")
    return data


def wait_for_graph(client: Any, scope: dict[str, str], expected_calls: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        response = client.get("/v1/graph/stats", params=scope,
                              timeout=max(0.01, min(5, deadline - time.monotonic())))
        response.raise_for_status()
        if response.json().get("calls", 0) >= expected_calls:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Graph indexing did not reach {expected_calls} calls for {scope['user_id']}")
        time.sleep(min(0.5, max(0, deadline - time.monotonic())))


def seed_scenario(client: Any, scenario: dict[str, Any], run_id: str,
                  graph_wait: float = 60.0) -> list[dict[str, Any]]:
    scope = resolve_scope(scenario, run_id)
    existing = client.get("/v1/graph/stats", params=scope)
    existing.raise_for_status()
    baseline = existing.json().get("calls", 0)
    calls = []
    for seed in scenario["seeds"]:
        result = chat(client, seed["prompt"], scope, graph=False, store=True, max_tokens=250)
        calls.append({"seed_id": seed["id"], "meta": result["meta"]})
    wait_for_graph(client, scope, baseline + len(calls), graph_wait)
    return calls


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {"scored_answers": len(results)}
    for mode in ("graph_off", "graph_on"):
        rows = [r for r in results if r["mode"] == mode]
        summary[mode] = {
            "answers": len(rows),
            "expected_fact_recall": sum(r["score"]["expected_fact_recall"] for r in rows) / len(rows)
            if rows else None,
            "forbidden_fact_matches": sum(len(r["score"]["forbidden_fact_matches"]) for r in rows),
            "answers_with_context": sum(bool(r["meta"].get("context_used")) for r in rows),
            "mean_latency_ms": sum(r["meta"].get("latency_ms", 0) for r in rows) / len(rows)
            if rows else None,
            "reported_generation_cost_usd": sum(r["meta"].get("cost") or 0 for r in rows),
            "unpriced_answers": sum(r["meta"].get("cost") is None for r in rows),
        }
    pairs: dict[tuple[str, str], dict[str, float]] = {}
    for result in results:
        key = (result["scenario_id"], result["question_id"])
        pairs.setdefault(key, {})[result["mode"]] = result["score"]["expected_fact_recall"]
    deltas = [m["graph_on"] - m["graph_off"] for m in pairs.values()
              if "graph_on" in m and "graph_off" in m]
    summary["paired_question_count"] = len(deltas)
    summary["recall_delta"] = sum(deltas) / len(deltas) if deltas else None
    return summary


def run_evaluation(client: Any, data: dict[str, Any], *, scenarios: list[dict[str, Any]],
                   run_id: str, seed: bool = True, graph_wait: float = 60,
                   max_tokens: int = 500) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": 1, "run_id": run_id, "started_at": datetime.now(UTC).isoformat(),
        "fixture_sha256": hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest(),
        "synthetic": True, "scoring_limitation": SCORE_LIMITATION,
        "parameters": {"seed": seed, "max_tokens": max_tokens, "cache": False,
                       "store_held_out": False, "order": "alternating by question"},
        "seed_calls": [], "results": [], "isolation_controls": [], "errors": [],
    }
    for scenario in scenarios:
        sid, scope = scenario["id"], resolve_scope(scenario, run_id)
        try:
            if seed:
                calls = seed_scenario(client, scenario, run_id, graph_wait)
                report["seed_calls"].append({"scenario_id": sid, "calls": calls})
            else:
                wait_for_graph(client, scope, len(scenario["seeds"]), graph_wait)
            for index, question in enumerate(scenario["questions"]):
                for use_graph in ((False, True) if index % 2 == 0 else (True, False)):
                    result = chat(client, question["prompt"], scope, graph=use_graph,
                                  store=False, max_tokens=max_tokens)
                    report["results"].append({
                        "scenario_id": sid, "question_id": question["id"], "scope": scope,
                        "mode": "graph_on" if use_graph else "graph_off",
                        "prompt": question["prompt"], "response": result["response"],
                        "meta": result["meta"], "score": score_answer(result["response"], question),
                    })
            for boundary in ("user_id", "feature_tag"):
                empty_scope = {**scope, boundary: scope[boundary] + "-unseeded"}
                result = chat(client, scenario["questions"][0]["prompt"], empty_scope,
                              graph=True, store=False, max_tokens=100)
                context = result["meta"].get("context_used", [])
                control = {"scenario_id": sid, "boundary": boundary, "scope": empty_scope,
                           "context_used": context, "passed": not context, "meta": result["meta"]}
                report["isolation_controls"].append(control)
                if context:
                    report["errors"].append({"scenario_id": sid, "error": f"{boundary} isolation failed"})
        except Exception as exc:
            report["errors"].append({"scenario_id": sid, "error_type": type(exc).__name__,
                                     "error": str(exc)})
    report["summary"] = summarize(report["results"])
    report["completed_at"] = datetime.now(UTC).isoformat()
    return report


def write_report(report: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_name(output.name + ".tmp")
    temp.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    temp.replace(output)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "run"))
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURES)
    parser.add_argument("--scenario", action="append", help="Repeat to select several packs; default all")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--run-id", help="Fresh identifier by default; reuse only with --skip-seed")
    parser.add_argument("--skip-seed", action="store_true")
    parser.add_argument("--graph-wait", type=float, default=60)
    parser.add_argument("--max-tokens", type=int, default=500)
    parser.add_argument("--output", type=Path, default=Path("artifacts/evaluation.json"))
    args = parser.parse_args()
    try:
        data = load_fixture(args.fixtures)
        scenarios = selected_scenarios(data, args.scenario)
        if args.command == "validate":
            print(json.dumps({"valid": True, "scenarios": len(scenarios),
                              "seed_memories": sum(len(s["seeds"]) for s in scenarios),
                              "held_out_questions": sum(len(s["questions"]) for s in scenarios)}))
            return 0
        if args.skip_seed and not args.run_id:
            raise ValueError("--skip-seed requires the --run-id used to seed memory")
        if args.graph_wait <= 0 or not 1 <= args.max_tokens <= 8192:
            raise ValueError("--graph-wait must be positive and --max-tokens must be 1–8192")
        token = os.environ.get("GATEWAY_API_KEY")
        if not token:
            raise ValueError("Set GATEWAY_API_KEY to an authorized gateway bearer token")
        import httpx
        with httpx.Client(base_url=args.base_url, headers={"Authorization": f"Bearer {token}"},
                          timeout=120) as client:
            report = run_evaluation(client, data, scenarios=scenarios,
                                    run_id=args.run_id or uuid.uuid4().hex[:12],
                                    seed=not args.skip_seed, graph_wait=args.graph_wait,
                                    max_tokens=args.max_tokens)
        write_report(report, args.output)
        print(json.dumps(report["summary"], indent=2))
        print(f"Report: {args.output.resolve()}\n{SCORE_LIMITATION}")
        if report["errors"]:
            print(json.dumps(report["errors"], indent=2), file=sys.stderr)
            return 1
        return 0
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
