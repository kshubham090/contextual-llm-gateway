import asyncio

import httpx
import pytest

from scripts.compare_memory import evaluate, summarize
from scripts.evaluate import load_fixture
from scripts.load_gateway import run_profile


async def test_three_way_comparison_uses_curated_seeds_private_reads_and_blind_review():
    calls, revision = [], 0

    def transport(request):
        nonlocal revision
        import json

        body = json.loads(request.content) if request.content else {}
        calls.append((request.method, request.url.path, body))
        if request.url.path == "/v1/memories" and request.method == "GET":
            return httpx.Response(200, json={"items": [], "scope_revision": 0})
        if request.url.path == "/v1/memories":
            revision += 1
            return httpx.Response(
                200, json={"memory": {"id": f"seed-{revision}"}, "scope_revision": revision}
            )
        if request.url.path == "/v1/graph/stats":
            return httpx.Response(200, json={"calls": 4})
        return httpx.Response(
            200, json={"response": "No fact invented.", "meta": {"context_used": [], "cost": None}}
        )

    scenario = load_fixture()["scenarios"][0]
    async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(transport)) as client:
        report, blind = await evaluate(client, [scenario], trials=3)
    assert not report["errors"]
    assert len(report["results"]) == 18
    assert report["summary"]["complete_triplets"] == 6
    assert all("mode" not in row for row in blind)
    chats = [body for _, path, body in calls if path == "/v1/chat"]
    assert all(body["store"] is False and body["use_cache"] is False for body in chats)
    orders = [row["order"] for row in report["results"] if row["question"] == scenario["questions"][0]["id"]]
    assert len({tuple(order) for order in orders}) == 3
    assert all(control["passed"] for control in report["isolation_controls"])


def test_summary_never_pairs_incomplete_answers_or_invents_zero_pricing():
    row = {
        "trial": 0,
        "scenario": "a",
        "question": "b",
        "mode": "graph",
        "http_ms": 30,
        "score": {"expected_fact_recall": 1, "forbidden_fact_matches": []},
        "meta": {"cost": None},
    }
    result = summarize([row])
    assert result["complete_triplets"] == 0
    assert result["paired_graph_minus_vector_coverage"] is None
    assert result["graph"]["unpriced_answers"] == 1
    assert result["none"]["p95_http_ms"] is None


def test_model_changes_excluded_from_retrieval_effect():
    rows = [
        {
            "trial": 0,
            "scenario": "a",
            "question": "b",
            "mode": mode,
            "http_ms": 30,
            "score": {"expected_fact_recall": 1, "forbidden_fact_matches": []},
            "meta": {"cost": None, "provider": "same", "model": model},
        }
        for mode, model in (("none", "small"), ("vector", "small"), ("graph", "large"))
    ]
    result = summarize(rows)
    assert result["complete_triplets"] == 1
    assert result["model_matched_triplets"] == 0
    assert result["confounded_or_missing_model_triplets"] == 1
    assert result["paired_graph_minus_vector_coverage"] is None


@pytest.mark.parametrize("body", [[], None, {"response": "a", "meta": {"cost": "free"}}])
async def test_malformed_success_counted_as_error_without_losing_load_report(body):
    async with httpx.AsyncClient(
        base_url="http://test", transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body))
    ) as client:
        report = await run_profile(client, {}, concurrency=2, duration=1, max_requests=4)
    assert report["attempts"] == 4
    assert report["errors"] == 4
    assert report["completed"] == 0


async def test_loadtest_bounds_concurrency_and_counts_errors_without_retrying():
    active = peak = total = 0

    async def transport(request):
        nonlocal active, peak, total
        total += 1
        number = total
        active += 1
        peak = max(active, peak)
        await asyncio.sleep(0.001)
        active -= 1
        if number % 3 == 0:
            return httpx.Response(503, json={"detail": "overload"})
        return httpx.Response(
            200,
            json={
                "response": "answer",
                "meta": {
                    "cost": None,
                    "tokens_in": 2,
                    "tokens_out": 3,
                    "provider": "test",
                    "model": "synthetic",
                    "context_used": ["a"],
                },
            },
        )

    async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(transport)) as client:
        report = await run_profile(client, {}, concurrency=4, duration=1, max_requests=20)
    assert peak <= 4
    assert report["attempts"] == total == 20
    assert report["errors"] == 6
    assert report["completed"] == report["unpriced_successes"] == 14
    assert report["http_statuses"] == {"200": 14, "503": 6}
    assert report["request_cap_reached"]
    assert report["success_latency_ms"]["p95"] > 0


@pytest.mark.parametrize("status", [429, 502])
async def test_failed_requests_are_not_successful_capacity(status):
    async with httpx.AsyncClient(
        base_url="http://test", transport=httpx.MockTransport(lambda _: httpx.Response(status))
    ) as client:
        report = await run_profile(client, {}, concurrency=1, duration=1, max_requests=3)
    assert report["completed"] == 0
    assert report["success_latency_ms"]["p95"] is None
    assert report["successful_requests_per_second"] == 0
