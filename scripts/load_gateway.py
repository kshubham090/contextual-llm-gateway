"""Bounded closed-loop HTTP load test with explicit errors, latency and cost coverage.

Uses no automatic retries. Live runs incur provider costs. This is a controlled
concurrency experiment, not an open-loop arrival-rate or capacity certification.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import platform
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.compare_memory import percentile  # noqa: E402


async def run_profile(client, payload, *, concurrency, duration, max_requests=100000):
    started = time.perf_counter()
    deadline = started + duration
    samples, statuses, models = [], Counter(), Counter()
    next_id = 0

    async def worker():
        nonlocal next_id
        while time.perf_counter() < deadline and next_id < max_requests:
            index = next_id
            next_id += 1
            start = time.perf_counter()
            sample = {"sequence": index, "ok": False, "cost": None}
            try:
                response = await client.post("/v1/chat", json=payload)
                statuses[str(response.status_code)] += 1
                if response.is_success:
                    result = response.json()
                    if (
                        not isinstance(result, dict)
                        or not isinstance(result.get("response"), str)
                        or not isinstance(result.get("meta"), dict)
                    ):
                        raise ValueError("Invalid completion response")
                    meta = result["meta"]
                    cost = meta.get("cost")
                    if cost is not None and (
                        type(cost) not in (int, float) or not math.isfinite(cost) or cost < 0
                    ):
                        raise ValueError("Invalid cost metadata")
                    if any(
                        type(meta.get(k, 0)) is not int or meta.get(k, 0) < 0
                        for k in ("tokens_in", "tokens_out")
                    ):
                        raise ValueError("Invalid token metadata")
                    if not isinstance(meta.get("context_used", []), list):
                        raise ValueError("Invalid context metadata")
                    sample.update(
                        ok=True,
                        cost=meta.get("cost"),
                        tokens=meta.get("tokens_in", 0) + meta.get("tokens_out", 0),
                        cache_hit=bool(meta.get("cache_hit")),
                        context_count=len(meta.get("context_used", [])),
                    )
                    models[f"{meta.get('provider')}/{meta.get('model')}"] += 1
                else:
                    sample["error"] = f"HTTP_{response.status_code}"
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                sample["error"] = type(exc).__name__
            sample["latency_ms"] = (time.perf_counter() - start) * 1000
            samples.append(sample)

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    elapsed = time.perf_counter() - started
    successes = [sample for sample in samples if sample["ok"]]
    successful_latencies = [sample["latency_ms"] for sample in successes]
    all_latencies = [sample["latency_ms"] for sample in samples]
    return {
        "concurrency": concurrency,
        "scheduled_duration_seconds": duration,
        "wall_seconds": elapsed,
        "attempts": len(samples),
        "completed": len(successes),
        "errors": len(samples) - len(successes),
        "request_cap_reached": len(samples) >= max_requests,
        "http_statuses": dict(statuses),
        "error_types": dict(Counter(s.get("error") for s in samples if not s["ok"])),
        "successful_requests_per_second": len(successes) / elapsed,
        "attempts_per_second": len(samples) / elapsed,
        "success_latency_ms": {
            label: percentile(successful_latencies, p)
            for label, p in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99))
        },
        "all_attempt_latency_ms": {
            label: percentile(all_latencies, p) for label, p in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99))
        },
        "reported_generation_cost_usd": sum(s["cost"] for s in successes if s["cost"] is not None),
        "unpriced_successes": sum(s["cost"] is None for s in successes),
        "tokens": sum(s["tokens"] for s in successes),
        "cache_hits": sum(s["cache_hit"] for s in successes),
        "mean_context_count": sum(s["context_count"] for s in successes) / len(successes)
        if successes
        else None,
        "models": dict(models),
        "samples": samples,
    }


async def run(args):
    token = os.environ.get("GATEWAY_API_KEY")
    if not token:
        raise ValueError("Set GATEWAY_API_KEY")
    payload = {
        "prompt": args.prompt,
        "user_id": args.user,
        "feature_tag": args.feature,
        "retrieval_mode": args.mode,
        "max_tokens": args.max_tokens,
        "store": False,
        "use_cache": False,
    }
    report = {
        "schema_version": 1,
        "label": args.label,
        "started_at": datetime.now(UTC).isoformat(),
        "load_generator": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "httpx": httpx.__version__,
        },
        "workload": payload,
        "warmup_requests": args.warmup,
        "profiles": [],
        "limitations": [
            "Closed-loop workers wait for each response; no fixed external arrival rate.",
            "Latency includes HTTP round trip and server work, excludes initial client construction.",
            "Cost excludes embeddings, infrastructure and provider work in failed or cancelled requests.",
            "store:false retains accounting only. Cache reuse is disabled.",
            "A synthetic generation backend measures plumbing, not LLM quality or real inference capacity.",
        ],
    }
    async with httpx.AsyncClient(
        base_url=args.url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=args.timeout,
        follow_redirects=False,
        limits=httpx.Limits(
            max_connections=max(args.concurrency), max_keepalive_connections=max(args.concurrency)
        ),
    ) as client:
        ready = await client.get("/health/ready")
        ready.raise_for_status()
        report["warmup_generation_cost_usd"] = 0.0
        report["unpriced_warmup_requests"] = 0
        for _ in range(args.warmup):
            response = await client.post("/v1/chat", json=payload)
            response.raise_for_status()
            cost = response.json()["meta"].get("cost")
            report["warmup_generation_cost_usd"] += cost or 0
            report["unpriced_warmup_requests"] += cost is None
        for concurrency in args.concurrency:
            result = await run_profile(
                client,
                payload,
                concurrency=concurrency,
                duration=args.duration,
                max_requests=args.max_requests,
            )
            report["profiles"].append(result)
            print(
                f"concurrency={concurrency}: {result['successful_requests_per_second']:.2f} completed/s; "
                f"errors={result['errors']}; p95={result['success_latency_ms']['p95']}",
                flush=True,
            )
    report["finished_at"] = datetime.now(UTC).isoformat()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return int(any(p["errors"] or not p["completed"] for p in report["profiles"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--user", required=True)
    parser.add_argument("--feature", required=True)
    parser.add_argument("--mode", choices=("none", "vector", "graph"), default="graph")
    parser.add_argument(
        "--prompt", default="The connector still fails after migration. What should I try next?"
    )
    parser.add_argument("--max-tokens", type=int, default=128)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 8, 32, 64])
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--max-requests", type=int, default=100000)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument(
        "--label", required=True, help="Describe server hardware, generation and embedding backend"
    )
    parser.add_argument("--output", type=Path, default=Path("artifacts/gateway-load.json"))
    args = parser.parse_args()
    if not (
        0 < args.duration <= 3600
        and args.timeout > 0
        and 1 <= args.max_requests <= 100000
        and 0 <= args.warmup <= 100
        and 1 <= args.max_tokens <= 8192
        and all(1 <= c <= 1024 for c in args.concurrency)
    ):
        parser.error("Invalid duration, concurrency, request cap, token budget, timeout or warmup count")
    try:
        return asyncio.run(run(args))
    except (ValueError, OSError, httpx.HTTPError) as exc:
        print(f"Load test failed: {type(exc).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
