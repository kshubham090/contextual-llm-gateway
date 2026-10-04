#!/usr/bin/env python3
"""Reproducible batch-size=1 versus microbatch embedding benchmark.

Default: synthetic CPU work, no credentials, downloads, GPU, or external service.
The synthetic fixed setup cost intentionally models per-invocation overhead. Its
speed ratio measures the queue/batching mechanism, NOT model inference performance.

For actual SentenceTransformers inference, install requirements-acceleration.txt
and pass --backend local --device cpu|cuda|mps. The first run may download a model;
loading and one representative batch warmup are timed separately and excluded from
measured latency. No hardware speedup is assumed.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import math
import platform
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import settings  # noqa: E402
from app.embeddings import EmbeddingClient, LocalBackend  # noqa: E402


class SyntheticCPUBackend:
    """Deterministic vectors plus an explicit, fixed CPU setup cost per batch."""

    def __init__(self, dimension: int, setup_rounds: int) -> None:
        self.dimension = dimension
        self.setup_rounds = setup_rounds
        self.calls = 0

    async def start(self) -> None:
        return None

    def _encode(self, texts: list[str]) -> list[list[float]]:
        hashlib.pbkdf2_hmac("sha256", b"synthetic-setup", b"gateway-benchmark", self.setup_rounds)
        vectors = []
        for text in texts:
            digest = hashlib.shake_256(text.encode()).digest(self.dimension * 2)
            vector = [(int.from_bytes(digest[index:index + 2], "big") - 32767.5) / 32768
                      for index in range(0, len(digest), 2)]
            norm = math.sqrt(sum(value * value for value in vector))
            vectors.append([value / norm for value in vector])
        return vectors

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        return await asyncio.to_thread(self._encode, texts)

    async def close(self) -> None:
        return None


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * quantile)]


async def measure(args, batch_size: int) -> dict:
    config = settings.model_copy(update={
        "embedding_backend": "local" if args.backend == "local" else "voyage",
        "embedding_dim": args.dimension,
        "embedding_batch_size": batch_size,
        "embedding_batch_wait_ms": args.batch_wait_ms if batch_size > 1 else 0,
        "embedding_workers": args.workers,
        "embedding_queue_size": max(args.concurrency, min(args.requests, batch_size)),
        "embedding_cache_size": 0,
        "embedding_timeout_seconds": 120,
        "local_embedding_model": args.model,
        "local_embedding_device": args.device,
        "local_embedding_revision": args.revision,
        "local_embedding_cpu_threads": args.cpu_threads,
    })
    backend = (LocalBackend(config) if args.backend == "local"
               else SyntheticCPUBackend(args.dimension, args.setup_rounds))
    client = EmbeddingClient(config=config, backend=backend)
    slots = asyncio.Semaphore(args.concurrency)
    latencies = []
    outputs = [None] * args.requests
    prompts = [
        f"Customer support case {index}: explain the tenant's deployment rollback policy and cite context."
        for index in range(args.requests)
    ]
    try:
        loading_started = time.perf_counter()
        await client.start()
        loading_seconds = time.perf_counter() - loading_started
        warmup_prompts = prompts[:batch_size]
        warmup_started = time.perf_counter()
        await asyncio.gather(*(
            client.embed(prompt, namespace="benchmark-warmup", cache=False) for prompt in warmup_prompts
        ))
        warmup_seconds = time.perf_counter() - warmup_started
        batches_before = client.health()["batches"]

        async def request(index: int, prompt: str) -> None:
            async with slots:
                start = time.perf_counter()
                vector = await client.embed(prompt, namespace="benchmark", cache=False)
                latencies.append((time.perf_counter() - start) * 1000)
                outputs[index] = vector

        started = time.perf_counter()
        await asyncio.gather(*(request(index, prompt) for index, prompt in enumerate(prompts)))
        elapsed = time.perf_counter() - started
        fingerprints = [hashlib.sha256(json.dumps(vector).encode()).hexdigest() for vector in outputs]
        native_threads = None
        if args.backend == "local":
            import torch

            native_threads = torch.get_num_threads()
        return {
            "batch_size": batch_size,
            "workers": args.workers,
            "native_cpu_threads": native_threads,
            "resolved_model_revision": getattr(backend, "resolved_revision", None),
            "batch_wait_ms": config.embedding_batch_wait_ms,
            "requests": args.requests,
            "concurrency": args.concurrency,
            "model_loading_seconds": round(loading_seconds, 6),
            "warmup_count": len(warmup_prompts),
            "warmup_backend_calls": batches_before,
            "warmup_seconds": round(warmup_seconds, 6),
            "backend_calls": client.health()["batches"] - batches_before,
            "elapsed_seconds": round(elapsed, 6),
            "requests_per_second": round(args.requests / elapsed, 2),
            "latency_ms": {
                "p50": round(percentile(latencies, 0.50), 3),
                "p95": round(percentile(latencies, 0.95), 3),
                "p99": round(percentile(latencies, 0.99), 3),
            },
            "vector_fingerprint": hashlib.sha256("".join(fingerprints).encode()).hexdigest(),
            "_vectors": outputs,
        }
    finally:
        await client.close()


async def main(args) -> dict:
    baseline = await measure(args, 1)
    batched = await measure(args, args.batch_size)
    synthetic = args.backend == "synthetic"
    reference_vectors = baseline.pop("_vectors")
    batched_vectors = batched.pop("_vectors")
    cosines, differences = [], []
    for reference, observed in zip(reference_vectors, batched_vectors, strict=True):
        dot = sum(a * b for a, b in zip(reference, observed, strict=True))
        norm = math.sqrt(sum(a * a for a in reference) * sum(b * b for b in observed))
        cosines.append(min(1.0, max(-1.0, dot / norm)))
        differences.extend(abs(a - b) for a, b in zip(reference, observed, strict=True))
    if min(cosines) < 0.999:
        raise RuntimeError("Batched and per-item embeddings disagree beyond cosine tolerance 0.999")
    if synthetic and baseline["vector_fingerprint"] != batched["vector_fingerprint"]:
        raise RuntimeError("Synthetic batch and per-item results differ")
    return {
        "benchmark": "synthetic_cpu_batch_overhead" if synthetic else "local_model_inference",
        "scope": (
            "Synthetic CPU vectors and artificial per-call setup cost. Measures batching overhead only; "
            "not evidence of LLM, GPU, network, retrieval-quality, or production performance."
            if synthetic else
            "Warm local embedding inference on the specified device. Model loading and a representative "
            "full batch warmup are timed separately and excluded from measured latency/throughput; "
            "one process, memoization disabled. Gateway workers share one local inference executor, "
            "whose Torch kernels use the reported native CPU threads or the chosen accelerator."
        ),
        "environment": {
            "python": platform.python_version(), "platform": platform.platform(),
            "machine": platform.machine(), "device": "cpu" if synthetic else args.device,
            "dimension": args.dimension,
            "model": None if synthetic else args.model,
            "model_revision": None if synthetic else args.revision,
            "resolved_model_revision": baseline["resolved_model_revision"],
            "torch": None if synthetic else importlib.metadata.version("torch"),
            "transformers": None if synthetic else importlib.metadata.version("transformers"),
            "sentence_transformers": (
                None if synthetic else importlib.metadata.version("sentence-transformers")
            ),
            "synthetic_setup_rounds": args.setup_rounds if synthetic else None,
        },
        "output_comparison": {
            "min_cosine_similarity": round(min(cosines), 9),
            "max_absolute_difference": round(max(differences), 9),
            "cosine_tolerance": 0.999,
            "within_tolerance": True,
        },
        "baseline": baseline,
        "batched": batched,
        "observed_throughput_ratio": round(
            batched["requests_per_second"] / baseline["requests_per_second"], 3,
        ),
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("synthetic", "local"), default="synthetic")
    parser.add_argument("--requests", type=int, default=256)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--cpu-threads", type=int, default=0, help="Local Torch CPU threads; 0 keeps default")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--batch-wait-ms", type=float, default=5)
    parser.add_argument("--dimension", type=int, default=384)
    parser.add_argument("--setup-rounds", type=int, default=2000)
    parser.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    parser.add_argument("--device", choices=("cpu", "cuda", "mps"), default="cpu")
    parser.add_argument("--revision", help="Pin a local model commit for reproducible inference")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    for name, upper in (("requests", 100000), ("concurrency", 10000), ("dimension", 2000),
                        ("batch_size", 128), ("setup_rounds", 1000000), ("workers", 32)):
        if not 1 <= getattr(args, name) <= upper:
            parser.error(f"--{name.replace('_', '-')} must be between 1 and {upper}")
    if not 0 <= args.cpu_threads <= 256:
        parser.error("--cpu-threads must be between 0 and 256")
    if not 0 <= args.batch_wait_ms <= 1000:
        parser.error("--batch-wait-ms must be between 0 and 1000")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    report = json.dumps(asyncio.run(main(arguments)), indent=2)
    print(report)
    if arguments.output is not None:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(report + "\n")
