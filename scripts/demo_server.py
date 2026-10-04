"""Loopback-only development harness with real stores and clearly synthetic generation.

This is not shipped in the production image. It exercises HTTP, memory, streaming,
client libraries and failure handling without paid generation. Never use its output
to claim language-model quality or hosted-provider throughput.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import math
import re
import sys
from contextlib import asynccontextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import settings  # noqa: E402
from app.db import Database  # noqa: E402
from app.embeddings import EmbeddingClient  # noqa: E402
from app.graph import MemoryGraph  # noqa: E402
from app.logs import setup_logging  # noqa: E402
from app.main import create_app  # noqa: E402
from app.outbox import GraphOutboxWorker  # noqa: E402
from app.pipeline import Pipeline  # noqa: E402
from app.providers import LLMProvider, ProviderChunk, ProviderOutput, Router  # noqa: E402
from app.rate_limit import RateLimiter  # noqa: E402


class SyntheticEmbeddings:
    """A deterministic near-constant fixture vector, not semantic inference."""

    async def start(self):
        pass

    async def close(self):
        pass

    async def embed_batch(self, texts):
        await asyncio.sleep(0.004)
        vectors = []
        for text in texts:
            vector = [0.0] * settings.embedding_dim
            vector[0] = 1.0
            digest = hashlib.sha256(text.encode()).digest()
            for index, byte in enumerate(digest):
                vector[(index + 1) % len(vector)] += byte / 2550
            norm = math.sqrt(sum(value * value for value in vector))
            vectors.append([value / norm for value in vector])
        return vectors


class SyntheticProvider(LLMProvider):
    name = "synthetic-demo"

    @staticmethod
    def answer(system):
        evidence = re.findall(r"Q: (.*?)\nA: (.*?)(?=\n\n|</related_past_calls>)", system or "", re.S)
        if evidence:
            facts = "\n\n".join(html.unescape(answer).strip() for _, answer in evidence[:4])
            return "[Synthetic fixture: echoes supplied memory; not a language-model answer.]\n\n" + facts
        return "[Synthetic fixture] No prior context was supplied. Add a verified memory or enable retrieval."

    async def complete(self, model, system, prompt, max_tokens, *, history=None):
        await asyncio.sleep(0.04)
        return ProviderOutput(self.answer(system), usage_available=False)

    async def stream(self, model, system, prompt, max_tokens, *, history=None):
        answer = self.answer(system)
        # These are fixture events for transport testing, never model tokens.
        for part in re.findall(r".{1,48}", answer, re.S):
            await asyncio.sleep(0.006)
            yield ProviderChunk(delta=part)
        yield ProviderChunk(output=ProviderOutput(answer, usage_available=False))


def build_demo_app(*, local_embeddings=False):
    @asynccontextmanager
    async def lifespan(app):
        if (
            settings.environment != "development"
            or not settings.auth_enabled
            or not settings.gateway_api_keys
        ):
            raise RuntimeError("Demo requires development mode, authentication and configured gateway keys")
        setup_logging()
        db, graph, limiter = Database(), MemoryGraph(), RateLimiter()
        embedder = EmbeddingClient(backend=None if local_embeddings else SyntheticEmbeddings())
        if not local_embeddings:
            embedder.space_id = f"synthetic-fixture:{settings.embedding_dim}:v1"
        router = Router(provider=SyntheticProvider())
        pipeline = Pipeline(db, graph, embedder, limiter, router)
        worker = GraphOutboxWorker(db, graph)
        for name, resource in {
            "db": db,
            "graph": graph,
            "limiter": limiter,
            "embedder": embedder,
            "router": router,
            "pipeline": pipeline,
            "outbox": worker,
        }.items():
            setattr(app.state, name, resource)
        app.state.ready = False
        try:
            await db.connect()
            await graph.connect()
            await limiter.connect()
            await embedder.start()
            worker.start()
            app.state.ready = True
            yield
        finally:
            app.state.ready = False
            await pipeline.close()
            await worker.stop()
            await asyncio.gather(*(resource.close() for resource in (router, embedder, limiter, graph, db)))

    return create_app(lifespan_handler=lifespan)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument(
        "--local-embeddings",
        action="store_true",
        help="Use real local embeddings configured in .env; generation remains synthetic",
    )
    args = parser.parse_args()
    if args.local_embeddings and settings.embedding_backend != "local":
        parser.error("Set EMBEDDING_BACKEND=local and matching dimensions/device for a dedicated database")
    settings.simple_model = settings.complex_model = "synthetic-demo"
    import uvicorn

    uvicorn.run(
        build_demo_app(local_embeddings=args.local_embeddings),
        host="127.0.0.1",
        port=args.port,
        access_log=False,
        timeout_graceful_shutdown=15,
    )


if __name__ == "__main__":
    main()
