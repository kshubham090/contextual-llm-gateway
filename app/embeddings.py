"""Bounded embedding microbatches with scoped memoization and optional local inference.

Only the optional acceleration extra imports a model runtime. Request handlers never
run local model loading or encoding on the event loop. Changing model/backend/dimension
creates a new embedding space; persisted vectors from distinct spaces must not mix.
"""
from __future__ import annotations

import asyncio
import hashlib
import math
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from numbers import Real
from typing import Protocol

import httpx

from .config import settings

VOYAGE_URL = "https://api.voyageai.com/v1/embeddings"


class EmbeddingError(RuntimeError):
    """Safe-to-classify embedding failure; upstream content is never the message."""


class EmbeddingOverloadedError(EmbeddingError):
    pass


class EmbeddingValidationError(EmbeddingError):
    pass


class EmbeddingBackendError(EmbeddingError):
    pass


class EmbeddingClosedError(EmbeddingError):
    pass


class EmbeddingBackend(Protocol):
    async def start(self) -> None: ...
    async def embed_batch(self, texts: list[str]) -> list[list[float]]: ...
    async def close(self) -> None: ...


class VoyageBackend:
    """One connection pool per application, with bounded connection and read waits."""

    def __init__(self, config, http_client: httpx.AsyncClient | None = None) -> None:
        self._model = config.embedding_model
        self._http = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                config.embedding_timeout_seconds,
                connect=min(5.0, config.embedding_timeout_seconds),
                pool=min(2.0, config.embedding_timeout_seconds),
            ),
            limits=httpx.Limits(
                max_connections=config.embedding_workers,
                max_keepalive_connections=config.embedding_workers,
            ),
            headers={"Authorization": f"Bearer {config.voyage_api_key}"},
        )

    async def start(self) -> None:
        return None

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        try:
            # Symmetric embeddings: the same input is used for query and storage.
            response = await self._http.post(
                VOYAGE_URL, json={"model": self._model, "input": texts},
            )
            response.raise_for_status()
            rows = response.json()["data"]
            if not isinstance(rows, list) or len(rows) != len(texts):
                raise ValueError("unexpected embedding count")
            indexed = {}
            for row in rows:
                index = row["index"]
                if type(index) is not int or index in indexed or index not in range(len(texts)):
                    raise ValueError("unexpected embedding index")
                indexed[index] = row["embedding"]
            return [indexed[index] for index in range(len(texts))]
        except httpx.HTTPError as exc:
            raise EmbeddingBackendError("Embedding service request failed") from exc
        except (KeyError, TypeError, ValueError) as exc:
            raise EmbeddingValidationError("Embedding service returned a malformed payload") from exc

    async def close(self) -> None:
        await self._http.aclose()


class LocalBackend:
    """SentenceTransformers on an explicit device, using a single inference thread.

    Native CPU/GPU kernels cannot be forcibly interrupted. If a request is cancelled,
    its inference slot stays occupied until its native work finishes, preventing an
    unbounded executor backlog. Shutdown stops accepting work without waiting forever
    for a native kernel. Install requirements-acceleration.txt to use this backend.
    """

    def __init__(self, config) -> None:
        self._config = config
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="gateway-embedding")
        self._slot = asyncio.Semaphore(1)
        self._model = None
        self.resolved_revision: str | None = None
        self._closed = False

    async def _run(self, fn, *args):
        if self._closed:
            raise EmbeddingClosedError("Embedding backend is closed")
        await self._slot.acquire()
        if self._closed:
            self._slot.release()
            raise EmbeddingClosedError("Embedding backend is closed")
        try:
            future = asyncio.get_running_loop().run_in_executor(self._executor, fn, *args)
        except BaseException:
            self._slot.release()
            raise

        def release(completed):
            self._slot.release()
            # Retrieve failures even after the waiter times out or is cancelled.
            if not completed.cancelled():
                completed.exception()

        future.add_done_callback(release)
        return await asyncio.shield(future)

    def _load(self) -> None:
        try:
            from sentence_transformers import SentenceTransformer
            if (self._config.local_embedding_device == "cpu"
                    and self._config.local_embedding_cpu_threads > 0):
                import torch

                torch.set_num_threads(self._config.local_embedding_cpu_threads)
        except ImportError as exc:
            raise EmbeddingBackendError(
                "Local embeddings require requirements-acceleration.txt"
            ) from exc
        self._model = SentenceTransformer(
            self._config.local_embedding_model,
            device=self._config.local_embedding_device,
            trust_remote_code=False,
            revision=self._config.local_embedding_revision,
        )
        if self._model.get_sentence_embedding_dimension() != self._config.embedding_dim:
            raise EmbeddingValidationError("Local model dimension does not match EMBEDDING_DIM")
        try:
            transformer_config = getattr(getattr(self._model[0], "auto_model", None), "config", None)
            revision = getattr(transformer_config, "_commit_hash", None)
            self.resolved_revision = revision if isinstance(revision, str) else None
        except (TypeError, AttributeError, IndexError, KeyError):
            # Custom/local models may have no Hugging Face commit metadata.
            self.resolved_revision = None

    async def start(self) -> None:
        try:
            await self._run(self._load)
        except EmbeddingError:
            raise
        except Exception as exc:
            raise EmbeddingBackendError("Local embedding model could not be loaded") from exc

    def _encode(self, texts: list[str]) -> list[list[float]]:
        return self._model.encode(
            texts,
            batch_size=self._config.embedding_batch_size,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        ).tolist()

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        try:
            return await self._run(self._encode, texts)
        except EmbeddingError:
            raise
        except Exception as exc:
            raise EmbeddingBackendError("Local embedding inference failed") from exc

    async def close(self) -> None:
        self._closed = True
        self._executor.shutdown(wait=False, cancel_futures=True)


@dataclass(slots=True)
class _Request:
    text: str
    key: tuple[str, str, str] | None
    future: asyncio.Future


class EmbeddingClient:
    """Batch concurrent callers; memoize only inside their explicit privacy scope.

    ``embed(text)`` remains supported for standalone callers. Multi-tenant applications
    must pass a namespace derived from the authenticated tenant and user. ``cache=False``
    bypasses both memoization and singleflight for requests that opt out of retention.
    Cache keys retain hashes, not raw prompts. Both queue and LRU are globally bounded.
    """

    def __init__(self, *, config=None, backend: EmbeddingBackend | None = None) -> None:
        self._config = config or settings
        self._backend = backend
        self._queue: asyncio.Queue[_Request] = asyncio.Queue(self._config.embedding_queue_size)
        self._cache: OrderedDict[tuple[str, str, str], tuple[float, tuple[float, ...]]] = OrderedDict()
        self._inflight: dict[tuple[str, str, str], asyncio.Future] = {}
        self._workers: list[asyncio.Task] = []
        self._start_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._started = False
        self._startup: asyncio.Task | None = None
        self._closed = False
        self._cache_hits = 0
        self._batches = 0
        model = (self._config.local_embedding_model if self._config.embedding_backend == "local"
                 else self._config.embedding_model)
        self.space_id = f"{self._config.embedding_backend}:{model}:{self._config.embedding_dim}:v1"
        if self._config.embedding_backend == "local" and self._config.local_embedding_revision:
            self.space_id += f":revision={self._config.local_embedding_revision}"

    async def start(self) -> None:
        async with self._start_lock:
            if self._closed:
                raise EmbeddingClosedError("Embedding client is closed")
            if self._started:
                return
            if self._backend is None:
                self._backend = (LocalBackend(self._config) if self._config.embedding_backend == "local"
                                 else VoyageBackend(self._config))
            try:
                self._startup = asyncio.create_task(self._backend.start())
                await asyncio.wait_for(self._startup, self._config.embedding_timeout_seconds)
            except BaseException:
                await self._backend.close()
                self._closed = True
                raise
            self._workers = [
                asyncio.create_task(self._worker(), name=f"embedding-batch-{index}")
                for index in range(self._config.embedding_workers)
            ]
            self._started = True

    async def embed(self, text: str, namespace: str = "default", *, cache: bool = True) -> list[float]:
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Embedding input must be nonempty text")
        if not isinstance(namespace, str) or not namespace:
            raise ValueError("Embedding namespace must be a nonempty string")
        await self.start()
        key = None
        if cache:
            key = (self.space_id, hashlib.sha256(namespace.encode()).hexdigest(),
                   hashlib.sha256(text.encode()).hexdigest())
            cached = self._cache.get(key)
            if cached is not None:
                expires, vector = cached
                if expires > time.monotonic():
                    self._cache_hits += 1
                    self._cache.move_to_end(key)
                    return list(vector)
                del self._cache[key]
            existing = self._inflight.get(key)
            if existing is not None:
                return list(await asyncio.shield(existing))
        future = asyncio.get_running_loop().create_future()
        # A caller may disconnect before a batch fails. Retrieve that exception too.
        future.add_done_callback(lambda f: None if f.cancelled() else f.exception())
        try:
            self._queue.put_nowait(_Request(text, key, future))
        except asyncio.QueueFull as exc:
            raise EmbeddingOverloadedError("Embedding queue is full; retry later") from exc
        if key is not None:
            self._inflight[key] = future
        return list(await asyncio.shield(future))

    def _validate(self, vectors, count: int) -> list[tuple[float, ...]]:
        if not isinstance(vectors, (list, tuple)) or len(vectors) != count:
            raise EmbeddingValidationError("Embedding result count does not match batch size")
        result = []
        for vector in vectors:
            if not isinstance(vector, (list, tuple)) or len(vector) != self._config.embedding_dim:
                raise EmbeddingValidationError("Embedding dimension does not match EMBEDDING_DIM")
            if any(isinstance(value, bool) or not isinstance(value, Real)
                   or not math.isfinite(value) for value in vector):
                raise EmbeddingValidationError("Embedding values must be finite numbers")
            if not any(value != 0 for value in vector):
                raise EmbeddingValidationError("Embedding vector must have nonzero norm")
            result.append(tuple(float(value) for value in vector))
        return result

    def _finish(self, request: _Request, *, vector=None, error: BaseException | None = None) -> None:
        if request.key is not None:
            self._inflight.pop(request.key, None)
        if not request.future.done():
            if error is not None:
                request.future.set_exception(error)
            else:
                if request.key is not None and self._config.embedding_cache_size > 0:
                    self._cache[request.key] = (
                        time.monotonic() + self._config.embedding_cache_ttl_seconds, vector,
                    )
                    self._cache.move_to_end(request.key)
                    while len(self._cache) > self._config.embedding_cache_size:
                        self._cache.popitem(last=False)
                request.future.set_result(vector)
        self._queue.task_done()

    async def _embed_requests(self, batch: list[_Request]) -> list[tuple[float, ...]]:
        # Bound aggregate payload size as well as item count. A single large input
        # remains subject to the selected model's own context/truncation rules.
        chunks: list[list[str]] = []
        texts: list[str] = []
        size = 0
        for request in batch:
            text_size = len(request.text.encode("utf-8"))
            if texts and size + text_size > self._config.embedding_batch_max_bytes:
                chunks.append(texts)
                texts, size = [], 0
            texts.append(request.text)
            size += text_size
        if texts:
            chunks.append(texts)
        vectors = []
        for texts in chunks:
            self._batches += 1
            vectors.extend(self._validate(await self._backend.embed_batch(texts), len(texts)))
        return vectors

    async def _worker(self) -> None:
        while True:
            batch = []
            try:
                batch.append(await self._queue.get())
                deadline = asyncio.get_running_loop().time() + self._config.embedding_batch_wait_ms / 1000
                while len(batch) < self._config.embedding_batch_size:
                    try:
                        batch.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        remaining = deadline - asyncio.get_running_loop().time()
                        if remaining <= 0:
                            break
                        try:
                            async with asyncio.timeout(remaining):
                                batch.append(await self._queue.get())
                        except TimeoutError:
                            break
                vectors = await asyncio.wait_for(
                    self._embed_requests(batch), timeout=self._config.embedding_timeout_seconds,
                )
            except asyncio.CancelledError:
                for request in batch:
                    self._finish(request, error=EmbeddingClosedError("Embedding client stopped"))
                raise
            except Exception as exc:
                error = exc if isinstance(exc, EmbeddingError) else EmbeddingBackendError(
                    "Embedding batch failed or timed out"
                )
                if error is not exc:
                    error.__cause__ = exc
                for request in batch:
                    self._finish(request, error=error)
            else:
                for request, vector in zip(batch, vectors, strict=True):
                    self._finish(request, vector=vector)

    def health(self) -> dict:
        return {
            "backend": self._config.embedding_backend,
            "space_id": self.space_id,
            "started": self._started,
            "closed": self._closed,
            "queue_depth": self._queue.qsize(),
            "cache_entries": len(self._cache),
            "cache_hits": self._cache_hits,
            "batches": self._batches,
            "batch_workers": self._config.embedding_workers,
        }

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            if self._startup is not None and not self._startup.done():
                self._startup.cancel()
            # Synchronize with model initialization before closing its resources.
            async with self._start_lock:
                if self._started:
                    try:
                        await asyncio.wait_for(
                            self._queue.join(), self._config.embedding_shutdown_timeout_seconds,
                        )
                    except TimeoutError:
                        pass
                    for worker in self._workers:
                        worker.cancel()
                    await asyncio.gather(*self._workers, return_exceptions=True)
                    while not self._queue.empty():
                        self._finish(self._queue.get_nowait(), error=EmbeddingClosedError(
                            "Embedding client stopped before queued work completed"
                        ))
                if self._backend is not None:
                    await self._backend.close()
                self._cache.clear()
                self._inflight.clear()
