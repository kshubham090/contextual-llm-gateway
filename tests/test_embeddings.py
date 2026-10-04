"""Offline contracts for real batching, admission, namespace isolation and lifecycle."""
import asyncio
import sys
import threading
import time
from types import SimpleNamespace

import httpx
import pytest

from app.embeddings import (
    EmbeddingBackendError,
    EmbeddingClient,
    EmbeddingClosedError,
    EmbeddingOverloadedError,
    EmbeddingValidationError,
    LocalBackend,
    VoyageBackend,
)


def config(**overrides):
    return SimpleNamespace(**{
        "embedding_backend": "voyage", "embedding_model": "test-model", "embedding_dim": 3,
        "voyage_api_key": "unused", "embedding_batch_size": 16, "embedding_batch_wait_ms": 3,
        "embedding_workers": 1, "embedding_queue_size": 32, "embedding_cache_size": 8,
        "embedding_cache_ttl_seconds": 60, "embedding_timeout_seconds": 2,
        "embedding_shutdown_timeout_seconds": 0.03, "local_embedding_model": "test-local",
        "local_embedding_device": "cpu", "local_embedding_revision": None,
        "local_embedding_cpu_threads": 0, "embedding_batch_max_bytes": 96000, **overrides,
    })


class FakeBackend:
    def __init__(self, gate=None, result=None):
        self.gate = gate
        self.result = result
        self.batches = []
        self.started = asyncio.Event()
        self.closed = False
        self.active = 0
        self.max_active = 0

    async def start(self):
        return None

    async def embed_batch(self, texts):
        self.batches.append(texts)
        self.started.set()
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.gate is not None:
                await self.gate.wait()
            return self.result if self.result is not None else [[len(text), 1.0, 2.0] for text in texts]
        finally:
            self.active -= 1

    async def close(self):
        self.closed = True


@pytest.fixture
async def clients():
    created = []

    def create(backend=None, **overrides):
        client = EmbeddingClient(config=config(**overrides), backend=backend or FakeBackend())
        created.append(client)
        return client

    yield create
    await asyncio.gather(*(client.close() for client in created))


async def test_concurrent_calls_form_real_batch_and_preserve_order(clients):
    backend = FakeBackend()
    client = clients(backend)
    texts = ["a" * size for size in range(1, 9)]
    vectors = await asyncio.gather(*(client.embed(text) for text in texts))
    assert backend.batches == [texts]
    assert [vector[0] for vector in vectors] == list(range(1, 9))


async def test_singleflight_deduplicates_only_same_namespace(clients):
    backend = FakeBackend()
    client = clients(backend)
    vectors = await asyncio.gather(
        client.embed("same", namespace="tenant-a:user-1"),
        client.embed("same", namespace="tenant-a:user-1"),
        client.embed("same", namespace="tenant-b:user-1"),
    )
    assert vectors[0] == vectors[1] == vectors[2]
    assert backend.batches == [["same", "same"]]
    assert client.health()["cache_entries"] == 2


async def test_cache_returns_copies_and_has_bounded_lru(clients):
    backend = FakeBackend()
    client = clients(backend, embedding_cache_size=2)
    vector = await client.embed("one")
    vector[0] = -100
    assert (await client.embed("one"))[0] == 3
    await client.embed("two")
    await client.embed("three")
    await client.embed("one")
    assert backend.batches == [["one"], ["two"], ["three"], ["one"]]
    assert client.health()["cache_entries"] == 2


async def test_expired_cache_requires_fresh_embedding(clients):
    backend = FakeBackend()
    client = clients(backend, embedding_cache_ttl_seconds=0.01)
    await client.embed("repeat")
    await asyncio.sleep(0.02)
    await client.embed("repeat")
    assert len(backend.batches) == 2


async def test_no_retention_bypasses_cache_and_singleflight(clients):
    backend = FakeBackend()
    client = clients(backend)
    await asyncio.gather(client.embed("private", cache=False), client.embed("private", cache=False))
    assert backend.batches == [["private", "private"]]
    assert client.health()["cache_entries"] == 0
    assert client._inflight == {}


async def test_cancelling_one_waiter_does_not_cancel_shared_result(clients):
    gate = asyncio.Event()
    backend = FakeBackend(gate=gate)
    client = clients(backend)
    first = asyncio.create_task(client.embed("shared"))
    await backend.started.wait()
    second = asyncio.create_task(client.embed("shared"))
    await asyncio.sleep(0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    gate.set()
    assert await second == [6.0, 1.0, 2.0]
    assert len(backend.batches) == 1


async def test_queue_saturation_fails_immediately_and_recovers(clients):
    gate = asyncio.Event()
    backend = FakeBackend(gate=gate)
    client = clients(backend, embedding_queue_size=1, embedding_batch_size=1)
    running = asyncio.create_task(client.embed("running"))
    await backend.started.wait()
    queued = asyncio.create_task(client.embed("queued"))
    await asyncio.sleep(0)
    with pytest.raises(EmbeddingOverloadedError):
        await client.embed("excess")
    gate.set()
    await asyncio.gather(running, queued)
    assert await client.embed("recovered") == [9.0, 1.0, 2.0]


async def test_worker_count_bounds_backend_concurrency(clients):
    gate = asyncio.Event()
    backend = FakeBackend(gate=gate)
    client = clients(backend, embedding_workers=2, embedding_batch_size=1)
    tasks = [asyncio.create_task(client.embed(str(index))) for index in range(8)]
    await backend.started.wait()
    await asyncio.sleep(0.005)
    assert backend.max_active == 2
    gate.set()
    await asyncio.gather(*tasks)
    assert backend.max_active == 2


@pytest.mark.parametrize("result", [
    [[1, 2]], [[1, float("nan"), 2]], [[1, float("inf"), 2]], [[1, "2", 3]],
    [[1, True, 3]], [[0, 0, 0]], [], [[1, 2, 3], [4, 5, 6]],
])
async def test_invalid_vectors_fail_without_poisoning_cache(clients, result):
    backend = FakeBackend(result=result)
    client = clients(backend)
    with pytest.raises(EmbeddingValidationError):
        await client.embed("bad")
    assert client.health()["cache_entries"] == 0
    assert client._inflight == {}
    backend.result = None
    assert await client.embed("bad") == [3.0, 1.0, 2.0]


async def test_batch_timeout_is_classified_and_worker_recovers(clients):
    backend = FakeBackend(gate=asyncio.Event())
    client = clients(backend, embedding_timeout_seconds=0.01)
    with pytest.raises(EmbeddingBackendError):
        await client.embed("slow")
    backend.gate.set()
    assert await client.embed("fast") == [4.0, 1.0, 2.0]


async def test_close_drains_successful_requests(clients):
    backend = FakeBackend()
    client = clients(backend)
    await client.start()
    tasks = [asyncio.create_task(client.embed(str(index))) for index in range(4)]
    await asyncio.sleep(0)
    await client.close()
    assert len(await asyncio.gather(*tasks)) == 4
    assert backend.closed
    assert all(worker.done() for worker in client._workers)
    with pytest.raises(EmbeddingClosedError):
        await client.embed("later")


async def test_shutdown_deadline_fails_running_and_queued_requests(clients):
    backend = FakeBackend(gate=asyncio.Event())
    client = clients(backend, embedding_batch_size=1)
    tasks = [asyncio.create_task(client.embed(str(index))) for index in range(3)]
    await backend.started.wait()
    await asyncio.wait_for(client.close(), 0.2)
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(result, EmbeddingClosedError) for result in results)
    assert backend.closed
    assert client._inflight == {}
    await asyncio.wait_for(client._queue.join(), 0.1)


async def test_space_identity_changes_with_model_or_dimension(clients):
    base = clients()
    assert base.space_id != clients(embedding_model="another-model").space_id
    assert base.space_id != clients(embedding_dim=4).space_id
    assert base.space_id != clients(embedding_backend="local").space_id


async def test_voyage_payload_orders_explicit_indexes_and_batches():
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, json={"data": [
            {"index": 1, "embedding": [4, 5, 6]}, {"index": 0, "embedding": [1, 2, 3]},
        ]})

    backend = VoyageBackend(config(), httpx.AsyncClient(transport=httpx.MockTransport(handle)))
    try:
        assert await backend.embed_batch(["one", "two"]) == [[1, 2, 3], [4, 5, 6]]
        assert b'"input":["one","two"]' in requests[0].content
    finally:
        await backend.close()


@pytest.mark.parametrize("payload", [
    {"data": [{"index": 0, "embedding": [1, 2, 3]}, {"index": 0, "embedding": [4, 5, 6]}]},
    {"data": [{"index": True, "embedding": [1, 2, 3]}]}, {"unexpected": []},
])
async def test_voyage_rejects_malformed_indexed_results(payload):
    backend = VoyageBackend(config(), httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=payload),
    )))
    try:
        with pytest.raises(EmbeddingValidationError):
            await backend.embed_batch(["one", "two"])
    finally:
        await backend.close()


async def test_voyage_http_failure_does_not_expose_upstream_body():
    backend = VoyageBackend(config(), httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(429, text="secret upstream body"),
    )))
    try:
        with pytest.raises(EmbeddingBackendError) as exc:
            await backend.embed_batch(["one"])
        assert "secret" not in str(exc.value)
    finally:
        await backend.close()


async def test_local_loading_and_batched_encoding_run_off_event_loop(monkeypatch):
    main_thread = threading.get_ident()
    seen = {}

    class Array:
        def tolist(self):
            return [[1, 2, 3], [4, 5, 6]]

    class Model:
        def __init__(self, name, **kwargs):
            seen["load_thread"] = threading.get_ident()
            seen["device"] = kwargs["device"]
            seen["revision"] = kwargs["revision"]
            assert kwargs["trust_remote_code"] is False

        def get_sentence_embedding_dimension(self):
            return 3

        def encode(self, texts, **kwargs):
            seen["encode_thread"] = threading.get_ident()
            seen["texts"] = texts
            assert kwargs["normalize_embeddings"] is True
            time.sleep(0.02)
            return Array()

    monkeypatch.setitem(sys.modules, "sentence_transformers", SimpleNamespace(SentenceTransformer=Model))
    backend = LocalBackend(config(local_embedding_revision="tested-commit"))
    try:
        await backend.start()
        task = asyncio.create_task(backend.embed_batch(["a", "b"]))
        await asyncio.sleep(0.005)
        assert not task.done()  # The event loop progressed while the encoder was sleeping.
        assert await task == [[1, 2, 3], [4, 5, 6]]
        assert seen["load_thread"] != main_thread
        assert seen["encode_thread"] == seen["load_thread"]
        assert seen["device"] == "cpu"
        assert seen["revision"] == "tested-commit"
        assert seen["texts"] == ["a", "b"]
    finally:
        await backend.close()


async def test_shutdown_interrupts_model_initialization_within_deadline(clients):
    class SlowStartupBackend(FakeBackend):
        async def start(self):
            self.started.set()
            await asyncio.Event().wait()

    backend = SlowStartupBackend()
    client = clients(backend)
    request = asyncio.create_task(client.embed("waiting for load"))
    await backend.started.wait()
    await asyncio.wait_for(client.close(), 0.2)
    assert request.cancelled()
    assert backend.closed
    with pytest.raises(EmbeddingClosedError):
        await client.embed("after shutdown")


async def test_local_dimension_mismatch_fails_startup(monkeypatch):
    class Model:
        def __init__(self, *args, **kwargs):
            pass

        def get_sentence_embedding_dimension(self):
            return 384

    monkeypatch.setitem(sys.modules, "sentence_transformers", SimpleNamespace(SentenceTransformer=Model))
    client = EmbeddingClient(config=config(embedding_backend="local", embedding_dim=3))
    with pytest.raises(EmbeddingValidationError, match="dimension"):
        await client.start()
    assert client.health()["closed"] is True
    assert client._workers == []


async def test_cancelling_local_work_keeps_native_inference_slot_bounded(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    calls = []

    class Array:
        def tolist(self):
            return [[1, 2, 3]]

    class Model:
        def __init__(self, *args, **kwargs):
            pass

        def get_sentence_embedding_dimension(self):
            return 3

        def encode(self, texts, **kwargs):
            calls.append(texts)
            entered.set()
            release.wait(timeout=2)
            return Array()

    monkeypatch.setitem(sys.modules, "sentence_transformers", SimpleNamespace(SentenceTransformer=Model))
    backend = LocalBackend(config())
    try:
        await backend.start()
        first = asyncio.create_task(backend.embed_batch(["first"]))
        async with asyncio.timeout(1):
            while not entered.is_set():
                await asyncio.sleep(0.001)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(backend.embed_batch(["second"]))
        await asyncio.sleep(0.005)
        assert calls == [["first"]]
        assert backend._slot.locked()
        release.set()
        assert await second == [[1, 2, 3]]
        assert calls == [["first"], ["second"]]
    finally:
        release.set()
        await backend.close()


async def test_local_model_revision_changes_persistent_embedding_space(clients):
    first = clients(embedding_backend="local", local_embedding_revision="commit-one")
    same = clients(embedding_backend="local", local_embedding_revision="commit-one")
    second = clients(embedding_backend="local", local_embedding_revision="commit-two")
    unpinned = clients(embedding_backend="local", local_embedding_revision=None)
    assert first.space_id == same.space_id
    assert len({first.space_id, second.space_id, unpinned.space_id}) == 3


async def test_batches_split_by_utf8_byte_budget_preserving_input_order(clients):
    backend = FakeBackend()
    client = clients(backend, embedding_batch_max_bytes=8)
    texts = ["😀😀", "abcd", "efgh", "a single larger input"]
    vectors = await asyncio.gather(*(client.embed(text) for text in texts))
    assert backend.batches == [["😀😀"], ["abcd", "efgh"], ["a single larger input"]]
    assert [vector[0] for vector in vectors] == [len(text) for text in texts]
    assert client.health()["batches"] == 3


async def test_cpu_thread_override_runs_inside_model_executor(monkeypatch):
    seen = {}

    def set_threads(count):
        seen["threads"] = count
        seen["thread_id"] = threading.get_ident()

    class Model:
        def __init__(self, *args, **kwargs):
            assert seen["threads"] == 2

        def get_sentence_embedding_dimension(self):
            return 3

    monkeypatch.setitem(sys.modules, "torch", SimpleNamespace(set_num_threads=set_threads))
    monkeypatch.setitem(sys.modules, "sentence_transformers", SimpleNamespace(SentenceTransformer=Model))
    backend = LocalBackend(config(local_embedding_cpu_threads=2))
    try:
        await backend.start()
        assert seen["thread_id"] != threading.get_ident()
    finally:
        await backend.close()


async def test_later_chunk_failure_never_populates_partial_batch_cache(clients):
    class FailingChunkBackend(FakeBackend):
        async def embed_batch(self, texts):
            result = await super().embed_batch(texts)
            return [[float("nan"), 1, 2]] if len(self.batches) == 2 else result

    backend = FailingChunkBackend()
    client = clients(backend, embedding_batch_max_bytes=3)
    results = await asyncio.gather(client.embed("aaa"), client.embed("bbb"), return_exceptions=True)
    assert all(isinstance(result, EmbeddingValidationError) for result in results)
    assert client.health()["cache_entries"] == 0
    assert client._inflight == {}
    assert backend.batches == [["aaa"], ["bbb"]]
