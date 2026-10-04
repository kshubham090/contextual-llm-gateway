"""Evaluation server boundaries without model downloads or paid inference.

The controlled native worker simulates an uninterruptible kernel; actual language
model quality is evaluated separately through the normal gateway HTTP API.
"""
import asyncio
import threading
from concurrent.futures import Future

import httpx
import pytest

from scripts.local_eval_provider import (
    DEFAULT_MODEL,
    BusyError,
    Generation,
    LocalGenerator,
    arguments,
    create_app,
    run_connected,
)

REVISION = "7ae557604adf67be50417f59c2c2f167def9a775"
MESSAGES = [{"role": "user", "content": "What changed?"}]


class NativeFixture(LocalGenerator):
    def __init__(self, **overrides):
        super().__init__(model=DEFAULT_MODEL, revision=REVISION, device="cpu", **overrides)
        self.entered, self.release = threading.Event(), threading.Event()
        self.started_jobs = 0

    def _load(self):
        pass

    def _infer(self, messages, max_tokens, stop):
        self.started_jobs += 1
        self.entered.set()
        if not self.release.wait(2):
            raise RuntimeError("Test did not release controlled native worker")
        if stop.is_set():
            raise InterruptedError("cancelled after native kernel finished")
        return Generation("actual worker result", 17, 4, "stop")


async def entered(engine):
    assert await asyncio.to_thread(engine.entered.wait, 1)


@pytest.mark.parametrize("outcome", ["cancel", "timeout"])
async def test_cancel_or_timeout_does_not_admit_a_second_native_job(outcome):
    engine = NativeFixture(timeout_seconds=0.03 if outcome == "timeout" else 1)
    await engine.start()
    try:
        first = asyncio.create_task(engine.generate(MESSAGES, 12))
        await entered(engine)
        if outcome == "cancel":
            first.cancel()
        with pytest.raises(asyncio.CancelledError if outcome == "cancel" else TimeoutError):
            await first
        assert engine.busy and engine._stop.is_set()
        for _ in range(3):
            with pytest.raises(BusyError):
                await engine.generate(MESSAGES, 12)
        assert engine.started_jobs == 1
        engine.release.set()
        await asyncio.gather(asyncio.wrap_future(engine._native), return_exceptions=True)
        result = await engine.generate(MESSAGES, 12)
        assert result.text == "actual worker result" and engine.started_jobs == 2
    finally:
        engine.release.set()
        await engine.close()


async def test_shutdown_waits_for_native_exit_even_after_http_cancellation():
    engine = NativeFixture()
    await engine.start()
    first = asyncio.create_task(engine.generate(MESSAGES, 12))
    await entered(engine)
    first.cancel()
    await asyncio.gather(first, return_exceptions=True)
    closing = asyncio.create_task(engine.close())
    await asyncio.sleep(0)
    assert not closing.done()
    engine.release.set()
    await closing
    assert not engine.busy
    with pytest.raises(BusyError):
        await engine.generate(MESSAGES, 12)


async def test_old_cancel_must_not_signal_replacement_job_stop():
    class ControlledExecutor:
        def __init__(self):
            self.jobs = []

        def submit(self, function, messages, maximum, stop):
            future = Future()
            self.jobs.append((future, stop))
            return future

        def shutdown(self, **kwargs):
            pass

    engine = LocalGenerator(model=DEFAULT_MODEL, revision=REVISION, device="cpu")
    engine._executor.shutdown()
    engine._executor = executor = ControlledExecutor()
    engine._loaded = True
    first = asyncio.create_task(engine.generate(MESSAGES, 12))
    await asyncio.sleep(0)
    assert len(executor.jobs) == 1
    second = asyncio.create_task(engine.generate(MESSAGES, 12))
    # Native completion frees admission before the old asyncio awaiter receives
    # its result. The next request starts before the old cancellation unwinds.
    executor.jobs[0][0].set_result(Generation("first", 1, 1, "stop"))
    first.cancel()
    try:
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.gather(first, return_exceptions=True)
        assert len(executor.jobs) == 2
        assert not executor.jobs[1][1].is_set()
    finally:
        for future, _ in executor.jobs:
            if not future.done():
                future.set_result(Generation("second", 1, 1, "stop"))
        await asyncio.gather(first, second, return_exceptions=True)
        await engine.close()


async def test_disconnect_signals_native_stop_without_claiming_immediate_kernel_cancellation():
    engine = NativeFixture()
    await engine.start()
    disconnect = asyncio.Event()

    class Request:
        async def receive(self):
            await disconnect.wait()
            return {"type": "http.disconnect"}

    request = asyncio.create_task(run_connected(Request(), engine, MESSAGES, 12))
    try:
        await entered(engine)
        disconnect.set()
        with pytest.raises(Exception) as exc:
            await request
        assert exc.value.status_code == 499
        assert engine._stop.is_set() and engine.busy
    finally:
        engine.release.set()
        await engine.close()


async def test_http_completion_returns_worker_usage_and_explicit_runtime_provenance():
    engine = NativeFixture(max_output_tokens=256)
    engine.release.set()
    await engine.start()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(engine)),
                                    base_url="http://test") as client:
            response = await client.post("/v1/chat/completions", json={
                "model": DEFAULT_MODEL, "messages": MESSAGES, "max_tokens": 12, "stream": False,
            })
            assert response.status_code == 200
            data = response.json()
            assert data["choices"][0]["message"]["content"] == "actual worker result"
            assert data["usage"] == {"prompt_tokens": 17, "completion_tokens": 4, "total_tokens": 21}
            assert data["local_evaluation"] == {"revision": REVISION, "device": "cpu"}
            health = (await client.get("/health")).json()
            assert health["evaluation_only"] and not health["streaming"]
            assert health["sampling"]["do_sample"] is False
            assert health["limits"]["queue_size"] == 0
    finally:
        await engine.close()


@pytest.mark.parametrize("changes,status", [
    ({"stream": True}, 422), ({"model": "unserved-model"}, 404),
    ({"max_tokens": 10, "max_completion_tokens": 10}, 422),
    ({"max_tokens": 257}, 422), ({"temperature": 0}, 422),
    ({"messages": [{"role": "tool", "content": "unsupported"}]}, 422),
    ({"messages": [{"role": "user", "content": [{"type": "text", "text": "hello"}]}]}, 422),
    ({"messages": [{"role": "user", "content": "x" * 64001}]}, 422),
])
async def test_unsupported_input_is_rejected_without_submitting_native_work(changes, status):
    engine = NativeFixture(max_output_tokens=256)
    engine.release.set()
    await engine.start()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(engine)),
                                    base_url="http://test") as client:
            response = await client.post("/v1/chat/completions", json={
                "model": DEFAULT_MODEL, "messages": MESSAGES, **changes,
            })
        assert response.status_code == status and engine.started_jobs == 0
    finally:
        await engine.close()


async def test_raw_body_limit_applies_before_json_decoding():
    engine = NativeFixture()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(engine)),
                                    base_url="http://test") as client:
            response = await client.post("/v1/chat/completions", content=b"x" * 262145)
        assert response.status_code == 413 and engine.started_jobs == 0
    finally:
        await engine.close()


@pytest.mark.parametrize("args", [[], ["--revision", "main", "--device", "cpu"],
                                  ["--revision", REVISION],
                                  ["--revision", REVISION, "--device", "cpu", "--host", "0.0.0.0"],
                                  ["--revision", REVISION, "--device", "cpu", "--model", "https://bad.test"]])
def test_cli_requires_immutable_revision_explicit_device_and_loopback_only(args):
    with pytest.raises(SystemExit):
        arguments(args)


def test_valid_cli_preserves_exact_pinned_revision():
    parsed = arguments(["--revision", REVISION, "--device", "mps", "--max-output-tokens", "256"])
    assert parsed.revision == REVISION and parsed.model == DEFAULT_MODEL and parsed.device == "mps"
