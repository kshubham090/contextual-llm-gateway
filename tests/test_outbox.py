"""The worker must recover failed and interrupted projections without losing events."""

import asyncio
import uuid

from app.outbox import GraphOutboxWorker


class DurableQueue:
    def __init__(self, count=1):
        self.events = [{
            "id": index, "call_id": uuid.uuid4(), "lease_token": uuid.uuid4(), "attempts": 1,
            "event": {"kind": "call", "payload": {"call_id": str(index)}},
        } for index in range(count)]
        self.acked = []
        self.retried = []

    async def claim_graph_events(self, **kwargs):
        return [item for item in self.events if item["id"] not in self.acked][:kwargs["limit"]]

    async def outbox_status(self):
        return {"pending": len(self.events) - len(self.acked), "failed": 0, "oldest_age_seconds": 0.0}

    async def ack_graph_event(self, event_id, token):
        self.acked.append(event_id)

    async def retry_graph_event(self, event_id, token, error, **kwargs):
        self.retried.append((event_id, error))


class Graph:
    def __init__(self, failing=False):
        self.failing = failing
        self.writes = set()
        self.inflight = 0
        self.maximum_inflight = 0

    async def write_call(self, call_id):
        self.inflight += 1
        self.maximum_inflight = max(self.maximum_inflight, self.inflight)
        try:
            await asyncio.sleep(0)
            if self.failing:
                raise ConnectionError("sensitive payload must never appear in persisted error")
            self.writes.add(call_id)
        finally:
            self.inflight -= 1

    async def write_cache_hit(self, **kwargs):
        await self.write_call(**kwargs)


async def test_failed_projection_survives_worker_restart_and_replays_once(caplog):
    queue, graph = DurableQueue(), Graph(failing=True)
    first_worker = GraphOutboxWorker(queue, graph)
    assert await first_worker.run_once() == 0
    assert queue.acked == []
    assert queue.retried == [(0, "ConnectionError")]
    failure = next(record for record in caplog.records if record.message == "graph_outbox_projection_failed")
    assert failure.data == {"event_id": 0, "error_type": "ConnectionError"}
    graph.failing = False
    replacement_worker = GraphOutboxWorker(queue, graph)
    assert await replacement_worker.run_once() == 1
    assert queue.acked == [0] and graph.writes == {"0"}
    assert await replacement_worker.run_once() == 0


async def test_ack_failure_retries_idempotent_projection():
    class AckFailsOnce(DurableQueue):
        fail = True

        async def ack_graph_event(self, event_id, token):
            if self.fail:
                self.fail = False
                raise ConnectionError("lost acknowledgement")
            await super().ack_graph_event(event_id, token)

    queue, graph = AckFailsOnce(), Graph()
    worker = GraphOutboxWorker(queue, graph)
    assert await worker.run_once() == 0
    assert graph.writes == {"0"} and queue.acked == []
    assert await worker.run_once() == 1
    assert graph.writes == {"0"} and queue.acked == [0]


async def test_projection_concurrency_is_bounded(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "graph_outbox_concurrency", 2)
    queue, graph = DurableQueue(count=12), Graph()
    assert await GraphOutboxWorker(queue, graph).run_once() == 12
    assert graph.maximum_inflight == 2


async def test_cancellation_does_not_acknowledge_unfinished_graph_event():
    started = asyncio.Event()

    class SlowGraph(Graph):
        async def write_call(self, call_id):
            started.set()
            await asyncio.Event().wait()

    queue = DurableQueue()
    worker = GraphOutboxWorker(queue, SlowGraph())
    task = asyncio.create_task(worker.run_once())
    await started.wait()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert queue.acked == []
