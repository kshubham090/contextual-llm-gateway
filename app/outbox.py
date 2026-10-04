"""Bounded, leased, at-least-once graph projection with idempotent consumers."""

import asyncio
import logging
import random
import time
from datetime import UTC, datetime

from .config import settings

logger = logging.getLogger(__name__)


class GraphOutboxWorker:
    def __init__(self, db, graph) -> None:
        self.db = db
        self.graph = graph
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._batch_lock = asyncio.Lock()
        self.last_success_at: str | None = None
        self.last_error: str | None = None
        self._last_maintenance = 0.0
        self._last_status_check = 0.0
        self.projected_total = 0
        self.failed_total = 0
        self.backlog = {"pending": None, "failed": None, "oldest_age_seconds": None}

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="graph-outbox-worker")

    async def stop(self, timeout: float = 5) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=timeout)
            except TimeoutError:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            finally:
                self._task = None

    def health(self) -> dict:
        return {
            "running": self._task is not None and not self._task.done(),
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
            "projected_total": self.projected_total,
            "failed_total": self.failed_total,
            **self.backlog,
        }

    async def _run(self) -> None:
        poll_seconds = getattr(settings, "graph_outbox_poll_seconds", 1)
        while not self._stop.is_set():
            try:
                projected = await self.run_once()
                if time.monotonic() - self._last_maintenance >= 60:
                    await self.db.purge_expired_memory(limit=1000)
                    await self.graph.purge_expired_memory(limit=1000)
                    self._last_maintenance = time.monotonic()
                if projected:
                    continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = type(exc).__name__
                logger.warning("graph_outbox_poll_failed", extra={"data": {"error_type": self.last_error}})
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=poll_seconds)
            except TimeoutError:
                pass

    async def run_once(self) -> int:
        """Project one bounded batch, returning the number acknowledged.

        Failed events stay durable. Expired leases recover process crashes;
        completion tokens prevent an old worker acknowledging a reclaimed lease.
        """
        async with self._batch_lock:
            lease_seconds = max(5, getattr(settings, "graph_outbox_lease_seconds", 60))
            concurrency = max(1, getattr(settings, "graph_outbox_concurrency", 4))
            events = await self.db.claim_graph_events(
                limit=getattr(settings, "graph_outbox_batch_size", 32), lease_seconds=lease_seconds
            )
            semaphore = asyncio.Semaphore(concurrency)

            async def project(item: dict) -> bool:
                try:
                    # Include time waiting for a slot so no graph action starts
                    # after this worker's lease has expired.
                    async with asyncio.timeout(lease_seconds * 0.8):
                        async with semaphore:
                            event = item["event"]
                            method = {"call": self.graph.write_call, "cache_hit": self.graph.write_cache_hit}
                            if event.get("kind") not in method:
                                raise ValueError("Unsupported graph event kind")
                            await method[event["kind"]](**event["payload"])
                    await self.db.ack_graph_event(item["id"], item["lease_token"])
                    return True
                except asyncio.CancelledError:
                    # Do not acknowledge. A replacement worker recovers the lease.
                    raise
                except Exception as exc:
                    self.last_error = type(exc).__name__
                    delay = min(300, 2 ** min(item["attempts"], 8)) + random.uniform(0, 1)
                    try:
                        await self.db.retry_graph_event(
                            item["id"], item["lease_token"], self.last_error, retry_delay_seconds=delay
                        )
                    except Exception as retry_exc:
                        logger.warning("graph_outbox_release_failed", extra={"data": {
                            "event_id": item["id"], "error_type": type(retry_exc).__name__,
                        }})
                    logger.warning("graph_outbox_projection_failed", extra={"data": {
                        "event_id": item["id"], "error_type": self.last_error,
                    }})
                    return False

            results = await asyncio.gather(*(project(item) for item in events))
            self.projected_total += sum(results)
            self.failed_total += len(results) - sum(results)
            if time.monotonic() - self._last_status_check >= 5:
                self.backlog = await self.db.outbox_status()
                self._last_status_check = time.monotonic()
            if all(results):
                self.last_success_at = datetime.now(UTC).isoformat()
                if events:
                    self.last_error = None
            return sum(results)
