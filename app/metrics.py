"""Low-cardinality Prometheus measurements; no prompts, IDs, or tenant labels."""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

registry = CollectorRegistry()
http_requests = Counter(
    "gateway_http_requests_total", "HTTP responses", ["route", "status"], registry=registry
)
http_latency = Histogram("gateway_http_duration_seconds", "HTTP latency", ["route"], registry=registry)
stage_latency = Histogram(
    "gateway_stage_duration_seconds",
    "Pipeline stage latency",
    ["stage"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
    registry=registry,
)
cache_requests = Counter("gateway_cache_total", "Cache outcomes", ["outcome"], registry=registry)
degraded_requests = Counter("gateway_degraded_total", "Degraded graph retrievals", registry=registry)
active_requests = Gauge("gateway_active_requests", "Requests holding an admission slot", registry=registry)


def render() -> bytes:
    return generate_latest(registry)


outbox_pending = Gauge("gateway_graph_outbox_pending", "Graph events awaiting projection", registry=registry)
outbox_age = Gauge(
    "gateway_graph_outbox_oldest_seconds", "Age of oldest pending graph event", registry=registry
)
outbox_failures = Gauge(
    "gateway_graph_outbox_failed", "Pending graph events with failed attempts", registry=registry
)
embedding_queue = Gauge(
    "gateway_embedding_queue_depth", "Embedding requests waiting for a batch", registry=registry
)
embedding_cache = Gauge("gateway_embedding_cache_entries", "Resident scoped embeddings", registry=registry)


def observe_runtime(state) -> None:
    worker = getattr(state, "outbox", None)
    if worker:
        snapshot = worker.health()
        outbox_pending.set(snapshot.get("pending", 0) or 0)
        outbox_age.set(snapshot.get("oldest_age_seconds", 0) or 0)
        outbox_failures.set(snapshot.get("failed", 0) or 0)
    embedder = getattr(state, "embedder", None)
    if embedder:
        snapshot = embedder.health()
        embedding_queue.set(snapshot.get("queue_depth", 0))
        embedding_cache.set(snapshot.get("cache_entries", 0))
