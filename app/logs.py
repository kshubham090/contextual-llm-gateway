"""Structured JSON logging with a request ID threaded through the pipeline.

Every log line is one JSON object carrying the request ID from the current
context, so a single request's journey (cache check, context ranking, LLM
call, write-back) can be grepped out of interleaved concurrent traffic.
"""
import json
import logging
import uuid
from contextvars import ContextVar

request_id_var: ContextVar[str] = ContextVar("request_id", default="-")


def new_request_id() -> str:
    rid = uuid.uuid4().hex[:12]
    request_id_var.set(rid)
    return rid


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "request_id": request_id_var.get(),
            "event": record.getMessage(),
        }
        data = getattr(record, "data", None)
        if data:
            payload.update(data)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    # uvicorn's plain-text access log is replaced by our JSON request log
    logging.getLogger("uvicorn.access").disabled = True


def log(logger: logging.Logger, event: str, **data) -> None:
    """logger.info with structured fields: log(logger, "cache_hit", score=0.97)"""
    logger.info(event, extra={"data": data})
