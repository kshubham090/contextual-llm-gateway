"""Evaluation-only loopback Chat Completions server backed by an actual local transformer.

Not a production inference service. One model, one native inference job, no queue,
no paid APIs, no streaming emulation. Native kernels stop only at token boundaries;
a cancelled HTTP request does not release inference admission until that job exits.
"""
from __future__ import annotations

import argparse
import asyncio
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass

import anyio
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

DEFAULT_MODEL = "Qwen/Qwen2.5-0.5B-Instruct"


class BusyError(Exception):
    pass


@dataclass(frozen=True)
class Generation:
    text: str
    prompt_tokens: int
    completion_tokens: int
    finish_reason: str


class LocalGenerator:
    """Keep admission tied to the real concurrent future, not its HTTP awaiter."""

    def __init__(self, *, model: str, revision: str, device: str, cpu_threads=0,
                 max_input_tokens=4096, max_output_tokens=512, timeout_seconds=120,
                 local_files_only=False):
        if not re.fullmatch(r"[a-fA-F0-9]{40}", revision):
            raise ValueError("Revision must be an immutable 40-character model commit SHA")
        if device not in {"cpu", "mps", "cuda"}:
            raise ValueError("Device must be explicitly cpu, mps or cuda")
        self.model_id, self.revision, self.device = model, revision.lower(), device
        self.cpu_threads, self.max_input_tokens = cpu_threads, max_input_tokens
        self.max_output_tokens, self.timeout_seconds = max_output_tokens, timeout_seconds
        self.local_files_only = local_files_only
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-eval-model")
        self._native = None
        self._stop = None
        self._loaded, self._closed = False, False
        self._versions = {}

    async def start(self):
        await asyncio.wrap_future(self._executor.submit(self._load))
        self._loaded = True

    def _load(self):
        # Optional libraries are imported only on actual model startup. Unit tests
        # use the same admission/HTTP paths with a small controlled native worker.
        import torch
        import transformers
        from transformers import AutoModelForCausalLM, AutoTokenizer

        if self.device == "mps" and not torch.backends.mps.is_available():
            raise ValueError("MPS is unavailable; select a device present on this machine")
        if self.device == "cuda" and not torch.cuda.is_available():
            raise ValueError("CUDA is unavailable; select a device present on this machine")
        if self.device == "cpu" and self.cpu_threads:
            torch.set_num_threads(self.cpu_threads)
        dtype = torch.float32 if self.device == "cpu" else torch.float16
        common = {"revision": self.revision, "trust_remote_code": False,
                  "local_files_only": self.local_files_only, "token": False}
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_id, **common)
        if not self.tokenizer.chat_template:
            raise ValueError("Evaluation requires the pinned model's own chat template")
        self.model = AutoModelForCausalLM.from_pretrained(
            self.model_id, use_safetensors=True, dtype=dtype, **common,
        ).to(self.device).eval()
        self._versions = {"torch": torch.__version__, "transformers": transformers.__version__,
                          "dtype": str(dtype), "cpu_threads": torch.get_num_threads(),
                          "loaded_revision": getattr(self.model.config, "_commit_hash", None)}

    def _infer(self, messages, max_tokens, stop):
        import torch
        from transformers import StoppingCriteria, StoppingCriteriaList

        class StopOnCancellation(StoppingCriteria):
            def __call__(self, input_ids, scores, **kwargs):
                return stop.is_set()

        if stop.is_set():
            raise InterruptedError("Evaluation request cancelled")
        encoded = self.tokenizer.apply_chat_template(
            messages, tokenize=True, add_generation_prompt=True, return_tensors="pt", return_dict=True,
        )
        incoming = encoded["input_ids"].shape[-1]
        if incoming > self.max_input_tokens:
            raise ValueError("Tokenized conversation exceeds the configured input limit")
        context_limit = getattr(self.model.config, "max_position_embeddings", None)
        if context_limit is not None and incoming + max_tokens > context_limit:
            raise ValueError("Requested input/output exceed the pinned model's context window")
        encoded = {key: value.to(self.device) for key, value in encoded.items()}
        with torch.inference_mode():
            output = self.model.generate(
                **encoded, max_new_tokens=max_tokens, do_sample=False, num_beams=1,
                pad_token_id=(self.tokenizer.pad_token_id if self.tokenizer.pad_token_id is not None
                              else self.tokenizer.eos_token_id),
                stopping_criteria=StoppingCriteriaList([StopOnCancellation()]), use_cache=True,
            )
        if stop.is_set():
            raise InterruptedError("Evaluation request cancelled")
        tokens = output[0, incoming:].tolist()
        eos = self.model.generation_config.eos_token_id
        eos = [eos] if isinstance(eos, int) else list(eos or [])
        finish = "stop" if tokens and tokens[-1] in eos else "length" if len(tokens) >= max_tokens else "stop"
        text = self.tokenizer.decode(tokens, skip_special_tokens=True)
        return Generation(text, incoming, len(tokens), finish)

    @property
    def busy(self):
        return self._native is not None and not self._native.done()

    async def generate(self, messages, max_tokens):
        if self._closed or not self._loaded:
            raise BusyError("Model is not ready")
        if self.busy:
            raise BusyError("The single evaluation inference slot is busy")
        if not 1 <= max_tokens <= self.max_output_tokens:
            raise ValueError("Requested output exceeds the configured output limit")
        # No await between checking admission and submitting: only one event-loop
        # caller can occupy the executor. Cancellation cannot create a hidden queue.
        stop = threading.Event()
        self._stop = stop
        self._native = self._executor.submit(self._infer, messages, max_tokens, stop)
        wrapped = asyncio.wrap_future(self._native)
        wrapped.add_done_callback(lambda future: future.exception() if not future.cancelled() else None)
        try:
            return await asyncio.wait_for(asyncio.shield(wrapped), self.timeout_seconds)
        except (asyncio.CancelledError, TimeoutError):
            # Native completion may admit another job before this HTTP awaiter
            # receives cancellation. Signal only this request's worker.
            stop.set()
            raise

    def health(self):
        return {"status": "ready" if self._loaded and not self._closed else "unavailable",
                "evaluation_only": True, "model": self.model_id, "revision": self.revision,
                "device": self.device, "busy": self.busy, "streaming": False,
                "sampling": {"do_sample": False, "num_beams": 1},
                "limits": {"input_tokens": self.max_input_tokens, "output_tokens": self.max_output_tokens,
                           "inference_slots": 1, "queue_size": 0, "timeout_seconds": self.timeout_seconds},
                **self._versions}

    async def close(self):
        self._closed = True
        if self._stop is not None:
            self._stop.set()
        if self._native is not None:
            await asyncio.gather(asyncio.wrap_future(self._native), return_exceptions=True)
        # Joining native work is intentional. A running kernel cannot safely be
        # killed by cancelling an asyncio awaiter. A hung kernel requires process termination.
        await asyncio.to_thread(self._executor.shutdown, wait=True, cancel_futures=True)


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: str
    content: str = Field(min_length=1, max_length=64000)


class CompletionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    model: str = Field(min_length=1, max_length=256)
    messages: list[Message] = Field(min_length=1, max_length=34)
    max_tokens: int | None = Field(default=None, ge=1, le=2048)
    max_completion_tokens: int | None = Field(default=None, ge=1, le=2048)
    stream: bool = False

    @model_validator(mode="after")
    def supported(self):
        if self.stream:
            raise ValueError("This evaluation server does not implement streaming")
        if self.max_tokens is not None and self.max_completion_tokens is not None:
            raise ValueError("Specify one output token budget")
        messages = self.messages[1:] if self.messages[0].role == "system" else self.messages
        if not messages or len(messages) % 2 != 1 or any(
            message.role != ("user" if i % 2 == 0 else "assistant") for i, message in enumerate(messages)
        ):
            raise ValueError("Support a leading system message and alternating text messages ending in user")
        if sum(len(message.content) for message in self.messages) > 64000:
            raise ValueError("Conversation exceeds 64000 characters")
        if any("\x00" in message.content or not message.content.strip() for message in self.messages):
            raise ValueError("Messages must contain text and no null bytes")
        return self


class BodyLimitMiddleware:
    def __init__(self, app, max_bytes=262144):
        self.app, self.max_bytes = app, max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        parts, total = [], 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            part = message.get("body", b"")
            total += len(part)
            if total > self.max_bytes:
                return await JSONResponse({"detail": "Evaluation request body too large"}, 413)(
                    scope, receive, send)
            parts.append(part)
            if not message.get("more_body", False):
                break
        consumed = False

        async def replay():
            nonlocal consumed
            if not consumed:
                consumed = True
                return {"type": "http.request", "body": b"".join(parts), "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


async def run_connected(request, engine, messages, max_tokens):
    async def disconnect():
        while (await request.receive())["type"] != "http.disconnect":
            pass

    inference = asyncio.create_task(engine.generate(messages, max_tokens))
    monitor = asyncio.create_task(disconnect())
    try:
        await asyncio.wait((inference, monitor), return_when=asyncio.FIRST_COMPLETED)
        if monitor.done():
            monitor.result()
            raise HTTPException(499, "Client disconnected")
        return inference.result()
    finally:
        with anyio.CancelScope(shield=True):
            monitor.cancel()
            inference.cancel()
            await asyncio.gather(monitor, inference, return_exceptions=True)


def create_app(engine):
    @asynccontextmanager
    async def lifespan(app):
        try:
            await engine.start()
            yield
        finally:
            await engine.close()

    app = FastAPI(title="Local transformer evaluation fixture", lifespan=lifespan, docs_url=None,
                  redoc_url=None, openapi_url=None)
    app.add_middleware(BodyLimitMiddleware)

    @app.get("/health")
    async def health():
        return engine.health()

    @app.post("/v1/chat/completions")
    async def complete(body: CompletionRequest, request: Request):
        if body.model != engine.model_id:
            raise HTTPException(404, "Only the configured evaluation model is served")
        budget = body.max_completion_tokens or body.max_tokens or min(256, engine.max_output_tokens)
        try:
            result = await run_connected(request, engine, [m.model_dump() for m in body.messages], budget)
        except BusyError:
            raise HTTPException(503, "Evaluation model busy or not ready", headers={"Retry-After": "1"})
        except TimeoutError:
            raise HTTPException(504, "Evaluation deadline exceeded; native work may still be stopping")
        except ValueError:
            raise HTTPException(422, "Conversation or output budget exceeds the model's configured limits")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(503, "Local evaluation model failed")
        return {"id": "chatcmpl-eval-" + uuid.uuid4().hex, "object": "chat.completion",
                "created": int(time.time()), "model": engine.model_id,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": result.text},
                             "finish_reason": result.finish_reason}],
                "usage": {"prompt_tokens": result.prompt_tokens,
                          "completion_tokens": result.completion_tokens,
                          "total_tokens": result.prompt_tokens + result.completion_tokens},
                "local_evaluation": {"revision": engine.revision, "device": engine.device}}

    return app


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help="Trusted operator-selected Hugging Face model ID")
    parser.add_argument("--revision", required=True, help="Required immutable 40-character model commit SHA")
    parser.add_argument("--device", required=True, choices=("cpu", "mps", "cuda"))
    parser.add_argument("--port", type=int, default=8003)
    parser.add_argument("--cpu-threads", type=int, default=0,
                        help="0 preserves PyTorch's default CPU thread count")
    parser.add_argument("--max-input-tokens", type=int, default=4096)
    parser.add_argument("--max-output-tokens", type=int, default=512)
    parser.add_argument("--timeout-seconds", type=float, default=120)
    parser.add_argument("--local-files-only", action="store_true",
                        help="Load only an already cached pinned model")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[a-fA-F0-9]{40}", args.revision):
        parser.error("--revision must be an immutable 40-character commit SHA, never main or a branch")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*", args.model):
        parser.error("--model must be an owner/repository model ID, not a path or URL")
    if not (1 <= args.port <= 65535 and 0 <= args.cpu_threads <= 256
            and 1 <= args.max_input_tokens <= 16384 and 1 <= args.max_output_tokens <= 2048
            and 0 < args.timeout_seconds <= 600):
        parser.error("Invalid port, thread, token or timeout limit")
    return args


def main():
    args = arguments()
    import uvicorn

    engine = LocalGenerator(**{key: value for key, value in vars(args).items() if key != "port"})
    print("Evaluation fixture only: real local transformer; no streaming; bound to 127.0.0.1.", flush=True)
    uvicorn.run(create_app(engine), host="127.0.0.1", port=args.port, workers=1,
                access_log=False, proxy_headers=False, timeout_graceful_shutdown=15)


if __name__ == "__main__":
    main()
