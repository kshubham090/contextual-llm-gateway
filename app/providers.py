"""Bounded provider calls, explicit budgets, and transient-only model fallback."""
from __future__ import annotations

import asyncio
import json
import math
import time
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass
from typing import Mapping

import anthropic
import httpx
from anthropic import AsyncAnthropic, DefaultAsyncHttpxClient

from .config import settings

# Public standard input/output USD per million tokens; excludes prompt caching,
# batch discounts and long-context premiums. Override MODEL_PRICING for your contract.
# Source: https://platform.claude.com/docs/en/about-claude/pricing
PRICING: dict[str, tuple[float, float]] = {
    "claude-haiku-4-5": (1.00, 5.00),
    "claude-sonnet-4-5": (3.00, 15.00),
}
ROUTING_POLICY_VERSION = "rules-v1"
COMPLEX_MARKERS = (
    "analyze", "compare", "design", "architect", "explain why", "trade-off", "tradeoff",
)
FALLBACK_MODEL: dict[str, str] = {
    settings.simple_model: settings.complex_model,
    settings.complex_model: settings.simple_model,
}


def choose_model(prompt: str, config=None) -> str:
    """Pure routing policy shared by generation and response-cache eligibility."""
    config = config or settings
    lowered = prompt.lower()
    if len(prompt) > config.complex_prompt_chars or any(marker in lowered for marker in COMPLEX_MARKERS):
        return config.complex_model
    return config.simple_model


class ProviderOverloadedError(RuntimeError):
    pass


class CircuitOpenError(RuntimeError):
    pass


class ProviderClosedError(RuntimeError):
    pass


def estimate_cost(
    model: str, tokens_in: int, tokens_out: int,
    pricing: Mapping[str, tuple[float, float]] | None = None,
) -> float | None:
    """Return an estimate, or None when the configured model has no known rate."""
    if tokens_in < 0 or tokens_out < 0:
        raise ValueError("Token counts must be nonnegative")
    rates = {**PRICING, **(settings.model_pricing if pricing is None else pricing)}
    if model not in rates:
        return None
    in_price, out_price = rates[model]
    if any(not math.isfinite(price) or price < 0 for price in (in_price, out_price)):
        raise ValueError("Model prices must be finite and nonnegative")
    return tokens_in * in_price / 1_000_000 + tokens_out * out_price / 1_000_000


@dataclass
class CompletionResult:
    text: str
    model: str
    provider: str
    tokens_in: int
    tokens_out: int
    fallback_used: bool
    fallback_provider: str | None
    usage_available: bool = True
    finish_reason: str = "stop"


@dataclass
class ProviderOutput:
    text: str
    tokens_in: int = 0
    tokens_out: int = 0
    usage_available: bool = True
    finish_reason: str = "stop"


@dataclass
class ProviderChunk:
    delta: str = ""
    output: ProviderOutput | None = None


@dataclass
class StreamEvent:
    delta: str = ""
    model: str = ""
    provider: str = ""
    result: CompletionResult | None = None


class ProviderProtocolError(RuntimeError):
    """The server did not complete the supported text-generation protocol."""


def provider_identity(config=None) -> dict:
    config = config or settings
    backend = getattr(config, "generation_backend", "anthropic")
    return {
        "backend": backend,
        "base_url": (config.openai_base_url if backend == "openai_compatible"
                     else getattr(config, "anthropic_base_url", "https://api.anthropic.com")).rstrip("/"),
        "token_field": getattr(config, "openai_max_tokens_field", "max_tokens")
        if backend == "openai_compatible" else "max_tokens",
    }


def _messages(prompt: str, system: str | None, history=None) -> list[dict]:
    messages = ([{"role": "system", "content": system}] if system else [])
    return messages + list(history or []) + [{"role": "user", "content": prompt}]


def _usage(value) -> tuple[int, int, bool]:
    if value is None:
        return 0, 0, False
    try:
        incoming, outgoing = value["prompt_tokens"], value["completion_tokens"]
        if any(type(number) is not int or number < 0 for number in (incoming, outgoing)):
            raise ValueError()
        return incoming, outgoing, True
    except (KeyError, TypeError, ValueError) as exc:
        raise ProviderProtocolError("Provider returned invalid usage") from exc


class LLMProvider:
    name: str

    async def complete(
        self, model: str, system: str | None, prompt: str, max_tokens: int, *, history=None,
    ) -> ProviderOutput:
        raise NotImplementedError

    async def close(self) -> None:
        return None


class AnthropicProvider(LLMProvider):
    name = "anthropic"

    def __init__(self, *, config=None) -> None:
        config = config or settings
        timeout = httpx.Timeout(
            config.provider_timeout_seconds,
            connect=min(5.0, config.provider_timeout_seconds),
            pool=min(2.0, config.provider_timeout_seconds),
        )
        self.client = AsyncAnthropic(
            api_key=config.anthropic_api_key,
            base_url=getattr(config, "anthropic_base_url", "https://api.anthropic.com"),
            timeout=timeout,
            max_retries=0,  # Router owns the single, observable fallback attempt.
            http_client=DefaultAsyncHttpxClient(
                timeout=timeout,
                limits=httpx.Limits(
                    max_connections=config.provider_max_concurrency,
                    max_keepalive_connections=config.provider_max_concurrency,
                ),
            ),
        )

    async def complete(
        self, model: str, system: str | None, prompt: str, max_tokens: int, *, history=None,
    ) -> ProviderOutput:
        kwargs: dict = {}
        if system:
            kwargs["system"] = system
        message = await self.client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=_messages(prompt, None, history),
            **kwargs,
        )
        text = "".join(block.text for block in message.content if block.type == "text")
        return ProviderOutput(text, message.usage.input_tokens, message.usage.output_tokens,
                              finish_reason="length" if message.stop_reason == "max_tokens" else "stop")

    async def stream(self, model, system, prompt, max_tokens, *, history=None):
        kwargs = {"system": system} if system else {}
        async with self.client.messages.stream(
            model=model, max_tokens=max_tokens, messages=_messages(prompt, None, history), **kwargs,
        ) as stream:
            parts = []
            async for text in stream.text_stream:
                parts.append(text)
                if text:
                    yield ProviderChunk(delta=text)
            message = await stream.get_final_message()
            yield ProviderChunk(output=ProviderOutput(
                "".join(parts), message.usage.input_tokens, message.usage.output_tokens,
                finish_reason="length" if message.stop_reason == "max_tokens" else "stop",
            ))

    async def close(self) -> None:
        await self.client.close()


class OpenAICompatibleProvider(LLMProvider):
    """Text Chat Completions over HTTP; supports hosted, vLLM and Ollama endpoints."""

    name = "openai_compatible"

    def __init__(self, *, config=None, http_client=None):
        self._config = config or settings
        self._limit = self._config.provider_max_response_bytes
        self._url = self._config.openai_base_url.rstrip("/") + "/chat/completions"
        headers = ({"Authorization": f"Bearer {self._config.openai_api_key}"}
                   if self._config.openai_api_key else {})
        self.client = http_client or httpx.AsyncClient(
            headers=headers, follow_redirects=False,
            timeout=httpx.Timeout(self._config.provider_timeout_seconds, connect=5.0, pool=2.0),
            limits=httpx.Limits(max_connections=self._config.provider_max_concurrency,
                               max_keepalive_connections=self._config.provider_max_concurrency),
        )

    def _body(self, model, system, prompt, max_tokens, history, stream):
        body = {"model": model, "messages": _messages(prompt, system, history), "stream": stream,
                self._config.openai_max_tokens_field: max_tokens}
        if stream and self._config.openai_stream_usage:
            body["stream_options"] = {"include_usage": True}
        return body

    async def complete(self, model, system, prompt, max_tokens, *, history=None):
        async with self.client.stream("POST", self._url, json=self._body(
            model, system, prompt, max_tokens, history, False,
        )) as response:
            response.raise_for_status()
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > self._limit:
                    raise ProviderProtocolError("Provider response exceeds configured byte limit")
        try:
            data = json.loads(body)
            choices = data["choices"]
            if len(choices) != 1:
                raise ValueError()
            choice = choices[0]
            message = choice["message"]
            if message.get("tool_calls") or message.get("function_call"):
                raise ValueError()
            text = message["content"]
            if (not isinstance(text, str)
                    or choice.get("finish_reason") not in {"stop", "length", "content_filter"}):
                raise ValueError()
            incoming, outgoing, available = _usage(data.get("usage"))
            return ProviderOutput(text, incoming, outgoing, available, choice["finish_reason"])
        except (KeyError, TypeError, ValueError, IndexError, AttributeError) as exc:
            raise ProviderProtocolError("Provider returned an unsupported completion payload") from exc

    async def _events(self, response):
        pending, data_lines, total = bytearray(), [], 0
        async for chunk in response.aiter_bytes():
            total += len(chunk)
            if total > self._limit:
                raise ProviderProtocolError("Provider stream exceeds configured byte limit")
            pending.extend(chunk)
            while b"\n" in pending:
                line, _, rest = pending.partition(b"\n")
                pending = bytearray(rest)
                line = line.rstrip(b"\r")
                if not line and data_lines:
                    try:
                        yield b"\n".join(data_lines).decode("utf-8")
                    except UnicodeDecodeError as exc:
                        raise ProviderProtocolError("Provider stream contains invalid UTF-8") from exc
                    data_lines = []
                elif line.startswith(b"data:"):
                    data_lines.append(bytes(line[5:]).lstrip(b" "))
        if pending or data_lines:
            raise ProviderProtocolError("Provider stream ended inside an SSE event")

    async def stream(self, model, system, prompt, max_tokens, *, history=None):
        parts, finish, usage, done = [], None, None, False
        async with self.client.stream("POST", self._url, json=self._body(
            model, system, prompt, max_tokens, history, True,
        )) as response:
            response.raise_for_status()
            async for payload in self._events(response):
                if payload == "[DONE]":
                    done = True
                    break
                try:
                    data = json.loads(payload)
                    if data.get("error"):
                        raise ValueError()
                    if data.get("usage") is not None:
                        usage = data["usage"]
                    choices = data["choices"]
                    if not isinstance(choices, list) or len(choices) > 1:
                        raise ValueError()
                    if not choices:
                        continue
                    choice = choices[0]
                    delta = choice.get("delta", {})
                    if delta.get("tool_calls") or delta.get("function_call") or choice.get("index", 0) != 0:
                        raise ValueError()
                    text = delta.get("content", "")
                    if text is None:
                        text = ""
                    if not isinstance(text, str) or (finish is not None and text):
                        raise ValueError()
                    if text:
                        parts.append(text)
                        yield ProviderChunk(delta=text)
                    if choice.get("finish_reason") is not None:
                        finish = choice["finish_reason"]
                        if finish not in {"stop", "length", "content_filter"}:
                            raise ValueError()
                except (KeyError, TypeError, ValueError, IndexError, AttributeError) as exc:
                    raise ProviderProtocolError("Provider returned an unsupported stream payload") from exc
        if not done or finish is None:
            raise ProviderProtocolError("Provider stream ended before completion")
        incoming, outgoing, available = _usage(usage)
        yield ProviderChunk(output=ProviderOutput("".join(parts), incoming, outgoing, available, finish))

    async def close(self):
        await self.client.aclose()


@dataclass
class _Circuit:
    failures: int = 0
    open_until: float = 0.0
    probe_inflight: bool = False
    epoch: int = 0


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, (anthropic.APIConnectionError, httpx.TimeoutException, httpx.NetworkError,
                        TimeoutError, CircuitOpenError)):
        return True
    status = (exc.status_code if isinstance(exc, anthropic.APIStatusError)
              else exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None)
    return status is not None and (status in (408, 429) or status >= 500)


class Router:
    """Route by prompt, with one fallback, per-model circuits, and bounded admission.

    At most ``provider_max_concurrency`` calls reach the SDK at a time; at most the
    same number wait for admission. Queue waits and each attempt have separate time
    budgets. An open model circuit permits a single probe after its cooldown.
    """

    COMPLEX_MARKERS = COMPLEX_MARKERS

    def __init__(self, *, provider: LLMProvider | None = None, config=None) -> None:
        self._config = config or settings
        self.provider = provider or (
            OpenAICompatibleProvider(config=self._config)
            if getattr(self._config, "generation_backend", "anthropic") == "openai_compatible"
            else AnthropicProvider(config=self._config)
        )
        self._slots = asyncio.Semaphore(self._config.provider_max_concurrency)
        self._tasks: set[asyncio.Task] = set()
        self._admitted = 0
        self._circuits: dict[str, _Circuit] = {}
        self._closed = False
        self._close_lock = asyncio.Lock()

    def choose_model(self, prompt: str) -> str:
        return choose_model(prompt, config=self._config)

    def _begin_attempt(self, model):
        circuit = self._circuits.setdefault(model, _Circuit())
        probe = False
        if circuit.open_until:
            if circuit.open_until > time.monotonic() or circuit.probe_inflight:
                raise CircuitOpenError("Model circuit is cooling down; retry later")
            circuit.probe_inflight = probe = True
        return circuit, circuit.epoch, probe

    def _finish_attempt(self, state, error=None):
        circuit, epoch, probe = state
        if epoch != circuit.epoch:
            return
        if isinstance(error, (asyncio.CancelledError, GeneratorExit)):
            if probe:
                circuit.probe_inflight = False
            return
        if error is not None and _is_transient(error):
            circuit.failures += 1
            if probe or circuit.failures >= self._config.provider_failure_threshold:
                circuit.epoch += 1
                circuit.open_until = time.monotonic() + self._config.provider_circuit_reset_seconds
        else:
            circuit.failures = 0
            circuit.open_until = 0.0
        if probe:
            circuit.probe_inflight = False

    @asynccontextmanager
    async def _admission(self):
        if self._closed:
            raise ProviderClosedError("Provider router is closed")
        if self._admitted >= self._config.provider_max_concurrency * 2:
            raise ProviderOverloadedError("Provider queue is full; retry later")
        self._admitted += 1
        task = asyncio.current_task()
        self._tasks.add(task)
        acquired = False
        try:
            try:
                async with asyncio.timeout(self._config.provider_queue_timeout_seconds):
                    await self._slots.acquire()
                    acquired = True
            except TimeoutError as exc:
                raise ProviderOverloadedError("Provider queue wait expired; retry later") from exc
            yield
        finally:
            if acquired:
                self._slots.release()
            self._admitted -= 1
            self._tasks.discard(task)

    async def _attempt(self, model, system, prompt, max_tokens, *, history=None):
        state = self._begin_attempt(model)
        try:
            kwargs = {"history": history} if history else {}
            result = await asyncio.wait_for(
                self.provider.complete(model, system, prompt, max_tokens, **kwargs),
                timeout=self._config.provider_timeout_seconds,
            )
        except BaseException as exc:
            self._finish_attempt(state, exc)
            raise
        self._finish_attempt(state)
        return result if isinstance(result, ProviderOutput) else ProviderOutput(*result)

    def _secondary(self, primary):
        return (self._config.complex_model if primary == self._config.simple_model
                else self._config.simple_model)

    def _result(self, output, model, fallback):
        return CompletionResult(
            output.text, model, self.provider.name, output.tokens_in, output.tokens_out,
            fallback, self.provider.name if fallback else None, output.usage_available, output.finish_reason,
        )

    async def complete(self, prompt, system, max_tokens, *, model=None, history=None) -> CompletionResult:
        async with self._admission():
            primary = model or self.choose_model(prompt)
            try:
                output = await self._attempt(primary, system, prompt, max_tokens, history=history)
                return self._result(output, primary, False)
            except Exception as exc:
                secondary = self._secondary(primary)
                if not _is_transient(exc) or secondary == primary:
                    raise
                output = await self._attempt(secondary, system, prompt, max_tokens, history=history)
                return self._result(output, secondary, True)

    async def stream(self, prompt, system, max_tokens, *, model=None, history=None):
        async with self._admission():
            primary = model or self.choose_model(prompt)
            selected, emitted = primary, False
            for attempt in range(2):
                state = None
                try:
                    state = self._begin_attempt(selected)
                    deadline = time.monotonic() + self._config.provider_timeout_seconds
                    kwargs = {"history": history} if history else {}
                    parts, size, output = [], 0, None
                    upstream = self.provider.stream(selected, system, prompt, max_tokens, **kwargs)
                    async with aclosing(upstream) as source:
                        while True:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                raise TimeoutError()
                            try:
                                chunk = await asyncio.wait_for(anext(source), remaining)
                            except StopAsyncIteration:
                                break
                            if chunk.output is not None:
                                output = chunk.output
                                break
                            if chunk.delta:
                                size += len(chunk.delta.encode("utf-8"))
                                if size > getattr(self._config, "provider_max_response_bytes", 8388608):
                                    raise ProviderProtocolError("Provider text exceeds configured byte limit")
                                emitted = True
                                parts.append(chunk.delta)
                                yield StreamEvent(delta=chunk.delta, model=selected,
                                                  provider=self.provider.name)
                    if output is None or output.text != "".join(parts):
                        raise ProviderProtocolError("Provider stream has no consistent final answer")
                    self._finish_attempt(state)
                    yield StreamEvent(result=self._result(output, selected, attempt > 0))
                    return
                except BaseException as exc:
                    if state is not None:
                        self._finish_attempt(state, exc)
                    secondary = self._secondary(primary)
                    if (isinstance(exc, (asyncio.CancelledError, GeneratorExit)) or emitted or attempt
                            or not _is_transient(exc) or secondary == primary):
                        raise
                    selected = secondary

    def health(self) -> dict:
        return {
            "closed": self._closed,
            "admitted_requests": self._admitted,
            "open_circuits": [
                model for model, state in self._circuits.items() if state.open_until > time.monotonic()
            ],
        }

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            tasks = list(self._tasks)
            if tasks:
                _, pending = await asyncio.wait(
                    tasks, timeout=self._config.provider_shutdown_timeout_seconds,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            close = getattr(self.provider, "close", None)
            if close is not None:
                await close()
