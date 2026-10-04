"""Bounded provider calls, explicit budgets, and transient-only model fallback."""
from __future__ import annotations

import asyncio
import math
import time
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


class LLMProvider:
    name: str

    async def complete(
        self, model: str, system: str | None, prompt: str, max_tokens: int,
    ) -> tuple[str, int, int]:
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
        self, model: str, system: str | None, prompt: str, max_tokens: int,
    ) -> tuple[str, int, int]:
        kwargs: dict = {}
        if system:
            kwargs["system"] = system
        message = await self.client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
            **kwargs,
        )
        text = "".join(block.text for block in message.content if block.type == "text")
        return text, message.usage.input_tokens, message.usage.output_tokens

    async def close(self) -> None:
        await self.client.close()


@dataclass
class _Circuit:
    failures: int = 0
    open_until: float = 0.0
    probe_inflight: bool = False
    epoch: int = 0


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, (anthropic.APIConnectionError, TimeoutError, CircuitOpenError)):
        return True
    return isinstance(exc, anthropic.APIStatusError) and (
        exc.status_code in (408, 429) or exc.status_code >= 500
    )


class Router:
    """Route by prompt, with one fallback, per-model circuits, and bounded admission.

    At most ``provider_max_concurrency`` calls reach the SDK at a time; at most the
    same number wait for admission. Queue waits and each attempt have separate time
    budgets. An open model circuit permits a single probe after its cooldown.
    """

    COMPLEX_MARKERS = COMPLEX_MARKERS

    def __init__(self, *, provider: LLMProvider | None = None, config=None) -> None:
        self._config = config or settings
        self.provider = provider or AnthropicProvider(config=self._config)
        self._slots = asyncio.Semaphore(self._config.provider_max_concurrency)
        self._tasks: set[asyncio.Task] = set()
        self._admitted = 0
        self._circuits: dict[str, _Circuit] = {}
        self._closed = False
        self._close_lock = asyncio.Lock()

    def choose_model(self, prompt: str) -> str:
        return choose_model(prompt, config=self._config)

    async def _attempt(self, model: str, system: str | None, prompt: str, max_tokens: int):
        circuit = self._circuits.setdefault(model, _Circuit())
        is_probe = False
        if circuit.open_until:
            if circuit.open_until > time.monotonic() or circuit.probe_inflight:
                raise CircuitOpenError("Model circuit is cooling down; retry later")
            circuit.probe_inflight = True
            is_probe = True
        epoch = circuit.epoch
        try:
            result = await asyncio.wait_for(
                self.provider.complete(model, system, prompt, max_tokens),
                timeout=self._config.provider_timeout_seconds,
            )
        except BaseException as exc:
            # Attempts admitted before a circuit trip do not own its new state.
            # Their late results must not close it or release a newer probe.
            if epoch != circuit.epoch:
                raise
            if isinstance(exc, asyncio.CancelledError):
                if is_probe:
                    circuit.probe_inflight = False
                raise
            if _is_transient(exc):
                circuit.failures += 1
                if is_probe or circuit.failures >= self._config.provider_failure_threshold:
                    circuit.epoch += 1
                    circuit.open_until = time.monotonic() + self._config.provider_circuit_reset_seconds
                if is_probe:
                    circuit.probe_inflight = False
            else:
                # A valid rejection (e.g. 400/401) proves reachability, and is never retried.
                circuit.failures = 0
                circuit.open_until = 0.0
                if is_probe:
                    circuit.probe_inflight = False
            raise
        else:
            if epoch == circuit.epoch:
                circuit.failures = 0
                circuit.open_until = 0.0
                if is_probe:
                    circuit.probe_inflight = False
            return result

    async def complete(self, prompt: str, system: str | None, max_tokens: int) -> CompletionResult:
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
            primary = self.choose_model(prompt)
            fallback = False
            model = primary
            try:
                text, tokens_in, tokens_out = await self._attempt(primary, system, prompt, max_tokens)
            except Exception as exc:
                if not _is_transient(exc):
                    raise
                model = (self._config.complex_model if primary == self._config.simple_model
                         else self._config.simple_model)
                if model == primary:
                    raise
                text, tokens_in, tokens_out = await self._attempt(model, system, prompt, max_tokens)
                fallback = True
            return CompletionResult(
                text=text, model=model, provider=self.provider.name,
                tokens_in=tokens_in, tokens_out=tokens_out,
                fallback_used=fallback, fallback_provider=self.provider.name if fallback else None,
            )
        finally:
            if acquired:
                self._slots.release()
            self._admitted -= 1
            self._tasks.discard(task)

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
