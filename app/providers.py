"""LLM provider layer: rule-based model routing with automatic fallback.

Anthropic is the only provider today, but everything downstream talks to the
`LLMProvider` interface, so adding OpenAI later is a new subclass + a routing
table entry — no pipeline changes.
"""
from dataclasses import dataclass

import anthropic
from anthropic import AsyncAnthropic

from .config import settings

# USD per 1M tokens (input, output) — powers cost attribution in Postgres.
PRICING: dict[str, tuple[float, float]] = {
    "claude-haiku-4-5": (1.00, 5.00),
    "claude-sonnet-5": (3.00, 15.00),
}

# On timeout / 429 / 5xx, retry once on the other tier.
FALLBACK_MODEL: dict[str, str] = {
    settings.simple_model: settings.complex_model,
    settings.complex_model: settings.simple_model,
}


def estimate_cost(model: str, tokens_in: int, tokens_out: int) -> float:
    in_price, out_price = PRICING.get(model, (0.0, 0.0))
    return tokens_in * in_price / 1_000_000 + tokens_out * out_price / 1_000_000


@dataclass
class CompletionResult:
    text: str
    model: str
    provider: str
    tokens_in: int
    tokens_out: int
    fallback_used: bool
    fallback_provider: str | None  # provider that served the retry, if any


class LLMProvider:
    name: str

    async def complete(
        self, model: str, system: str | None, prompt: str, max_tokens: int
    ) -> tuple[str, int, int]:
        raise NotImplementedError


class AnthropicProvider(LLMProvider):
    name = "anthropic"

    def __init__(self) -> None:
        self.client = AsyncAnthropic(api_key=settings.anthropic_api_key)

    async def complete(
        self, model: str, system: str | None, prompt: str, max_tokens: int
    ) -> tuple[str, int, int]:
        kwargs: dict = {}
        if system:
            kwargs["system"] = system
        msg = await self.client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
            **kwargs,
        )
        text = "".join(b.text for b in msg.content if b.type == "text")
        return text, msg.usage.input_tokens, msg.usage.output_tokens


class Router:
    """Picks a model per prompt, calls the provider, falls back on failure."""

    # Signals that a prompt likely needs the stronger model even if short.
    COMPLEX_MARKERS = ("analyze", "compare", "design", "architect", "explain why", "trade-off", "tradeoff")

    def __init__(self) -> None:
        self.provider = AnthropicProvider()

    def choose_model(self, prompt: str) -> str:
        lowered = prompt.lower()
        if len(prompt) > settings.complex_prompt_chars or any(
            marker in lowered for marker in self.COMPLEX_MARKERS
        ):
            return settings.complex_model
        return settings.simple_model

    async def complete(
        self, prompt: str, system: str | None, max_tokens: int
    ) -> CompletionResult:
        primary = self.choose_model(prompt)
        try:
            text, tin, tout = await self.provider.complete(primary, system, prompt, max_tokens)
            return CompletionResult(
                text=text, model=primary, provider=self.provider.name,
                tokens_in=tin, tokens_out=tout,
                fallback_used=False, fallback_provider=None,
            )
        except (
            anthropic.RateLimitError,
            anthropic.APITimeoutError,
            anthropic.InternalServerError,
            anthropic.APIConnectionError,
        ):
            secondary = FALLBACK_MODEL[primary]
            text, tin, tout = await self.provider.complete(secondary, system, prompt, max_tokens)
            return CompletionResult(
                text=text, model=secondary, provider=self.provider.name,
                tokens_in=tin, tokens_out=tout,
                fallback_used=True, fallback_provider=self.provider.name,
            )
