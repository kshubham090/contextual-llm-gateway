import anthropic
import httpx
import pytest

from app.config import settings
from app.providers import FALLBACK_MODEL, Router, estimate_cost


def test_estimate_cost_haiku():
    # $1/MTok in, $5/MTok out
    assert estimate_cost("claude-haiku-4-5", 1_000_000, 1_000_000) == pytest.approx(6.0)


def test_estimate_cost_sonnet():
    # $3/MTok in, $15/MTok out
    assert estimate_cost("claude-sonnet-5", 500_000, 100_000) == pytest.approx(3.0)


def test_estimate_cost_unknown_model_is_zero():
    assert estimate_cost("some-future-model", 1000, 1000) == 0.0


def test_short_prompt_routes_to_simple_model():
    assert Router().choose_model("What is a bake time?") == settings.simple_model


def test_long_prompt_routes_to_complex_model():
    assert Router().choose_model("x" * (settings.complex_prompt_chars + 1)) == settings.complex_model


def test_complex_marker_routes_to_complex_model():
    assert Router().choose_model("Compare blue-green and canary deploys") == settings.complex_model


def test_fallback_models_point_at_the_other_tier():
    assert FALLBACK_MODEL[settings.simple_model] == settings.complex_model
    assert FALLBACK_MODEL[settings.complex_model] == settings.simple_model


class FlakyProvider:
    """Times out on the first call, succeeds on the second."""

    name = "anthropic"

    def __init__(self) -> None:
        self.models_called: list[str] = []

    async def complete(self, model, system, prompt, max_tokens):
        self.models_called.append(model)
        if len(self.models_called) == 1:
            raise anthropic.APITimeoutError(request=httpx.Request("POST", "http://test"))
        return "recovered", 10, 5


async def test_router_falls_back_on_timeout():
    router = Router()
    router.provider = FlakyProvider()

    result = await router.complete("short prompt", None, 100)

    assert result.fallback_used is True
    assert result.text == "recovered"
    # primary was the simple model (short prompt), retry went to the other tier
    assert router.provider.models_called == [settings.simple_model, settings.complex_model]


class AlwaysWorksProvider:
    name = "anthropic"

    async def complete(self, model, system, prompt, max_tokens):
        return "ok", 10, 5


async def test_router_no_fallback_on_success():
    router = Router()
    router.provider = AlwaysWorksProvider()

    result = await router.complete("short prompt", None, 100)

    assert result.fallback_used is False
    assert result.model == settings.simple_model
