from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prompt: str = Field(min_length=1, max_length=64000)
    user_id: str = Field(min_length=1, max_length=128, pattern=r"^[\w.@:-]+$")
    feature_tag: str = Field(min_length=1, max_length=128, pattern=r"^[\w.@:-]+$")
    max_tokens: int | None = Field(default=None, ge=1, le=8192)
    use_graph: bool = True
    use_cache: bool = True
    cache_mode: Literal["exact", "semantic"] = "exact"
    store: bool = True

    @field_validator("prompt")
    @classmethod
    def nonblank_prompt(cls, value: str) -> str:
        if not value.strip() or "\x00" in value:
            raise ValueError("Prompt must contain text and no null bytes")
        return value


class ChatMetadata(BaseModel):
    call_id: str | None = None
    cache_hit: bool = False
    cache_mode: str | None = None
    cached_call_id: str | None = None
    context_used: list[str] = Field(default_factory=list)
    model: str | None = None
    provider: str | None = None
    fallback_used: bool = False
    tokens_in: int = 0
    tokens_out: int = 0
    cost: float | None = 0.0
    cost_is_estimate: bool = True
    latency_ms: int = 0
    timings_ms: dict[str, float] = Field(default_factory=dict)
    degraded: list[str] = Field(default_factory=list)
    memory_write: Literal["queued", "disabled"] = "disabled"


class ChatResponse(BaseModel):
    response: str
    meta: ChatMetadata


class UsageRow(BaseModel):
    user_id: str
    feature_tag: str
    day: str
    calls: int
    cache_hits: int
    tokens_in: int
    tokens_out: int
    cost: float
    unpriced_calls: int = 0
    avg_latency_ms: float
