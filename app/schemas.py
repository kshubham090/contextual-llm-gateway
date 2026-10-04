from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ChatMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=64000)


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prompt: str = Field(min_length=1, max_length=64000)
    user_id: str = Field(min_length=1, max_length=128, pattern=r"^[\w.@:-]+$")
    feature_tag: str = Field(min_length=1, max_length=128, pattern=r"^[\w.@:-]+$")
    max_tokens: int | None = Field(default=None, ge=1, le=8192)
    use_graph: bool = True
    retrieval_mode: Literal["none", "vector", "graph"] | None = None
    system_prompt: str | None = Field(default=None, max_length=16000)
    history: list[ChatMessage] = Field(default_factory=list, max_length=32)
    model: str | None = Field(default=None, min_length=1, max_length=256)
    use_cache: bool = True
    cache_mode: Literal["exact", "semantic"] = "exact"
    store: bool = True

    @model_validator(mode="after")
    def validate_conversation(self):
        if self.retrieval_mode is not None and "use_graph" in self.model_fields_set:
            if self.use_graph != (self.retrieval_mode == "graph"):
                raise ValueError("use_graph conflicts with retrieval_mode; prefer retrieval_mode alone")
        if self.retrieval_mode is not None:
            self.use_graph = self.retrieval_mode == "graph"
        if len(self.history) % 2 or any(
            message.role != ("user" if index % 2 == 0 else "assistant")
            for index, message in enumerate(self.history)
        ):
            raise ValueError("History must contain alternating user/assistant pairs before the current user")
        contents = [self.prompt, self.system_prompt or "", *(message.content for message in self.history)]
        if sum(map(len, contents)) > 64000 or any("\x00" in text for text in contents):
            raise ValueError("Conversation must fit 64000 characters and contain no null bytes")
        return self

    @property
    def effective_retrieval_mode(self) -> str:
        return self.retrieval_mode or ("graph" if self.use_graph else "none")

    @field_validator("prompt")
    @classmethod
    def nonblank_prompt(cls, value: str) -> str:
        if not value.strip() or "\x00" in value:
            raise ValueError("Prompt must contain text and no null bytes")
        return value


class ChatMetadata(BaseModel):
    call_id: str | None = None
    durable: bool = True
    retrieval_mode: Literal["none", "vector", "graph"] = "graph"
    memory_epoch: int = 0
    usage_available: bool = True
    finish_reason: str = "stop"
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
