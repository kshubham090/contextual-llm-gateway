from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    prompt: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    feature_tag: str = Field(min_length=1)
    max_tokens: int | None = Field(default=None, ge=1, le=8192)
    use_graph: bool = True   # set False to bypass graph context — used for A/B demos
    use_cache: bool = True   # set False to skip the semantic-cache fast path
    store: bool = True       # set False to keep this call out of graph memory
                             # (still logged for cost, but never used as context)


class ChatMetadata(BaseModel):
    call_id: str | None = None
    cache_hit: bool = False
    cached_call_id: str | None = None
    context_used: list[str] = []
    model: str | None = None
    provider: str | None = None
    fallback_used: bool = False
    tokens_in: int = 0
    tokens_out: int = 0
    cost: float = 0.0
    latency_ms: int = 0


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
    avg_latency_ms: float
