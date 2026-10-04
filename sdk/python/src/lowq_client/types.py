"""Wire types. Additional response fields may be introduced by the server."""

from typing import Any, Literal, NotRequired, TypedDict


class ChatMessage(TypedDict):
    role: Literal["user", "assistant"]
    content: str


class ChatRequest(TypedDict):
    prompt: str
    user_id: str
    feature_tag: str
    max_tokens: NotRequired[int]
    use_graph: NotRequired[bool]
    retrieval_mode: NotRequired[Literal["none", "vector", "graph"]]
    system_prompt: NotRequired[str]
    history: NotRequired[list[ChatMessage]]
    model: NotRequired[str]
    use_cache: NotRequired[bool]
    cache_mode: NotRequired[Literal["exact", "semantic"]]
    store: NotRequired[bool]


class ChatResponse(TypedDict):
    response: str
    meta: dict[str, Any]


class DeltaEvent(TypedDict):
    type: Literal["delta"]
    delta: str
    model: NotRequired[str]
    provider: NotRequired[str]


class FinalEvent(TypedDict):
    type: Literal["final"]
    response: ChatResponse


StreamEvent = DeltaEvent | FinalEvent


class Memory(TypedDict):
    id: str
    user_id: str
    feature_tag: str
    prompt: str | None
    response: str | None
    status: Literal["active", "deleted", "superseded", "invalidated", "expired"]
    memory_kind: Literal["generated", "curated"]
    revision: int
    created_at: str
    expires_at: str | None
    source_ids: list[str]
    supersedes_id: str | None
    cache_eligible: bool
    retrieval: dict[str, Any]


class MemoryPage(TypedDict):
    items: list[Memory]
    next_cursor: str | None
    scope_revision: int


class MemoryMutation(TypedDict):
    memory: Memory
    scope_revision: int
    invalidated_count: int
    graph_write: Literal["queued"]


class MemoryDeletion(TypedDict):
    deleted_ids: list[str]
    deleted_count: int
    deleted_ids_truncated: bool
    invalidated_count: int
    scope_revision: int
    graph_write: Literal["queued"]
