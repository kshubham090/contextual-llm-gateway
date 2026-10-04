"""Validated operating limits. Credentials are checked at service startup."""

from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    environment: Literal["development", "production"] = "development"
    auth_enabled: bool = True
    gateway_api_keys: dict[str, str] = Field(default_factory=dict, repr=False)
    metrics_bearer_token: str = Field(default="", repr=False)
    anthropic_api_key: str = Field(default="", repr=False)
    voyage_api_key: str = Field(default="", repr=False)

    database_url: str = Field(default="postgresql://gateway:gateway@localhost:5433/gateway", repr=False)
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = Field(default="gatewaypass", repr=False)
    redis_url: str = Field(default="redis://localhost:6379/0", repr=False)
    database_pool_min_size: int = Field(default=2, ge=1, le=100)
    database_pool_max_size: int = Field(default=10, ge=1, le=100)
    database_command_timeout_seconds: float = Field(default=15, gt=0, le=120)
    graph_timeout_seconds: float = Field(default=5, gt=0, le=60)
    redis_timeout_seconds: float = Field(default=2, gt=0, le=30)

    embedding_backend: Literal["voyage", "local"] = "voyage"
    embedding_model: str = Field(default="voyage-3.5", min_length=1, max_length=256)
    embedding_dim: int = Field(default=1024, ge=1, le=2000)
    embedding_batch_size: int = Field(default=32, ge=1, le=128)
    embedding_batch_wait_ms: float = Field(default=5, ge=0, le=1000)
    embedding_queue_size: int = Field(default=512, ge=1, le=10000)
    embedding_batch_max_bytes: int = Field(default=96000, ge=1024, le=1048576)
    embedding_workers: int = Field(default=2, ge=1, le=32)
    embedding_cache_size: int = Field(default=2048, ge=0, le=100000)
    embedding_cache_ttl_seconds: float = Field(default=300, gt=0, le=86400)
    embedding_timeout_seconds: float = Field(default=30, gt=0, le=120)
    embedding_shutdown_timeout_seconds: float = Field(default=5, gt=0, le=60)
    local_embedding_model: str = Field(default="sentence-transformers/all-MiniLM-L6-v2", min_length=1)
    local_embedding_revision: str | None = Field(default=None, min_length=1)
    local_embedding_cpu_threads: int = Field(default=0, ge=0, le=256)
    local_embedding_device: Literal["cpu", "cuda", "mps"] = "cpu"

    cache_hit_threshold: float = Field(default=0.95, ge=0, le=1)
    cache_ttl_seconds: int = Field(default=3600, ge=1, le=2592000)
    memory_ttl_seconds: int = Field(default=2592000, ge=1, le=31536000)
    graph_similarity_threshold: float = Field(default=0.75, ge=0, le=1)
    graph_context_limit: int = Field(default=6, ge=1, le=32)
    graph_candidate_pool: int = Field(default=24, ge=1, le=256)
    context_snippet_chars: int = Field(default=700, ge=32, le=4000)
    context_max_chars: int = Field(default=10000, ge=1024, le=64000)
    rank_weight_similarity: float = Field(default=0.5, ge=0, le=1)
    rank_weight_recency: float = Field(default=0.3, ge=0, le=1)
    rank_weight_feature: float = Field(default=0.2, ge=0, le=1)
    recency_half_life_days: float = Field(default=7, gt=0, le=365)

    simple_model: str = Field(default="claude-haiku-4-5", min_length=1, max_length=256)
    complex_model: str = Field(default="claude-sonnet-4-5", min_length=1, max_length=256)
    complex_prompt_chars: int = Field(default=600, ge=1, le=64000)
    default_max_tokens: int = Field(default=1024, ge=1, le=8192)
    provider_timeout_seconds: float = Field(default=45, gt=0, le=300)
    provider_max_concurrency: int = Field(default=32, ge=1, le=1000)
    provider_queue_timeout_seconds: float = Field(default=2, gt=0, le=60)
    provider_failure_threshold: int = Field(default=3, ge=1, le=100)
    provider_circuit_reset_seconds: float = Field(default=30, gt=0, le=600)
    provider_shutdown_timeout_seconds: float = Field(default=5, gt=0, le=60)
    model_pricing: dict[str, tuple[float, float]] = Field(default_factory=dict)

    rate_limit_per_minute: int = Field(default=30, ge=1, le=100000)
    tenant_rate_limit_per_minute: int = Field(default=600, ge=1, le=1000000)
    request_max_bytes: int = Field(default=262144, ge=1024, le=1048576)
    request_body_timeout_seconds: float = Field(default=10, gt=0, le=120)
    request_timeout_seconds: float = Field(default=100, gt=0, le=600)
    max_concurrent_requests: int = Field(default=64, ge=1, le=10000)
    admission_timeout_seconds: float = Field(default=0.1, gt=0, le=10)
    shutdown_timeout_seconds: float = Field(default=20, gt=0, le=120)
    graph_outbox_batch_size: int = Field(default=32, ge=1, le=1000)
    graph_outbox_poll_seconds: float = Field(default=1, gt=0, le=60)
    graph_outbox_concurrency: int = Field(default=4, ge=1, le=32)
    graph_outbox_lease_seconds: int = Field(default=60, ge=10, le=600)

    @model_validator(mode="after")
    def validate_limits(self):
        if self.environment == "production" and not self.auth_enabled:
            raise ValueError("Authentication cannot be disabled in production")
        if self.database_pool_min_size > self.database_pool_max_size:
            raise ValueError("Database pool minimum exceeds maximum")
        if self.cache_hit_threshold < self.graph_similarity_threshold:
            raise ValueError("Cache threshold must be at least the graph threshold")
        if self.graph_candidate_pool < self.graph_context_limit:
            raise ValueError("Candidate pool must cover the context limit")
        if self.cache_ttl_seconds > self.memory_ttl_seconds:
            raise ValueError("Cache TTL cannot exceed memory TTL")
        if sum((self.rank_weight_similarity, self.rank_weight_recency, self.rank_weight_feature)) <= 0:
            raise ValueError("At least one ranking weight must be positive")
        for prices in self.model_pricing.values():
            if any(p < 0 or p != p or p == float("inf") for p in prices):
                raise ValueError("Model prices must be finite nonnegative numbers")
        return self

    def validate_runtime(self) -> None:
        if self.auth_enabled and not self.gateway_api_keys:
            raise ValueError("Set GATEWAY_API_KEYS to a JSON object mapping secret tokens to tenants")
        for token, tenant in self.gateway_api_keys.items():
            if len(token) < 32 or token.strip() != token:
                raise ValueError("Gateway tokens must contain at least 32 characters and no outer whitespace")
            if not tenant or len(tenant) > 128 or tenant.startswith("__") or tenant.strip() != tenant:
                raise ValueError("Tenant IDs must be nonempty, <=128 characters, and not reserved")
        if not self.anthropic_api_key:
            raise ValueError("ANTHROPIC_API_KEY is required")
        if self.embedding_backend == "voyage" and not self.voyage_api_key:
            raise ValueError("VOYAGE_API_KEY is required for the voyage backend")
        if self.metrics_bearer_token and len(self.metrics_bearer_token) < 32:
            raise ValueError("METRICS_BEARER_TOKEN must contain at least 32 characters")


settings = Settings()
