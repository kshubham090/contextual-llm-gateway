from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # API keys
    anthropic_api_key: str
    voyage_api_key: str

    # Infrastructure
    database_url: str = "postgresql://gateway:gateway@localhost:5433/gateway"
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "gatewaypass"
    redis_url: str = "redis://localhost:6379/0"

    # Embeddings
    embedding_model: str = "voyage-3.5"
    embedding_dim: int = 1024

    # Cache / graph thresholds
    cache_hit_threshold: float = 0.95   # >= this cosine similarity → serve cached response
    graph_similarity_threshold: float = 0.75  # >= this → "related", inject as context
    graph_context_limit: int = 6        # max past calls injected into a new prompt
    graph_candidate_pool: int = 24      # neighborhood size fetched before ranking
    context_snippet_chars: int = 700    # trim injected prompts/responses to this length

    # Hybrid context ranking: score = w_sim*similarity + w_rec*recency + w_feat*feature_match
    rank_weight_similarity: float = 0.5
    rank_weight_recency: float = 0.3
    rank_weight_feature: float = 0.2
    recency_half_life_days: float = 7.0  # recency score halves every N days

    # Routing
    simple_model: str = "claude-haiku-4-5"
    complex_model: str = "claude-sonnet-5"
    complex_prompt_chars: int = 600     # prompts longer than this route to the complex model
    default_max_tokens: int = 1024

    # Rate limiting
    rate_limit_per_minute: int = 30


settings = Settings()
