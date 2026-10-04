import pytest
from pydantic import ValidationError

from app.config import Settings


@pytest.mark.parametrize(
    "overrides",
    [
        {"environment": "production", "auth_enabled": False},
        {"database_pool_min_size": 20, "database_pool_max_size": 2},
        {"cache_hit_threshold": 0.5},
        {"embedding_batch_size": 0},
        {"recency_half_life_days": 0},
        {"model_pricing": {"custom": (-1, 0)}},
        {"model_pricing": {"custom": (float("nan"), 1)}},
    ],
)
def test_invalid_limits_fail_at_configuration_time(overrides):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **overrides)


@pytest.mark.parametrize("keys", [{}, {"short": "tenant"}, {"x" * 40: "__legacy_quarantine__"}])
def test_runtime_requires_valid_credentials_and_active_tenant(keys):
    config = Settings(_env_file=None, gateway_api_keys=keys, anthropic_api_key="test", voyage_api_key="test")
    with pytest.raises(ValueError):
        config.validate_runtime()


def test_optional_local_embeddings_need_no_voyage_secret():
    config = Settings(
        _env_file=None,
        gateway_api_keys={"x" * 40: "team"},
        embedding_backend="local",
        embedding_dim=384,
        anthropic_api_key="test",
        voyage_api_key="",
    )
    config.validate_runtime()
    assert "xxxxxxxx" not in repr(config)


def test_configuration_errors_do_not_expose_other_credential_fields():
    secret = "never-show-this-private-token-0123456789"
    with pytest.raises(ValidationError) as exc:
        Settings(_env_file=None, gateway_api_keys={secret: "tenant"},
                 database_pool_min_size=10, database_pool_max_size=1)
    assert secret not in str(exc.value)
