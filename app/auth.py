"""A key authenticates a trusted application; tenant identity is never in the body."""

import hmac
from dataclasses import dataclass

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .config import settings

bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class Principal:
    tenant_id: str


async def authenticate(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> Principal:
    if not settings.auth_enabled and settings.environment == "development":
        return Principal(tenant_id="local")
    token = credentials.credentials if credentials else ""
    tenant = None
    for secret, candidate in settings.gateway_api_keys.items():
        if hmac.compare_digest(token.encode(), secret.encode()):
            tenant = candidate
    if tenant is None:
        raise HTTPException(
            401, "Valid gateway bearer token required", headers={"WWW-Authenticate": "Bearer"}
        )
    return Principal(tenant_id=tenant)


async def authenticate_metrics(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> None:
    token = credentials.credentials if credentials else ""
    expected = settings.metrics_bearer_token
    if not expected or not hmac.compare_digest(token.encode(), expected.encode()):
        raise HTTPException(401, "Metrics bearer token required", headers={"WWW-Authenticate": "Bearer"})
