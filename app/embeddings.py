import httpx

from .config import settings

VOYAGE_URL = "https://api.voyageai.com/v1/embeddings"


class EmbeddingClient:
    """Thin async client for the Voyage AI embeddings endpoint."""

    def __init__(self) -> None:
        self._http = httpx.AsyncClient(
            timeout=30.0,
            headers={"Authorization": f"Bearer {settings.voyage_api_key}"},
        )

    async def embed(self, text: str) -> list[float]:
        # No input_type: queries and stored prompts must embed symmetrically,
        # otherwise an identical repeat prompt won't clear the 0.95 cache bar.
        resp = await self._http.post(
            VOYAGE_URL,
            json={
                "model": settings.embedding_model,
                "input": [text],
            },
        )
        resp.raise_for_status()
        return resp.json()["data"][0]["embedding"]

    async def close(self) -> None:
        await self._http.aclose()
