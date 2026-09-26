"""Vector index access. Tests swap BehaviorIndex for a fake.

Queries go straight to Qdrant's REST API with httpx: qdrant-client's response
models roughly double the time of a 100-query batch, which matters inside the
engine's 20ms budget.
"""

import asyncio
from collections.abc import Callable
from typing import Protocol

import httpx


class BehaviorIndex(Protocol):
    async def neighbors(self, vectors: list[list[float]]) -> list[list[tuple[float, str]]]:
        """For each vector, its nearest labeled neighbors as (similarity, label)."""

    async def ready(self) -> bool: ...


class QdrantIndex:
    """Queries Qdrant over REST.

    If the connection pool is exhausted (connections left stuck by requests
    that timed out, e.g. after the host slept), the client is replaced with a
    fresh one from `client_factory` and the request retried once, so the
    service recovers on its own instead of failing every request.
    """

    def __init__(
        self,
        http: httpx.AsyncClient,
        collection: str,
        k: int,
        hnsw_ef: int,
        client_factory: Callable[[], httpx.AsyncClient] | None = None,
    ):
        self.http = http
        self.collection = collection
        self.k = k
        self.hnsw_ef = hnsw_ef
        self.client_factory = client_factory
        self._reset_lock = asyncio.Lock()

    async def _post(self, path: str, body: dict) -> httpx.Response:
        client = self.http
        try:
            return await client.post(path, json=body)
        except httpx.PoolTimeout:
            if self.client_factory is None:
                raise
            await self._reset(client)
            return await self.http.post(path, json=body)

    async def _reset(self, broken: httpx.AsyncClient) -> None:
        async with self._reset_lock:
            if self.http is not broken:  # another request already replaced it
                return
            self.http = self.client_factory()
        await broken.aclose()

    async def neighbors(self, vectors: list[list[float]]) -> list[list[tuple[float, str]]]:
        body = {
            "searches": [
                {"query": v, "limit": self.k, "with_payload": ["label"], "params": {"hnsw_ef": self.hnsw_ef}}
                for v in vectors
            ]
        }
        r = await self._post(f"/collections/{self.collection}/points/query/batch", body)
        r.raise_for_status()
        return [
            [(p["score"], (p.get("payload") or {}).get("label", "")) for p in res["points"]]
            for res in r.json()["result"]
        ]

    async def ready(self) -> bool:
        try:
            r = await self._post(f"/collections/{self.collection}/points/count", {"exact": False})
        except httpx.HTTPError:
            return False
        return r.status_code == 200 and r.json()["result"]["count"] > 0
