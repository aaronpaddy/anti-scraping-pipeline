"""Vector index access. Tests swap BehaviorIndex for a fake.

Queries go straight to Qdrant's REST API with httpx: qdrant-client's response
models roughly double the time of a 100-query batch, which matters inside the
engine's 20ms budget.
"""

from typing import Protocol

import httpx


class BehaviorIndex(Protocol):
    async def neighbors(self, vectors: list[list[float]]) -> list[list[tuple[float, str]]]:
        """For each vector, its nearest labeled neighbors as (similarity, label)."""

    async def ready(self) -> bool: ...


class QdrantIndex:
    def __init__(self, http: httpx.AsyncClient, collection: str, k: int, hnsw_ef: int):
        self.http = http
        self.collection = collection
        self.k = k
        self.hnsw_ef = hnsw_ef

    async def neighbors(self, vectors: list[list[float]]) -> list[list[tuple[float, str]]]:
        body = {
            "searches": [
                {"query": v, "limit": self.k, "with_payload": ["label"], "params": {"hnsw_ef": self.hnsw_ef}}
                for v in vectors
            ]
        }
        r = await self.http.post(f"/collections/{self.collection}/points/query/batch", json=body)
        r.raise_for_status()
        return [
            [(p["score"], (p.get("payload") or {}).get("label", "")) for p in res["points"]]
            for res in r.json()["result"]
        ]

    async def ready(self) -> bool:
        try:
            r = await self.http.post(f"/collections/{self.collection}/points/count", json={"exact": False})
        except httpx.HTTPError:
            return False
        return r.status_code == 200 and r.json()["result"]["count"] > 0
