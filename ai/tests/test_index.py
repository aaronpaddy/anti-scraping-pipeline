"""QdrantIndex against a mocked Qdrant REST API."""

import asyncio
import json

import httpx

from app.index import QdrantIndex


def run(coro):
    return asyncio.run(coro)


def index_with(handler) -> QdrantIndex:
    http = httpx.AsyncClient(base_url="http://qdrant", transport=httpx.MockTransport(handler))
    return QdrantIndex(http, "profiles", k=3, hnsw_ef=32)


def test_neighbors_request_and_parse():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["path"] = req.url.path
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"result": [
            {"points": [{"id": 1, "score": 0.9, "payload": {"label": "bot"}},
                        {"id": 2, "score": 0.5, "payload": {"label": "human"}}]},
            {"points": []},
        ], "status": "ok", "time": 0.001})

    hits = run(index_with(handler).neighbors([[0.1] * 5, [0.2] * 5]))
    assert seen["path"] == "/collections/profiles/points/query/batch"
    searches = seen["body"]["searches"]
    assert len(searches) == 2
    assert searches[0] == {"query": [0.1] * 5, "limit": 3, "with_payload": ["label"], "params": {"hnsw_ef": 32}}
    assert hits == [[(0.9, "bot"), (0.5, "human")], []]


def test_neighbors_raises_on_error():
    idx = index_with(lambda req: httpx.Response(404, json={"status": {"error": "Not found"}}))
    try:
        run(idx.neighbors([[0.1] * 5]))
    except httpx.HTTPStatusError:
        return
    raise AssertionError("expected HTTPStatusError")


def test_ready():
    def counting(n):
        return lambda req: httpx.Response(200, json={"result": {"count": n}})

    assert run(index_with(counting(40000)).ready())
    assert not run(index_with(counting(0)).ready())
    assert not run(index_with(lambda req: httpx.Response(404, json={})).ready())

    def down(req):
        raise httpx.ConnectError("refused")

    assert not run(index_with(down).ready())


def test_pool_exhaustion_replaces_client():
    def exhausted(req):
        raise httpx.PoolTimeout("pool exhausted")

    def healthy(req):
        return httpx.Response(200, json={"result": [{"points": [{"score": 0.9, "payload": {"label": "bot"}}]}]})

    broken = httpx.AsyncClient(base_url="http://qdrant", transport=httpx.MockTransport(exhausted))
    made = []

    def factory():
        c = httpx.AsyncClient(base_url="http://qdrant", transport=httpx.MockTransport(healthy))
        made.append(c)
        return c

    idx = QdrantIndex(broken, "profiles", k=3, hnsw_ef=32, client_factory=factory)
    assert run(idx.neighbors([[0.1] * 5])) == [[(0.9, "bot")]]
    assert len(made) == 1 and idx.http is made[0] and broken.is_closed


def test_pool_exhaustion_without_factory_raises():
    def exhausted(req):
        raise httpx.PoolTimeout("pool exhausted")

    try:
        run(index_with(exhausted).neighbors([[0.1] * 5]))
    except httpx.PoolTimeout:
        return
    raise AssertionError("expected PoolTimeout")
