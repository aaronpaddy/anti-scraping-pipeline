from fastapi.testclient import TestClient

from app.main import app, get_index


class FakeIndex:
    def __init__(self, ready: bool = True):
        self._ready = ready
        self.calls: list[list[list[float]]] = []

    async def neighbors(self, vectors):
        self.calls.append(vectors)
        # First feature > 0.5 looks like a bot.
        return [[(0.9, "bot" if v[0] > 0.5 else "human")] * 3 for v in vectors]

    async def ready(self):
        return self._ready


def client_with(index: FakeIndex) -> TestClient:
    app.dependency_overrides[get_index] = lambda: index
    return TestClient(app)


def test_batch_scores_in_order():
    index = FakeIndex()
    body = {"items": [
        {"event_id": "a", "viewer_user_id": "u1", "features": [0.9, 0.1, 0.0, 1.0, 0.8]},
        {"event_id": "b", "viewer_user_id": "u2", "features": [0.1, 0.7, 0.5, 0.3, 0.3]},
    ]}
    r = client_with(index).post("/evaluate/batch", json=body)
    assert r.status_code == 200
    assert r.json() == {"results": [
        {"event_id": "a", "anomaly_score": 1.0},
        {"event_id": "b", "anomaly_score": 0.0},
    ]}
    assert len(index.calls) == 1  # one index round trip per batch


def test_empty_batch_skips_index():
    index = FakeIndex()
    r = client_with(index).post("/evaluate/batch", json={"items": []})
    assert r.json() == {"results": []}
    assert index.calls == []


def test_wrong_dimensions_rejected():
    body = {"items": [{"event_id": "a", "viewer_user_id": "u", "features": [0.1, 0.2]}]}
    assert client_with(FakeIndex()).post("/evaluate/batch", json=body).status_code == 422


def test_health():
    assert client_with(FakeIndex(ready=True)).get("/health").status_code == 200
    assert client_with(FakeIndex(ready=False)).get("/health").status_code == 503
