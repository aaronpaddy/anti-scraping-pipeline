"""AI risk engine (spec §4.4): scores batches of behavior vectors via k-NN."""

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request, Response
from pydantic import BaseModel, Field
import httpx

from . import config
from .index import BehaviorIndex, QdrantIndex
from .scoring import anomaly_score


class ScoreItem(BaseModel):
    event_id: str
    viewer_user_id: str
    features: list[float] = Field(min_length=config.FEATURE_DIMS, max_length=config.FEATURE_DIMS)


class ScoreRequest(BaseModel):
    items: list[ScoreItem]


class ScoreResult(BaseModel):
    event_id: str
    anomaly_score: float


class ScoreResponse(BaseModel):
    results: list[ScoreResult]


@asynccontextmanager
async def lifespan(app: FastAPI):
    http = httpx.AsyncClient(
        base_url=config.QDRANT_URL,
        timeout=1.0,
        limits=httpx.Limits(max_connections=32, max_keepalive_connections=32),
    )
    app.state.index = QdrantIndex(http, config.COLLECTION, config.KNN_K, config.HNSW_EF)
    yield
    await http.aclose()


app = FastAPI(title="AI Risk Engine", lifespan=lifespan)


def get_index(request: Request) -> BehaviorIndex:
    return request.app.state.index


@app.post("/evaluate/batch", response_model=ScoreResponse)
async def evaluate_batch(req: ScoreRequest, index: BehaviorIndex = Depends(get_index)) -> ScoreResponse:
    if not req.items:
        return ScoreResponse(results=[])
    hits = await index.neighbors([item.features for item in req.items])
    return ScoreResponse(
        results=[
            ScoreResult(event_id=item.event_id, anomaly_score=anomaly_score(h))
            for item, h in zip(req.items, hits, strict=True)
        ]
    )


@app.get("/health")
async def health(response: Response, index: BehaviorIndex = Depends(get_index)) -> dict:
    """Ready once the index holds seeded vectors."""
    ok = await index.ready()
    if not ok:
        response.status_code = 503
    return {"ready": ok}
